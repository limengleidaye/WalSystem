use std::{
    alloc::{Layout, alloc_zeroed, dealloc},
    cell::UnsafeCell,
    fs::{File, OpenOptions},
    hint::spin_loop,
    ops::{AddAssign, Deref, DerefMut},
    os::{
        fd::{AsRawFd, RawFd},
        unix::fs::{FileExt, OpenOptionsExt},
    },
    path::Path,
    slice::from_raw_parts,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread::{JoinHandle, spawn},
};

use anyhow::{Result, bail};
use hdrhistogram::Histogram;
use io_uring::{IoUring, Submitter, cqueue, squeue};
use libc::{fallocate, iovec};
use tokio_util::sync::CancellationToken;

use oxalium_infra::convert::IntoLossy;

use crate::wal::slab::SlabRing;

mod slab {
    use std::{
        cell::UnsafeCell,
        hint::spin_loop,
        ptr::write_bytes,
        slice::from_raw_parts_mut,
        sync::atomic::{AtomicU64, AtomicUsize, Ordering},
        time::Duration,
    };

    use anyhow::{Result, bail};
    use hdrhistogram::Histogram;
    use io_uring::{opcode, squeue, types::Fixed};
    use minstant::Instant;
    use tokio::{sync::Notify, task::yield_now};
    use tokio_util::sync::CancellationToken;

    use oxalium_infra::convert::IntoLossy;

    use crate::wal::{CL, REGISTERED_MEMORY_CAPACITY, RegisteredMemory, WalSystemMetrics};

    const SLAB_RESERVED_CAPACITY: usize = 8;
    const SLAB_PAGE: usize = 64;

    const SLAB_BITS: usize = 4;
    const SLAB_MASK: usize = (1 << SLAB_BITS) - 1;

    const SLAB_NONE: usize = 0;
    const SLAB_WRITING: usize = 0b0001;
    const SLAB_READY: usize = 0b0010;
    const SLAB_IN_FLIGHT: usize = 0b0100;
    const SLAB_COMPLETED: usize = 0b1000;

    const _: () = {
        assert!(std::mem::size_of::<SlabProperties>() == 64);
        assert!(std::mem::size_of::<SlabMetrics>() == 64);
        assert!(std::mem::size_of::<CL<AtomicU64>>() == 64);
        assert!(std::mem::size_of::<CL<AtomicUsize>>() == 64);

        assert!(std::mem::align_of::<SlabProperties>() == 64);
        assert!(std::mem::align_of::<SlabMetrics>() == 64);
        assert!(std::mem::align_of::<CL<AtomicU64>>() == 64);
        assert!(std::mem::align_of::<CL<AtomicUsize>>() == 64);
    };

    #[repr(align(64))]
    struct SlabProperties {
        capacity: usize,
        ptr: *mut u8,
        uring_slot: u16,
    }

    #[repr(align(64))]
    struct SlabMetrics {
        mut_inst: UnsafeCell<Instant>,
        mut_elapsed: UnsafeCell<Duration>,
        submit_inst: UnsafeCell<Instant>,
    }

    pub struct Slab {
        properties: SlabProperties,
        metrics: SlabMetrics,
        state: CL<AtomicU64>,
        pending_writers: CL<AtomicUsize>,
        assigned: CL<AtomicUsize>,
        written: CL<AtomicUsize>,
        waker: Notify,
    }

    impl Slab {
        fn build(registered_memories: &[RegisteredMemory], slab_capacity: usize) -> Vec<Slab> {
            let slabs_per_registered_memory = REGISTERED_MEMORY_CAPACITY / slab_capacity;
            registered_memories
                .iter()
                .enumerate()
                .flat_map(|(slot, registered_memory)| {
                    Slab::build_slab(
                        registered_memory,
                        slab_capacity,
                        slabs_per_registered_memory,
                        slot,
                    )
                })
                .collect()
        }

        fn build_slab(
            registered_memory: &RegisteredMemory,
            slab_capacity: usize,
            slabs_per_registered_memory: usize,
            slot: usize,
        ) -> impl Iterator<Item = Slab> {
            (0..slabs_per_registered_memory).map(move |cursor| {
                let ptr = unsafe { registered_memory.ptr.add(cursor * slab_capacity) };
                Slab {
                    properties: SlabProperties {
                        capacity: slab_capacity,
                        ptr,
                        uring_slot: registered_memory.uring_slot,
                    },
                    metrics: SlabMetrics {
                        mut_inst: UnsafeCell::new(Instant::ZERO),
                        mut_elapsed: UnsafeCell::new(Duration::ZERO),
                        submit_inst: UnsafeCell::new(Instant::ZERO),
                    },
                    state: AtomicU64::new(Slab::pack(
                        slot * slabs_per_registered_memory + cursor,
                        SLAB_NONE,
                    ))
                    .into(),
                    pending_writers: AtomicUsize::new(0).into(),
                    assigned: AtomicUsize::new(SLAB_RESERVED_CAPACITY).into(),
                    written: AtomicUsize::new(SLAB_RESERVED_CAPACITY).into(),
                    waker: Notify::new(),
                }
            })
        }

        fn pack(cursor: usize, state: usize) -> u64 {
            (cursor << SLAB_BITS | state & SLAB_MASK).into_lossy()
        }

        fn unpack(v: u64) -> (usize, usize) {
            let v: usize = v.into_lossy();
            let cursor = v >> SLAB_BITS;
            let state = v & SLAB_MASK;
            (cursor, state)
        }

        pub fn mark_inflight(&self, owner: usize) -> Duration {
            let mut_elapsed = unsafe {
                *self.properties.ptr.cast::<u64>() = owner.into_lossy();
                *self.metrics.submit_inst.get() = Instant::now();
                *self.metrics.mut_elapsed.get()
            };
            self.state
                .store(Slab::pack(owner, SLAB_IN_FLIGHT), Ordering::Release);
            mut_elapsed
        }

        pub fn mark_completed(&self, owner: usize) -> Duration {
            let submit_elapsed = unsafe { (*self.metrics.submit_inst.get()).elapsed() };
            self.state
                .store(Slab::pack(owner, SLAB_COMPLETED), Ordering::Release);
            submit_elapsed
        }

        pub fn op_write(&self, slot: u32, cursor: usize) -> squeue::Entry {
            opcode::WriteFixed::new(
                Fixed(slot),
                self.properties.ptr,
                self.properties.capacity.into_lossy(),
                self.properties.uring_slot,
            )
            .offset((cursor * self.properties.capacity).into_lossy())
            .rw_flags(libc::RWF_DSYNC)
            .build()
        }

        fn b(&self, assigned: usize, capacity: usize) -> &mut [u8] {
            unsafe {
                let ptr = self.properties.ptr.add(assigned);
                from_raw_parts_mut(ptr, capacity)
            }
        }

        fn acquire_ref(&self) {
            self.pending_writers.fetch_add(1, Ordering::Relaxed);
        }

        fn release_ref(&self) {
            self.pending_writers.fetch_sub(1, Ordering::Release);
        }

        fn inspect(&self) -> (usize, usize) {
            Slab::unpack(self.state.load(Ordering::Acquire))
        }

        fn try_claim(&self, owner: usize) -> Result<(), (usize, usize)> {
            match self.state.compare_exchange(
                Slab::pack(owner, SLAB_NONE),
                Slab::pack(owner, SLAB_WRITING),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    unsafe {
                        *self.metrics.mut_inst.get() = Instant::now();
                    }
                    Ok(())
                }
                Err(actual) => Err(Slab::unpack(actual)),
            }
        }

        fn prepare_write(&self, capacity: usize) -> usize {
            self.assigned.fetch_add(capacity, Ordering::Relaxed)
        }

        fn complete_write(&self, capacity: usize) -> usize {
            self.written.fetch_add(capacity, Ordering::Release) + capacity
        }

        fn submit_with_padding(&self, assigned: usize) -> usize {
            let padding = self.properties.capacity - assigned;
            unsafe {
                let ptr = self.properties.ptr.add(assigned);
                write_bytes(ptr, 0, padding);
            }
            self.complete_write(padding)
        }

        fn mark_ready(&self, owner: usize) {
            unsafe {
                *self.metrics.mut_elapsed.get() = (*self.metrics.mut_inst.get()).elapsed();
            }
            self.state
                .store(Slab::pack(owner, SLAB_READY), Ordering::Release);
        }

        fn is_reusable(&self) -> bool {
            self.pending_writers.load(Ordering::Acquire) == 0
        }

        fn reuse(&self, owner: usize) {
            self.assigned
                .store(SLAB_RESERVED_CAPACITY, Ordering::Relaxed);
            self.written
                .store(SLAB_RESERVED_CAPACITY, Ordering::Relaxed);
            self.state
                .store(Slab::pack(owner, SLAB_NONE), Ordering::Release);
        }
    }

    unsafe impl Send for Slab {}
    unsafe impl Sync for Slab {}

    pub struct SlabRing {
        slab_capacity: usize,
        slab_amount: usize,
        slabs: Vec<Slab>,
        used_cursor: CL<AtomicUsize>,
        persist_cursor: CL<AtomicUsize>,
    }

    impl SlabRing {
        pub fn build(
            registered_memories: &[RegisteredMemory],
            slab_capacity: usize,
            slab_amount: usize,
        ) -> SlabRing {
            SlabRing {
                slab_capacity,
                slab_amount,
                slabs: Slab::build(registered_memories, slab_capacity),
                used_cursor: AtomicUsize::new(0).into(),
                persist_cursor: AtomicUsize::new(0).into(),
            }
        }

        pub fn persist_cursor(&self) -> usize {
            self.persist_cursor.load(Ordering::Acquire)
        }

        pub fn slab(&self, cursor: usize) -> &Slab {
            &self.slabs[cursor % self.slab_amount]
        }

        pub fn fetch_ready_slabs(
            &self,
            uring_inflight_limit: usize,
            idle_spin_limit: usize,
            submit_cursor: &mut usize,
            ready_slabs: &mut Vec<usize>,
            idle_spins: &mut usize,
            retire_hist: &mut Histogram<u64>,
        ) {
            let used_cursor = self.used_cursor.load(Ordering::Acquire);
            let upper_bound = used_cursor.min(*submit_cursor + uring_inflight_limit);

            let mut contiguous_inflight = true;
            let mut advance_submit_cursor = *submit_cursor;

            for cursor in *submit_cursor..upper_bound {
                let slab = self.slab(cursor);
                let (_, state) = slab.inspect();
                match state {
                    SLAB_READY | SLAB_IN_FLIGHT | SLAB_COMPLETED => {
                        if state == SLAB_READY {
                            ready_slabs.push(cursor);
                        }
                        if contiguous_inflight {
                            advance_submit_cursor += 1;
                        }
                    }
                    _ => contiguous_inflight = false,
                }
            }

            if advance_submit_cursor != *submit_cursor {
                *idle_spins = 0;
                *submit_cursor = advance_submit_cursor;
            } else {
                let slab = self.slab(advance_submit_cursor);
                let (owner, state) = slab.inspect();
                if state == SLAB_WRITING {
                    *idle_spins += 1;
                    if *idle_spins >= idle_spin_limit {
                        let assigned = slab.prepare_write(self.slab_capacity);
                        if assigned < self.slab_capacity {
                            let written = slab.submit_with_padding(assigned);
                            if written == self.slab_capacity {
                                slab.mark_ready(owner);
                            }
                            if advance_submit_cursor == used_cursor {
                                self.try_advance_used_cursor(used_cursor);
                            }
                            retire_hist.saturating_record(assigned.into_lossy());
                        }
                        *idle_spins = 0;
                    }
                } else {
                    *idle_spins = 0;
                }
            }
        }

        pub fn advance_persisted_lsn(&self) {
            let mut persist_cursor = self.persist_cursor.load(Ordering::Acquire);
            let mut advanced = false;
            loop {
                let slab = self.slab(persist_cursor);

                let (_, state) = slab.inspect();
                if state != SLAB_COMPLETED {
                    break;
                }

                while !slab.is_reusable() {
                    spin_loop();
                }

                slab.waker.notify_waiters();

                slab.reuse(persist_cursor + self.slab_amount);

                persist_cursor += 1;
                advanced = true;
            }
            if advanced {
                self.persist_cursor.store(persist_cursor, Ordering::Release);
            }
        }

        pub async fn reserve(
            &self,
            capacity: usize,
            sink: impl FnOnce(&mut [u8]),
            metrics: &WalSystemMetrics,
            cancellation_token: &CancellationToken,
        ) -> Result<()> {
            let request_capacity = capacity;
            let capacity = capacity.max(SLAB_PAGE);

            let mut step = 0;

            loop {
                let used_cursor = self.used_cursor.load(Ordering::Acquire);
                let persist_cursor = self.persist_cursor.load(Ordering::Acquire);
                if used_cursor >= persist_cursor + self.slab_amount {
                    metrics.record_backpressure();
                    async_spin_loop(&mut step).await;
                    continue;
                }

                let slab = self.slab(used_cursor);

                slab.acquire_ref();

                let (owner, state) = slab.inspect();
                if owner != used_cursor {
                    slab.release_ref();
                    metrics.record_owner_mismatch();
                    async_spin_loop(&mut step).await;
                    continue;
                }

                if state == SLAB_NONE {
                    if let Err((owner, state)) = slab.try_claim(owner) {
                        metrics.record_claim_contention();
                        if owner != used_cursor || state != SLAB_WRITING {
                            slab.release_ref();
                            async_spin_loop(&mut step).await;
                            continue;
                        }
                    }
                }

                if cancellation_token.is_cancelled() {
                    slab.release_ref();

                    bail!("cancelled");
                }

                step = 0;

                let assigned = slab.prepare_write(capacity);

                if assigned + capacity <= self.slab_capacity {
                    if assigned + capacity == self.slab_capacity {
                        self.try_advance_used_cursor(used_cursor);
                    }

                    sink(slab.b(assigned, request_capacity));

                    let fut = slab.waker.notified();

                    let written = slab.complete_write(capacity);

                    slab.release_ref();

                    if written == self.slab_capacity {
                        slab.mark_ready(owner);
                    }

                    if !self.is_persisted(used_cursor) {
                        metrics.record_notify_await();

                        fut.await;

                        while !self.is_persisted(used_cursor) {
                            spin_loop();
                        }
                    }

                    return Ok(());
                } else if assigned < self.slab_capacity {
                    let written = slab.submit_with_padding(assigned);

                    slab.release_ref();

                    if written == self.slab_capacity {
                        slab.mark_ready(owner);
                    }

                    self.try_advance_used_cursor(used_cursor);
                } else {
                    slab.release_ref();

                    self.try_advance_used_cursor(used_cursor);
                }
            }
        }

        fn try_advance_used_cursor(&self, cursor: usize) {
            let _ = self.used_cursor.compare_exchange(
                cursor,
                cursor + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
        }

        fn is_persisted(&self, target_lsn: usize) -> bool {
            self.persist_cursor.load(Ordering::Acquire) > target_lsn
        }
    }

    async fn async_spin_loop(step: &mut usize) {
        const SPIN_LIMIT: usize = 6;

        if *step <= SPIN_LIMIT {
            let spins = 1 << *step;
            for _ in 0..spins {
                spin_loop();
            }
            *step += 1;
        } else {
            yield_now().await;
        }
    }
}

const REGISTERED_MEMORY_PAGE: usize = 4096;
const REGISTERED_MEMORY_CAPACITY: usize = 16 * 1024 * 1024;

#[repr(align(64))]
struct CL<T>(T);

impl<T> Deref for CL<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> DerefMut for CL<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T> From<T> for CL<T> {
    fn from(value: T) -> CL<T> {
        CL(value)
    }
}

struct RegisteredMemory {
    layout: Layout,
    ptr: *mut u8,
    uring_slot: u16,
}

impl RegisteredMemory {
    fn build_and_register(
        slab_capacity: usize,
        slab_amount: usize,
        submitter: &Submitter,
    ) -> Result<Vec<RegisteredMemory>> {
        if slab_capacity % REGISTERED_MEMORY_PAGE != 0 {
            bail!("slab capacity must be 4096-byte aligned");
        }
        if slab_capacity > REGISTERED_MEMORY_CAPACITY {
            bail!("slab capacity exceeds registered memory capacity");
        }

        let slabs_per_registered_memory = REGISTERED_MEMORY_CAPACITY / slab_capacity;
        let registered_memory_amount = slab_amount.div_ceil(slabs_per_registered_memory);

        if registered_memory_amount > 16384 {
            bail!("registered memory amount exceeds io-uring limit");
        }

        let layout = Layout::from_size_align(REGISTERED_MEMORY_CAPACITY, REGISTERED_MEMORY_PAGE)?;

        let mut registered_memories = Vec::with_capacity(registered_memory_amount);
        let mut iovecs = Vec::with_capacity(registered_memory_amount);

        for uring_slot in 0..registered_memory_amount {
            let ptr = unsafe { alloc_zeroed(layout) };
            registered_memories.push(RegisteredMemory {
                layout,
                ptr,
                uring_slot: uring_slot.into_lossy(),
            });
            iovecs.push(iovec {
                iov_base: ptr.cast(),
                iov_len: REGISTERED_MEMORY_CAPACITY,
            });
        }

        unsafe {
            submitter.register_buffers(&iovecs)?;
        }

        Ok(registered_memories)
    }
}

impl Drop for RegisteredMemory {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr, self.layout);
        }
    }
}

unsafe impl Send for RegisteredMemory {}
unsafe impl Sync for RegisteredMemory {}

pub type SQR = JoinHandle<Result<(Histogram<u64>, Histogram<u64>, Histogram<u64>, f64)>>;
pub type CQR = JoinHandle<Result<(Histogram<u64>, Histogram<u64>, f64)>>;

#[derive(Default)]
pub struct WalSystemMetrics {
    backpressure: UnsafeCell<u64>,
    owner_mismatch: UnsafeCell<u64>,
    claim_contention: UnsafeCell<u64>,
    notify_await: UnsafeCell<u64>,
}

impl WalSystemMetrics {
    pub fn backpressure(&self) -> u64 {
        unsafe { *self.backpressure.get() }
    }

    pub fn owner_mismatch(&self) -> u64 {
        unsafe { *self.owner_mismatch.get() }
    }

    pub fn claim_contention(&self) -> u64 {
        unsafe { *self.claim_contention.get() }
    }

    pub fn notify_await(&self) -> u64 {
        unsafe { *self.notify_await.get() }
    }

    fn record_backpressure(&self) {
        unsafe {
            *self.backpressure.get() += 1;
        }
    }

    fn record_owner_mismatch(&self) {
        unsafe {
            *self.owner_mismatch.get() += 1;
        }
    }

    fn record_claim_contention(&self) {
        unsafe {
            *self.claim_contention.get() += 1;
        }
    }

    fn record_notify_await(&self) {
        unsafe {
            *self.notify_await.get() += 1;
        }
    }
}

impl AddAssign for WalSystemMetrics {
    fn add_assign(&mut self, rhs: WalSystemMetrics) {
        *self.backpressure.get_mut() += rhs.backpressure.into_inner();
        *self.owner_mismatch.get_mut() += rhs.owner_mismatch.into_inner();
        *self.claim_contention.get_mut() += rhs.claim_contention.into_inner();
        *self.notify_await.get_mut() += rhs.notify_await.into_inner();
    }
}

pub struct WalSystem {
    _registered_memories: Vec<RegisteredMemory>,
    ring: SlabRing,
    submit_cursor: CL<AtomicUsize>,
}

impl WalSystem {
    pub fn create_target(
        path: impl AsRef<Path>,
        preallocate: usize,
        use_physical_preallocate: bool,
    ) -> Result<File> {
        const B: usize = 256 * 1024;

        let mut options = OpenOptions::new();
        options
            .append(false)
            .create(true)
            .write(true)
            .truncate(true);
        options
            .custom_flags(libc::O_DIRECT)
            .custom_flags(libc::O_NOATIME);

        let target = options.open(path)?;
        if preallocate != 0 {
            unsafe {
                let errno = fallocate(
                    target.as_raw_fd(),
                    0,
                    0,
                    preallocate.cast_signed().into_lossy(),
                );
                if errno < 0 {
                    let err = std::io::Error::last_os_error();
                    bail!("{}", err);
                }

                if use_physical_preallocate {
                    let layout = Layout::from_size_align(B, 4096)?;
                    let ptr = alloc_zeroed(layout);
                    let b = from_raw_parts(ptr, B);

                    let mut written = 0;
                    while written < preallocate {
                        target.write_all_at(b, written.into_lossy())?;
                        written += b.len();
                    }
                    target.sync_all()?;

                    dealloc(ptr, layout);
                }
            }
        }

        Ok(target)
    }

    pub fn bootstrap(
        cancellation_token: CancellationToken,
        target: RawFd,
        slab_capacity: usize,
        slab_amount: usize,
        uring_queue_depth: u32,
        uring_inflight_limit: usize,
        idle_spin_limit: usize,
    ) -> Result<(Arc<WalSystem>, SQR, CQR)> {
        let uring_queue_depth = uring_queue_depth.min(4096);
        let ring = setup_io_uring(target, uring_queue_depth)?;
        let registered_memories =
            RegisteredMemory::build_and_register(slab_capacity, slab_amount, &ring.submitter())?;
        let slab_ring = SlabRing::build(&registered_memories, slab_capacity, slab_amount);
        let wal = Arc::new(WalSystem {
            _registered_memories: registered_memories,
            ring: slab_ring,
            submit_cursor: AtomicUsize::new(0).into(),
        });
        let ring = Arc::new(ring);
        let sq = spawn_sq_thread(
            cancellation_token.clone(),
            slab_capacity,
            wal.clone(),
            ring.clone(),
            uring_inflight_limit,
            idle_spin_limit,
        );
        let cq = spawn_cq_thread(cancellation_token, wal.clone(), ring);
        Ok((wal, sq, cq))
    }

    pub fn inflight(&self) -> usize {
        let submit_cursor = self.submit_cursor.load(Ordering::Relaxed);
        let persist_cursor = self.ring.persist_cursor();
        submit_cursor.saturating_sub(persist_cursor)
    }

    pub fn reserve(
        &self,
        capacity: usize,
        sink: impl FnOnce(&mut [u8]),
        metrics: &WalSystemMetrics,
        cancellation_token: &CancellationToken,
    ) -> impl Future<Output = Result<()>> {
        self.ring
            .reserve(capacity, sink, metrics, cancellation_token)
    }
}

fn setup_io_uring(target: RawFd, uring_queue_depth: u32) -> Result<IoUring> {
    let mut builder = IoUring::<squeue::Entry, cqueue::Entry>::builder();
    builder.setup_clamp();
    builder.setup_cqsize(uring_queue_depth * 2);
    builder.setup_sqpoll(3000);
    let ring = builder.build(uring_queue_depth)?;
    ring.submitter().register_files(&[target])?;
    Ok(ring)
}

fn spawn_sq_thread(
    cancellation_token: CancellationToken,
    slab_capacity: usize,
    wal: Arc<WalSystem>,
    ring: Arc<IoUring>,
    uring_inflight_limit: usize,
    idle_spin_limit: usize,
) -> SQR {
    const FD_URING_SLOT: u32 = 0;

    spawn(move || {
        let mut mut_hist = Histogram::new_with_max(6000_0000, 4)?;
        let mut retire_hist = Histogram::new_with_max(slab_capacity.into_lossy(), 3)?;
        let mut sq_batch_hist = Histogram::new_with_max(16384, 3)?;

        let mut submit_loop = 0.;
        let mut idle_loop = 0.;

        let submitter = ring.submitter();
        let mut sq = unsafe { ring.submission_shared() };

        let mut submit_cursor = 0;
        let mut ready_slabs = Vec::with_capacity(uring_inflight_limit);
        let mut idle_spins = 0;

        loop {
            ready_slabs.clear();

            wal.ring.fetch_ready_slabs(
                uring_inflight_limit,
                idle_spin_limit,
                &mut submit_cursor,
                &mut ready_slabs,
                &mut idle_spins,
                &mut retire_hist,
            );

            if ready_slabs.is_empty() {
                if cancellation_token.is_cancelled() {
                    break;
                }
                idle_loop += 1.;
                spin_loop();
                continue;
            }

            submit_loop += 1.;

            sq_batch_hist.saturating_record(ready_slabs.len().into_lossy());

            for &cursor in &ready_slabs {
                let slab = wal.ring.slab(cursor);

                let mut_elapsed = slab.mark_inflight(cursor);
                mut_hist.saturating_record(mut_elapsed.as_micros().into_lossy());

                let sqe_write = slab
                    .op_write(FD_URING_SLOT, cursor)
                    .user_data(cursor.into_lossy());
                unsafe {
                    while sq.push(&sqe_write).is_err() {
                        sq.sync();
                        submitter.submit()?;
                        sq.sync();
                    }
                }
            }

            sq.sync();
            submitter.submit()?;

            wal.submit_cursor.store(submit_cursor, Ordering::Relaxed);
        }

        Ok((
            mut_hist,
            retire_hist,
            sq_batch_hist,
            idle_loop / (idle_loop + submit_loop),
        ))
    })
}

fn spawn_cq_thread(
    cancellation_token: CancellationToken,
    wal: Arc<WalSystem>,
    ring: Arc<IoUring>,
) -> CQR {
    spawn(move || {
        let mut submit_hist = Histogram::new_with_max(6000_0000, 4)?;
        let mut cq_batch_hist = Histogram::new_with_max(16384, 3)?;

        let mut complete_loop = 0.;
        let mut idle_loop = 0.;

        let mut cq = unsafe { ring.completion_shared() };

        loop {
            cq.sync();

            let mut lsn = 0;
            for cqe in &mut cq {
                lsn += 1;

                if cqe.result() < 0 {
                    bail!("I/O");
                }

                let cursor = cqe.user_data().into_lossy();
                let submit_elapsed = wal.ring.slab(cursor).mark_completed(cursor);
                submit_hist.saturating_record(submit_elapsed.as_micros().into_lossy());
            }

            if lsn == 0 {
                if cancellation_token.is_cancelled() {
                    break;
                }
                idle_loop += 1.;
                spin_loop();
                continue;
            }

            complete_loop += 1.;

            cq_batch_hist.saturating_record(lsn);

            wal.ring.advance_persisted_lsn();
        }

        Ok((
            submit_hist,
            cq_batch_hist,
            idle_loop / (idle_loop + complete_loop),
        ))
    })
}
