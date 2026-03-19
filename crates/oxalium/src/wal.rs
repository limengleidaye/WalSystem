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

/// Slab 管理模块，处理 WAL 的底层内存块逻辑
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

    const SLAB_RESERVED_CAPACITY: usize = 8; // 每个 Slab 开头预留 8 字节存 owner，用于持久化校验
    const SLAB_PAGE: usize = 64; // 最小写入单位（对齐到 64 字节）

    const SLAB_BITS: usize = 4; // 状态位占用的位数
    const SLAB_MASK: usize = (1 << SLAB_BITS) - 1;

    // Slab 生命周期状态
    const SLAB_NONE: usize = 0;      // 空闲，可被认领
    const SLAB_WRITING: usize = 0b0001; // 正在被一个或多个生产者写入
    const SLAB_READY: usize = 0b0010;   // 写入完成，等待 SQ 线程提交到磁盘
    const SLAB_IN_FLIGHT: usize = 0b0100; // 已提交给 io_uring，正在执行磁盘 IO
    const SLAB_COMPLETED: usize = 0b1000; // 磁盘写入已完成，数据已持久化

    // 编译时断言，确保核心结构体对齐到缓存行（64字节），防止伪共享
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

    /// Slab 的静态属性
    #[repr(align(64))]
    struct SlabProperties {
        capacity: usize,
        ptr: *mut u8,
        uring_slot: u16, // 对应 io_uring 注册内存的索引
    }

    /// Slab 的运行时指标记录
    #[repr(align(64))]
    struct SlabMetrics {
        mut_inst: UnsafeCell<Instant>,
        mut_elapsed: UnsafeCell<Duration>,
        submit_inst: UnsafeCell<Instant>,
    }

    /// 代表 WAL 中的一个物理内存块
    pub struct Slab {
        properties: SlabProperties,
        metrics: SlabMetrics,
        state: CL<AtomicU64>,             // 打包了 owner (lsn) 和 state
        pending_writers: CL<AtomicUsize>, // 当前活跃的写入者数量（引用计数）
        assigned: CL<AtomicUsize>,        // 已分配的写入偏移量
        written: CL<AtomicUsize>,         // 已完成的写入总量
        waker: Notify,                    // 用于唤醒等待该块持久化的任务
    }

    impl Slab {
        /// 初始化 Slab 向量
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

        /// 将逻辑位置和状态打包进 u64
        fn pack(cursor: usize, state: usize) -> u64 {
            (cursor << SLAB_BITS | state & SLAB_MASK).into_lossy()
        }

        /// 从 u64 中解析出逻辑位置 (owner/cursor) 和状态 (state)
        fn unpack(v: u64) -> (usize, usize) {
            let v: usize = v.into_lossy();
            let cursor = v >> SLAB_BITS;
            let state = v & SLAB_MASK;
            (cursor, state)
        }

        /// 标记 Slab 正在进行磁盘 IO
        pub fn mark_inflight(&self, owner: usize) -> Duration {
            let mut_elapsed = unsafe {
                // 将 owner (LSN) 写入内存块开头，用于故障恢复时的校验
                *self.properties.ptr.cast::<u64>() = owner.into_lossy();
                *self.metrics.submit_inst.get() = Instant::now();
                *self.metrics.mut_elapsed.get()
            };
            self.state
                .store(Slab::pack(owner, SLAB_IN_FLIGHT), Ordering::Release);
            mut_elapsed
        }

        /// 标记 Slab 磁盘 IO 已完成
        pub fn mark_completed(&self, owner: usize) -> Duration {
            let submit_elapsed = unsafe { (*self.metrics.submit_inst.get()).elapsed() };
            self.state
                .store(Slab::pack(owner, SLAB_COMPLETED), Ordering::Release);
            submit_elapsed
        }

        /// 生成用于 io_uring 的异步写入指令 (WriteFixed)
        pub fn op_write(&self, slot: u32, cursor: usize) -> squeue::Entry {
            opcode::WriteFixed::new(
                Fixed(slot),
                self.properties.ptr,
                self.properties.capacity.into_lossy(),
                self.properties.uring_slot,
            )
            .offset((cursor * self.properties.capacity).into_lossy())
            .rw_flags(libc::RWF_DSYNC) // 确保数据持久化到介质
            .build()
        }

        /// 获取 Slab 内指定区域的切片以便写入
        fn b(&self, assigned: usize, capacity: usize) -> &mut [u8] {
            unsafe {
                let ptr = self.properties.ptr.add(assigned);
                from_raw_parts_mut(ptr, capacity)
            }
        }

        /// 增加写入者引用计数
        fn acquire_ref(&self) {
            self.pending_writers.fetch_add(1, Ordering::Relaxed);
        }

        /// 减少写入者引用计数
        fn release_ref(&self) {
            self.pending_writers.fetch_sub(1, Ordering::Release);
        }

        /// 查看 Slab 当前的 owner 和状态
        fn inspect(&self) -> (usize, usize) {
            Slab::unpack(self.state.load(Ordering::Acquire))
        }

        /// 尝试通过 CAS “认领”这个 Slab 进入 WRITING 状态
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

        /// 原子地预分配一段空间
        fn prepare_write(&self, capacity: usize) -> usize {
            self.assigned.fetch_add(capacity, Ordering::Relaxed)
        }

        /// 标记一段空间的写入已结束
        fn complete_write(&self, capacity: usize) -> usize {
            self.written.fetch_add(capacity, Ordering::Release) + capacity
        }

        /// 填充剩余空间并完成写入
        fn submit_with_padding(&self, assigned: usize) -> usize {
            let padding = self.properties.capacity - assigned;
            unsafe {
                let ptr = self.properties.ptr.add(assigned);
                write_bytes(ptr, 0, padding);
            }
            self.complete_write(padding)
        }

        /// 标记 Slab 已经填满且就绪，等待提交
        fn mark_ready(&self, owner: usize) {
            unsafe {
                *self.metrics.mut_elapsed.get() = (*self.metrics.mut_inst.get()).elapsed();
            }
            self.state
                .store(Slab::pack(owner, SLAB_READY), Ordering::Release);
        }

        /// 检查是否所有写入者都已退出且 I/O 已完成
        fn is_reusable(&self) -> bool {
            self.pending_writers.load(Ordering::Acquire) == 0
        }

        /// 重置 Slab 以供在环形缓冲区的下一圈循环使用
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

    /// Slab 环形缓冲区管理
    pub struct SlabRing {
        slab_capacity: usize,
        slab_amount: usize,
        slabs: Vec<Slab>,
        used_cursor: CL<AtomicUsize>,    // 当前正被生产者使用的 LSN
        persist_cursor: CL<AtomicUsize>, // 已持久化完成的 LSN
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

        /// SQ 线程调用：扫描并收集处于 READY 状态的 Slab 进行批处理提交
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
                // 如果没有新块就绪，且当前活跃块已超时，则强制退休（Retire）该块
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

        /// CQ 线程调用：推进已持久化的 LSN 指针
        pub fn advance_persisted_lsn(&self) {
            let mut persist_cursor = self.persist_cursor.load(Ordering::Acquire);
            let mut advanced = false;
            loop {
                let slab = self.slab(persist_cursor);

                let (_, state) = slab.inspect();
                if state != SLAB_COMPLETED {
                    break;
                }

                // 确保异步写入者都已完成
                while !slab.is_reusable() {
                    spin_loop();
                }

                // 唤醒所有等待此 LSN 的 future
                slab.waker.notify_waiters();

                // 重用此物理块供未来的 LSN 使用
                slab.reuse(persist_cursor + self.slab_amount);

                persist_cursor += 1;
                advanced = true;
            }
            if advanced {
                self.persist_cursor.store(persist_cursor, Ordering::Release);
            }
        }

        /// 生产者入口：保留空间并执行数据写入
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
                
                // 背压机制：如果所有 Slab 都在处理中，产生背压
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

                // 状态机：如果块还是 NONE，尝试将其切换到 WRITING
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

                // 原子预占空间
                let assigned = slab.prepare_write(capacity);

                if assigned + capacity <= self.slab_capacity {
                    // 情况 A：空间足够，直接写入
                    if assigned + capacity == self.slab_capacity {
                        // 刚好填满，尝试推进逻辑游标到下一个 Slab
                        self.try_advance_used_cursor(used_cursor);
                    }

                    // 调用 sink 函数拷贝数据
                    sink(slab.b(assigned, request_capacity));

                    let fut = slab.waker.notified();
                    let written = slab.complete_write(capacity);

                    slab.release_ref();

                    if written == self.slab_capacity {
                        slab.mark_ready(owner);
                    }

                    // 如果尚未持久化，等待 Notify
                    if !self.is_persisted(used_cursor) {
                        metrics.record_notify_await();
                        fut.await;
                        while !self.is_persisted(used_cursor) {
                            spin_loop();
                        }
                    }

                    return Ok(());
                } else if assigned < self.slab_capacity {
                    // 情况 B：当前 Slab 空间不足，执行填充（Padding）并退役此块
                    let written = slab.submit_with_padding(assigned);
                    slab.release_ref();
                    if written == self.slab_capacity {
                        slab.mark_ready(owner);
                    }
                    self.try_advance_used_cursor(used_cursor);
                } else {
                    // 情况 C：Slab 已被抢占光，直接尝试下个块
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

    /// 异步自旋，逐渐退避
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
const REGISTERED_MEMORY_CAPACITY: usize = 16 * 1024 * 1024; // 每个注册内存块固定 16MiB

/// 对齐到 64 字节的包装器
#[repr(align(64))]
struct CL<T>(T);

impl<T> Deref for CL<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.0 }
}

impl<T> DerefMut for CL<T> {
    fn deref_mut(&mut self) -> &mut T { &mut self.0 }
}

impl<T> From<T> for CL<T> {
    fn from(value: T) -> CL<T> { CL(value) }
}

/// io_uring 注册内存，用于提供高性能、零拷贝的 I/O 缓冲区
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

/// 全局运行指标统计
#[derive(Default)]
pub struct WalSystemMetrics {
    backpressure: UnsafeCell<u64>,
    owner_mismatch: UnsafeCell<u64>,
    claim_contention: UnsafeCell<u64>,
    notify_await: UnsafeCell<u64>,
}

impl WalSystemMetrics {
    pub fn backpressure(&self) -> u64 { unsafe { *self.backpressure.get() } }
    pub fn owner_mismatch(&self) -> u64 { unsafe { *self.owner_mismatch.get() } }
    pub fn claim_contention(&self) -> u64 { unsafe { *self.claim_contention.get() } }
    pub fn notify_await(&self) -> u64 { unsafe { *self.notify_await.get() } }

    fn record_backpressure(&self) { unsafe { *self.backpressure.get() += 1; } }
    fn record_owner_mismatch(&self) { unsafe { *self.owner_mismatch.get() += 1; } }
    fn record_claim_contention(&self) { unsafe { *self.claim_contention.get() += 1; } }
    fn record_notify_await(&self) { unsafe { *self.notify_await.get() += 1; } }
}

impl AddAssign for WalSystemMetrics {
    fn add_assign(&mut self, rhs: WalSystemMetrics) {
        *self.backpressure.get_mut() += rhs.backpressure.into_inner();
        *self.owner_mismatch.get_mut() += rhs.owner_mismatch.into_inner();
        *self.claim_contention.get_mut() += rhs.claim_contention.into_inner();
        *self.notify_await.get_mut() += rhs.notify_await.into_inner();
    }
}

/// WAL 系统门面，负责资源协调和引导
pub struct WalSystem {
    _registered_memories: Vec<RegisteredMemory>, // 保持所有权以防止内存释放
    ring: SlabRing,
    submit_cursor: CL<AtomicUsize>, // SQ 线程最近提交的 LSN
}

impl WalSystem {
    /// 创建并预分配 WAL 目标文件
    pub fn create_target(
        path: impl AsRef<Path>,
        preallocate: usize,
        use_physical_preallocate: bool,
    ) -> Result<File> {
        const B: usize = 256 * 1024;

        let mut options = OpenOptions::new();
        options.append(false).create(true).write(true).truncate(true);
        // 使用 O_DIRECT 以绕过内核页缓存，实现极致可控性
        options.custom_flags(libc::O_DIRECT).custom_flags(libc::O_NOATIME);

        let target = options.open(path)?;
        if preallocate != 0 {
            unsafe {
                let errno = fallocate(target.as_raw_fd(), 0, 0, preallocate.cast_signed().into_lossy());
                if errno < 0 { bail!("{}", std::io::Error::last_os_error()); }

                if use_physical_preallocate {
                    // 通过实际写入 0 填充，强制文件系统分配物理磁盘块（减少 IO 时的元数据开销）
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

    /// 启动 WAL 系统，生成控制句柄和对应的 IO 线程
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

    /// 获取当前正在进行磁盘 IO 的数据量（以 Slab 为单位）
    pub fn inflight(&self) -> usize {
        let submit_cursor = self.submit_cursor.load(Ordering::Relaxed);
        let persist_cursor = self.ring.persist_cursor();
        submit_cursor.saturating_sub(persist_cursor)
    }

    /// 生产者核心接口：持久化写入数据
    pub fn reserve(
        &self,
        capacity: usize,
        sink: impl FnOnce(&mut [u8]),
        metrics: &WalSystemMetrics,
        cancellation_token: &CancellationToken,
    ) -> impl Future<Output = Result<()>> {
        self.ring.reserve(capacity, sink, metrics, cancellation_token)
    }
}

/// 配置 io_uring，开启 SQPOLL 模式以减少系统调用
fn setup_io_uring(target: RawFd, uring_queue_depth: u32) -> Result<IoUring> {
    let mut builder = IoUring::<squeue::Entry, cqueue::Entry>::builder();
    builder.setup_clamp();
    builder.setup_cqsize(uring_queue_depth * 2);
    builder.setup_sqpoll(3000); // 开启 SQPOLL（3秒自动休眠），内核线程会自动轮询提交
    let ring = builder.build(uring_queue_depth)?;
    ring.submitter().register_files(&[target])?; // 注册文件句柄，加速 IO
    Ok(ring)
}

/// Submission Queue (SQ) 线程：轮询 READY 状态的 Slab 并提交给磁盘
fn spawn_sq_thread(
    cancellation_token: CancellationToken,
    slab_capacity: usize,
    wal: Arc<WalSystem>,
    ring: Arc<IoUring>,
    uring_inflight_limit: usize,
    idle_spin_limit: usize,
) -> SQR {
    const FD_URING_SLOT: u32 = 0; // 对应注册文件的索引

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

            // 1. 扫描就绪的 Slab
            wal.ring.fetch_ready_slabs(
                uring_inflight_limit,
                idle_spin_limit,
                &mut submit_cursor,
                &mut ready_slabs,
                &mut idle_spins,
                &mut retire_hist,
            );

            if ready_slabs.is_empty() {
                if cancellation_token.is_cancelled() { break; }
                idle_loop += 1.;
                spin_loop();
                continue;
            }

            submit_loop += 1.;
            sq_batch_hist.saturating_record(ready_slabs.len().into_lossy());

            // 2. 将就绪块批量压入 io_uring 提交队列
            for &cursor in &ready_slabs {
                let slab = wal.ring.slab(cursor);
                let mut_elapsed = slab.mark_inflight(cursor);
                mut_hist.saturating_record(mut_elapsed.as_micros().into_lossy());

                let sqe_write = slab.op_write(FD_URING_SLOT, cursor).user_data(cursor.into_lossy());
                unsafe {
                    while sq.push(&sqe_write).is_err() {
                        sq.sync();
                        submitter.submit()?;
                        sq.sync();
                    }
                }
            }

            // 3. 提交磁盘 IO
            sq.sync();
            submitter.submit()?;

            wal.submit_cursor.store(submit_cursor, Ordering::Relaxed);
        }

        Ok((mut_hist, retire_hist, sq_batch_hist, idle_loop / (idle_loop + submit_loop)))
    })
}

/// Completion Queue (CQ) 线程：轮询 io_uring 的完成事件并更新 LSN
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
            // 1. 处理所有已完成的 IO 事件
            for cqe in &mut cq {
                lsn += 1;
                if cqe.result() < 0 { bail!("I/O Error"); }

                let cursor = cqe.user_data().into_lossy();
                let submit_elapsed = wal.ring.slab(cursor).mark_completed(cursor);
                submit_hist.saturating_record(submit_elapsed.as_micros().into_lossy());
            }

            if lsn == 0 {
                if cancellation_token.is_cancelled() { break; }
                idle_loop += 1.;
                spin_loop();
                continue;
            }

            complete_loop += 1.;
            cq_batch_hist.saturating_record(lsn);

            // 2. 推进已持久化指针并重用已完成的 Slab
            wal.ring.advance_persisted_lsn();
        }

        Ok((submit_hist, cq_batch_hist, idle_loop / (idle_loop + complete_loop)))
    })
}
