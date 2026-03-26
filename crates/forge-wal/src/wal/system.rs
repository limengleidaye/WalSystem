use anyhow::{Ok, Result, bail};
use io_uring::IoUring;
use std::{
    ffi::OsStr,
    fs::{File, OpenOptions},
    os::{
        fd::{AsRawFd, RawFd},
        unix::fs::OpenOptionsExt,
    },
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio_util::sync::CancellationToken;

use crate::wal::{
    io::{CQR, SQR, spawn_cq_thread, spawn_sq_thread},
    ring::SlabRing,
    slab::RegisteredMemory,
};

pub struct WalSystemMetrics;

impl WalSystemMetrics {
    pub fn backpressure(&self) -> usize {
        0
    }

    pub fn owner_mismatch(&self) -> usize {
        0
    }

    pub fn claim_contention(&self) -> usize {
        0
    }

    pub fn notify_await(&self) -> usize {
        0
    }
}

pub struct WalSystem {
    _registered_memories: Vec<RegisteredMemory>,
    pub ring: SlabRing,
    pub(crate) submit_cursor: AtomicUsize,
}

impl WalSystem {
    pub fn create_target(
        path: impl AsRef<OsStr>,
        preallocate: u64,
        _physical: bool,
    ) -> Result<File> {
        let path = Path::new(path.as_ref());

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .custom_flags(libc::O_DIRECT | libc::O_NOATIME)
            .open(path)?;
        if preallocate > 0 {
            unsafe {
                let ret = libc::fallocate(file.as_raw_fd(), 0, 0, preallocate as i64);
                if ret < 0 {
                    anyhow::bail!("{}", std::io::Error::last_os_error());
                }
            }
        }
        Ok(file)
    }

    pub fn bootstrap(
        cancellation_token: CancellationToken,
        fd: i32,
        slab_capacity: usize,
        slab_amount: usize,
        uring_queue_depth: u32,
        uring_inflight_limit: usize,
    ) -> Result<(Arc<WalSystem>, SQR, CQR)> {
        let slab_capacity = if slab_capacity == 0 {
            4096
        } else {
            slab_capacity
        };
        if slab_capacity % 4096 != 0 {
            bail!("slab_capacity must be 4096-aligned for O_DIRECT");
        }
        let slab_amount = if slab_amount == 0 { 4 } else { slab_amount };
        let uring_inflight_limit = uring_inflight_limit.max(1);

        let uring = setup_io_uring(fd, uring_queue_depth)?;
        let submitter = uring.submitter();
        let registered_memories =
            RegisteredMemory::build_and_register(slab_capacity, slab_amount, &submitter)?;
        let ring = SlabRing::new(&registered_memories, slab_capacity, slab_amount);
        let wal = Arc::new(WalSystem {
            _registered_memories: registered_memories,
            ring,
            submit_cursor: AtomicUsize::new(0),
        });

        let uring = Arc::new(uring);

        let sq = spawn_sq_thread(
            cancellation_token.clone(),
            wal.clone(),
            uring.clone(),
            uring_inflight_limit,
            32768,
        );

        let cq = spawn_cq_thread(cancellation_token, wal.clone(), uring);

        Ok((wal, sq, cq))
    }

    pub async fn reserve<F>(
        &self,
        capacity: usize,
        sink: F,
        _metrics: &'static WalSystemMetrics,
        cancellation_token: &CancellationToken,
    ) -> Result<()>
    where
        F: FnOnce(&mut [u8]),
    {
        if cancellation_token.is_cancelled() {
            bail!("wal reserve cancelled");
        }
        self.ring.reserve(capacity, sink, cancellation_token).await
    }

    pub fn inflight(&self) -> usize {
        let submit = self.submit_cursor.load(Ordering::Relaxed);
        let persist = self.ring.persist_cursor();
        submit.saturating_sub(persist)
    }
}

impl Drop for WalSystem {
    fn drop(&mut self) {
        self.ring.flush_remaining_for_shutdown();
    }
}

fn setup_io_uring(fd: RawFd, queue_depth: u32) -> Result<IoUring> {
    let mut builder = IoUring::builder();
    builder.setup_clamp();
    builder.setup_cqsize(queue_depth * 2);
    builder.setup_sqpoll(3000); // 内核 SQ 轮询线程，3秒空闲休眠
    let ring = builder.build(queue_depth)?;

    // 注册 fd，后续用 Fixed(0) 引用
    ring.submitter().register_files(&[fd])?;

    Ok(ring)
}
