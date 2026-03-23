use anyhow::{Ok, Result, bail};
use io_uring::{IoUring, cqueue::notif};
use std::{
    ffi::OsStr,
    fs::{File, OpenOptions},
    os::fd::AsRawFd,
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
            .open(path)?;
        if preallocate > 0 {
            file.set_len(preallocate)?;
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
        let slab_amount = if slab_amount == 0 { 4 } else { slab_amount };
        let uring_queue_depth = uring_queue_depth.max(8);
        let uring_inflight_limit = uring_inflight_limit.max(1);

        let wal = Arc::new(WalSystem {
            ring: SlabRing::new(slab_capacity, slab_amount),
            submit_cursor: AtomicUsize::new(0),
        });

        let uring = Arc::new(IoUring::new(uring_queue_depth)?);

        let sq = spawn_sq_thread(
            cancellation_token.clone(),
            wal.clone(),
            uring.clone(),
            fd,
            uring_inflight_limit,
            128,
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
