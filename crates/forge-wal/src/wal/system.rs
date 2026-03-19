use anyhow::{Ok, Result, bail};
use io_uring::{IoUring, cqueue::notif};
use std::{
    ffi::OsStr,
    fs::{File, OpenOptions},
    os::fd::AsRawFd,
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::wal::{
    io::{CQR, SQR, spawn_cq_thread, spawn_sq_thread},
    ring::{ReserveAction, SlabRing},
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
    pub(crate) ring: Arc<Mutex<SlabRing>>,
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
        file: File,
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
        let fd = file.as_raw_fd();

        let wal = Arc::new(WalSystem {
            ring: Arc::new(Mutex::new(SlabRing::new(slab_capacity, slab_amount))),
        });

        let uring = Arc::new(Mutex::new(IoUring::new(uring_queue_depth)?));

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
        let mut sink = Some(sink);

        let written = loop {
            let action = {
                let mut ring = self.ring.lock().expect("wal mutex poisoned");
                ring.reserve_once(capacity, &mut sink)?
            };

            match action {
                ReserveAction::Wait(notify) => {
                    if cancellation_token.is_cancelled() {
                        bail!("wal reserve cancelled");
                    }
                    notify.notified().await;
                }
                ReserveAction::Written { cursor, notify } => break (cursor, notify),
            }
        };
        let (cursor, notify) = written;

        loop {
            {
                let ring = self.ring.lock().expect("wal mutex poisoned");
                if ring.is_persisted(cursor) {
                    return Ok(());
                }
            }

            if cancellation_token.is_cancelled() {
                bail!("wal reserve cancelled");
            }
            notify.notified().await;
        }
    }

    pub fn inflight(&self) -> usize {
        let ring = self.ring.lock().expect("wal mutex poisoned");
        ring.inflight()
    }
}

impl Drop for WalSystem {
    fn drop(&mut self) {
        let mut ring = self.ring.lock().expect("wal mutex poisoned");
        let _ = ring.flush_remaining_for_shutdown();
    }
}
