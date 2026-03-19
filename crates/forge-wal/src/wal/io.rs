use crate::wal::system::WalSystem;
use anyhow::Ok;
use anyhow::Result;
use anyhow::bail;
use io_uring::IoUring;
use io_uring::opcode;
use io_uring::types;
use std::hint::spin_loop;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub type SQR = JoinHandle<Result<()>>;
pub type CQR = JoinHandle<Result<()>>;

pub fn spawn_sq_thread(
    cancellation_token: CancellationToken,
    wal: Arc<WalSystem>,
    uring: Arc<Mutex<IoUring>>,
    fd: RawFd,
    inflight_limit: usize,
    idle_spin_limit: usize,
) -> SQR {
    thread::spawn(move || -> Result<()> {
        let mut idle_spins = 0usize;
        loop {
            let submissions = {
                let mut ring = wal.ring.lock().expect("wal mutex poisoned");
                let subs = ring.fetch_ready_submissions(inflight_limit);
                if subs.is_empty() && ring.retire_idle_writing_slab() {
                    idle_spins = 0;
                    Vec::new()
                } else {
                    subs
                }
            };

            if submissions.is_empty() {
                if cancellation_token.is_cancelled() {
                    break;
                }

                idle_spins += 1;
                if idle_spins >= idle_spin_limit {
                    thread::sleep(Duration::from_micros(50));
                } else {
                    spin_loop();
                }
                continue;
            }

            idle_spins = 0;

            let ring = uring.lock().expect("io_uring mutex poisoned");
            let submitter = ring.submitter();
            let mut sq = unsafe { ring.submission_shared() };

            for sub in &submissions {
                let entry = opcode::Write::new(types::Fd(fd), sub.ptr, sub.len)
                    .offset(sub.offset)
                    .build()
                    .user_data(sub.cursor as u64);

                unsafe {
                    while sq.push(&entry).is_err() {
                        sq.sync();
                        submitter.submit()?;
                        sq.sync();
                    }
                }
            }
            sq.sync();
            submitter.submit()?;
        }
        Ok(())
    })
}

pub fn spawn_cq_thread(
    cancellation_token: CancellationToken,
    wal: Arc<WalSystem>,
    uring: Arc<Mutex<IoUring>>,
) -> CQR {
    thread::spawn(move || {
        loop {
            {
                let ring = uring.lock().expect("io_uring mutex poisoned");
                ring.submit_and_wait(1)?;
                let mut cq = unsafe { ring.completion_shared() };
                cq.sync();

                let mut completed = Vec::new();

                for cqe in &mut cq {
                    if cqe.result() < 0 {
                        bail!("io_uring completion failed: {}", cqe.result());
                    }
                    completed.push(cqe.user_data() as usize);
                }

                drop(cq);
                drop(ring);

                if !completed.is_empty() {
                    let mut slab_ring = wal.ring.lock().expect("wal mutex poisoned");
                    for cursor in completed {
                        slab_ring.mark_completed(cursor)?;
                    }
                    slab_ring.advance_persisted_lsn();
                    continue;
                }
            }
            if cancellation_token.is_cancelled() {
                break;
            }
            spin_loop();
        }
        Ok(())
    })
}
