use crate::wal::system::WalSystem;
use anyhow::Ok;
use anyhow::Result;
use anyhow::bail;
use io_uring::IoUring;
use io_uring::opcode;
use io_uring::types::Fixed;
use std::hint::spin_loop;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::thread::JoinHandle;
use tokio_util::sync::CancellationToken;

pub type SQR = JoinHandle<Result<()>>;
pub type CQR = JoinHandle<Result<()>>;

const FD_SLOT: u32 = 0; // register_files 注册的第 0 个 fd   

pub fn spawn_sq_thread(
    cancellation_token: CancellationToken,
    wal: Arc<WalSystem>,
    uring: Arc<IoUring>,
    inflight_limit: usize,
    idle_spin_limit: usize,
) -> SQR {
    thread::spawn(move || -> Result<()> {
        let submitter = uring.submitter();
        let mut sq = unsafe { uring.submission_shared() };
        let mut submit_cursor = 0usize;
        let mut ready_slabs: Vec<usize> = Vec::with_capacity(inflight_limit);
        let mut idle_spins = 0usize;

        loop {
            ready_slabs.clear();

            // 1. 无锁扫描就绪的 slab
            wal.ring.fetch_ready_slabs(
                inflight_limit,
                idle_spin_limit,
                &mut submit_cursor,
                &mut ready_slabs,
                &mut idle_spins,
            );

            if ready_slabs.is_empty() {
                if cancellation_token.is_cancelled() {
                    break;
                }
                if sq.cq_overflow() {
                    submitter.submit()?;
                }
                spin_loop();
                continue;
            }

            // 2. 将就绪 slab 提交到 io_uring
            for &cursor in &ready_slabs {
                let slab = wal.ring.slab(cursor);
                slab.mark_inflight(cursor);

                let entry = opcode::WriteFixed::new(
                    Fixed(FD_SLOT),
                    slab.as_ptr(),
                    slab.len_for_flush() as u32,
                    slab.uring_slot(),
                )
                .offset((cursor * slab.capacity()) as u64)
                .rw_flags(libc::RWF_DSYNC)
                .build()
                .user_data(cursor as u64);
                unsafe {
                    while sq.push(&entry).is_err() {
                        sq.sync();
                        submitter.submit()?;
                        sq.sync();
                    }
                }
            }

            // 3. 提交
            sq.sync();
            submitter.submit()?;

            // 4. 更新 submit_cursor 到 WalSystem（用于 inflight 统计）
            wal.submit_cursor.store(submit_cursor, Ordering::Relaxed);
        }
        Ok(())
    })
}

pub fn spawn_cq_thread(
    cancellation_token: CancellationToken,
    wal: Arc<WalSystem>,
    uring: Arc<IoUring>,
) -> CQR {
    thread::spawn(move || {
        let mut cq = unsafe { uring.completion_shared() };

        loop {
            cq.sync();

            let mut count = 0usize;
            for cqe in &mut cq {
                if cqe.result() < 0 {
                    println!("{}", cqe.result());
                    bail!("io_uring completion failed: {}", cqe.result());
                }
                let cursor = cqe.user_data() as usize;
                wal.ring.slab(cursor).mark_completed(cursor);
                count += 1;
            }
            if count == 0 {
                if cancellation_token.is_cancelled() {
                    break;
                }
                spin_loop();
                continue;
            }
            wal.ring.advance_persisted_lsn();
        }
        Ok(())
    })
}
