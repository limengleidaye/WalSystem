use anyhow::bail;
use rand::seq;
use std::{
    fmt::Debug,
    hint::spin_loop,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::task::yield_now;
use tokio_util::sync::CancellationToken;

use anyhow::{Ok, Result};

use crate::wal::CL;

use crate::wal::slab::{SLAB_COMPLETED, SLAB_IN_FLIGHT, SLAB_NONE, SLAB_READY, SLAB_WRITING, Slab};

use crate::wal::slab::RegisteredMemory;

pub struct Submission {
    pub cursor: usize,
    pub ptr: *const u8,
    pub len: u32,
    pub offset: u64,
}

pub struct SlabRing {
    slabs: Vec<Slab>,
    slab_capacity: usize,
    slab_amount: usize,

    // persist_cursor <= used_cursor
    persist_cursor: CL<AtomicUsize>, // 已持久化完成的逻辑编号（下一个待持久化的位置）
    used_cursor: CL<AtomicUsize>,    // 当前正在被写入的逻辑编号
}

impl Debug for SlabRing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlabRing")
            .field("slabs", &self.slabs)
            .field(
                "persist_cursor",
                &self.persist_cursor.load(Ordering::Relaxed),
            )
            .field("used_cursor", &self.used_cursor.load(Ordering::Relaxed))
            .finish()
    }
}

unsafe impl Send for SlabRing {}
unsafe impl Sync for SlabRing {}

impl SlabRing {
    pub fn new(
        registered_memories: &[RegisteredMemory],
        slab_capacity: usize,
        slab_amount: usize,
    ) -> Self {
        let slabs: Vec<Slab> = Slab::build(registered_memories, slab_capacity);
        Self {
            slabs,
            slab_capacity,
            slab_amount,
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

    pub fn is_persisted(&self, target_lsn: usize) -> bool {
        self.persist_cursor.load(Ordering::Acquire) > target_lsn
    }

    fn try_advance_used_cursor(&self, cursor: usize) {
        let _ = self.used_cursor.compare_exchange(
            cursor,
            cursor + 1,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }

    // =========================================================
    // 生产者入口：无锁 reserve
    // =========================================================
    pub async fn reserve(
        &self,
        capacity: usize,
        sink: impl FnOnce(&mut [u8]),
        cancellation_token: &CancellationToken,
    ) -> Result<()> {
        let mut step = 0;

        loop {
            let used_cursor = self.used_cursor.load(Ordering::Acquire);
            let persisted_cursor = self.persist_cursor.load(Ordering::Acquire);

            // ---- 背压检查 ----
            if used_cursor >= persisted_cursor + self.slab_amount {
                async_spin_loop(&mut step).await;
                continue;
            }

            let slab = self.slab(used_cursor);
            // 增加引用计数，防止 CQ 线程在我们操作期间 reuse 这个 slab
            slab.acquire_ref();

            let (owner, state) = slab.inspect();

            if owner != used_cursor {
                slab.release_ref();
                async_spin_loop(&mut step).await;
                continue;
            }

            if state == SLAB_NONE {
                if let Err((actual_owner, actual_state)) = slab.try_claim(used_cursor) {
                    if actual_owner != used_cursor || actual_state != SLAB_WRITING {
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

            if assigned + capacity <= slab.capacity() {
                if assigned + capacity == slab.capacity() {
                    self.try_advance_used_cursor(used_cursor);
                }

                // 写入数据
                let slice = slab.slice_at(assigned, capacity);
                slice.fill(0);
                sink(slice);
                // 先注册 notified future，再 complete_write，避免错过通知
                let fut = slab.waker().notified();
                let written = slab.complete_write(capacity);
                slab.release_ref();
                // 如果我是最后一个写完的人，负责 mark_ready
                if written == slab.capacity() {
                    slab.mark_ready(owner);
                }
                // 等待持久化完成
                if !self.is_persisted(used_cursor) {
                    fut.await;
                    // double-check
                    while !self.is_persisted(used_cursor) {
                        spin_loop();
                    }
                }

                return Ok(());
            } else if assigned < slab.capacity() {
                // 情况 B：空间不够但 slab 还有剩余 → 补零退休
                let written = slab.submit_with_padding(assigned);
                slab.release_ref();
                if written == slab.capacity() {
                    slab.mark_ready(owner);
                }
                self.try_advance_used_cursor(used_cursor);
                // 继续循环，在下一个 slab 重试
            } else {
                // 情况 C：空间已被其他 producer 抢完 → 推进到下一个 slab
                slab.release_ref();
                self.try_advance_used_cursor(used_cursor);
            }
        }
    }

    // =========================================================
    // SQ 线程：无锁扫描 READY slab
    // =========================================================
    pub fn fetch_ready_slabs(
        &self,
        inflight_limit: usize,
        idle_spin_limit: usize,
        submit_cursor: &mut usize,
        ready_slabs: &mut Vec<usize>,
        idle_spins: &mut usize,
    ) {
        let used_cursor = self.used_cursor.load(Ordering::Acquire);
        let upper = used_cursor.min(*submit_cursor + inflight_limit);

        let mut advance = *submit_cursor;

        for cursor in *submit_cursor..upper {
            let slab = self.slab(cursor);
            let (_, state) = slab.inspect();
            match state {
                SLAB_READY => {
                    ready_slabs.push(cursor);
                    advance += 1;
                }
                SLAB_IN_FLIGHT | SLAB_COMPLETED => {
                    advance += 1;
                }
                _ => {
                    break;
                }
            }
        }

        if advance != *submit_cursor {
            *idle_spins = 0;
            *submit_cursor = advance;
        } else {
            // 没有新 slab 就绪，检查是否需要 retire 当前 WRITING slab
            let slab = self.slab(*submit_cursor);
            let (owner, state) = slab.inspect();
            if state == SLAB_WRITING {
                *idle_spins += 1;
                if *idle_spins >= idle_spin_limit {
                    // 强制 retire：原子抢占剩余空间
                    let assigned = slab.prepare_write(self.slab_capacity);
                    if assigned < self.slab_capacity {
                        let written = slab.submit_with_padding(assigned);
                        if written == self.slab_capacity {
                            slab.mark_ready(owner);
                        }
                        if *submit_cursor == used_cursor {
                            self.try_advance_used_cursor(used_cursor);
                        }
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
        let mut advance = false;
        loop {
            let slab = self.slab(persist_cursor);
            let (_, state) = slab.inspect();
            if state != SLAB_COMPLETED {
                break;
            }

            // 等待所有写入者退出
            while !slab.is_reusable() {
                spin_loop();
            }

            // 唤醒等待此 LSN 的 producer
            slab.notify_waiters();

            // 重置 slab 供下一轮使用
            slab.reset(persist_cursor + self.slab_amount);
            persist_cursor += 1;
            advance = true;
        }
        if advance {
            self.persist_cursor.store(persist_cursor, Ordering::Release);
        }
    }

    pub fn flush_remaining_for_shutdown(&self) {
        let used_cursor = self.used_cursor.load(Ordering::Acquire);
        let slab = self.slab(used_cursor);
        let (owner, state) = slab.inspect();

        if owner != used_cursor {
            return;
        }

        if state == SLAB_WRITING {
            let assigned = slab.prepare_write(self.slab_capacity);
            if assigned < self.slab_capacity {
                let written = slab.submit_with_padding(assigned);
                if written == self.slab_capacity {
                    slab.mark_ready(owner);
                }
                self.try_advance_used_cursor(used_cursor);
            }
        }
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
