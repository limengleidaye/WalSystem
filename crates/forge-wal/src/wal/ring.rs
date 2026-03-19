use anyhow::bail;
use std::{
    fs::File,
    io::{Seek, SeekFrom},
    sync::Arc,
};
use tokio::{sync::Notify, task::LocalEnterGuard};

use anyhow::{Ok, Result};

use crate::wal::slab::{self, Slab, SlabState};

pub struct Submission {
    pub cursor: usize,
    pub ptr: *const u8,
    pub len: u32,
    pub offset: u64,
}

pub enum ReserveAction {
    Wait(Arc<Notify>),
    Written { cursor: usize, notify: Arc<Notify> },
}

pub struct SlabRing {
    slabs: Vec<Slab>,
    slab_capacity: usize,
    slab_amount: usize,

    // persist_cursor <= submit_cursor <= used_cursor
    used_cursor: usize,    // 当前正在被写入的逻辑编号
    submit_cursor: usize,  // SQ 线程已经处理到的逻辑 slab 编号
    persist_cursor: usize, // 已持久化完成的逻辑编号（下一个待持久化的位置）

    space_notify: Arc<Notify>,
}

impl SlabRing {
    pub fn new(slab_capacity: usize, slab_amount: usize) -> Self {
        let slabs: Vec<Slab> = (0..slab_amount)
            .map(|i| Slab::new(slab_capacity, i))
            .collect();
        Self {
            slabs,
            slab_capacity,
            slab_amount,
            used_cursor: 0,
            submit_cursor: 0,
            persist_cursor: 0,
            space_notify: Arc::new(Notify::new()),
        }
    }

    pub fn persist_cursor(&self) -> usize {
        self.persist_cursor
    }

    pub fn inflight(&self) -> usize {
        self.submit_cursor.saturating_sub(self.persist_cursor)
    }

    fn slab(&self, cursor: usize) -> &Slab {
        &self.slabs[cursor % self.slab_amount]
    }

    fn slab_mut(&mut self, cursor: usize) -> &mut Slab {
        &mut self.slabs[cursor % self.slab_amount]
    }

    /// 检查是否产生背压：所有 slab 都被占满，无法继续写入
    fn is_backpressure(&self) -> bool {
        self.used_cursor >= self.persist_cursor + self.slab_amount
    }

    pub fn is_persisted(&self, cursor: usize) -> bool {
        self.persist_cursor > cursor
    }

    /// 生产者入口
    pub fn reserve_once<F>(
        &mut self,
        capacity: usize,
        sink: &mut Option<F>,
    ) -> Result<ReserveAction>
    where
        F: FnOnce(&mut [u8]),
    {
        if capacity > self.slab_capacity {
            bail!(
                "reserve capacity {} exceeds slab capacity {}",
                capacity,
                self.slab_capacity
            );
        }

        loop {
            if self.is_backpressure() {
                return Ok(ReserveAction::Wait(self.space_notify.clone()));
            }

            let cursor = self.used_cursor;
            let slab = self.slab_mut(cursor);
            slab.ensure_owner(cursor)?;
            slab.set_writing();
            if slab.remaining() < capacity {
                if slab.is_empty() {
                    bail!("fresh slab cannot fit capacity {}", capacity);
                }
                slab.pad_zeroes();
                slab.mark_ready();
                self.used_cursor += 1;
                continue;
            }

            let offset = slab
                .prepare_write(capacity)
                .expect("remaining space checked above");
            let slice = slab.slice_mut(offset, capacity);
            slice.fill(0);
            let sink = sink.take().expect("sink must be called exactly once");
            sink(slice);
            slab.complete_write(capacity);

            let notify = slab.notify_handle().clone();
            if slab.is_full() {
                slab.mark_ready();
                self.used_cursor += 1;
            }
            return Ok(ReserveAction::Written { cursor, notify });
        }
    }

    /// 把一个一直没写满、但又迟迟没有后续写入的 WRITING slab，强制补零并退休，让它进入 READY，从而可以被 SQ 线程提交。
    pub fn retire_idle_writing_slab(&mut self) -> bool {
        if self.submit_cursor != self.used_cursor {
            return false;
        }

        let slab = self.slab_mut(self.used_cursor);
        if slab.state() != SlabState::Writing || slab.is_empty() {
            return false;
        }
        slab.pad_zeroes();
        slab.mark_ready();
        self.used_cursor += 1;
        true
    }

    /// 从 SlabRing 里把当前已经可以提交到磁盘的 slab 扫出来，转成一批 Submission，同时把这些 slab 的状态从 READY 改成 IN_FLIGHT，并推进 submit_cursor。
    pub fn fetch_ready_submissions(&mut self, limit: usize) -> Vec<Submission> {
        let mut out = Vec::new();
        let upper = self.used_cursor.min(self.submit_cursor + limit);
        while self.submit_cursor <= upper {
            let cursor = self.submit_cursor;
            let slab = self.slab(cursor);
            match slab.state() {
                SlabState::Ready => {
                    let entry = Submission {
                        cursor: cursor,
                        ptr: slab.as_ptr(),
                        len: slab.len_for_flush() as u32,
                        offset: (cursor * self.slab_capacity) as u64,
                    };
                    self.slab_mut(cursor).mark_inflight();
                    out.push(entry);
                    self.submit_cursor += 1;
                }
                SlabState::InFlight | SlabState::Completed => self.submit_cursor += 1,
                SlabState::None | SlabState::Writing => break,
            }
        }
        out
    }

    pub fn mark_completed(&mut self, cursor: usize) -> Result<()> {
        let slab = self.slab_mut(cursor);
        slab.ensure_owner(cursor)?;
        if slab.state() != SlabState::InFlight {
            bail!(
                "slab {} completion state invalid: {:?}",
                cursor,
                slab.state()
            );
        }
        slab.mark_completed();
        Ok(())
    }

    pub fn advance_persisted_lsn(&mut self) {
        let mut advanced = false;

        while self.persist_cursor < self.submit_cursor {
            let cursor = self.persist_cursor;
            let slab = self.slab(cursor);
            if slab.state() != SlabState::Completed {
                break;
            }
            slab.notify_waiters();
            let next_owner = slab.owner() + self.slab_amount;
            self.slab_mut(cursor).reset(next_owner);
            self.persist_cursor += 1;
            advanced = true;
        }

        if advanced {
            self.space_notify.notify_waiters();
        }
    }

    pub fn flush_remaining_for_shutdown(&mut self) {
        let cursor = self.used_cursor;
        let slab = self.slab_mut(cursor);

        if slab.owner() != cursor {
            return;
        }

        if slab.state() == SlabState::Writing && !slab.is_empty() {
            slab.pad_zeroes();
            slab.mark_ready();
            self.used_cursor += 1;
        }
    }
}
