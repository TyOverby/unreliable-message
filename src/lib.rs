#![feature(collections)]
#![allow(unused)]

use std::collections::{VecMap, HashMap};

#[derive(Clone, Copy, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub struct MsgId(u64);

#[derive(Clone, Copy, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub struct PieceNum(u16, u16);

#[derive(Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub struct MsgChunk(MsgId, PieceNum, Vec<u8>);

#[derive(Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub struct CompleteMessage(MsgId, Vec<u8>);

struct MsgStage {
    this_id: MsgId,
    total_pieces: u16,
    pieces: VecMap<MsgChunk>
}

pub struct MsgQueue {
    last_released: Option<MsgId>,
    stages: HashMap<MsgId, MsgStage>,
}

impl MsgQueue {
    pub fn new() -> MsgQueue {
        MsgQueue {
            last_released: None,
            stages: HashMap::new()
        }
    }

    fn mark_published(&mut self, id: MsgId) {
        self.last_released = Some(id);
        let kept: Vec<_> = self.stages.keys().cloned().collect();
        for open in kept {
            if id > open {
                self.stages.remove(&open);
            }
        }
    }

    pub fn insert_chunk(&mut self, chunk: MsgChunk) -> Option<CompleteMessage> {
        let id = chunk.0;

        // If the last published message was released before this chunk,
        // don't do anything and ignore it.
        if let Some(last) = self.last_released {
            if last.0 >= id.0 {
                return None;
            }
        }

        // If the chunk has only one piece to it, publish it immediately.
        if (chunk.1).1 == 1 {
            self.mark_published(id);
            return Some(CompleteMessage(id, chunk.2));
        }

        // If we are building a stage with the same message id, add it
        // to the stage.
        if self.stages.contains_key(&id) {
            let ready = {
                let stage = self.stages.get_mut(&id).unwrap();
                stage.add_chunk(chunk);
                stage.is_ready()
            };

            if ready {
                let mut stage = self.stages.remove(&id).unwrap();
                self.mark_published(id);
                return Some(stage.merge());
            } else {
                return None;
            }
        } else {
            self.stages.insert(chunk.0, MsgStage::new(chunk));
            return None;
        }
    }


}

impl MsgStage {
    fn new(starter: MsgChunk) -> MsgStage {
        let PieceNum(_, out_of) = starter.1;

        let mut stage = MsgStage {
            this_id: starter.0,
            total_pieces: out_of,
            pieces: VecMap::with_capacity(out_of as usize)
        };

        stage.add_chunk(starter);
        stage
    }

    fn is_ready(&self) -> bool {
        self.total_pieces as usize == self.pieces.len()
    }

    fn add_chunk(&mut self, chunk: MsgChunk) {
        let PieceNum(this, _) = chunk.1;
        if !self.pieces.contains_key(&(this as usize)) {
            self.pieces.insert(this as usize, chunk);
        }
    }

    fn merge(mut self) -> CompleteMessage {
        let mut size = 0;

        for (_, &MsgChunk(_, _, ref bytes)) in self.pieces.iter() {
            size += bytes.len();
        }

        let mut v = Vec::with_capacity(size);

        for (_, &mut MsgChunk(_, _, ref mut bytes)) in self.pieces.iter_mut() {
            v.append(bytes);
        }

        CompleteMessage(self.this_id, v)
    }
}


// Stage tests

#[test] fn is_ready_single_complete() {
    let comp_chunk = MsgChunk(MsgId(0), PieceNum(1, 1), vec![0]);
    let stage = MsgStage::new(comp_chunk);
    assert!(stage.is_ready());
    assert!(stage.merge() == CompleteMessage(MsgId(0), vec![0]));
}

#[test] fn is_ready_single_incomplete() {
    let incomp_chunk = MsgChunk(MsgId(0), PieceNum(1, 2), vec![0]);
    let stage = MsgStage::new(incomp_chunk);
    assert!(!stage.is_ready());
}

#[test] fn is_ready_double_complete() {
    let c1 = MsgChunk(MsgId(0), PieceNum(1, 2), vec![0]);
    let c2 = MsgChunk(MsgId(0), PieceNum(2, 2), vec![1]);

    let mut stage = MsgStage::new(c1.clone());
    stage.add_chunk(c2.clone());
    assert!(stage.is_ready());
    assert!(stage.merge() == CompleteMessage(MsgId(0), vec![0, 1]));

    // Now in the opposite order

    let mut stage = MsgStage::new(c2.clone());
    stage.add_chunk(c1.clone());
    assert!(stage.is_ready());
    assert!(stage.merge() == CompleteMessage(MsgId(0), vec![0, 1]));
}

#[test] fn is_ready_double_same() {
    let c1 = MsgChunk(MsgId(0), PieceNum(1, 2), vec![0]);

    let mut stage = MsgStage::new(c1.clone());
    stage.add_chunk(c1);
    assert!(!stage.is_ready());
}

// Queue tests

#[test] fn queue_single() {
    let mut queue = MsgQueue::new();
    let c1 = MsgChunk(MsgId(1), PieceNum(1, 1), vec![0]);

    let res = queue.insert_chunk(c1.clone());

    assert!(res.is_some());
    assert!(res.unwrap() == CompleteMessage(MsgId(1), vec![0]));
    assert!(queue.last_released == Some(MsgId(1)));

    // try to requeue the message.  It shouldn't go through this time.
    let res = queue.insert_chunk(c1);
    assert!(res.is_none());
}

#[test] fn queue_double() {
    let mut queue = MsgQueue::new();
    let c1 = MsgChunk(MsgId(1), PieceNum(1, 2), vec![0]);
    let c2 = MsgChunk(MsgId(1), PieceNum(2, 2), vec![1]);

    let res = queue.insert_chunk(c1.clone());
    assert!(res.is_none());
    let res = queue.insert_chunk(c2.clone());
    assert!(res.is_some());
    assert!(res.unwrap() == CompleteMessage(MsgId(1), vec![0, 1]));
    assert!(queue.last_released == Some(MsgId(1)));

    assert!(queue.insert_chunk(c1).is_none());
    assert!(queue.insert_chunk(c2).is_none());
}

#[test] fn out_of_order() {
    let mut queue = MsgQueue::new();
    let c1 = MsgChunk(MsgId(1), PieceNum(1, 1), vec![0]);
    let c2 = MsgChunk(MsgId(2), PieceNum(1, 1), vec![1]);

    assert!(queue.insert_chunk(c2.clone()).is_some());
    assert!(queue.insert_chunk(c1).is_none());
    assert!(queue.insert_chunk(c2).is_none());
}

#[test] fn odd_orders() {
    let a1 = MsgChunk(MsgId(1), PieceNum(1, 2), vec![0]);
    let a2 = MsgChunk(MsgId(1), PieceNum(2, 2), vec![1]);

    let b1 = MsgChunk(MsgId(2), PieceNum(1, 2), vec![2]);
    let b2 = MsgChunk(MsgId(2), PieceNum(2, 2), vec![3]);

    let mut queue = MsgQueue::new();
    assert!(queue.insert_chunk(a1.clone()).is_none());
    assert!(queue.insert_chunk(b1.clone()).is_none());
    assert!(queue.insert_chunk(a2.clone()).is_some());
    assert!(queue.insert_chunk(b2.clone()).is_some());


    let mut queue = MsgQueue::new();
    assert!(queue.insert_chunk(a1.clone()).is_none());
    assert!(queue.insert_chunk(b1.clone()).is_none());
    assert!(queue.insert_chunk(b2.clone()).is_some());
    assert!(queue.insert_chunk(a2.clone()).is_none());


    let mut queue = MsgQueue::new();
    assert!(queue.insert_chunk(b1.clone()).is_none());
    assert!(queue.insert_chunk(b2.clone()).is_some());
    assert!(queue.insert_chunk(a2.clone()).is_none());
}
