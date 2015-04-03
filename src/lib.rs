#![feature(collections)]
#![allow(unused)]

use std::collections::{VecMap, HashMap};

#[derive(Clone, Copy, Hash, Ord, PartialOrd, Eq, PartialEq)]
struct MsgId(u64);

#[derive(Clone, Copy, Hash, Ord, PartialOrd, Eq, PartialEq)]
struct PieceNum(u16, u16);

#[derive(Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
struct MsgChunk(MsgId, PieceNum, Vec<u8>);

struct CompleteMessage(MsgId, Vec<u8>);

struct MsgStage {
    this_id: MsgId,
    total_pieces: u16,
    pieces: VecMap<MsgChunk>
}

struct MsgQueue {
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

        // If we already have a stage with the same message id, add it
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
