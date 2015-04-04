use std::sync::mpsc;
use std::collections::VecDeque;
use std::net::{UdpSocket, ToSocketAddrs, SocketAddr};
use std::rc::Rc;
use std::io::Result as IoResult;

use super::msgqueue::*;
use bincode;

static MAX_MSG: u16 = ::std::u16::MAX;

struct Sender {
    out_queue: VecDeque<(MsgChunk, Rc<AddrsContainer>)>,
    last_id: u64,
    socket: UdpSocket,
    pub msg_length: u16,
    pub replication: u8
}

struct AddrsContainer{
    v: Vec<SocketAddr>
}

impl AddrsContainer {
    fn from_to_sock<T: ToSocketAddrs>(socket_addrs: T) -> IoResult<AddrsContainer> {
        let iter = try!(socket_addrs.to_socket_addrs());
        let vec = iter.collect();
        Ok(AddrsContainer{v: vec})
    }
}

impl ToSocketAddrs for AddrsContainer {
    type Iter = ::std::vec::IntoIter<SocketAddr>;

    fn to_socket_addrs(&self) -> IoResult<<AddrsContainer as ToSocketAddrs>::Iter> {
        let slice: Vec<_> = self.v[..].iter().cloned().collect();
        Ok(slice.into_iter())
    }
}

impl Sender {
    fn enqueue<T: ToSocketAddrs>(&mut self, message: Vec<u8>, addrs: T) -> IoResult<()> {
        self.last_id += 1;
        let id = self.last_id;
        let addrs = Rc::new(try!(AddrsContainer::from_to_sock(addrs)));
        let num_chunks = message.len() / (self.msg_length as usize);

        for _ in 0 .. self.replication {
            let mut chunk_count = 0;
            for chunk in message[..].chunks(self.msg_length as usize) {
                let mut v = Vec::with_capacity(chunk.len());
                v.push_all(chunk);
                let chunk = MsgChunk(
                    MsgId(id), PieceNum(chunk_count, num_chunks as u16), v);
                self.out_queue.push_back((chunk, addrs.clone()));
                chunk_count += 1;
            }
        }

        Ok(())
    }

    fn send_one(&mut self) -> bincode::EncodingResult<()> {
        let bound = bincode::SizeLimit::Bounded(MAX_MSG as u64);
        if let Some((next, addrs)) = self.out_queue.pop_front() {
            let bytes = try!(bincode::encode(&next, bound));
            self.socket.send_to(&bytes[..], &*addrs);
        }

        Ok(())
    }
}
