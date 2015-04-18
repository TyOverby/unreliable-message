#![feature(ip_addr)]
extern crate unreliable;

use std::net::{UdpSocket, SocketAddr, IpAddr, Ipv4Addr};

fn get_sockets() -> (SocketAddr, SocketAddr) {
    let localhost = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let mut args = std::env::args();
    args.next(); // drop how.
    let port_1 = args.next().expect("Expected port number")
                   .parse().ok().expect("Expected port number to be a number");
    let port_2 = args.next().expect("Expected port number")
                   .parse().ok().expect("Expected port number to be a number");

    (SocketAddr::new(localhost, port_1), SocketAddr::new(localhost, port_2))
}

const MSG_SIZE:u16 = 50;

fn main() {
    use std::thread;
    use unreliable::{Sender, Receiver};
    use unreliable::msgqueue::CompleteMessage;

    let (ours, theirs) = get_sockets();
    let udp_in = UdpSocket::bind(ours).unwrap();
    let udp_out = udp_in.try_clone().unwrap();

    // sending thread
    let h1 = thread::spawn(move || {
        let mut sender = Sender::from_socket(udp_out, MSG_SIZE, 1);
        let mut buf = String::new();
        loop {
            match std::io::stdin().read_line(&mut buf) {
                Ok(_) => {
                    sender.enqueue(buf.into_bytes(), theirs).unwrap();
                    buf = String::new();
                    sender.send_all().unwrap();
                },
                Err(_) => {}
            }

        }
    });

    // receiving thread
    let h2 = thread::spawn(move || {
        let mut receiver = Receiver::from_socket(udp_in, MSG_SIZE);
        loop {
            match receiver.poll() {
                Ok((_, CompleteMessage(_, v))) => {
                    let m = String::from_utf8(v).unwrap();
                    println!("{}", m);
                }
                Err(e) => println!("Error! {:?}", e)
            }
        }
    });

    let _ = h1.join();
    let _ = h2.join();
}
