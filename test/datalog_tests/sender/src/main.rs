use std::io::prelude::*;
use std::net::{SocketAddr, TcpStream};
use observe::{Observer};

pub struct TcpSender {
    addr: SocketAddr,
    stream: Option<TcpStream>,
}

impl TcpSender {
    pub fn new(socket: SocketAddr) -> Self {
        TcpSender {
            addr: socket,
            stream: None
        }
    }
}

impl Observer<usize, String> for TcpSender {
    fn on_start(&mut self) -> Result<(), String> {
        if let None = self.stream.take() {
            self.stream = Some(TcpStream::connect(self.addr).unwrap());
        } else {
            panic!("Attempting to start another transaction \
                    while one is already in progress");
        }
        Ok(())
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = usize> + 'a>) -> Result<(), String> {
        if let Some(ref mut stream) = self.stream {
            for upd in updates {
                // TODO understand the difference between write methods
                match stream.write(&[upd as u8]) {
                    Ok(_) => {},
                    Err(e) => println!("{:?}", e),
                }
            }
        }
        Ok(())
    }

    fn on_commit(&mut self) -> Result<(), String> {
        // Transmit the updates
        println!("actuallly commiting");
        if let Some(mut stream) = self.stream.take() {
            match stream.flush() {
                Ok(_) => {},
                Err(e) => println!("{:?}", e),
            }
        }
        Ok(())
    }

    fn on_completed(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn on_error(&self, _error: String) {}
}

fn main() -> Result<(), String> {
    let addr_s = "127.0.0.1:8787";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut sender  = TcpSender::new(addr);
    let upds = vec![7, 2, 3, 4];
    sender.on_start()?;
    sender.on_updates(Box::new(upds.into_iter()))?;
    sender.on_commit()?;
    sender.on_completed()?;
    Ok(())
}
