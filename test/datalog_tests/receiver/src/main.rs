use std::net::{TcpListener, TcpStream, SocketAddr};
use std::io::prelude::*;
use std::io;
use std::sync::{Arc, Mutex};
use observe::{Observer, Observable, Subscription};

pub struct TcpReceiver {
    addr: SocketAddr,
    listener: Option<TcpListener>,
    observer: Arc<Mutex<Option<Box<dyn Observer<usize, String> + Sync>>>>
}

impl TcpReceiver {
    fn new(addr: SocketAddr) -> Self {
        TcpReceiver {
            addr: addr,
            listener: None,
            observer: Arc::new(Mutex::new(None))
        }
    }

    fn listen(&mut self) {
        let listener = TcpListener::bind(self.addr);
        let observer = self.observer.clone();
        let mut observer = observer.lock().unwrap();
        if let Some(observer) = observer.take() {
            if let Some(listener) = self.listener.take() {
                // Read everything then call on_updates
                for stream in listener.incoming() {
                    handle_client(stream.unwrap());
                }
            }
        }
    }
}

struct TcpSubscription {
    observer: Arc<Mutex<Option<Box<dyn Observer<usize, String> + Sync>>>>
}

impl Subscription for TcpSubscription {
    fn unsubscribe(self: Box<Self>) {
        let obs = self.observer.clone();
        let mut obs = obs.lock().unwrap();
        *obs = None;
    }
}

impl Observable<usize, String> for TcpReceiver {
    fn subscribe(&mut self, observer: Box<dyn Observer<usize, String> + Sync>) -> Box<dyn Subscription> {
        let obs = self.observer.clone();
        let mut obs = obs.lock().unwrap();
        *obs = Some(observer);

        Box::new(TcpSubscription {
            observer: self.observer.clone()
        })
    }
}

fn handle_client(mut stream: TcpStream) {
    let mut buffer = String::new();
    // TODO understand different reads
    stream.read_to_string(&mut buffer).unwrap();
    println!("{:?}", buffer);
}

fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8787")?;

    // accept connections and process them serially
    for stream in listener.incoming() {
        handle_client(stream?);
    }
    Ok(())
}
