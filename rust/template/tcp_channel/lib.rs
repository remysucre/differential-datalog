extern crate serde_json;

use serde::ser::Serialize;
use serde_json::ser::to_string;
use std::net::{TcpStream, TcpListener, SocketAddr};
use std::io::prelude::*;
use std::io;
use std::sync::{Arc, Mutex};
use observe::{Observer, Observable, Subscription};

use serde::de::DeserializeOwned;
use serde_json::from_str;
use std::fmt::Debug;

pub struct TcpReceiver<T> {
    addr: SocketAddr,
    observer: Arc<Mutex<Option<Box<dyn Observer<T, String> + Sync>>>>
}

impl <T: DeserializeOwned + Debug + Send> TcpReceiver<T> {
    pub fn new(addr: SocketAddr) -> Self {
        TcpReceiver {
            addr: addr,
            observer: Arc::new(Mutex::new(None))
        }
    }

    pub fn listen(&mut self) {
        let listener = TcpListener::bind(self.addr).unwrap();
        let observer = self.observer.clone();
        let mut observer = observer.lock().unwrap();
        if let Some(mut observer) = observer.take() {
            observer.on_start();
            // Read everything then call on_updates
            for stream in listener.incoming() {
                let stream = stream.unwrap();
                let reader = std::io::BufReader::new(stream);
                let upds = reader.lines().map(|line| {
                    let v: T = from_str(&line.unwrap()).unwrap();
                    v
                });

                observer.on_updates(Box::new(upds));
                observer.on_commit();
            }
        }
    }
}

struct TcpSubscription<T> {
    observer: Arc<Mutex<Option<Box<dyn Observer<T, String> + Sync>>>>
}

impl <T> Subscription for TcpSubscription<T> {
    fn unsubscribe(self: Box<Self>) {
        let obs = self.observer.clone();
        let mut obs = obs.lock().unwrap();
        *obs = None;
    }
}

impl <T: Send + 'static> Observable<T, String> for TcpReceiver<T> {
    fn subscribe(&mut self, observer: Box<dyn Observer<T, String> + Sync>) -> Box<dyn Subscription> {
        let obs = self.observer.clone();
        let mut obs = obs.lock().unwrap();
        *obs = Some(observer);

        Box::new(TcpSubscription {
            observer: self.observer.clone()
        })
    }
}


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

impl<T: Send + Serialize> Observer<T, String> for TcpSender {
    // Start a TCP connection given an adress
    fn on_start(&mut self) -> Result<(), String> {
        if let None = &self.stream {
            self.stream = Some(
                TcpStream::connect(self.addr).unwrap()
            );
        } else {
            panic!("Attempting to start another transaction \
                    while one is already in progress");
        }
        Ok(())
    }

    // Write the updates to the TCP stream
    fn on_updates<'a>(&mut self,
                      updates: Box<dyn Iterator<Item = T> + 'a>)
                      -> Result<(), String> {
        if let Some(ref mut stream) = self.stream {
            for upd in updates {
                let s = to_string(&upd).unwrap() + "\n";
                stream.write(s.as_bytes());
            }
        }
        Ok(())
    }

    // Flush the TCP stream
    fn on_commit(&mut self) -> Result<(), String> {
        if let Some(ref mut stream) = self.stream {
            stream.flush();
        }
        Ok(())
    }

    // Close the TCP connection
    fn on_completed(&mut self) -> Result<(), String> {
        // Move the TcpStream to close the connection
        self.stream.take();
        Ok(())
    }

    fn on_error(&self, _error: String) {}
}
