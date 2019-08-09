extern crate serde_json;

use differential_datalog::program::{RelId, Update, Response};
use observe::{Observable, Observer, Subscription};

use ddd_ddlog::*;

use tokio::net::{TcpListener};
use tokio::prelude::*;
use futures::sync::oneshot;

use std::sync::{Arc, Mutex};
use std::net::{SocketAddr};

use std::boxed::*;

pub struct TcpReceiver {
    addr: SocketAddr,
    server: Option<Box<dyn Future<Item = (), Error = ()> + Send + Sync>>,
}

impl TcpReceiver{
    pub fn new(socket: SocketAddr) -> Self {
        TcpReceiver{
            addr: socket,
            server: None,
        }
    }
}

struct TcpSubscription {
    shutdown: oneshot::Sender<()>,
}

impl Subscription for TcpSubscription {
    fn unsubscribe(self: Box<Self>) {
        println!("unsubscribing");
        self.shutdown.send(());
    }
}

impl Observable<Update<Value>, String> for TcpReceiver {
    fn subscribe(&mut self, observer: Box<dyn Observer<Update<Value>, String> + Sync>) -> Box<dyn Subscription> {
        let listener = TcpListener::bind(&self.addr).unwrap();
        let observer = Arc::new(Mutex::new(observer));
        let server = listener.incoming().for_each(move |socket| {

            let observer = observer.clone();

            let stream = std::io::BufReader::new(socket);

            let work = tokio::io::lines(stream).map(move |line| {
                let (relid, v, pol): (RelId, Value, bool) = serde_json::from_str(&line).unwrap();
                if pol {
                    Update::Insert{relid: relid, v: v}
                } else {
                    Update::DeleteValue{relid: relid, v: v}
                }
            }).collect().and_then(move |upds| {
                let mut obs = observer.lock().unwrap();
                obs.on_start();
                obs.on_updates(Box::new(upds.into_iter()));
                obs.on_commit();
                Ok(())
            }).map_err(|err| {
                print!("error {:?}", err)
            });

            tokio::spawn(work);
            Ok(())
        }).map_err(|_err| {
            oneshot::Canceled
        });

        let (shutdown_sender, shutdown_receiver) = oneshot::channel();

        let f = server.select(shutdown_receiver)
            .and_then(|_| Ok(()))
            .map_err(|_err| {
            println!("err");
        });

        self.server = Some(Box::new(f));

        Box::new(TcpSubscription{
            shutdown: shutdown_sender,
        })
    }
}

impl TcpReceiver {
    pub fn listen(&mut self) {
        if let Some(server) = self.server.take() {
            println!("server running");
            tokio::run(server);
        }
    }
}

struct TestObserver{}

impl Observer<Update<Value>, String> for TestObserver {
    fn on_start(&mut self) -> Response<()> {
        println!("startin!");
        Ok(())
    }
    fn on_commit(&mut self) -> Response<()> {
        println!("commiting!");
        Ok(())
    }
    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = Update<Value>> + 'a>) -> Response<()> {
        let upds:Vec<_> = updates.collect();
        println!("{:?}", upds.len());
        Ok(())
    }
    fn on_completed(&mut self) -> Response<()> {
        println!("completing!");
        Ok(())
    }

    fn on_error(&self, _error: String) {}
}

fn main() {
    let addr_s = "127.0.0.1:8000";
    let addr = addr_s.parse::<SocketAddr>().unwrap();

    let mut receiver = TcpReceiver::new(addr);

    let obs = TestObserver{};

    let sub = receiver.subscribe(Box::new(obs));

    if let Some(server) = receiver.server {
        //sub.unsubscribe();
        tokio::run(server);
    }
}
