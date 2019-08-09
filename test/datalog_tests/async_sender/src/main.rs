extern crate serde_json;

use differential_datalog::program::{Update, Response};
use observe::{Observer};
use ddd_ddlog::*;

use tokio::net::{TcpStream};
use tokio::net::tcp::ConnectFuture;
use tokio::prelude::*;

use std::net::{SocketAddr};

pub struct TcpSender{
    addr: SocketAddr,
    client: Option<Box<dyn Future<Item = (), Error = ()> + Send>>,
    stream: Option<ConnectFuture>,
}

impl TcpSender{
    pub fn new(socket: SocketAddr) -> Self {
        TcpSender{
            addr: socket,
            client: None,
            stream: None,
        }
    }
}

impl Observer<Update<Value>, String> for TcpSender {
    fn on_start(&mut self) -> Response<()> {
        self.stream = Some(TcpStream::connect(&self.addr));
        Ok(())
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = Update<Value>> + 'a>) -> Response<()> {

        let upds: Vec<_> = updates.map(|upd| {
            match upd {
                Update::Insert{relid, v} =>
                    serde_json::to_string(&(relid, v, true)).unwrap() + "\n", // TODO what's the right way to do this?
                Update::DeleteValue{relid, v} =>
                    serde_json::to_string(&(relid, v, false)).unwrap() + "\n", // TODO what's the right way to do this?
                _ => panic!("Committed update is neither insert or delete")
            }
        }).collect();

        if let Some(stream) = self.stream.take() {
            // TODO FIX: if we allow multiple on_update calls between on_commit,
            // this will only execute the last call
            self.client = Some(Box::new(stream.and_then(move |stream| {
                // TODO check buffering
                stream::iter_ok(upds).fold(stream, |writer, buf| {
                    tokio::io::write_all(writer, buf)
                        .map(|(writer, _buf)| writer)
                }).then(|_: Result<_, std::io::Error>| Ok(()))
            }).map_err(|err| {
                println!("connection error = {:?}", err);
            })));
        };

        Ok(())
    }

    fn on_commit(&mut self) -> Response<()> {
        if let Some(client) = self.client.take() {
            tokio::run(client);
        }
        Ok(())
    }

    fn on_completed(&mut self) -> Response<()> {
        Ok(())
    }

    fn on_error(&self, _error: String) {}
}

fn main() -> Response<()> {
    let addr_s = "127.0.0.1:8000";
    let addr = addr_s.parse::<SocketAddr>().unwrap();

    let mut sender = TcpSender::new(addr);

    let updates = vec![
        Update::Insert{relid: 3, v: Value::bool(false)},
        Update::DeleteValue{relid: 2, v: Value::bool(true)},
        Update::DeleteValue{relid: 2, v: Value::bool(true)},
        Update::DeleteValue{relid: 2, v: Value::bool(true)},
        Update::DeleteValue{relid: 2, v: Value::bool(true)},
    ];
    let upds: Box<dyn Iterator<Item = Update<Value>>> = Box::new(updates.into_iter());

    sender.on_start()?;
    sender.on_updates(upds)?;
    sender.on_commit()
}
