use differential_datalog::program::{RelId, Update, Response};
use differential_datalog::record::{Record, UpdCmd, RelIdentifier};
use observe::{Observer, Observable, Subscription};
use tcp_channel::TcpReceiver;

use ddd_ddlog::*;
use ddd_ddlog::api::*;
use ddd_ddlog::Relations::*;

use std::collections::{HashSet, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

fn main() -> Result<(), String> {
    let addr_s = "127.0.0.1:8787";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut r = TcpReceiver::new(addr);

    // Construct right server, redirect Middle table
    let prog2 = HDDlog::run(1, false, |_,_:&Record, _| {});
    let mut redirect2 = HashMap::new();
    redirect2.insert(lr_left_Middle as usize, lr_right_Middle as usize);
    let s2 = server::DDlogServer::new(prog2, redirect2);

    // Right server subscribes to the stream
    let s2 = Arc::new(Mutex::new(s2));
    let sub = {
        let s2_a = server::ADDlogServer(s2.clone());
        let adapter = Adapter{observer: Box::new(s2_a)};
        r.subscribe(Box::new(adapter))
    };

    r.listen()?;
    sub.unsubscribe();
    r.listen()?;

    Ok(())
}


struct Adapter {
    observer: Box<dyn Observer<Update<Value>, String>>
}

struct AdapterSub;

impl Subscription for AdapterSub {
    fn unsubscribe(self: Box<Self>) {
    }
}

impl Observable<Update<Value>, String>  for Adapter {
    fn subscribe(&mut self,
                 observer: Box<dyn Observer<Update<Value>, String>>) -> Box<dyn Subscription>{
        self.observer = observer;
        Box::new(AdapterSub)
    }
}

impl Observer<(usize, Value, bool), String> for Adapter {
    fn on_start(&mut self) -> Result<(), String> {
        self.observer.on_start()
    }
    fn on_commit(&mut self) -> Result<(), String> {
        self.observer.on_commit()
    }
    fn on_next(&mut self, item: (usize, Value, bool)) -> Result<(), String> {
        let (relid, v, b) = item;
        let item = if b {
            Update::Insert{relid, v}
        } else {
            Update::DeleteValue{relid, v}
        };
        self.observer.on_next(item)
    }
    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = (usize, Value, bool)> + 'a>) -> Result<(), String> {
        self.observer.on_updates(Box::new(updates.map(|(relid, v, b)| {
            if b {
                Update::Insert{relid, v}
            } else {
                Update::DeleteValue{relid, v}
            }
        })))
    }
    fn on_completed(&mut self) -> Result<(), String> {
        self.observer.on_completed()
    }
    fn on_error(&self, error: String) {
        self.observer.on_error(error)
    }
}
