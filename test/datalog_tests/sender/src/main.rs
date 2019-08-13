use differential_datalog::program::{Update, Response};
use ddd_ddlog::*;
use observe::Observer;
use tcp_channel::TcpSender;
use std::net::SocketAddr;

fn main() -> Result<(), String> {
    let addr_s = "127.0.0.1:8787";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut sender = TcpSender::new(addr);


    let updates = vec![
        Update::Insert{relid: 3, v: Value::bool(false)},
        Update::DeleteValue{relid: 2, v: Value::bool(true)},
        Update::DeleteValue{relid: 2, v: Value::bool(true)},
        Update::DeleteValue{relid: 2, v: Value::bool(true)},
        Update::DeleteValue{relid: 2, v: Value::bool(true)},
    ];
    let upds = updates.into_iter().map(|upd| match upd {
        Update::Insert{relid, v} => (relid, v, true),
        Update::DeleteValue{relid, v} => (relid, v, false),
        _ => panic!("Committed update is neither insert or delete")
    });



    // We need to use the explicit method syntax to specify
    // the type parameters for Observer
    Observer::<(usize, Value, bool), String>::on_start(&mut sender)?;
    sender.on_updates(Box::new(upds))?;
    Observer::<(usize, Value, bool), String>::on_commit(&mut sender)?;
    Observer::<(usize, Value, bool), String>::on_completed(&mut sender)?;

    Ok(())
}
