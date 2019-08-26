use differential_datalog::program::Update;
use differential_datalog::record::{Record, UpdCmd, RelIdentifier};
use observe::{Observer, Observable, Subscription};
use tcp_channel::TcpSender;

use ddd_ddlog::*;
use ddd_ddlog::api::*;
use ddd_ddlog::Relations::*;

use std::net::SocketAddr;
use std::collections::{HashSet, HashMap};

fn main() -> Result<(), String> {

    // Construct left server with no redirect
    let prog1 = HDDlog::run(1, false, |_,_:&Record, _| {});
    let mut redirect1 = HashMap::new();
    redirect1.insert(lr_left_Left as usize, lr_left_Left as usize);
    let mut s1 = server::DDlogServer::new(prog1, redirect1);

    // First TCP channel
    let addr_s = "127.0.0.1:8001";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut chan1 = TcpSender::new(addr);
    chan1.connect();

    // Second TCP channel
    let addr_s = "127.0.0.1:8002";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut chan2 = TcpSender::new(addr);
    chan2.connect();

    // Stream Up table from left server
    let mut tup = HashSet::new();
    tup.insert(lr_left_Up as usize);
    let mut outlet_up = s1.add_stream(tup);

    // First TcpSender subscribes to the stream
    let _sub1 = {
        outlet_up.subscribe(Box::new(chan1))
    };

    // Stream Down table from left server
    let mut tdown = HashSet::new();
    tdown.insert(lr_left_Down as usize);
    let mut outlet_down = s1.add_stream(tdown);

    // Second TcpSender subscribes to the stream
    let _sub2 = {
        outlet_down.subscribe(Box::new(chan2))
    };

    // Insert `true` to Left in left server
    let rec = Record::Bool(false);
    let table_id = RelIdentifier::RelId(lr_left_Left as usize);
    let updates = &[UpdCmd::Insert(table_id, rec)];

    // Execute and transmit the update
    s1.on_start()?;
    s1.on_updates(Box::new(updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap())))?;
    s1.on_commit()?;
    s1.on_completed()?;

    s1.shutdown()
}
