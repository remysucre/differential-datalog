use differential_datalog::program::Update;
use differential_datalog::record::{Record, UpdCmd, RelIdentifier};
use observe::{Observer, Observable, Subscription};
use tcp_channel::{TcpSender, ATcpSender, TcpReceiver};

use vip_fwd_ddlog::*;
use vip_fwd_ddlog::api::*;
use vip_fwd_ddlog::Relations::*;

use std::net::SocketAddr;
use std::collections::{HashSet, HashMap};
use std::sync::{Arc, Mutex};

fn main() {
    // Construct server a, redirect Out
    let prog = HDDlog::run(1, false, |_,_:&Record, _| {});
    let mut redirect = HashMap::new();
    redirect.insert(vip_fwd_host_VM as usize, vip_fwd_controller_VM as usize);
    let mut s = server::DDlogServer::new(prog, redirect);

    // Receiving TCP channel 1
    let addr_s = "127.0.0.1:8001";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    // TODO replace bool
    let mut receiver1 = TcpReceiver::<(bool)>::new(addr);

    let rec_con_1 = receiver1.connect();

    // Receiving TCP channel 2
    let addr_s = "127.0.0.1:8002";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    // TODO replace bool
    let mut receiver2 = TcpReceiver::<(bool)>::new(addr);

    let rec_con_2 = receiver2.connect();

    // Sending TCP channel 1
    let addr_s = "127.0.0.1:8010";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut sender = TcpSender::new(addr);

    sender.connect();

    let sender = Arc::new(Mutex::new(sender));
    let a_sender = ATcpSender(sender.clone());

    // Sending TCP channel 2
    let addr_s = "127.0.0.1:8020";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut sender = TcpSender::new(addr);

    sender.connect();

    let sender = Arc::new(Mutex::new(sender));
    let a_sender = ATcpSender(sender.clone());

    rec_con_1.join();
    rec_con_2.join();

    // Stream table from left server
    let mut tup = HashSet::new();
    tup.insert(vip_fwd_controller_FwdTable as usize);
    let mut outlet = s.add_stream(tup);

}
