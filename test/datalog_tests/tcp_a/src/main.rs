use differential_datalog::program::Update;
use differential_datalog::record::{Record, UpdCmd, RelIdentifier};
use observe::{Observer, Observable, Subscription};
use tcp_channel::{TcpSender, ATcpSender, TcpReceiver};

use roundtrip_ddlog::*;
use roundtrip_ddlog::api::*;
use roundtrip_ddlog::Relations::*;

use std::net::SocketAddr;
use std::collections::{HashSet, HashMap};
use std::sync::{Arc, Mutex};

fn main() -> Result<(), String> {

    // Construct server a, redirect Out
    let prog = HDDlog::run(1, false, |_,_:&Record, _| {});
    let mut redirect = HashMap::new();
    redirect.insert(rt_b_Out as usize, rt_a_FromB as usize);
    let mut s = server::DDlogServer::new(prog, redirect);

    // Receiving TCP channel
    let addr_s = "127.0.0.1:8002";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut receiver = TcpReceiver::new(addr);

    let rec_con = receiver.connect();

    // Sending TCP channel
    let addr_s = "127.0.0.1:8001";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut sender = TcpSender::new(addr);

    sender.connect();

    let sender = Arc::new(Mutex::new(sender));
    let a_sender = ATcpSender(sender.clone());

    rec_con.join();

    // Stream table from left server
    let mut tup = HashSet::new();
    tup.insert(rt_a_ToB as usize);
    let mut outlet = s.add_stream(tup);

    // Server subscribes to the upstream TCP channel
    let s = Arc::new(Mutex::new(s));
    let _sub1 = {
        let s_a = server::ADDlogServer(s.clone());
        receiver.subscribe(Box::new(s_a))
    };

    // Downstream TCP channel subscribes to the stream
    let sub2 = {
        outlet.subscribe(Box::new(a_sender))
    };

    // Insert `true` to Left in left server
    let rec = Record::Bool(true);
    let table_id = RelIdentifier::RelId(rt_a_In as usize);
    let updates = &[UpdCmd::Insert(table_id, rec)];

    let handle = receiver.listen();

    // Execute and transmit the update
    {
        let s = s.clone();
        let mut s = s.lock().unwrap();

        s.on_start()?;
        s.on_updates(Box::new(updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap())))?;
        s.on_commit()?;
        //s.on_completed()?;
    }

    //sender.lock().unwrap().disconnect();
    handle.join();

    //receiver.disconnect();

    let mut s = s.lock().unwrap();
    s.shutdown()
}
