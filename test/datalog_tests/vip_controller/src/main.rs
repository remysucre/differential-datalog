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
use std::borrow::Cow;

fn main() {

    println!("fwd table is {:?}", vip_fwd_controller_FwdTable as usize);
    println!("vm table is {:?}", vip_fwd_controller_VM_ as usize);

    // Construct server
    let prog = HDDlog::run(1, false, |_,_:&Record, _| {});
    let redirect: HashMap<_, _> =
        vec![(vip_fwd_host_VM as usize,
              vip_fwd_controller_VM as usize)]
        .into_iter().collect();
    let mut s = server::DDlogServer::new(prog, redirect);

    // Receiving channel 1
    let addr_s = "127.0.0.1:8001";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut receiver1 = TcpReceiver::new(addr);
    let rec_con_1 = receiver1.connect();

    // Receiving channel 2
    let addr_s = "127.0.0.1:8002";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut receiver2 = TcpReceiver::new(addr);
    let rec_con_2 = receiver2.connect();

    // Sending TCP channel 1
    let addr_s = "127.0.0.1:8010";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut sender1 = TcpSender::new(addr);
    sender1.connect();

    // Sending TCP channel 2
    let addr_s = "127.0.0.1:8020";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut sender2 = TcpSender::new(addr);
    sender2.connect();

    // Wait for receiver connections
    rec_con_1.join();
    rec_con_2.join();

    // Stream table from left server
    let t_out: HashSet<_> = vec![vip_fwd_controller_FwdTable as usize]
        .into_iter().collect();
    let mut outlet = s.add_stream(t_out);

    // Server subscribes to the upstream TCP channel
    let s_a = Arc::new(Mutex::new(s));
    let _sub = {
        let s_a = server::ADDlogServer(s_a.clone());
        receiver1.subscribe(Box::new(s_a))
    };

    // Server subscribes to the upstream TCP channel
    let _sub = {
        let s_a = server::ADDlogServer(s_a.clone());
        receiver2.subscribe(Box::new(s_a))
    };

    // Downstream TCP channel subscribes to the stream
    let _sub = {
        outlet.subscribe(Box::new(sender1))
    };

    let _sub = {
        outlet.subscribe(Box::new(sender2))
    };

    // Insert `true` to Left in left server
    //let rec = Record::NamedStruct(
    //    Cow::from("vip_fwd.controller.Host"),
    //    vec![(Cow::from("id"), Record::Bool(true)),
    //         (Cow::from("ip"), Record::Bool(true))]);
    let rec = Record::Tuple(vec![
        Record::Bool(true),
        Record::Bool(true)
    ]);
    let table_id = RelIdentifier::RelId(vip_fwd_controller_Host as usize);
    let updates = &[UpdCmd::Insert(table_id, rec)];

    // Execute and transmit the update
    {
        let mut s = s_a.lock().unwrap();

        s.on_start();
        s.on_updates(Box::new(updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap())));
        s.on_commit();
    }

    let handle1 = receiver1.listen();
    let handle2 = receiver2.listen();

    //sender.lock().unwrap().disconnect();
    handle1.join();
    handle2.join();


    let mut s = s_a.lock().unwrap();
    s.shutdown();
}
