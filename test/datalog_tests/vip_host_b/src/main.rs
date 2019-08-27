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
    // Construct server
    let prog = HDDlog::run(1, false, |_,_:&Record, _| {});
    let redirect: HashMap<_, _> =
        vec![(vip_fwd_controller_FwdTable as usize,
              vip_fwd_host_FwdTable_ as usize)]
        .into_iter().collect();
    let mut s = server::DDlogServer::new(prog, redirect);

    // Receiving channel
    let addr_s = "127.0.0.1:8020";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut receiver = TcpReceiver::new(addr);
    let rec_con = receiver.connect();

    // Sending TCP channel
    let addr_s = "127.0.0.1:8002";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut sender = TcpSender::new(addr);
    sender.connect();

    // Wait for receiver connections
    rec_con.join();

    // Stream table from left server
    let tup: HashSet<_> = vec![vip_fwd_host_VM as usize]
        .into_iter().collect();
    let mut outlet = s.add_stream(tup);

    // Server subscribes to the upstream TCP channel
    let s_a = Arc::new(Mutex::new(s));
    let _sub = {
        let s_a = server::ADDlogServer(s_a.clone());
        receiver.subscribe(Box::new(s_a))
    };

    // Downstream TCP channel subscribes to the stream
    let _sub = {
        outlet.subscribe(Box::new(sender))
    };

    // Insert `true` to Left in left server
    //let host_val = Record::NamedStruct(
    //    Cow::from("vip_fwd.host.HostId"),
    //    vec![(Cow::from("id"), Record::Bool(true))]);
    let host_val = Record::Bool(true);
    let host_id = RelIdentifier::RelId(vip_fwd_host_HostId as usize);

    //let vm_val = Record::NamedStruct(
    //    Cow::from("vip_fwd.host.VM_"),
    //    vec![(Cow::from("vip"), Record::Bool(true))]);
    let vm_val = Record::Bool(true);
    let vm_id = RelIdentifier::RelId(vip_fwd_host_VM_ as usize);

    let updates = &[UpdCmd::Insert(host_id, host_val),
                    UpdCmd::Insert(vm_id, vm_val)];

    let handle = receiver.listen();

    // Execute and transmit the update
    {
        let mut s = s_a.lock().unwrap();
        s.on_start();
        s.on_updates(Box::new(updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap())));
        s.on_commit();
    }

    //sender.lock().unwrap().disconnect();
    handle.join();

    //receiver.disconnect();

    let mut s = s_a.lock().unwrap();
    s.shutdown();
}
