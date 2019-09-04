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
use std::time::Duration;
use std::thread;

fn main() {
    // Construct server
    let prog = HDDlog::run(1, false, |_,_:&Record, _| {});
    let redirect: HashMap<_, _> =
        vec![(vip_fwd_controller_Forward ,
              vip_fwd_host_Forward_ )]
        .into_iter().collect();
    let mut s = server::DDlogServer::new(prog, redirect);

    // Receiving channel
    let addr_s = "127.0.0.1:8010";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut receiver = TcpReceiver::new(addr);
    let rec_con = receiver.connect();

    // Sending TCP channel
    let addr_s = "127.0.0.1:8001";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut sender = TcpSender::new(addr);
    sender.connect();

    // Wait for receiver connections
    rec_con.join();

    // Stream table from left server
    let t_out: HashSet<_> = vec![vip_fwd_host_VM_Host ]
        .into_iter().collect();
    let mut outlet = s.add_stream(t_out);

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

    let host_val = Record::String("id1".to_string());
    let host_id = RelIdentifier::RelId(vip_fwd_host_Host as usize);

    let vm_val = Record::String("vip1".to_string());
    let vm_id = RelIdentifier::RelId(vip_fwd_host_VM as usize);


    let handle = receiver.listen();

    // Execute and transmit the update
    thread::spawn( move ||{
        loop {
            {
                let mut s = s_a.lock().unwrap();
                let updates = &[UpdCmd::Insert(host_id.clone(), host_val.clone()),
                                UpdCmd::Insert(vm_id.clone(), vm_val.clone())];
                s.on_start();
                s.on_updates(Box::new(updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap())));
                s.on_commit();

                thread::sleep(Duration::from_millis(4000));
                let updates = &[UpdCmd::Delete(host_id.clone(), host_val.clone()),
                                UpdCmd::Delete(vm_id.clone(), vm_val.clone())];
                s.on_start();
                s.on_updates(Box::new(updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap())));
                s.on_commit();
            };
            thread::sleep(Duration::from_millis(4000));
        }
    });

    //sender.lock().unwrap().disconnect();
    handle.join();

    //receiver.disconnect();

    //let mut s = s_a.lock().unwrap();
    //s.shutdown();
}
