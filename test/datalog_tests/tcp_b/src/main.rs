use differential_datalog::program::{RelId, Update, Response};
use differential_datalog::record::{Record, UpdCmd, RelIdentifier};
use observe::{Observer, Observable, Subscription};
use tcp_channel::{TcpReceiver, ATcpSender, TcpSender};

use roundtrip_ddlog::*;
use roundtrip_ddlog::api::*;
use roundtrip_ddlog::Relations::*;

use std::collections::{HashSet, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

fn main() -> Result<(), String> {
    // Read from this port
    let addr_s = "127.0.0.1:8001";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut receiver = TcpReceiver::new(addr);

    let rec_con = receiver.connect();

    // Write to this port
    let addr_s = "127.0.0.1:8002";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut sender = TcpSender::new(addr);

    sender.connect();

    let sender = Arc::new(Mutex::new(sender));
    let a_sender = ATcpSender(sender.clone());

    rec_con.join();

    // Construct up server, redirect input table
    let prog = HDDlog::run(1, false, |_,_:&Record, _| {});
    let mut redirect = HashMap::new();
    redirect.insert(rt_a_ToB as usize, rt_b_In as usize);
    let mut s = server::DDlogServer::new(prog, redirect);

    // Stream right table from up server
    let mut table = HashSet::new();
    table.insert(rt_b_Out as usize);
    let mut outlet = s.add_stream(table);

    // Downstream TCP channel subscribes to the server
    let _sub2 = {
        outlet.subscribe(Box::new(a_sender))
    };

    // Server subscribes to the upstream TCP channel
    let s = Arc::new(Mutex::new(s));
    let _sub1 = {
        let s_a = server::ADDlogServer(s.clone());
        receiver.subscribe(Box::new(s_a))
    };

    // Listen for updates on the upstream channel
    let handle = receiver.listen();

    handle.join();

    //sender.lock().unwrap().disconnect();

    // Shutdown server
    s.lock().unwrap().shutdown()?;
    Ok(())
}

