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
    // Listen to updates from Up and Down server
    let addr_s = "127.0.0.1:8010";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut r1 = TcpReceiver::new(addr);
    r1.connect().join();

    let addr_s = "127.0.0.1:8020";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut r2 = TcpReceiver::new(addr);
    r2.connect().join();

    // Construct right server, redirect tables
    let prog2 = HDDlog::run(1, false, |_,_:&Record, _| {});
    let mut redirect2 = HashMap::new();
    redirect2.insert(lr_up_Right as usize, lr_right_Up as usize);
    redirect2.insert(lr_down_Right as usize, lr_right_Down as usize);
    let s2 = server::DDlogServer::new(prog2, redirect2);

    // Right server subscribes to the streams
    let s2 = Arc::new(Mutex::new(s2));
    let sub1 = {
        let s2_a = server::ADDlogServer(s2.clone());
        r1.subscribe(Box::new(s2_a))
    };
    let sub2 = {
        let s2_a = server::ADDlogServer(s2.clone());
        r2.subscribe(Box::new(s2_a))
    };

    // Listen for updates
    let h1 = r1.listen();
    let h2 = r2.listen();
    h1.join();
    h2.join();

    // Shutdown server
    s2.lock().unwrap().shutdown()?;
    Ok(())
}

