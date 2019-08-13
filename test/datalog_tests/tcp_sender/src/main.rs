use observe::Observer;
use tcp_channel::TcpSender;
use std::net::SocketAddr;

fn main() -> Result<(), String> {
    let addr_s = "127.0.0.1:8787";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut sender = TcpSender::new(addr);

    // We need to use the explicit method syntax to specify
    // the type parameters for Observer
    Observer::<usize, String>::on_start(&mut sender)?;
    sender.on_updates(Box::new(vec![7, 2, 3, 4].into_iter()))?;
    Observer::<usize, String>::on_commit(&mut sender)?;
    Observer::<usize, String>::on_completed(&mut sender)?;

    Ok(())
}
