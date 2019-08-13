use observe::{Observer, Observable};
use tcp_channel::TcpReceiver;

use std::io;
use std::net::SocketAddr;

struct TestObserver;

impl Observer<usize, String> for TestObserver {
    fn on_start(&mut self) -> Result<(), String> {Ok(())}
    fn on_updates<'a>(&mut self,
                      updates: Box<dyn Iterator<Item = usize> + 'a>)
                      -> Result<(), String> {
        for upd in updates {
            println!("{:?}", upd + 6);
        }
        Ok(())
    }
    fn on_commit(&mut self) -> Result<(), String> {Ok(())}
    fn on_completed(&mut self) -> Result<(), String> {Ok(())}
    fn on_error(&self, _error: String) {}
}

fn main() -> io::Result<()> {
    let addr_s = "127.0.0.1:8787";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut r = TcpReceiver::new(addr);

    let sub = r.subscribe(Box::new(TestObserver));

    let h1 = r.listen();
    h1.join().unwrap();

    sub.unsubscribe();

    let h2 = r.listen();
    h2.join().unwrap();
    Ok(())
}
