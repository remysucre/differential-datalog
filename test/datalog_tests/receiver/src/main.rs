use differential_datalog::program::{RelId, Response};
use ddd_ddlog::*;
use observe::{Observer, Observable};
use tcp_channel::TcpReceiver;

use std::net::SocketAddr;

struct TestObserver;

impl Observer<(RelId, Value, bool), String> for TestObserver {
    fn on_start(&mut self) -> Result<(), String> {Ok(())}
    fn on_updates<'a>(&mut self,
                      updates: Box<dyn Iterator<Item = (RelId, Value, bool)> + 'a>)
                      -> Response<()> {
        for upd in updates {
            println!("{:?}", upd);
        }
        Ok(())
    }
    fn on_next(&mut self, item: (RelId, Value, bool)) -> Result<(), String> {
        println!("{:?}", item);
        Ok(())
    }
    fn on_commit(&mut self) -> Result<(), String> {Ok(())}
    fn on_completed(&mut self) -> Result<(), String> {Ok(())}
    fn on_error(&self, _error: String) {}
}

fn main() -> Result<(), String> {
    let addr_s = "127.0.0.1:8787";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut r = TcpReceiver::new(addr);

    let sub = r.subscribe(Box::new(TestObserver));
    r.listen()?;
    sub.unsubscribe();
    r.listen()?;

    Ok(())
}
