use std::net::{TcpListener, TcpStream};
use std::io::prelude::*;
use std::io;

fn handle_client(mut stream: TcpStream) {
    let mut buffer = String::new();
    // TODO understand different writes
    stream.read_to_string(&mut buffer).unwrap();
    println!("{:?}", buffer);
}

fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8787")?;

    // accept connections and process them serially
    for stream in listener.incoming() {
        handle_client(stream?);
    }
    Ok(())
}
