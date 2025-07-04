use std::net::{Ipv4Addr, SocketAddr};

use env::MockUtpEnvironment;
use transport::MockUtpTransport;

use crate::{UtpSocket, UtpStream, socket::Dispatcher};

pub mod cmphead;
pub mod env;
pub mod transport;

pub type MockUtpSocket = UtpSocket<MockUtpTransport, MockUtpEnvironment>;
pub type MockUtpStream = UtpStream;
pub type MockDispatcher = Dispatcher<MockUtpTransport, MockUtpEnvironment>;

pub fn setup_test_logging() {
    unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    if std::env::var("RUST_LOG").is_err() {
        unsafe { std::env::set_var("RUST_LOG", "trace") };
    }
    let _ = tracing_subscriber::fmt::try_init();
}

pub const ADDR_1: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 1);
pub const ADDR_2: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 2);
