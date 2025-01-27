use std::{net::SocketAddr, sync::Arc};

use env::MockUtpEnvironment;
use transport::MockUtpTransport;

use crate::{UtpSocket, UtpStream};

pub mod env;
pub mod transport;

pub type MockUtpSocket = UtpSocket<MockUtpTransport, MockUtpEnvironment>;
pub type MockUtpStream = UtpStream<MockUtpTransport, MockUtpEnvironment>;

pub fn setup_test_logging() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "trace");
    }
    let _ = tracing_subscriber::fmt::try_init();
}
