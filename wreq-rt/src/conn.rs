//! Runtime capabilities and shared types.

use std::{
    fmt::Debug,
    future::Future,
    net::{SocketAddr, TcpStream},
    pin::Pin,
};
#[cfg(unix)]
use std::{path::Path, sync::Arc};

use ::tokio::io::{AsyncRead, AsyncWrite};

use super::BoxError;

/// A trait for inspecting connection socket state and options.
pub trait ConnectionSocket {
    /// Returns the remote address that this stream is connected to.
    fn peer_addr(&self) -> Option<SocketAddr>;

    /// Returns the local address that this stream is bound to.
    fn local_addr(&self) -> Option<SocketAddr>;

    /// Set the value of the `TCP_NODELAY` option on the underlying TCP stream.
    /// For non-TCP streams (e.g. Unix sockets) the default implementation is a no-op.
    fn set_nodelay(&self, nodelay: bool) -> Result<(), BoxError>;
}

/// A marker trait for connection streams that implement `AsyncRead`, `AsyncWrite`, and socket
/// access.
pub trait Connection: AsyncRead + AsyncWrite + ConnectionSocket + Debug {}

/// A boxed connection stream.
pub type BoxConnection = Box<dyn Connection + Send + Sync + Unpin + 'static>;

/// The future returned by a connection attempt.
pub type Connecting = Pin<Box<dyn Future<Output = Result<BoxConnection, BoxError>> + Send>>;

/// Connect to a destination, returning an IO transport.
pub trait Connect {
    /// Establish a TCP connection from the given socket to the address.
    fn tcp_connect(&self, socket: TcpStream, addr: SocketAddr) -> Connecting;

    /// Establish a Unix domain socket connection to the given path.
    #[cfg(unix)]
    fn unix_connect(&self, path: Arc<Path>) -> Connecting;
}

impl<T: AsyncRead + AsyncWrite + ConnectionSocket + Debug> Connection for T {}
