//! Runtime capabilities and shared types.

use std::{future::Future, net::SocketAddr, pin::Pin};

use super::BoxError;

/// Alias for an `Iterator` trait object over `SocketAddr`.
pub type Addrs = Box<dyn Iterator<Item = SocketAddr> + Send>;

/// Alias for the `Future` type returned by a DNS resolver.
pub type Resolving = Pin<Box<dyn Future<Output = Result<Addrs, BoxError>> + Send>>;

/// A DNS resolver.
pub trait DnsResolver {
    /// Performs a DNS resolution.
    fn resolve(&self, name: Box<str>) -> Resolving;
}
