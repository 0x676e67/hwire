//! Runtime components
//!
//! This crate exposes runtime capabilities and concrete runtime adapters.
//! The capability traits and shared types live in [`runtime`], while concrete
//! integrations such as [`tokio`] and [`compio`] implement them.
//!
//! - Task execution
//! - Resolvers
//! - Connectors
//! - Timers
//! - IO transports

#![deny(unused)]
#![deny(unsafe_code)]
#![deny(missing_docs)]
#![cfg_attr(test, deny(warnings))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod conn;
pub mod dns;
pub mod timer;

#[cfg(feature = "compio-rt")]
pub mod compio;

#[cfg(feature = "tokio-rt")]
pub mod tokio;

use std::error::Error as StdError;

/// Alias for a boxed error type that is `Send` and `Sync`.
pub type BoxError = Box<dyn StdError + Send + Sync>;

/// A task executor.
///
/// This trait abstracts over runtimes that can execute futures.
pub trait Executor<Fut> {
    /// Place the future into the executor to be run.
    fn execute(&self, fut: Fut);
}
