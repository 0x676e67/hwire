//! Runtime components
//!
//! This module provides traits and types that allow wreq-proto to be runtime-agnostic.
//! By abstracting over async runtimes, wreq-proto can work with different executors, timers, and IO
//! transports.
//!
//! The main runtime components are:
//!
//! - **Executors**: Traits for spawning and running futures, enabling integration with any async
//!   runtime.
//! - **Timers**: Abstractions for sleeping and scheduling tasks, allowing time-based operations to
//!   be runtime-independent.
//! - **IO Transports**: [`tokio::io::AsyncRead`] and [`tokio::io::AsyncWrite`] provide asynchronous
//!   reading and writing; `wreq-rt` provides adapters for other IO backends.
//!
//! By implementing these traits, you can customize how wreq-proto interacts with your chosen
//! runtime environment.

pub mod bounds;
mod timer;

pub use self::timer::{Sleep, Time, Timer};

/// An executor of futures.
///
/// This trait allows abstract over async runtimes. Implement this trait for your own type.
pub trait Executor<Fut> {
    /// Place the future into the executor to be run.
    fn execute(&self, fut: Fut);
}
