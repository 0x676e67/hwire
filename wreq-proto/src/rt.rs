//! Runtime components
//!
//! The traits and types within this module are used to allow plugging in
//! runtime types. These include:
//!
//! - Executors
//! - Timers
//! - IO transports

pub mod bounds;

pub(crate) use wreq_rt::{
    timer::{Sleep, Time, Timer},
    Executor,
};
