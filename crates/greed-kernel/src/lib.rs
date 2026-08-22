//! Greenfield strategy kernel.
//!
//! This crate deliberately contains no exchange, filesystem, clock or broker
//! implementation.  A replay and a live paper process therefore execute the
//! exact same graph against the same point-in-time [`MarketFrame`].

pub mod artifact;
pub mod contracts;
pub mod graph;

pub use artifact::*;
pub use contracts::*;
pub use graph::*;
