#![allow(non_snake_case, non_camel_case_types, non_upper_case_globals)]
#![doc = include_str!("../README.md")]

// Raw C bindings remain available alongside the owning Rust wrappers below.
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

mod file;
pub use file::{File, Open3fsFile};

mod error;
pub use error::{Error, Result};

mod ior;
pub use ior::Ior;

mod iov;
pub use iov::Iov;

mod ring;
pub use ring::{Ring, RingConfig};

mod job;
pub use job::{ReadJob, ReadResult, WriteJob};

mod worker;
pub use worker::{BatchReadJobs, Callback, ReadWorker, SendableReadJob};

mod workers;
pub use workers::ReadWorkers;
