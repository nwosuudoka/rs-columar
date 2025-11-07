use crate::encoding::{StreamingEncoder, strings::doc_writer::DocWriter};
use fastbloom::BloomFilter;
use std::cell::RefCell;
use std::io;
use xxhash_rust::xxh3;
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};
