//! Raw PIR runtime integration that more than one serving library repeated.
//!
//! Three separate APIs, so a product can keep each stage on the process that
//! owns it (an Enhance worker or a Status worker evaluates; a packing router
//! or coordinator packs; a Transparent worker does both locally):
//!
//! - [`params`]: the pinned SimplePIR profile parameters;
//! - [`evaluation`]: opaque `PreparedDatabase` handles, loading their stored
//!   coefficients and combining evaluation contributions;
//! - [`packing`]: opaque `PackingSetup`, `PreparedPacking` and `PackingKeys`
//!   handles; preparing hint blocks, validating query keys and packing an
//!   intermediate into serialized response bodies. Products never borrow raw
//!   top images or preprocessed blocks from these handles. Mapped artifacts
//!   retain the upstream opaque mapping handle and unchanged file format.
//!
//! Everything here calls the upstream cryptography (ipir-sp, inspiring)
//! unchanged: no parameter, algorithm or byte format is chosen here. Record
//! layouts, wire framing, session validation, setup seeds and the profile a
//! product selects remain in the product's serving library. The crate reaches
//! no chain reader, ingestion or product package (`tools/ci/boundary.py`).

pub mod evaluation;
pub mod packing;
pub mod params;
