pub use request_identification::{RequestIdentification, RequestIdentifier};
pub use sidecar_interface::{SidecarInterface, SidecarInterfaceRequest, SidecarInterfaceResponse};
mod request_identification;
mod sidecar_interface; // in sidecar/src/service/mod.rs
