use super::{RequestIdentification, RequestIdentifier, RuntimeMetadata};
use crate::interface::{
    InstanceId, QueueId, SerializedTracerHeaderTags, SessionConfig, SidecarAction,
};
use anyhow::Result;
use datadog_ipc::platform::ShmHandle;
use datadog_ipc::tarpc;

#[datadog_sidecar_macros::extract_request_id]
#[datadog_ipc_macros::impl_transfer_handles]
#[tarpc::service]
pub trait SidecarInterface {
    async fn enqueue_actions(
        instance_id: InstanceId,
        queue_id: QueueId,
        actions: Vec<SidecarAction>,
    );
    async fn register_service_and_flush_queued_actions(
        instance_id: InstanceId,
        queue_id: QueueId,
        meta: RuntimeMetadata,
        service_name: String,
        env_name: String,
    );
    async fn set_session_config(session_id: String, config: SessionConfig);
    async fn shutdown_runtime(instance_id: InstanceId);
    async fn shutdown_session(session_id: String);
    async fn send_trace_v04_shm(
        instance_id: InstanceId,
        #[SerializedHandle] handle: ShmHandle,
        headers: SerializedTracerHeaderTags,
    );
    async fn send_trace_v04_bytes(
        instance_id: InstanceId,
        data: Vec<u8>,
        headers: SerializedTracerHeaderTags,
    );
    async fn ping();
    async fn dump() -> String;
}
