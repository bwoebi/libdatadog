// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2023-Present Datadog, Inc.

use hyper::http::HeaderValue;
use hyper::{body::Buf, Body, Client, HeaderMap, Method, Request, StatusCode};
use hyper_rustls::HttpsConnectorBuilder;
use log::{error, info};
use prost::Message;
use std::collections::HashMap;
use std::{env, str};

use datadog_trace_normalization::normalizer;
use datadog_trace_protobuf::pb;

const TRACE_INTAKE_URL: &str = "https://trace.agent.datadoghq.com/api/v0.2/traces";

/// Span metric the mini agent must set for the backend to recognize top level span
const TOP_LEVEL_KEY: &str = "_top_level";
/// Span metric the tracer sets to denote a top level span
const TRACER_TOP_LEVEL_KEY: &str = "_dd.top_level";

macro_rules! parse_string_header {
    (
        $header_map:ident,
        { $($header_key:literal => $($field:ident).+ ,)+ }
    ) => {
        $(
            if let Some(header_value) = $header_map.get($header_key) {
                if let Ok(h) = header_value.to_str() {
                    $($field).+ = h;
                }
            }
        )+
    }
}

pub async fn get_traces_from_request_body(body: Body) -> anyhow::Result<Vec<Vec<pb::Span>>> {
    let buffer = hyper::body::aggregate(body).await?;

    let traces: Vec<Vec<pb::Span>> = match rmp_serde::from_read(buffer.reader()) {
        Ok(res) => res,
        Err(err) => {
            anyhow::bail!("Error deserializing trace from request body: {err}")
        }
    };

    if traces.is_empty() {
        anyhow::bail!("No traces deserialized from the request body.")
    }

    Ok(traces)
}

#[derive(Default)]
pub struct TracerHeaderTags<'a> {
    pub lang: &'a str,
    pub lang_version: &'a str,
    pub lang_interpreter: &'a str,
    pub lang_vendor: &'a str,
    pub tracer_version: &'a str,
    pub container_id: &'a str,
    // specifies that the client has marked top-level spans, when set. Any non-empty value will mean 'yes'.
    pub client_computed_top_level: bool,
    // specifies whether the client has computed stats so that the agent doesn't have to. Any non-empty value will mean 'yes'.
    pub client_computed_stats: bool,
}

// Tags gathered from a trace's root span
#[derive(Default)]
pub struct RootSpanTags<'a> {
    pub env: &'a str,
    pub app_version: &'a str,
    pub hostname: &'a str,
    pub runtime_id: &'a str,
}

pub fn get_tracer_header_tags(headers: &HeaderMap<HeaderValue>) -> TracerHeaderTags {
    let mut tags = TracerHeaderTags::default();
    parse_string_header!(
        headers,
        {
            "datadog-meta-lang" => tags.lang,
            "datadog-meta-lang-version" => tags.lang_version,
            "datadog-meta-lang-interpreter" => tags.lang_interpreter,
            "datadog-meta-lang-vendor" => tags.lang_vendor,
            "datadog-meta-tracer-version" => tags.tracer_version,
            "datadog-container-id" => tags.container_id,
        }
    );
    if headers.get("datadog-client-computed-top-level").is_some() {
        tags.client_computed_top_level = true;
    }
    if headers.get("datadog-client-computed-stats").is_some() {
        tags.client_computed_stats = true;
    }
    tags
}

pub fn construct_agent_payload(tracer_payloads: Vec<pb::TracerPayload>) -> pb::AgentPayload {
    pb::AgentPayload {
        host_name: "".to_string(),
        env: "".to_string(),
        agent_version: "".to_string(),
        error_tps: 60.0,
        target_tps: 60.0,
        tags: HashMap::new(),
        tracer_payloads,
    }
}

pub fn construct_trace_chunk(trace: Vec<pb::Span>) -> pb::TraceChunk {
    pb::TraceChunk {
        priority: normalizer::SamplerPriority::None as i32,
        origin: "".to_string(),
        spans: trace,
        tags: HashMap::new(),
        dropped_trace: false,
    }
}

pub fn construct_tracer_payload(
    chunks: Vec<pb::TraceChunk>,
    tracer_tags: TracerHeaderTags,
    root_span_tags: RootSpanTags,
) -> pb::TracerPayload {
    pb::TracerPayload {
        app_version: root_span_tags.app_version.to_string(),
        language_name: tracer_tags.lang.to_string(),
        container_id: tracer_tags.container_id.to_string(),
        env: root_span_tags.env.to_string(),
        runtime_id: root_span_tags.runtime_id.to_string(),
        chunks,
        hostname: root_span_tags.hostname.to_string(),
        language_version: tracer_tags.lang_version.to_string(),
        tags: HashMap::new(),
        tracer_version: tracer_tags.tracer_version.to_string(),
    }
}

pub fn serialize_agent_payload(payload: pb::AgentPayload) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::new();
    buf.reserve(payload.encoded_len());
    payload.encode(&mut buf)?;
    Ok(buf)
}

pub async fn send(data: Vec<u8>) -> anyhow::Result<()> {
    let api_key = match env::var("DD_API_KEY") {
        Ok(key) => key,
        Err(_) => anyhow::bail!("oopsy, no DD_API_KEY was provided"),
    };

    let req = Request::builder()
        .method(Method::POST)
        .uri(TRACE_INTAKE_URL)
        .header("Content-type", "application/x-protobuf")
        .header("DD-API-KEY", &api_key)
        .body(Body::from(data))?;

    let https = HttpsConnectorBuilder::new()
        .with_native_roots()
        .https_only()
        .enable_http1()
        .build();
    let client: Client<_, hyper::Body> = Client::builder().build(https);
    match client.request(req).await {
        Ok(response) => {
            if response.status() != StatusCode::ACCEPTED {
                let body_bytes = hyper::body::to_bytes(response.into_body()).await?;
                let response_body = String::from_utf8(body_bytes.to_vec()).unwrap_or_default();
                anyhow::bail!("Server did not accept traces: {}", response_body);
            }
            Ok(())
        }
        Err(e) => anyhow::bail!("Failed to send traces: {e}"),
    }
}

pub fn get_root_span_index(trace: &Vec<pb::Span>) -> anyhow::Result<usize> {
    if trace.is_empty() {
        anyhow::bail!("Cannot find root span index in an empty trace.");
    }

    // parent_id -> (child_span, index_of_child_span_in_trace)
    let mut parent_id_to_child_map: HashMap<u64, (&pb::Span, usize)> = HashMap::new();

    // look for the span with parent_id == 0 (starting from the end) since some clients put the root span last.
    for i in (0..trace.len()).rev() {
        let cur_span = &trace[i];
        if cur_span.parent_id == 0 {
            return Ok(i);
        }
        parent_id_to_child_map.insert(cur_span.parent_id, (cur_span, i));
    }

    for span in trace {
        if parent_id_to_child_map.contains_key(&span.span_id) {
            parent_id_to_child_map.remove(&span.span_id);
        }
    }

    // if the trace is valid, parent_id_to_child_map should just have 1 entry at this point.
    if parent_id_to_child_map.len() != 1 {
        error!(
            "Could not find the root span for trace with trace_id: {}",
            &trace[0].trace_id,
        );
    }

    // pick a span without a parent
    let span_tuple = match parent_id_to_child_map.values().copied().next() {
        Some(res) => res,
        None => {
            // just return the index of the last span in the trace.
            info!("Returning index of last span in trace as root span index.");
            return Ok(trace.len() - 1);
        }
    };

    Ok(span_tuple.1)
}

/// Updates all the spans top-level attribute.
/// A span is considered top-level if:
///   - it's a root span
///   - OR its parent is unknown (other part of the code, distributed trace)
///   - OR its parent belongs to another service (in that case it's a "local root" being the highest
///     ancestor of other spans belonging to this service and attached to it).
pub fn compute_top_level_span(trace: &mut [pb::Span]) {
    let mut span_id_to_service: HashMap<u64, String> = HashMap::new();
    for span in trace.iter() {
        span_id_to_service.insert(span.span_id, span.service.clone());
    }
    for span in trace.iter_mut() {
        if span.parent_id == 0 {
            set_top_level_span(span, true);
            continue;
        }
        match span_id_to_service.get(&span.parent_id) {
            Some(parent_span_service) => {
                if !parent_span_service.eq(&span.service) {
                    // parent is not in the same service
                    set_top_level_span(span, true)
                }
            }
            None => {
                // span has no parent in chunk
                set_top_level_span(span, true)
            }
        }
    }
}

fn set_top_level_span(span: &mut pb::Span, is_top_level: bool) {
    if !is_top_level {
        if span.metrics.contains_key(TOP_LEVEL_KEY) {
            span.metrics.remove(TOP_LEVEL_KEY);
        }
        return;
    }
    span.metrics.insert(TOP_LEVEL_KEY.to_string(), 1.0);
}

pub fn set_serverless_root_span_tags(span: &mut pb::Span) {
    span.r#type = "serverless".to_string();
    span.meta
        .insert("_dd.origin".to_string(), "gcp_function".to_string());
    span.meta.insert(
        "functionname".to_string(),
        env::var("K_SERVICE").unwrap_or_default(),
    );
}

pub fn update_tracer_top_level(span: &mut pb::Span) {
    if span.metrics.contains_key(TRACER_TOP_LEVEL_KEY) {
        span.metrics.insert(TOP_LEVEL_KEY.to_string(), 1.0);
    }
}

#[cfg(test)]
mod tests {
    use hyper::Request;
    use serde_json::json;
    use std::collections::HashMap;

    use super::get_root_span_index;
    use crate::trace_utils;
    use datadog_trace_protobuf::pb;

    #[tokio::test]
    async fn test_get_traces_from_request_body() {
        let pairs = vec![
            (
                json!([{
                    "service": "test-service",
                    "name": "test-service-name",
                    "resource": "test-service-resource",
                    "trace_id": 111,
                    "span_id": 222,
                    "parent_id": 333,
                    "start": 1,
                    "duration": 5,
                    "error": 0,
                    "meta": {},
                    "metrics": {},
                }]),
                vec![vec![pb::Span {
                    service: "test-service".to_string(),
                    name: "test-service-name".to_string(),
                    resource: "test-service-resource".to_string(),
                    trace_id: 111,
                    span_id: 222,
                    parent_id: 333,
                    start: 1,
                    duration: 5,
                    error: 0,
                    meta: HashMap::new(),
                    metrics: HashMap::new(),
                    meta_struct: HashMap::new(),
                    r#type: "".to_string(),
                }]],
            ),
            (
                json!([{
                    "name": "test-service-name",
                    "resource": "test-service-resource",
                    "trace_id": 111,
                    "span_id": 222,
                    "start": 1,
                    "duration": 5,
                    "meta": {},
                }]),
                vec![vec![pb::Span {
                    service: "".to_string(),
                    name: "test-service-name".to_string(),
                    resource: "test-service-resource".to_string(),
                    trace_id: 111,
                    span_id: 222,
                    parent_id: 0,
                    start: 1,
                    duration: 5,
                    error: 0,
                    meta: HashMap::new(),
                    metrics: HashMap::new(),
                    meta_struct: HashMap::new(),
                    r#type: "".to_string(),
                }]],
            ),
        ];

        for (trace_input, output) in pairs {
            let bytes = rmp_serde::to_vec(&vec![&trace_input]).unwrap();
            let request = Request::builder()
                .body(hyper::body::Body::from(bytes))
                .unwrap();
            let res = trace_utils::get_traces_from_request_body(request.into_body()).await;
            assert!(res.is_ok());
            assert_eq!(res.unwrap(), output);
        }
    }

    fn create_test_span(trace_id: u64, span_id: u64, parent_id: u64) -> pb::Span {
        pb::Span {
            trace_id,
            span_id,
            service: "service".to_string(),
            name: "name".to_string(),
            resource: "".to_string(),
            parent_id,
            start: 0,
            duration: 5,
            error: 0,
            meta: HashMap::new(),
            metrics: HashMap::new(),
            r#type: "".to_string(),
            meta_struct: HashMap::new(),
        }
    }

    #[test]
    fn test_get_root_span_index_from_complete_trace() {
        let trace = vec![
            create_test_span(1234, 12341, 0),
            create_test_span(1234, 12342, 12341),
            create_test_span(1234, 12343, 12342),
        ];

        let root_span_index = get_root_span_index(&trace);
        assert!(root_span_index.is_ok());
        assert_eq!(root_span_index.unwrap(), 0);
    }

    #[test]
    fn test_get_root_span_index_from_partial_trace() {
        let trace = vec![
            create_test_span(1234, 12342, 12341),
            create_test_span(1234, 12341, 12340), // this is the root span, it's parent is not in the trace
            create_test_span(1234, 12343, 12342),
        ];

        let root_span_index = get_root_span_index(&trace);
        assert!(root_span_index.is_ok());
        assert_eq!(root_span_index.unwrap(), 1);
    }
}
