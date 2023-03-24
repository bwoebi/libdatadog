use std::collections::HashMap;
use std::error::Error;
use std::fs::OpenOptions;
use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use std::process;
use std::task::Poll;
use std::time::{Duration, SystemTime};

use datadog_trace_protobuf::pb::{AgentPayload, TracerPayload};
use datadog_trace_protobuf::prost::Message;
use ddcommon::HttpClient;
use hyper::service::Service;
use hyper::{Body, Method, Request, Response, Server, StatusCode};

use tokio::net::UnixListener;
use tokio::sync::mpsc::Sender;

use crate::connections::UnixListenerTracked;
use crate::data::v04::{self};

// Example traced app: go install github.com/DataDog/trace-examples/go/heartbeat@latest
#[derive(Debug, Clone)]
struct V04Handler {
    builder: v04::AssemblerBuilder,
    payload_sender: Sender<TracerPayload>,
}

impl V04Handler {
    fn new(tx: Sender<TracerPayload>) -> Self {
        Self {
            builder: Default::default(),
            payload_sender: tx,
        }
    }
}

#[derive(Debug)]
struct MiniAgent {
    v04_handler: V04Handler,
}

impl MiniAgent {
    fn new(tx: Sender<TracerPayload>) -> Self {
        Self {
            v04_handler: V04Handler::new(tx),
        }
    }
}

impl Service<Request<Body>> for MiniAgent {
    type Response = Response<Body>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open("/tmp/mini-agent-logs.txt")
            .unwrap();

        match (req.method(), req.uri().path()) {
            // exit, shutting down the subprocess process.
            (&Method::GET, "/exit") => {
                println!("/exit called. shutting down.");
                writeln!(f, "/exit called. shutting down.").unwrap();
                std::process::exit(0);
            }
            // node.js does put while Go does POST whoa
            (&Method::POST | &Method::PUT, "/v0.4/traces") => {
                println!("POST or PUT received at /v0.4/traces");
                writeln!(f, "POST or PUT received at /v0.4/traces").unwrap();
                let handler = self.v04_handler.clone();
                Box::pin(async move { handler.handle(req).await })
            }

            // Return the 404 Not Found for other routes.
            _ => Box::pin(async move {
                println!("404 not found being returned.");
                writeln!(f, "404 not found being returned.").unwrap();
                let mut not_found = Response::default();
                *not_found.status_mut() = StatusCode::NOT_FOUND;
                Ok(not_found)
            }),
        }
    }
}

impl V04Handler {
    async fn handle(&self, mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open("/tmp/mini-agent-logs.txt")
            .unwrap();

        println!("handling recently received request.");
        writeln!(f, "handling recently received request.").unwrap();
        let body = match hyper::body::to_bytes(req.body_mut()).await {
            Ok(res) => res,
            Err(e) => {
                println!("error consuming request body into bytes. Err: {}", e);
                writeln!(f, "error consuming request body into bytes. Err: {}", e).unwrap();
                panic!("error consuming request body into bytes. Err: {}", e);
            }
        };
        println!("consumed request body into bytes.");
        let src: v04::Payload = match rmp_serde::from_slice(&body) {
            Ok(res) => res,
            Err(e) => {
                println!("error processing bytes into v04::Payload. Err: {}", e);
                writeln!(f, "error processing bytes into v04::Payload. Err: {}", e).unwrap();
                panic!("error processing bytes into v04::Payload. Err: {}", e);
            }
        };
        println!("processed bytes into v04::Payload");
        let payload = self
            .builder
            .with_headers(req.headers())
            .assemble_payload(src);
        
        println!("tracer payload assembled.");

        self.payload_sender.send(payload).await?;

        println!("tracer payload sent to backend trace intake.");
        writeln!(f, "handling recently received request.").unwrap();

        Ok(Response::default())
    }
}

struct MiniAgentSpawner {
    payload_sender: Sender<TracerPayload>,
}

impl<'t, Target> Service<&'t Target> for MiniAgentSpawner {
    type Response = MiniAgent;
    type Error = Box<dyn Error + Send + Sync>;

    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: &'t Target) -> Self::Future {
        let agent = MiniAgent::new(self.payload_sender.clone());

        Box::pin(async { Ok(agent) })
    }
}

struct Uploader {
    tracing_config: crate::config::TracingConfig,
    system_info: crate::config::SystemInfo,
    client: HttpClient,
}

impl Uploader {
    fn init(cfg: &crate::config::Config) -> Self {
        let client = hyper::Client::builder()
            .pool_idle_timeout(Duration::from_secs(30))
            .build(ddcommon::connector::Connector::new());

        Self {
            tracing_config: cfg.tracing_config(),
            system_info: cfg.system_info(),
            client,
        }
    }

    pub async fn submit(&self, mut payloads: Vec<TracerPayload>) -> anyhow::Result<()> {
        let req = match self.tracing_config.protocol {
            crate::config::TracingProtocol::BackendProtobufV01 => {
                let mut tags = HashMap::new();
                tags.insert("some_tag".into(), "value".into());

                for head_span in payloads
                    .iter_mut()
                    .flat_map(|f| f.chunks.iter_mut().flat_map(|t| t.spans.first_mut()))
                {
                    head_span.metrics.insert("_dd.agent_psr".into(), 1.0);
                    head_span.metrics.insert("_sample_rate".into(), 1.0);
                    head_span
                        .metrics
                        .insert("_sampling_priority_v1".into(), 1.0);
                    head_span.metrics.insert("_top_level".into(), 1.0);
                }

                let payload = AgentPayload {
                    host_name: self.system_info.hostname.clone(),
                    env: self.system_info.env.clone(),
                    tracer_payloads: payloads,
                    tags, //TODO: parse DD_TAGS
                    agent_version: "libdatadog".into(),
                    target_tps: 60.0,
                    error_tps: 60.0,
                };

                let mut req_builder = Request::builder()
                    .method(Method::POST)
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Datadog-Reported-Languages", "rust,TODO")
                    .uri(&self.tracing_config.url);

                for (key, value) in &self.tracing_config.http_headers {
                    req_builder = req_builder.header(key, value);
                }
                let data = payload.encode_to_vec();

                req_builder.body(data.into())?
            }
            crate::config::TracingProtocol::AgentV04 => {
                let data: Vec<v04::Trace> = payloads
                    .iter()
                    .flat_map(|p| p.chunks.iter().map(|c| c.into()))
                    .collect();
                let data = v04::Payload { traces: data };
                let data = serde_json::to_vec(&data)?;

                // TODO: fix msgpack serialization
                // let data = rmp_serde::to_vec(&data)?;

                let mut req_builder = Request::builder()
                    .method(Method::POST)
                    .header("Content-Type", "application/json")
                    .uri(&self.tracing_config.url);

                for (key, value) in &self.tracing_config.http_headers {
                    req_builder = req_builder.header(key, value);
                }
                req_builder.body(data.into())?
            }
        };

        let mut resp = self.client.request(req).await?;
        let _data = hyper::body::to_bytes(resp.body_mut()).await?;
        Ok(())
    }
}

pub(crate) async fn main(listener: UnixListener) -> anyhow::Result<()> {
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open("/tmp/mini-agent-logs.txt")
        .unwrap();

    writeln!(f, "in mini_agent main|").unwrap();

    // println!("in mini_agent main");
    let (tx, mut rx) = tokio::sync::mpsc::channel::<TracerPayload>(1);
    let uploader = Uploader::init(&crate::config::Config::init());
    tokio::spawn(async move {
        let mut payloads = vec![];
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        loop {
            tokio::select! {
                // if there are no connections for 1 second, exit the main loop
                Some(d) = rx.recv() => {
                    println!("rx.recv has new item. pushing into payloads buffer.");
                    writeln!(f, "rx.recv has new item. pushing into payloads buffer.|").unwrap();
                    payloads.push(d);
                }

                _ = interval.tick() => {
                    if payloads.is_empty() {
                        continue
                    }
                    match uploader.submit(payloads.drain(..).collect()).await {
                        Ok(()) => {
                            println!("sending trace to trace intake.");
                            writeln!(f, "sending trace to trace intake.|").unwrap();
                        },
                        Err(e) => {eprintln!("{:?}", e)}
                    }
                }
            }
        }
    });

    println!("mini agent PID: {}", process::id());

    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open("/tmp/mini-agent-logs.txt")
        .unwrap();

    writeln!(f, "mini agent PID: {}|", process::id()).unwrap();
    writeln!(f, "timestamp: {}|", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis()).unwrap();

    let listener = UnixListenerTracked::from(listener);
    let watcher = listener.watch();
    let server = Server::builder(listener).serve(MiniAgentSpawner { payload_sender: tx });
    tokio::select! {
        // if there are no connections for 5 seconds, exit the main loop
        _ = watcher.wait_for_no_instances(Duration::from_secs(1)) => {
            println!("no connections for 5 seconds. Exiting main loop.");
            Ok(())
        }
        res = server => {
            res?;
            Ok(())
        }
    }
}
