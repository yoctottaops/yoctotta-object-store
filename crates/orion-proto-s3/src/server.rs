use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use orion_ext_auth::AuthProvider;
use tokio::net::TcpListener;

use orion_core::*;

use crate::handler::S3Handler;
use crate::metrics::{MetricsCollector, MetricsConfig};

pub struct S3Server {
    handler: Arc<S3Handler>,
    addr: SocketAddr,
}

impl S3Server {
    pub fn new(
        store: Arc<dyn StorageBackend>,
        buckets: Arc<dyn BucketManager>,
        meta: Arc<dyn MetadataStore>,
        extensions: Arc<ExtensionRegistry>,
        addr: SocketAddr,
        metrics_config: MetricsConfig,
        auth: Option<Arc<AuthProvider>>,
        data_dir: &Path,
    ) -> Self {
        let metrics = MetricsCollector::new(metrics_config, data_dir);
        let handler = Arc::new(S3Handler {
            store,
            buckets,
            meta,
            extensions,
            metrics,
            auth,
        });
        Self { handler, addr }
    }

    pub async fn run(&self) -> orion_core::Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        tracing::info!(addr = %self.addr, "S3 server listening");

        loop {
            let (stream, peer) = listener.accept().await?;
            let handler = self.handler.clone();

            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req| {
                    let handler = handler.clone();
                    async move {
                        let resp = handler.handle(req).await;
                        Ok::<_, hyper::Error>(resp)
                    }
                });

                if let Err(e) = http1::Builder::new()
                    .keep_alive(true)
                    .serve_connection(io, svc)
                    .await
                {
                    if !e.is_incomplete_message() {
                        tracing::debug!(peer = %peer, error = %e, "Connection error");
                    }
                }
            });
        }
    }
}
