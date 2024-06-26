use std::error::Error;

use http::header;
use http_body::Body;
use motore::service::Service;
use volo::context::Context;

use crate::{
    context::ClientContext,
    error::client::{status_error, ClientError},
    request::ClientRequest,
    response::ClientResponse,
};

#[derive(Clone)]
pub struct MetaService<S> {
    inner: S,
}

impl<S> MetaService<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, B> Service<ClientContext, ClientRequest<B>> for MetaService<S>
where
    S: Service<ClientContext, ClientRequest<B>, Response = ClientResponse, Error = ClientError>
        + Send
        + Sync
        + 'static,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>> + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    async fn call(
        &self,
        cx: &mut ClientContext,
        mut req: ClientRequest<B>,
    ) -> Result<Self::Response, Self::Error> {
        // `Content-Length` must be set here because the body may be changed in previous layer(s).
        let exact_len = req.body().size_hint().exact();
        if let Some(len) = exact_len {
            if len > 0 && req.headers().get(header::CONTENT_LENGTH).is_none() {
                req.headers_mut().insert(header::CONTENT_LENGTH, len.into());
            }
        }

        let stat_enabled = cx.stat_enabled();

        if stat_enabled {
            if let Some(req_size) = exact_len {
                cx.common_stats.set_req_size(req_size);
            }
        }

        tracing::trace!("sending request: {} {}", req.method(), req.uri());
        tracing::trace!("headers: {:?}", req.headers());

        let res = self.inner.call(cx, req).await;

        if stat_enabled {
            if let Ok(response) = res.as_ref() {
                cx.common_stats.set_status_code(response.status());
                if let Some(resp_size) = response.size_hint().exact() {
                    cx.common_stats.set_resp_size(resp_size);
                }
            }
        }

        if !cx.rpc_info().config().fail_on_error_status {
            return res;
        }

        let resp = res?;

        let status = resp.status();
        if status.is_client_error() || status.is_server_error() {
            Err(status_error(status))
        } else {
            Ok(resp)
        }
    }
}
