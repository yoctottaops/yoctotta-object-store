use http::Response;
use http_body_util::Full;
use bytes::Bytes;
use uuid::Uuid;

use orion_core::OrionError;
use crate::xml;

pub fn error_response(err: &OrionError, resource: &str) -> Response<Full<Bytes>> {
    let request_id = Uuid::new_v4().to_string();
    let body = xml::error_xml(err.s3_error_code(), &err.to_string(), resource, &request_id);

    Response::builder()
        .status(err.http_status())
        .header("Content-Type", "application/xml")
        .header("x-amz-request-id", &request_id)
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

pub fn not_implemented(operation: &str) -> Response<Full<Bytes>> {
    let request_id = Uuid::new_v4().to_string();
    let body = xml::error_xml(
        "NotImplemented",
        &format!("Operation not implemented: {}", operation),
        "",
        &request_id,
    );

    Response::builder()
        .status(501)
        .header("Content-Type", "application/xml")
        .header("x-amz-request-id", &request_id)
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}
