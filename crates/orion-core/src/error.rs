use thiserror::Error;

pub type Result<T> = std::result::Result<T, OrionError>;

#[derive(Debug, Error)]
pub enum OrionError {
    #[error("object not found: {0}")]
    NotFound(String),

    #[error("bucket not found: {0}")]
    BucketNotFound(String),

    #[error("bucket already exists: {0}")]
    BucketAlreadyExists(String),

    #[error("object already exists: {0}")]
    ObjectAlreadyExists(String),

    #[error("insufficient storage space")]
    NoSpace,

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    #[error("metadata error: {0}")]
    Metadata(String),

    #[error("extension error ({extension}): {message}")]
    Extension {
        extension: String,
        message: String,
    },

    #[error("authentication failed: {0}")]
    AuthFailed(String),

    #[error("access denied: {0}")]
    AccessDenied(String),

    #[error("request blocked by extension hook")]
    HookDenied,

    #[error("internal error: {0}")]
    Internal(String),
}

impl OrionError {
    /// Map to an S3-compatible error code.
    pub fn s3_error_code(&self) -> &'static str {
        match self {
            OrionError::NotFound(_) => "NoSuchKey",
            OrionError::BucketNotFound(_) => "NoSuchBucket",
            OrionError::BucketAlreadyExists(_) => "BucketAlreadyExists",
            OrionError::NoSpace => "InsufficientStorage",
            OrionError::InvalidArgument(_) => "InvalidArgument",
            OrionError::ChecksumMismatch { .. } => "BadDigest",
            OrionError::AuthFailed(_) => "InvalidAccessKeyId",
            OrionError::AccessDenied(_) => "AccessDenied",
            OrionError::HookDenied => "AccessDenied",
            _ => "InternalError",
        }
    }

    /// HTTP status code for S3 error responses.
    pub fn http_status(&self) -> u16 {
        match self {
            OrionError::NotFound(_) | OrionError::BucketNotFound(_) => 404,
            OrionError::BucketAlreadyExists(_) | OrionError::ObjectAlreadyExists(_) => 409,
            OrionError::NoSpace => 507,
            OrionError::InvalidArgument(_) | OrionError::ChecksumMismatch { .. } => 400,
            OrionError::AuthFailed(_) => 401,
            OrionError::AccessDenied(_) | OrionError::HookDenied => 403,
            _ => 500,
        }
    }
}
