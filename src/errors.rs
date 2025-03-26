// src/errors.rs - Renamed from error.rs to avoid confusion with log::error
use macready::error::AgentError;

/// Convert from macready's error type to anyhow's error type
pub fn to_anyhow_error(error: AgentError) -> anyhow::Error {
    anyhow::anyhow!(error.to_string())
}

/// Convert from anyhow's error type to macready's error type
pub fn to_macready_error(error: anyhow::Error) -> AgentError {
    AgentError::Other(error.to_string())
}

/// Convert from macready's Result to anyhow's Result
pub fn to_anyhow_result<T>(result: macready::error::Result<T>) -> anyhow::Result<T> {
    result.map_err(to_anyhow_error)
}

/// Convert from anyhow's Result to macready's Result
pub fn to_macready_result<T>(result: anyhow::Result<T>) -> macready::error::Result<T> {
    result.map_err(to_macready_error)
}
