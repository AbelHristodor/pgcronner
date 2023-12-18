use pyo3::exceptions::{PyOSError, PyValueError};
use pyo3::prelude::*;
use std::fmt;

/// ValidationError
///
/// A ValidationError is raised when a validation error occurs
///
/// # Arguments
/// * `message` - Error message
///
#[derive(Debug, Clone)]
pub struct ValidationError {
    message: String,
}

impl ValidationError {
    pub fn new(message: String) -> Self {
        Self { message }
    }
}

impl std::error::Error for ValidationError {}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ValidationError: {}", self.message)
    }
}

impl std::convert::From<ValidationError> for PyErr {
    fn from(error: ValidationError) -> PyErr {
        PyValueError::new_err(error.message)
    }
}

impl From<String> for ValidationError {
    fn from(error: String) -> ValidationError {
        ValidationError { message: error }
    }
}

impl From<ValidationError> for String {
    fn from(error: ValidationError) -> String {
        error.message
    }
}

/// DbError
///
/// A DbError is raised when a database error occurs
///
/// # Arguments
/// * `message` - Error message
///
#[derive(Debug, Clone)]
pub struct DbError {
    message: String,
}

impl std::error::Error for DbError {}

impl DbError {
    pub fn new(message: String) -> Self {
        Self { message }
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DbError: {}", self.message)
    }
}

impl std::convert::From<DbError> for PyErr {
    fn from(error: DbError) -> PyErr {
        PyOSError::new_err(error.message)
    }
}

impl AsRef<str> for DbError {
    fn as_ref(&self) -> &str {
        &self.message
    }
}

impl From<String> for DbError {
    fn from(error: String) -> DbError {
        DbError { message: error }
    }
}

impl From<DbError> for String {
    fn from(error: DbError) -> String {
        error.message
    }
}

/// DbError
///
/// A DbError is raised when a database error occurs
///
/// # Arguments
/// * `message` - Error message
///
#[derive(Debug, Clone)]
pub struct ConvertError {
    message: String,
}

impl std::error::Error for ConvertError {}

impl ConvertError {
    pub fn new(message: String) -> Self {
        Self { message }
    }
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ConvertError: {}", self.message)
    }
}

impl std::convert::From<ConvertError> for PyErr {
    fn from(error: ConvertError) -> PyErr {
        PyValueError::new_err(error.message)
    }
}

impl From<String> for ConvertError {
    fn from(error: String) -> ConvertError {
        ConvertError { message: error }
    }
}

impl From<ConvertError> for String {
    fn from(error: ConvertError) -> String {
        error.message
    }
}
