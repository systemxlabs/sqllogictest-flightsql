use arrow::error::ArrowError;
use arrow_flight::error::FlightError;

#[derive(Debug, thiserror::Error)]
pub enum FlightSqlLogicTestError {
    #[error("Tonic error: {0}")]
    Tonic(#[from] tonic::transport::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),

    #[error("Flight SQL error: {0}")]
    FlightSql(#[from] FlightError),

    #[error("Other error: {0}")]
    Other(String),
}

impl From<String> for FlightSqlLogicTestError {
    fn from(value: String) -> Self {
        Self::Other(value)
    }
}

impl From<&str> for FlightSqlLogicTestError {
    fn from(value: &str) -> Self {
        Self::Other(value.to_string())
    }
}
