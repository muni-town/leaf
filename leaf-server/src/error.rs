pub trait LogError {
    fn log_error(self, description: &str) -> Self;
}

impl<T, E: std::fmt::Display> LogError for Result<T, E> {
    fn log_error(self, description: &str) -> Self {
        if let Err(e) = &self {
            tracing::error!(error = %e, "{}", description);
        }
        self
    }
}
