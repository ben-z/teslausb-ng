use std::fmt::{Display, Formatter};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    message: String,
}

impl Error {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn context(self, context: impl AsRef<str>) -> Self {
        Self::new(format!("{}: {}", context.as_ref(), self.message))
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::new(value.to_string())
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(value: std::num::ParseIntError) -> Self {
        Self::new(value.to_string())
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(value: std::num::ParseFloatError) -> Self {
        Self::new(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_and_context_preserve_messages() {
        let err = Error::new("low-level failure").context("while archiving");

        assert_eq!(err.to_string(), "while archiving: low-level failure");
    }

    #[test]
    fn common_std_errors_convert_to_project_error() {
        let io_err: Error = std::io::Error::new(std::io::ErrorKind::NotFound, "missing").into();
        assert!(io_err.to_string().contains("missing"));

        let int_err: Error = "nan".parse::<u64>().unwrap_err().into();
        assert!(int_err.to_string().contains("invalid digit"));

        let float_err: Error = "nanx".parse::<f64>().unwrap_err().into();
        assert!(!float_err.to_string().is_empty());
    }
}
