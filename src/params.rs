use anyhow::Result;
use serde_json::Value;

use crate::backend::{BackendError, BackendErrorKind};

pub(crate) trait Params {
    fn optional_string<'a>(&'a self, name: &str) -> Result<Option<&'a str>>;
    fn require_bool(&self, name: &str) -> Result<bool>;
    fn require_string<'a>(&'a self, name: &str) -> Result<&'a str>;
    fn require_u32(&self, name: &str) -> Result<u32>;

    fn require_strings<'a>(&'a self, first: &str, second: &str) -> Result<(&'a str, &'a str)> {
        Ok((self.require_string(first)?, self.require_string(second)?))
    }
}

impl Params for Value {
    fn optional_string<'a>(&'a self, name: &str) -> Result<Option<&'a str>> {
        match self.get(name) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) if !value.is_empty() => Ok(Some(value)),
            _ => Err(BackendError::new(
                BackendErrorKind::InvalidInput,
                format!("invalid optional string parameter '{name}'"),
            )
            .into()),
        }
    }

    fn require_bool(&self, name: &str) -> Result<bool> {
        self.get(name).and_then(Value::as_bool).ok_or_else(|| {
            BackendError::new(
                BackendErrorKind::InvalidInput,
                format!("missing or invalid boolean parameter '{name}'"),
            )
            .into()
        })
    }

    fn require_string<'a>(&'a self, name: &str) -> Result<&'a str> {
        self.get(name)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                BackendError::new(
                    BackendErrorKind::InvalidInput,
                    format!("missing or invalid string parameter '{name}'"),
                )
                .into()
            })
    }

    fn require_u32(&self, name: &str) -> Result<u32> {
        self.get(name)
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| {
                BackendError::new(
                    BackendErrorKind::InvalidInput,
                    format!("missing or invalid unsigned integer parameter '{name}'"),
                )
                .into()
            })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::Params;

    #[test]
    fn required_parameters_reject_missing_empty_and_out_of_range_values() {
        let params = json!({ "name": "", "count": u64::from(u32::MAX) + 1, "adapter_key": 42 });
        assert!(params.require_string("name").is_err());
        assert!(params.optional_string("adapter_key").is_err());
        assert_eq!(
            json!({ "adapter_key": null })
                .optional_string("adapter_key")
                .unwrap(),
            None
        );
        assert!(params.require_bool("enabled").is_err());
        assert!(params.require_u32("count").is_err());
    }
}
