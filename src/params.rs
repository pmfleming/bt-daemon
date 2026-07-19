use anyhow::{Context, Result};
use serde_json::Value;

pub(crate) trait Params {
    fn require_bool(&self, name: &str) -> Result<bool>;
    fn require_string<'a>(&'a self, name: &str) -> Result<&'a str>;
    fn require_u32(&self, name: &str) -> Result<u32>;

    fn require_strings<'a>(&'a self, first: &str, second: &str) -> Result<(&'a str, &'a str)> {
        Ok((self.require_string(first)?, self.require_string(second)?))
    }
}

impl Params for Value {
    fn require_bool(&self, name: &str) -> Result<bool> {
        self.get(name)
            .and_then(Value::as_bool)
            .with_context(|| format!("missing or invalid boolean parameter '{name}'"))
    }

    fn require_string<'a>(&'a self, name: &str) -> Result<&'a str> {
        self.get(name)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .with_context(|| format!("missing or invalid string parameter '{name}'"))
    }

    fn require_u32(&self, name: &str) -> Result<u32> {
        self.get(name)
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .with_context(|| format!("missing or invalid unsigned integer parameter '{name}'"))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::Params;

    #[test]
    fn required_parameters_reject_missing_empty_and_out_of_range_values() {
        let params = json!({ "name": "", "count": u64::from(u32::MAX) + 1 });
        assert!(params.require_string("name").is_err());
        assert!(params.require_bool("enabled").is_err());
        assert!(params.require_u32("count").is_err());
    }
}
