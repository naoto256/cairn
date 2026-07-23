use std::fmt;

use serde::de::{Unexpected, Visitor};
use serde::{Deserializer, Serializer};

pub(crate) fn serialize<S>(job_id: &i64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.collect_str(job_id)
}

pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    struct JobIdVisitor;

    impl Visitor<'_> for JobIdVisitor {
        type Value = i64;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a signed 64-bit job id as a decimal string or integer")
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
            Ok(value)
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            i64::try_from(value).map_err(|_| E::invalid_value(Unexpected::Unsigned(value), &self))
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            value
                .parse()
                .map_err(|_| E::invalid_value(Unexpected::Str(value), &self))
        }
    }

    deserializer.deserialize_any(JobIdVisitor)
}
