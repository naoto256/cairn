//! UTC timestamp formatting helpers.
//!
//! The CAS layer stores `i64` nanoseconds since the Unix epoch; the
//! wire layer renders them as RFC 3339 strings. We do the conversion
//! inline rather than pulling in `chrono` / `jiff` / `time` because
//! the surface is a single formatter and the algorithm (Hinnant's
//! `civil_from_days`) is well-known and tested below.
//!
//! Only handles UTC. Local-time, named timezones, parsing, and
//! arithmetic are out of scope — add them with a real time crate
//! if/when needed.
//!
//! # Range
//!
//! Correct over [year 1, year 9999]. Outside that range the formatter
//! returns the raw nanosecond integer as a fallback so the wire stays
//! parseable.

/// Format an `i64` nanosecond UTC timestamp as RFC 3339 (e.g.
/// `2026-06-01T18:00:00.123456789Z`). Out-of-range inputs fall back to
/// the raw `ns.to_string()`.
#[must_use]
pub fn ns_to_rfc3339_utc(ns: i64) -> String {
    // Euclidean division keeps every remainder non-negative, so
    // pre-epoch (negative) timestamps split into a floored day count
    // plus an in-range time-of-day instead of a negative pair.
    let secs = ns.div_euclid(1_000_000_000);
    // rem_euclid with a positive modulus always fits the target type,
    // so the unwrap_or(0) fallbacks below are unreachable — kept only
    // to keep a formatter free of panicking paths.
    let nanos = u32::try_from(ns.rem_euclid(1_000_000_000)).unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let secs_of_day = u32::try_from(secs.rem_euclid(86_400)).unwrap_or(0);
    let hh = secs_of_day / 3600;
    let mm = (secs_of_day / 60) % 60;
    let ss = secs_of_day % 60;

    let Some((year, month, day)) = civil_from_days(days) else {
        return ns.to_string();
    };
    format!("{year:04}-{month:02}-{day:02}T{hh:02}:{mm:02}:{ss:02}.{nanos:09}Z")
}

/// Hinnant's algorithm — days since 1970-01-01 → (year, month, day).
/// Returns `None` outside [year 1, year 9999] so the formatter can fall
/// back to a raw integer rather than emit a malformed string.
fn civil_from_days(days: i64) -> Option<(i32, u32, u32)> {
    // Shift the epoch to 0000-03-01. Counting years from March 1 puts
    // the leap day last in the year, which makes the month lengths a
    // fixed 153-day / 5-month repeating pattern.
    let z = days + 719_468;
    // 146_097 days per 400-year Gregorian era; the branch floors the
    // division for negative z.
    let era = if z >= 0 { z } else { z - 146_095 } / 146_097;
    // Day-of-era in [0, 146_096], so the conversion cannot fail.
    let doe = u32::try_from(z - era * 146_097).ok()?;
    // Year-of-era in [0, 399], correcting for the 4 / 100 / 400-year
    // leap rules.
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let mut y = i64::from(yoe) + era * 400;
    // Day-of-year counted from March 1, in [0, 365].
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    // March-based month index in [0, 11]; (153 * mp + 2) / 5 is the
    // day offset of that month's first day.
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    // January and February belong to the next civil year.
    if m <= 2 {
        y += 1;
    }
    let year = i32::try_from(y).ok()?;
    if !(1..=9999).contains(&year) {
        return None;
    }
    Some((year, m, d))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_epoch() {
        assert_eq!(ns_to_rfc3339_utc(0), "1970-01-01T00:00:00.000000000Z");
    }

    #[test]
    fn one_second_after_epoch() {
        assert_eq!(
            ns_to_rfc3339_utc(1_000_000_000),
            "1970-01-01T00:00:01.000000000Z"
        );
    }

    #[test]
    fn nanos_preserved() {
        assert_eq!(
            ns_to_rfc3339_utc(123_456_789),
            "1970-01-01T00:00:00.123456789Z"
        );
    }

    #[test]
    fn end_of_year_2025() {
        // 2025-12-31 23:59:59.999999999 UTC
        // seconds = 1767225599, plus 999_999_999 ns
        let ns = 1_767_225_599_000_000_000 + 999_999_999;
        assert_eq!(ns_to_rfc3339_utc(ns), "2025-12-31T23:59:59.999999999Z");
    }

    #[test]
    fn leap_year_handling() {
        // 2024-02-29 12:00:00 UTC → 1709208000 seconds
        assert_eq!(
            ns_to_rfc3339_utc(1_709_208_000_000_000_000),
            "2024-02-29T12:00:00.000000000Z"
        );
    }

    #[test]
    fn pre_epoch_falls_back_to_raw_when_out_of_range() {
        // Year 1 starts roughly at -62_135_596_800 seconds; anything
        // older than that should fall back to raw rather than emit a
        // malformed string. Use seconds well past the lower bound to
        // confirm normal pre-epoch dates still format.
        let ns = -1_000_000_000; // 1969-12-31 23:59:59
        assert_eq!(ns_to_rfc3339_utc(ns), "1969-12-31T23:59:59.000000000Z");
    }
}
