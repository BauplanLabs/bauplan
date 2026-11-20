use chrono::{DateTime, Local, Utc};

use crate::cli::color::DIM;

pub(crate) fn format_datetime(
    dt: Option<DateTime<Utc>>,
    utc: bool,
    include_elapsed: bool,
) -> String {
    let Some(dt) = dt else {
        return format!("{DIM}-{DIM:#}");
    };

    let rfc3339 = if utc {
        dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    } else {
        dt.with_timezone(&Local)
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    };

    let elapsed = Utc::now() - dt;
    if include_elapsed && elapsed.num_days() < 1 {
        // A date from the server can be a bit ahead of the local clock, so
        // we show it as `0s ago`.
        let elapsed = elapsed.to_std().unwrap_or_default();

        // "Round" by splitting the string and taking the most significant
        // "digit". For example, "23h 24m 10s" becomes "23h".
        let elapsed_human = humantime::format_duration(elapsed).to_string();
        let elapsed_short = elapsed_human.split_ascii_whitespace().next().unwrap();

        format!("{rfc3339} {DIM}[{elapsed_short} ago]{DIM:#}")
    } else {
        rfc3339
    }
}
