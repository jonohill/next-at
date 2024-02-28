use std::fmt::Display;

use chrono::{DateTime, Days, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use chrono_tz::Tz;
use regex::Regex;

#[derive(thiserror::Error, Debug)]
pub struct DateError(String);

impl Display for DateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DateError: {}", self.0)
    }
}

pub type DateResult<T> = Result<T, DateError>;

/// Parse date/time manually, accomodates times that go past 24 hours
pub struct GtfsDateTimeParser {
    re_time: Regex,
    tz: Tz,
}

impl GtfsDateTimeParser {
    pub fn new() -> Self {
        // It's important this is compiled once, it's by far the most expensive part
        let re_time = Regex::new(r"(\d{2}):(\d{2}):(\d{2})").unwrap();
        Self {
            re_time,
            tz: Tz::UTC,
        }
    }

    pub fn parse_date(&self, date: &str) -> DateResult<NaiveDate> {
        NaiveDate::parse_from_str(date, "%Y%m%d").map_err(|e| DateError(e.to_string()))
    }

    pub fn parse_time(
        &mut self,
        date: &NaiveDate,
        time: &str,
        tz: &str,
    ) -> DateResult<DateTime<Tz>> {
        if self.tz.name() != tz {
            self.tz = tz
                .parse()
                .map_err(|_| DateError("Invalid timezone".to_string()))?;
        }

        let captures = self.re_time.captures(time).unwrap();
        let raw_hour = captures
            .get(1)
            .unwrap()
            .as_str()
            .parse::<u32>()
            .map_err(|e| DateError(e.to_string()))?;
        let minute = captures
            .get(2)
            .unwrap()
            .as_str()
            .parse::<u32>()
            .map_err(|e| DateError(e.to_string()))?;
        let second = captures
            .get(3)
            .unwrap()
            .as_str()
            .parse::<u32>()
            .map_err(|e| DateError(e.to_string()))?;

        let days_offset = Days::new(raw_hour as u64 / 24);
        let hour = raw_hour % 24;

        let time = NaiveTime::from_hms_opt(hour, minute, second)
            .ok_or_else(|| DateError("Invalid time".to_string()))?;
        let dt_local = NaiveDateTime::new(*date, time) + days_offset;

        let dt = self
            .tz
            .from_local_datetime(&dt_local)
            .earliest()
            .ok_or_else(|| DateError("Invalid time".to_string()))?;
        Ok(dt)
    }
}
