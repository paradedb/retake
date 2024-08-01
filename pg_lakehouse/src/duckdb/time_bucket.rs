use pgrx::iter::TableIterator;
use pgrx::pg_sys::Datum;
use pgrx::*;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

pub enum TimeBucketInput {
    Date(Date),
    Timestamp(Timestamp),
}

pub enum TimeBucketOffset {
    Interval(Interval),
    Date(Date),
}

impl Display for TimeBucketInput {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeBucketInput::Date(input) => {
                write!(f, "{}::DATE", input.to_string())
            }
            TimeBucketInput::Timestamp(input) => {
                write!(f, "{}", input.to_string())
            }
        }
    }
}

impl Display for TimeBucketOffset {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeBucketOffset::Date(input) => {
                write!(f, "DATE {}", input.to_string())
            }
            TimeBucketOffset::Interval(input) => {
                write!(f, "INTERVAL {}", input.to_string())
            }
        }
    }
}

fn create_time_bucket(
    bucket_width: Interval,
    input: TimeBucketInput,
    offset: Option<TimeBucketOffset>,
) -> String {
    if let Some(bucket_offset) = offset {
        format!(
            "time_bucket(INTERVAL {}, {}, {})",
            bucket_width, input, bucket_offset
        )
    } else {
        format!("time_bucket(INTERVAL {}, {})", bucket_width, input)
    }
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_date_no_offset(
    bucket_width: Interval,
    input: Date,
) -> TableIterator<'static, (name!(time_bucket, Date),)> {
    let bucket_query = create_time_bucket(bucket_width, TimeBucketInput::Date(input), None);

    TableIterator::once((bucket_query
        .parse()
        .unwrap_or_else(|err| panic!("There was an error while parsing time_bucket(): {}", err)),))
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_date_offset_date(
    bucket_width: Interval,
    input: Date,
    offset: Date,
) -> TableIterator<'static, (name!(time_bucket, Date),)> {
    let bucket_query = create_time_bucket(
        bucket_width,
        TimeBucketInput::Date(input),
        Some(TimeBucketOffset::Date(offset)),
    );

    TableIterator::once((bucket_query
        .parse()
        .unwrap_or_else(|err| panic!("There was an error while parsing time_bucket(): {}", err)),))
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_date_offset_interval(
    bucket_width: Interval,
    input: Date,
    offset: Interval,
) -> TableIterator<'static, (name!(time_bucket, Date),)> {
    let bucket_query = create_time_bucket(
        bucket_width,
        TimeBucketInput::Date(input),
        Some(TimeBucketOffset::Interval(offset)),
    );

    TableIterator::once((bucket_query
        .parse()
        .unwrap_or_else(|err| panic!("There was an error while parsing time_bucket(): {}", err)),))
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_timestamp(
    bucket_width: Interval,
    input: Timestamp,
) -> TableIterator<'static, (name!(time_bucket, Date),)> {
    let bucket_query = create_time_bucket(bucket_width, TimeBucketInput::Timestamp(input), None);

    TableIterator::once((bucket_query
        .parse()
        .unwrap_or_else(|err| panic!("There was an error while parsing time_bucket(): {}", err)),))
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_timestamp_offset_date(
    bucket_width: Interval,
    input: Timestamp,
    offset: Date,
) -> TableIterator<'static, (name!(time_bucket, Date),)> {
    let bucket_query = create_time_bucket(
        bucket_width,
        TimeBucketInput::Timestamp(input),
        Some(TimeBucketOffset::Date(offset)),
    );

    TableIterator::once((bucket_query
        .parse()
        .unwrap_or_else(|err| panic!("There was an error while parsing time_bucket(): {}", err)),))
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_timestamp_offset_interval(
    bucket_width: Interval,
    input: Timestamp,
    offset: Interval,
) -> TableIterator<'static, (name!(time_bucket, Date),)> {
    let bucket_query = create_time_bucket(
        bucket_width,
        TimeBucketInput::Timestamp(input),
        Some(TimeBucketOffset::Interval(offset)),
    );

    TableIterator::once((bucket_query
        .parse()
        .unwrap_or_else(|err| panic!("There was an error while parsing time_bucket(): {}", err)),))
}
