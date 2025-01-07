// Copyright (c) 2023-2024 Retake, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use crate::index::writer::index::IndexError;
use crate::postgres::types::TantivyValue;
use crate::schema::{SearchDocument, SearchField, SearchIndexSchema};
use anyhow::{anyhow, Result};
use chrono::{NaiveDate, NaiveTime};
use pgrx::itemptr::{item_pointer_get_both, item_pointer_set_all};
use pgrx::*;
use std::str::FromStr;

/// Finds and returns the `USING bm25` index on the specified relation with the
/// highest OID, or [`None`] if there aren't any.
pub fn locate_bm25_index(heaprelid: pg_sys::Oid) -> Option<PgRelation> {
    unsafe {
        let heaprel = PgRelation::open(heaprelid);
        let indices = heaprel.indices(pg_sys::AccessShareLock as _);

        // Find all bm25 indexes and keep the one with highest OID
        indices
            .into_iter()
            .filter(|index| pg_sys::get_index_isvalid(index.oid()))
            .filter(|index| {
                !index.rd_indam.is_null()
                    && (*index.rd_indam).ambuild == Some(crate::postgres::build::ambuild)
            })
            .max_by_key(|index| index.oid().as_u32())
    }
}

/// Rather than using pgrx' version of this function, we use our own, which doesn't leave 2
/// empty bytes in the middle of the 64bit representation.  A ctid being only 48bits means
/// if we leave the upper 16 bits (2 bytes) empty, tantivy will have a better chance of
/// bitpacking or compressing these values.
#[allow(dead_code)]
#[inline(always)]
pub fn item_pointer_to_u64(ctid: pg_sys::ItemPointerData) -> u64 {
    let (blockno, offno) = item_pointer_get_both(ctid);
    let blockno = blockno as u64;
    let offno = offno as u64;

    // shift the BlockNumber left 16 bits -- the length of the OffsetNumber we OR onto the end
    // pgrx's version shifts left 32, which is wasteful
    (blockno << 16) | offno
}

pub fn ctid_to_u64(ctid: tantivy::Ctid) -> u64 {
    let (blockno, offno) = (ctid.0 as u64, ctid.1 as u64);
    (blockno << 16) | offno
}

/// Rather than using pgrx' version of this function, we use our own, which doesn't leave 2
/// empty bytes in the middle of the 64bit representation.  A ctid being only 48bits means
/// if we leave the upper 16 bits (2 bytes) empty, tantivy will have a better chance of
/// bitpacking or compressing these values.
#[inline(always)]
pub fn u64_to_item_pointer(value: u64, tid: &mut pg_sys::ItemPointerData) {
    // shift right 16 bits to pop off the OffsetNumber, leaving only the BlockNumber
    // pgrx's version must shift right 32 bits to be in parity with `item_pointer_to_u64()`
    let blockno = (value >> 16) as pg_sys::BlockNumber;
    let offno = value as pg_sys::OffsetNumber;
    item_pointer_set_all(tid, blockno, offno);
}

pub struct CategorizedFieldData {
    pub attno: usize,
    pub base_oid: PgOid,
    pub is_array: bool,
    pub is_json: bool,
}

pub fn categorize_fields(
    tupdesc: &PgTupleDesc,
    schema: &SearchIndexSchema,
) -> Vec<(SearchField, CategorizedFieldData)> {
    let mut categorized_fields = Vec::new();

    let mut alias_lookup = schema.alias_lookup();

    // Create a vector of index entries from the postgres row.
    for (attno, attribute) in tupdesc.iter().enumerate() {
        let attname = attribute.name().to_string();
        let attribute_type_oid = attribute.type_oid();

        // List any indexed fields that use this column as source data.
        let mut search_fields = alias_lookup.remove(&attname).unwrap_or_default();

        // If there's an indexed field with the same name as a this column, add it to the list.
        if let Some(index_field) = schema.get_search_field(&attname.clone().into()) {
            search_fields.push(index_field)
        };

        for search_field in search_fields {
            let array_type = unsafe { pg_sys::get_element_type(attribute_type_oid.value()) };
            let (base_oid, is_array) = if array_type != pg_sys::InvalidOid {
                (PgOid::from(array_type), true)
            } else {
                (attribute_type_oid, false)
            };

            let is_json = matches!(
                base_oid,
                PgOid::BuiltIn(pg_sys::BuiltinOid::JSONBOID | pg_sys::BuiltinOid::JSONOID)
            );

            categorized_fields.push((
                search_field.clone(),
                CategorizedFieldData {
                    attno,
                    base_oid,
                    is_array,
                    is_json,
                },
            ));
        }
    }

    categorized_fields
}

pub unsafe fn row_to_search_document(
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    key_field_name: &str,
    categorized_fields: &Vec<(SearchField, CategorizedFieldData)>,
    document: &mut SearchDocument,
) -> Result<(), IndexError> {
    for (
        search_field,
        CategorizedFieldData {
            attno,
            base_oid,
            is_array,
            is_json,
        },
    ) in categorized_fields
    {
        let datum = *values.add(*attno);
        let isnull = *isnull.add(*attno);

        if key_field_name == search_field.name.as_ref() && isnull {
            return Err(IndexError::KeyIdNull(key_field_name.to_string()));
        }

        if isnull {
            continue;
        }

        if *is_array {
            for value in TantivyValue::try_from_datum_array(datum, *base_oid)? {
                document.insert(search_field.id, value.tantivy_schema_value());
            }
        } else if *is_json {
            for value in TantivyValue::try_from_datum_json(datum, *base_oid)? {
                document.insert(search_field.id, value.tantivy_schema_value());
            }
        } else {
            document.insert(
                search_field.id,
                TantivyValue::try_from_datum(datum, *base_oid)?.tantivy_schema_value(),
            );
        }
    }
    Ok(())
}

/// Utility function for easy `f64` to `u32` conversion
fn f64_to_u32(n: f64) -> Result<u32> {
    let truncated = n.trunc();
    if truncated.is_nan()
        || truncated.is_infinite()
        || truncated < 0.0
        || truncated > u32::MAX.into()
    {
        return Err(anyhow!("overflow in f64 to u32"));
    }

    Ok(truncated as u32)
}

/// Seconds are represented by `f64` in pgrx, with a maximum of microsecond precision
fn convert_pgrx_seconds_to_chrono(orig: f64) -> Result<(u32, u32, u32)> {
    let seconds = f64_to_u32(orig)?;
    let microseconds = f64_to_u32((orig * 1_000_000.0) % 1_000_000.0)?;
    let nanoseconds = f64_to_u32((orig * 1_000_000_000.0) % 1_000_000_000.0)?;
    Ok((seconds, microseconds, nanoseconds))
}

pub fn convert_pg_date_string(typeoid: PgOid, date_string: &str) -> tantivy::DateTime {
    match typeoid {
        PgOid::BuiltIn(PgBuiltInOids::DATEOID | PgBuiltInOids::DATERANGEOID) => {
            let d = pgrx::datum::Date::from_str(date_string)
                .expect("must be valid postgres date format");
            let micros = NaiveDate::from_ymd_opt(d.year(), d.month().into(), d.day().into())
                .expect("must be able to parse date format")
                .and_hms_opt(0, 0, 0)
                .expect("must be able to set date default time")
                .and_utc()
                .timestamp_micros();
            tantivy::DateTime::from_timestamp_micros(micros)
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID | PgBuiltInOids::TSRANGEOID) => {
            // Since [`pgrx::Timestamp`]s are tied to the Postgres instance's timezone,
            // to figure out *which* timezone it's actually in, we convert to a
            // [`pgrx::TimestampWithTimeZone`].
            // Once the offset is known, we can create and return a [`chrono::NaiveDateTime`]
            // with the appropriate offset.
            let t = pgrx::datum::Timestamp::from_str(date_string)
                .expect("must be a valid postgres timestamp");
            let twtz: datum::TimestampWithTimeZone = t.into();
            let (seconds, _micros, _nanos) = convert_pgrx_seconds_to_chrono(twtz.second())
                .expect("must not overflow converting pgrx seconds");
            let micros =
                NaiveDate::from_ymd_opt(twtz.year(), twtz.month().into(), twtz.day().into())
                    .expect("must be able to convert date timestamp")
                    .and_hms_opt(twtz.hour().into(), twtz.minute().into(), seconds)
                    .expect("must be able to parse timestamp format")
                    .and_utc()
                    .timestamp_micros();
            tantivy::DateTime::from_timestamp_micros(micros)
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID | pg_sys::BuiltinOid::TSTZRANGEOID) => {
            let twtz = pgrx::datum::TimestampWithTimeZone::from_str(date_string)
                .expect("must be a valid postgres timestamp with time zone")
                .to_utc();
            let (seconds, _micros, _nanos) = convert_pgrx_seconds_to_chrono(twtz.second())
                .expect("must not overflow converting pgrx seconds");
            let micros =
                NaiveDate::from_ymd_opt(twtz.year(), twtz.month().into(), twtz.day().into())
                    .expect("must be able to convert timestamp with timezone")
                    .and_hms_opt(twtz.hour().into(), twtz.minute().into(), seconds)
                    .expect("must be able to parse timestamp with timezone")
                    .and_utc()
                    .timestamp_micros();
            tantivy::DateTime::from_timestamp_micros(micros)
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMEOID) => {
            let t =
                pgrx::datum::Time::from_str(date_string).expect("must be a valid postgres time");
            let (hour, minute, second, micros) = t.to_hms_micro();
            let naive_time =
                NaiveTime::from_hms_micro_opt(hour.into(), minute.into(), second.into(), micros)
                    .expect("must be able to parse time");
            let naive_date = NaiveDate::from_ymd_opt(1970, 1, 1).expect("default date");
            let micros = naive_date.and_time(naive_time).and_utc().timestamp_micros();
            tantivy::DateTime::from_timestamp_micros(micros)
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMETZOID) => {
            let twtz = pgrx::datum::TimeWithTimeZone::from_str(date_string)
                .expect("must be a valid postgres time with time zone")
                .to_utc();
            let (hour, minute, second, micros) = twtz.to_hms_micro();
            let naive_time =
                NaiveTime::from_hms_micro_opt(hour.into(), minute.into(), second.into(), micros)
                    .expect("must be able to parse time with time zone");
            let naive_date = NaiveDate::from_ymd_opt(1970, 1, 1).expect("default date");
            let micros = naive_date.and_time(naive_time).and_utc().timestamp_micros();
            tantivy::DateTime::from_timestamp_micros(micros)
        }
        _ => panic!("Unsupported typeoid: {typeoid:?}"),
    }
}
