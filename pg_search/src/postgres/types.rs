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

use crate::postgres::datetime::{datetime_components_to_tantivy_date, MICROSECONDS_IN_SECOND};
use crate::postgres::range::RangeToTantivyValue;
use crate::postgres::utils::CoercibleTo;
use crate::schema::AnyEnum;
use ordered_float::OrderedFloat;
use pgrx::datum::datetime_support::DateTimeConversionError;
use pgrx::pg_sys::Datum;
use pgrx::pg_sys::Oid;
use pgrx::IntoDatum;
use pgrx::PostgresType;
use pgrx::{FromDatum, PgBuiltInOids, PgOid};
use serde::ser::{Serialize, SerializeStruct, Serializer};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::num::ParseFloatError;
use std::str::FromStr;
use tantivy::schema::OwnedValue;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq, PostgresType)]
pub struct TantivyValue(pub tantivy::schema::OwnedValue);

impl Default for TantivyValue {
    fn default() -> Self {
        TantivyValue(tantivy::schema::OwnedValue::Null)
    }
}

impl TantivyValue {
    pub fn tantivy_schema_value(&self) -> tantivy::schema::OwnedValue {
        self.0.clone()
    }

    pub unsafe fn try_into_datum(self, oid: PgOid) -> Result<Option<Datum>, TantivyValueError> {
        if matches!(self.0, OwnedValue::Null) {
            return Ok(None);
        }

        match &oid {
            PgOid::BuiltIn(builtin) => {
                let datum = match builtin {
                    PgBuiltInOids::BOOLOID => bool::try_from(self)?.into_datum(),
                    PgBuiltInOids::INT2OID => i16::try_from(self)?.into_datum(),
                    PgBuiltInOids::INT4OID => i32::try_from(self)?.into_datum(),
                    PgBuiltInOids::INT8OID => i64::try_from(self)?.into_datum(),
                    PgBuiltInOids::OIDOID => Oid::from(u32::try_from(self)?).into_datum(),
                    PgBuiltInOids::FLOAT4OID => f32::try_from(self)?.into_datum(),
                    PgBuiltInOids::FLOAT8OID => f64::try_from(self)?.into_datum(),
                    PgBuiltInOids::NUMERICOID => pgrx::AnyNumeric::try_from(self)?.into_datum(),
                    PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID => {
                        String::try_from(self)?.into_datum()
                    }
                    PgBuiltInOids::JSONOID => pgrx::datum::JsonString::try_from(self)?.into_datum(),
                    PgBuiltInOids::JSONBOID => pgrx::JsonB::try_from(self)?.into_datum(),
                    PgBuiltInOids::DATEOID => pgrx::datum::Date::try_from(self)?.into_datum(),
                    PgBuiltInOids::TIMESTAMPOID => {
                        pgrx::datum::Timestamp::try_from(self)?.into_datum()
                    }
                    PgBuiltInOids::TIMESTAMPTZOID => {
                        pgrx::datum::TimestampWithTimeZone::try_from(self)?.into_datum()
                    }
                    PgBuiltInOids::TIMEOID => pgrx::datum::Time::try_from(self)?.into_datum(),
                    PgBuiltInOids::TIMETZOID => {
                        pgrx::datum::TimeWithTimeZone::try_from(self)?.into_datum()
                    }
                    PgBuiltInOids::UUIDOID => pgrx::datum::Uuid::try_from(self)?.into_datum(),
                    _ => return Err(TantivyValueError::UnsupportedOid(oid.value())),
                };
                Ok(datum)
            }
            _ => Err(TantivyValueError::InvalidOid),
        }
    }

    fn json_value_to_tantivy_value(value: Value) -> Vec<TantivyValue> {
        let mut tantivy_values = vec![];
        match value {
            // A tantivy JSON value can't be a top-level array, so we have to make
            // separate values out of each entry.
            Value::Array(value_vec) => {
                for value in value_vec {
                    tantivy_values.extend_from_slice(&Self::json_value_to_tantivy_value(value));
                }
            }
            _ => tantivy_values.push(TantivyValue(tantivy::schema::OwnedValue::from(value))),
        }
        tantivy_values
    }

    pub unsafe fn try_from_datum_array(
        datum: Datum,
        oid: PgOid,
    ) -> Result<Vec<Self>, TantivyValueError> {
        const SUPPORTED_ARRAY_OIDS: &[PgBuiltInOids] = &[
            PgBuiltInOids::BOOLOID,
            PgBuiltInOids::INT2OID,
            PgBuiltInOids::INT4OID,
            PgBuiltInOids::INT8OID,
            PgBuiltInOids::OIDOID,
            PgBuiltInOids::FLOAT4OID,
            PgBuiltInOids::FLOAT8OID,
            PgBuiltInOids::NUMERICOID,
            PgBuiltInOids::TEXTOID,
            PgBuiltInOids::VARCHAROID,
            PgBuiltInOids::DATEOID,
            PgBuiltInOids::TIMESTAMPOID,
            PgBuiltInOids::TIMESTAMPTZOID,
            PgBuiltInOids::TIMEOID,
            PgBuiltInOids::TIMETZOID,
            PgBuiltInOids::UUIDOID,
        ];

        let is_supported_array_oid = SUPPORTED_ARRAY_OIDS
            .iter()
            .any(|&builtin| oid.is_coercible_to(builtin));

        if let PgOid::Invalid = oid {
            Err(TantivyValueError::InvalidOid)
        } else if is_supported_array_oid {
            let array: pgrx::Array<Datum> =
                pgrx::Array::from_datum(datum, false).ok_or(TantivyValueError::DatumDeref)?;
            array
                .iter()
                .flatten()
                .map(|element_datum| Self::try_from_datum(element_datum, oid))
                .collect()
        } else {
            Err(TantivyValueError::UnsupportedArrayOid(oid.value()))
        }
    }

    pub unsafe fn try_from_datum_json(
        datum: Datum,
        oid: PgOid,
    ) -> Result<Vec<Self>, TantivyValueError> {
        // Tantivy has a limitation that prevents JSON top-level arrays from being
        // inserted into the index. Therefore, we need to flatten the array elements
        // individually before converting them into Tantivy values.
        if oid.is_coercible_to(PgBuiltInOids::JSONBOID) {
            let pgrx_value =
                pgrx::JsonB::from_datum(datum, false).ok_or(TantivyValueError::DatumDeref)?;
            let json_value: Value = serde_json::from_slice(&serde_json::to_vec(&pgrx_value.0)?)?;
            Ok(Self::json_value_to_tantivy_value(json_value))
        } else if oid.is_coercible_to(PgBuiltInOids::JSONOID) {
            let pgrx_value =
                pgrx::Json::from_datum(datum, false).ok_or(TantivyValueError::DatumDeref)?;
            let json_value: Value = serde_json::from_slice(&serde_json::to_vec(&pgrx_value.0)?)?;
            Ok(Self::json_value_to_tantivy_value(json_value))
        } else if matches!(oid, PgOid::BuiltIn(_)) {
            // Any other built-in type is not supported as JSON.
            Err(TantivyValueError::UnsupportedJsonOid(oid.value()))
        } else {
            Err(TantivyValueError::InvalidOid)
        }
    }

    pub unsafe fn try_from_datum(datum: Datum, oid: PgOid) -> Result<Self, TantivyValueError> {
        match oid {
            PgOid::Custom(custom) => {
                if pgrx::pg_sys::type_is_enum(custom) {
                    let (_, _, ordinal) = pgrx::enum_helper::lookup_enum_by_oid(
                        pgrx::pg_sys::Oid::from_datum(datum, false)
                            .ok_or(TantivyValueError::DatumDeref)?,
                    );
                    return TantivyValue::try_from(ordinal);
                }
            }
            PgOid::Invalid => return Err(TantivyValueError::InvalidOid),
            _ => (),
        }

        if oid.is_coercible_to(PgBuiltInOids::BOOLOID) {
            TantivyValue::try_from(
                bool::from_datum(datum, false).ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::INT2OID) {
            TantivyValue::try_from(
                i16::from_datum(datum, false).ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::INT4OID) {
            TantivyValue::try_from(
                i32::from_datum(datum, false).ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::INT8OID) {
            TantivyValue::try_from(
                i64::from_datum(datum, false).ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::OIDOID) {
            TantivyValue::try_from(
                u32::from_datum(datum, false).ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::FLOAT4OID) {
            TantivyValue::try_from(
                f32::from_datum(datum, false).ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::FLOAT8OID) {
            TantivyValue::try_from(
                f64::from_datum(datum, false).ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::NUMERICOID) {
            TantivyValue::try_from(
                pgrx::AnyNumeric::from_datum(datum, false).ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::TEXTOID)
            || oid.is_coercible_to(PgBuiltInOids::VARCHAROID)
        {
            TantivyValue::try_from(
                String::from_datum(datum, false).ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::DATEOID) {
            TantivyValue::try_from(
                pgrx::datum::Date::from_datum(datum, false).ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::TIMESTAMPOID) {
            TantivyValue::try_from(
                pgrx::datum::Timestamp::from_datum(datum, false)
                    .ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::TIMESTAMPTZOID) {
            TantivyValue::try_from(
                pgrx::datum::TimestampWithTimeZone::from_datum(datum, false)
                    .ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::TIMEOID) {
            TantivyValue::try_from(
                pgrx::datum::Time::from_datum(datum, false).ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::TIMETZOID) {
            TantivyValue::try_from(
                pgrx::datum::TimeWithTimeZone::from_datum(datum, false)
                    .ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::UUIDOID) {
            TantivyValue::try_from(
                pgrx::datum::Uuid::from_datum(datum, false).ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::INT4RANGEOID) {
            TantivyValue::from_range(
                pgrx::datum::Range::<i32>::from_datum(datum, false)
                    .ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::INT8RANGEOID) {
            TantivyValue::from_range(
                pgrx::datum::Range::<i64>::from_datum(datum, false)
                    .ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::NUMRANGEOID) {
            TantivyValue::from_range(
                pgrx::datum::Range::<pgrx::AnyNumeric>::from_datum(datum, false)
                    .ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::DATERANGEOID) {
            TantivyValue::from_range(
                pgrx::datum::Range::<pgrx::datum::Date>::from_datum(datum, false)
                    .ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::TSRANGEOID) {
            TantivyValue::from_range(
                pgrx::datum::Range::<pgrx::datum::Timestamp>::from_datum(datum, false)
                    .ok_or(TantivyValueError::DatumDeref)?,
            )
        } else if oid.is_coercible_to(PgBuiltInOids::TSTZRANGEOID) {
            TantivyValue::from_range(
                pgrx::datum::Range::<pgrx::datum::TimestampWithTimeZone>::from_datum(datum, false)
                    .ok_or(TantivyValueError::DatumDeref)?,
            )
        } else {
            Err(TantivyValueError::UnsupportedOid(oid.value()))
        }
    }

    pub unsafe fn try_from_anyelement(
        any_element: pgrx::AnyElement,
    ) -> Result<Self, TantivyValueError> {
        Self::try_from_datum(any_element.datum(), PgOid::from_untagged(any_element.oid()))
    }
}

impl fmt::Display for TantivyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.tantivy_schema_value() {
            tantivy::schema::OwnedValue::Str(string) => write!(f, "{}", string.clone()),
            tantivy::schema::OwnedValue::U64(u64) => write!(f, "{}", u64),
            tantivy::schema::OwnedValue::I64(i64) => write!(f, "{}", i64),
            tantivy::schema::OwnedValue::F64(f64) => write!(f, "{}", f64),
            tantivy::schema::OwnedValue::Bool(bool) => write!(f, "{}", bool),
            tantivy::schema::OwnedValue::Date(datetime) => {
                write!(f, "{}", datetime.into_primitive())
            }
            tantivy::schema::OwnedValue::Bytes(bytes) => {
                write!(
                    f,
                    "{}",
                    String::from_utf8(bytes.clone()).expect("bytes should be valid utf-8")
                )
            }
            tantivy::schema::OwnedValue::Object(_) => write!(f, "json object"),
            _ => panic!("tantivy owned value not supported"),
        }
    }
}

impl Hash for TantivyValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self.tantivy_schema_value() {
            tantivy::schema::OwnedValue::Str(string) => string.hash(state),
            tantivy::schema::OwnedValue::U64(u64) => u64.hash(state),
            tantivy::schema::OwnedValue::I64(i64) => i64.hash(state),
            tantivy::schema::OwnedValue::F64(f64) => OrderedFloat(f64).hash(state),
            tantivy::schema::OwnedValue::Bool(bool) => bool.hash(state),
            tantivy::schema::OwnedValue::Date(datetime) => datetime.hash(state),
            tantivy::schema::OwnedValue::Bytes(bytes) => bytes.hash(state),
            _ => panic!("tantivy owned value not supported"),
        }
    }
}

impl PartialOrd for TantivyValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.tantivy_schema_value() {
            tantivy::schema::OwnedValue::Str(string) => {
                if let tantivy::schema::OwnedValue::Str(other_string) = other.tantivy_schema_value()
                {
                    string.partial_cmp(&other_string)
                } else {
                    None
                }
            }
            tantivy::schema::OwnedValue::U64(u64) => {
                if let tantivy::schema::OwnedValue::U64(other_u64) = other.tantivy_schema_value() {
                    u64.partial_cmp(&other_u64)
                } else {
                    None
                }
            }
            tantivy::schema::OwnedValue::I64(i64) => {
                if let tantivy::schema::OwnedValue::I64(other_i64) = other.tantivy_schema_value() {
                    i64.partial_cmp(&other_i64)
                } else {
                    None
                }
            }
            tantivy::schema::OwnedValue::F64(f64) => {
                if let tantivy::schema::OwnedValue::F64(other_f64) = other.tantivy_schema_value() {
                    f64.partial_cmp(&other_f64)
                } else {
                    None
                }
            }
            tantivy::schema::OwnedValue::Bool(bool) => {
                if let tantivy::schema::OwnedValue::Bool(other_bool) = other.tantivy_schema_value()
                {
                    bool.partial_cmp(&other_bool)
                } else {
                    None
                }
            }
            tantivy::schema::OwnedValue::Date(datetime) => {
                if let tantivy::schema::OwnedValue::Date(other_datetime) =
                    other.tantivy_schema_value()
                {
                    datetime.partial_cmp(&other_datetime)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

impl Serialize for TantivyValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut ser = serializer.serialize_struct("TantivyValue", 1)?;
        ser.serialize_field("val", &self.0)?;
        ser.end()
    }
}

impl<'de> Deserialize<'de> for TantivyValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let inner_val = tantivy::schema::OwnedValue::deserialize(deserializer)?;

        Ok(TantivyValue(inner_val))
    }
}

impl TryFrom<Vec<u8>> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(TantivyValue(tantivy::schema::OwnedValue::Bytes(val)))
    }
}

impl TryFrom<TantivyValue> for Vec<u8> {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::Bytes(val) = value.0 {
            Ok(val)
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "Vec<u8>".to_string(),
            ))
        }
    }
}

impl TryFrom<String> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: String) -> Result<Self, Self::Error> {
        Ok(TantivyValue(tantivy::schema::OwnedValue::Str(val)))
    }
}

impl TryFrom<TantivyValue> for String {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::Str(val) = value.0 {
            Ok(val)
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "String".to_string(),
            ))
        }
    }
}

impl TryFrom<i8> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: i8) -> Result<Self, Self::Error> {
        Ok(TantivyValue(tantivy::schema::OwnedValue::I64(val as i64)))
    }
}

impl TryFrom<TantivyValue> for i8 {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::I64(val) = value.0 {
            Ok(val as i8)
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "i8".to_string(),
            ))
        }
    }
}

impl TryFrom<i16> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: i16) -> Result<Self, Self::Error> {
        Ok(TantivyValue(tantivy::schema::OwnedValue::I64(val as i64)))
    }
}

impl TryFrom<TantivyValue> for i16 {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::I64(val) = value.0 {
            Ok(val as i16)
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "i16".to_string(),
            ))
        }
    }
}

impl TryFrom<i32> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: i32) -> Result<Self, Self::Error> {
        Ok(TantivyValue(tantivy::schema::OwnedValue::I64(val as i64)))
    }
}

impl TryFrom<TantivyValue> for i32 {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::I64(val) = value.0 {
            Ok(val as i32)
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "i32".to_string(),
            ))
        }
    }
}

impl TryFrom<i64> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: i64) -> Result<Self, Self::Error> {
        Ok(TantivyValue(tantivy::schema::OwnedValue::I64(val)))
    }
}

impl TryFrom<TantivyValue> for i64 {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::I64(val) = value.0 {
            Ok(val)
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "i64".to_string(),
            ))
        }
    }
}

impl TryFrom<f32> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: f32) -> Result<Self, Self::Error> {
        // Casting f32 to f64 causes some precision errors when Tantivy writes the document.
        //     To avoid this, we string format the f32 and then read it as f64.
        let f32_string = format!("{}", val);
        let val_as_f64 = f64::from_str(&f32_string)?;

        Ok(TantivyValue(tantivy::schema::OwnedValue::F64(val_as_f64)))
    }
}

impl TryFrom<TantivyValue> for f32 {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::F64(val) = value.0 {
            // Casting f32 to f64 causes some precision errors when Tantivy writes the document.
            //     To avoid this, we string format the stored f64 and then read it as f32.
            let f64_string = format!("{}", val);
            let val_as_f32 = f32::from_str(&f64_string)?;

            Ok(val_as_f32)
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "f32".to_string(),
            ))
        }
    }
}

impl TryFrom<f64> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: f64) -> Result<Self, Self::Error> {
        Ok(TantivyValue(tantivy::schema::OwnedValue::F64(val)))
    }
}

impl TryFrom<TantivyValue> for f64 {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::F64(val) = value.0 {
            Ok(val)
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "f64".to_string(),
            ))
        }
    }
}

impl TryFrom<u32> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: u32) -> Result<Self, Self::Error> {
        Ok(TantivyValue(tantivy::schema::OwnedValue::U64(val as u64)))
    }
}

impl TryFrom<TantivyValue> for u32 {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::U64(val) = value.0 {
            Ok(val as u32)
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "u32".to_string(),
            ))
        }
    }
}

impl TryFrom<u64> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: u64) -> Result<Self, Self::Error> {
        Ok(TantivyValue(tantivy::schema::OwnedValue::U64(val)))
    }
}

impl TryFrom<TantivyValue> for u64 {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::U64(val) = value.0 {
            Ok(val)
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "u64".to_string(),
            ))
        }
    }
}

impl TryFrom<pgrx::AnyNumeric> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: pgrx::AnyNumeric) -> Result<Self, Self::Error> {
        Ok(TantivyValue(tantivy::schema::OwnedValue::F64(
            val.try_into()?,
        )))
    }
}

impl TryFrom<TantivyValue> for pgrx::AnyNumeric {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::F64(val) = value.0 {
            Ok(val.try_into()?)
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "numeric".to_string(),
            ))
        }
    }
}

impl TryFrom<bool> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: bool) -> Result<Self, Self::Error> {
        Ok(TantivyValue(tantivy::schema::OwnedValue::Bool(val)))
    }
}

impl TryFrom<TantivyValue> for bool {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::Bool(val) = value.0 {
            Ok(val)
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "bool".to_string(),
            ))
        }
    }
}

impl TryFrom<pgrx::datum::JsonString> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: pgrx::datum::JsonString) -> Result<Self, Self::Error> {
        let json_value: Value = serde_json::from_slice(&serde_json::to_vec(&val.0)?)?;
        Ok(TantivyValue(tantivy::schema::OwnedValue::from(json_value)))
    }
}

impl TryFrom<TantivyValue> for pgrx::datum::JsonString {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::Object(val) = value.0 {
            Ok(pgrx::datum::JsonString(serde_json::to_string(&val)?))
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "json".to_string(),
            ))
        }
    }
}

impl TryFrom<pgrx::JsonB> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: pgrx::JsonB) -> Result<Self, Self::Error> {
        let json_value: Value = serde_json::from_slice(&serde_json::to_vec(&val.0)?)?;
        Ok(TantivyValue(tantivy::schema::OwnedValue::from(json_value)))
    }
}

impl TryFrom<TantivyValue> for pgrx::JsonB {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::Object(val) = value.0 {
            Ok(pgrx::JsonB(serde_json::to_value(val)?))
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "jsonb".to_string(),
            ))
        }
    }
}

impl TryFrom<pgrx::datum::Date> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: pgrx::datum::Date) -> Result<Self, Self::Error> {
        Ok(TantivyValue(datetime_components_to_tantivy_date(
            Some((val.year(), val.month(), val.day())),
            (0, 0, 0, 0),
        )?))
    }
}

impl TryFrom<TantivyValue> for pgrx::datum::Date {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::Date(val) = value.0 {
            let prim_dt = val.into_primitive();
            Ok(pgrx::datum::Date::new(
                prim_dt.year(),
                prim_dt.month().into(),
                prim_dt.day(),
            )?)
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "date".to_string(),
            ))
        }
    }
}

impl TryFrom<pgrx::datum::Time> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: pgrx::datum::Time) -> Result<Self, Self::Error> {
        let (v_h, v_m, v_s, v_ms) = val.to_hms_micro();
        Ok(TantivyValue(datetime_components_to_tantivy_date(
            None,
            (v_h, v_m, v_s, v_ms),
        )?))
    }
}

impl TryFrom<TantivyValue> for pgrx::datum::Time {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::Date(val) = value.0 {
            let prim_dt = val.into_primitive();
            let (h, m, s, micro) = prim_dt.as_hms_micro();
            Ok(pgrx::datum::Time::new(
                h,
                m,
                s as f64 + ((micro as f64) / (MICROSECONDS_IN_SECOND as f64)),
            )?)
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "time".to_string(),
            ))
        }
    }
}

impl TryFrom<pgrx::datum::Timestamp> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: pgrx::datum::Timestamp) -> Result<Self, Self::Error> {
        let (v_h, v_m, v_s, v_ms) = val.to_hms_micro();
        Ok(TantivyValue(datetime_components_to_tantivy_date(
            Some((val.year(), val.month(), val.day())),
            (v_h, v_m, v_s, v_ms),
        )?))
    }
}

impl TryFrom<TantivyValue> for pgrx::datum::Timestamp {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::Date(val) = value.0 {
            let prim_dt = val.into_primitive();
            let (h, m, s, micro) = prim_dt.as_hms_micro();
            Ok(pgrx::datum::Timestamp::new(
                prim_dt.year(),
                prim_dt.month().into(),
                prim_dt.day(),
                h,
                m,
                s as f64 + ((micro as f64) / (MICROSECONDS_IN_SECOND as f64)),
            )?)
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "timestamp".to_string(),
            ))
        }
    }
}

impl TryFrom<pgrx::datum::TimeWithTimeZone> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: pgrx::datum::TimeWithTimeZone) -> Result<Self, Self::Error> {
        let (v_h, v_m, v_s, v_ms) = val.to_utc().to_hms_micro();
        Ok(TantivyValue(datetime_components_to_tantivy_date(
            None,
            (v_h, v_m, v_s, v_ms),
        )?))
    }
}

impl TryFrom<TantivyValue> for pgrx::datum::TimeWithTimeZone {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::Date(val) = value.0 {
            let prim_dt = val.into_primitive();
            let (h, m, s, micro) = prim_dt.as_hms_micro();
            Ok(pgrx::datum::TimeWithTimeZone::with_timezone(
                h,
                m,
                s as f64 + ((micro as f64) / (MICROSECONDS_IN_SECOND as f64)),
                "UTC",
            )?)
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "timetz".to_string(),
            ))
        }
    }
}

impl TryFrom<pgrx::datum::TimestampWithTimeZone> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: pgrx::datum::TimestampWithTimeZone) -> Result<Self, Self::Error> {
        let val = val.to_utc();
        let (v_h, v_m, v_s, v_ms) = val.to_hms_micro();
        Ok(TantivyValue(datetime_components_to_tantivy_date(
            Some((val.year(), val.month(), val.day())),
            (v_h, v_m, v_s, v_ms),
        )?))
    }
}

impl TryFrom<TantivyValue> for pgrx::datum::TimestampWithTimeZone {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::Date(val) = value.0 {
            let prim_dt = val.into_primitive();
            let (h, m, s, micro) = prim_dt.as_hms_micro();
            Ok(pgrx::datum::TimestampWithTimeZone::with_timezone(
                prim_dt.year(),
                prim_dt.month().into(),
                prim_dt.day(),
                h,
                m,
                s as f64 + ((micro as f64) / (MICROSECONDS_IN_SECOND as f64)),
                "UTC",
            )?)
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "timestamptz".to_string(),
            ))
        }
    }
}

impl TryFrom<pgrx::Uuid> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: pgrx::Uuid) -> Result<Self, Self::Error> {
        Ok(TantivyValue(tantivy::schema::OwnedValue::Str(
            val.to_string(),
        )))
    }
}

impl TryFrom<TantivyValue> for pgrx::Uuid {
    type Error = TantivyValueError;

    fn try_from(value: TantivyValue) -> Result<Self, Self::Error> {
        if let tantivy::schema::OwnedValue::Str(val) = value.0 {
            let uuid = uuid::Uuid::parse_str(&val)?;
            Ok(pgrx::Uuid::from_slice(uuid.as_bytes())
                .map_err(TantivyValueError::UuidConversionError)?)
        } else {
            Err(TantivyValueError::UnsupportedIntoConversion(
                "uuid".to_string(),
            ))
        }
    }
}

impl TryFrom<AnyEnum> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(val: AnyEnum) -> Result<Self, Self::Error> {
        match val.ordinal() {
            Some(ordinal) => Ok(TantivyValue(tantivy::schema::OwnedValue::F64(
                ordinal.into(),
            ))),
            None => Ok(TantivyValue(tantivy::schema::OwnedValue::Null)),
        }
    }
}

impl TryFrom<pgrx::AnyArray> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(_val: pgrx::AnyArray) -> Result<Self, Self::Error> {
        Err(TantivyValueError::UnsupportedFromConversion(
            "array".to_string(),
        ))
    }
}

impl TryFrom<pgrx::pg_sys::BOX> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(_val: pgrx::pg_sys::BOX) -> Result<Self, Self::Error> {
        Err(TantivyValueError::UnsupportedFromConversion(
            "box".to_string(),
        ))
    }
}

impl TryFrom<pgrx::pg_sys::Point> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(_val: pgrx::pg_sys::Point) -> Result<Self, Self::Error> {
        Err(TantivyValueError::UnsupportedFromConversion(
            "point".to_string(),
        ))
    }
}

impl TryFrom<pgrx::pg_sys::ItemPointerData> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(_val: pgrx::pg_sys::ItemPointerData) -> Result<Self, Self::Error> {
        Err(TantivyValueError::UnsupportedFromConversion(
            "tid".to_string(),
        ))
    }
}

impl TryFrom<pgrx::Inet> for TantivyValue {
    type Error = TantivyValueError;

    fn try_from(_val: pgrx::Inet) -> Result<Self, Self::Error> {
        Err(TantivyValueError::UnsupportedFromConversion(
            "inet".to_string(),
        ))
    }
}

#[derive(Error, Debug)]
pub enum TantivyValueError {
    #[error(transparent)]
    PgrxNumericError(#[from] pgrx::datum::numeric_support::error::Error),

    #[error(transparent)]
    UuidError(#[from] uuid::Error),

    #[error("Could not generate datetime datum")]
    DateTimeConversionError(#[from] DateTimeConversionError),

    #[error("Failed UUID conversion: {0}")]
    UuidConversionError(String),

    #[error("Could not dereference postgres datum")]
    DatumDeref,

    #[error("Could not deserialize json object")]
    JsonDeserializeError,

    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    #[error(transparent)]
    ParseFloatError(#[from] ParseFloatError),

    #[error("Cannot convert oid of InvalidOid to TantivyValue")]
    InvalidOid,

    #[error("Type {0:?} is not yet supported")]
    UnsupportedOid(Oid),

    #[error("Arrays of type {0:?} are not yet supported")]
    UnsupportedArrayOid(Oid),

    #[error("Cannot convert builtin json oid of {0:?} to TantivyValue")]
    UnsupportedJsonOid(Oid),

    #[error("Cannot convert type {0} to a TantivyValue")]
    UnsupportedFromConversion(String),

    #[error("Cannot convert TantivyValue to type {0}")]
    UnsupportedIntoConversion(String),
}
