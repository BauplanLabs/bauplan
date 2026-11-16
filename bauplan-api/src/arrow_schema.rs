//! A json shape for an arrow schema, so we can pass it around without the
//! ipc binary. Every arrow type has its own variant, the names follow the
//! arrow ones and the metadata goes through untouched. The serde feature of
//! arrow-schema puts the rust enums on the wire, this is a shape we own and
//! document.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// An arrow schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ArrowSchema {
    /// The fields of the schema.
    pub fields: Vec<ArrowField>,
    /// The metadata of the schema.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

/// A field of an arrow schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ArrowField {
    /// The field name.
    pub name: String,
    /// The field type.
    #[serde(rename = "type")]
    pub data_type: ArrowType,
    /// Whether the field can be null.
    pub nullable: bool,
    /// Whether the dictionary of a dictionary field is ordered.
    #[serde(default, skip_serializing_if = "is_false")]
    pub dictionary_ordered: bool,
    /// The metadata of the field. The documentation of the field is under
    /// `doc`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

/// A member of a union.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ArrowUnionField {
    /// The type id of the member.
    pub id: i8,
    /// The member field.
    pub field: ArrowField,
}

/// The unit of a time, a timestamp or a duration.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
pub enum ArrowTimeUnit {
    #[serde(rename = "s")]
    Second,
    #[serde(rename = "ms")]
    Millisecond,
    #[serde(rename = "us")]
    Microsecond,
    #[serde(rename = "ns")]
    Nanosecond,
}

/// The unit of an interval.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ArrowIntervalUnit {
    YearMonth,
    DayTime,
    MonthDayNano,
}

/// The layout of a union.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ArrowUnionMode {
    Sparse,
    Dense,
}

/// An arrow type: a string for the types with no parameters, an object with
/// one key for the others.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ArrowType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    #[serde(rename = "uint8")]
    UInt8,
    #[serde(rename = "uint16")]
    UInt16,
    #[serde(rename = "uint32")]
    UInt32,
    #[serde(rename = "uint64")]
    UInt64,
    Float16,
    Float32,
    Float64,
    Timestamp {
        unit: ArrowTimeUnit,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tz: Option<String>,
    },
    Date32,
    Date64,
    Time32(ArrowTimeUnit),
    Time64(ArrowTimeUnit),
    Duration(ArrowTimeUnit),
    Interval(ArrowIntervalUnit),
    Binary,
    FixedSizeBinary(i32),
    LargeBinary,
    BinaryView,
    Utf8,
    LargeUtf8,
    Utf8View,
    List(Box<ArrowField>),
    ListView(Box<ArrowField>),
    FixedSizeList {
        field: Box<ArrowField>,
        size: i32,
    },
    LargeList(Box<ArrowField>),
    LargeListView(Box<ArrowField>),
    Struct(Vec<ArrowField>),
    Union {
        mode: ArrowUnionMode,
        fields: Vec<ArrowUnionField>,
    },
    Dictionary {
        key: Box<ArrowType>,
        value: Box<ArrowType>,
    },
    Decimal32 {
        precision: u8,
        scale: i8,
    },
    Decimal64 {
        precision: u8,
        scale: i8,
    },
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    Decimal256 {
        precision: u8,
        scale: i8,
    },
    Map {
        entries: Box<ArrowField>,
        keys_sorted: bool,
    },
    RunEndEncoded {
        run_ends: Box<ArrowField>,
        values: Box<ArrowField>,
    },
}

fn is_false(value: &bool) -> bool {
    !value
}
