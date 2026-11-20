//! The arrow schema of the json shape that `bauplan-api` puts on the wire.

use std::sync::Arc;

use arrow::datatypes::{
    DataType, Field, FieldRef, Fields, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
};
use arrow::error::ArrowError;
use bauplan_api::arrow_schema::{
    ArrowField, ArrowIntervalUnit, ArrowTimeUnit, ArrowType, ArrowUnionMode,
};

/// The arrow schema made of `columns`.
pub(crate) fn to_arrow_schema(columns: Vec<ArrowField>) -> Result<Schema, ArrowError> {
    Ok(Schema::new(to_fields(columns)?))
}

fn to_field(field: ArrowField) -> Result<Field, ArrowError> {
    let data_type = to_data_type(field.data_type)?;
    Ok(Field::new(field.name, data_type, field.nullable)
        .with_dict_is_ordered(field.dictionary_ordered)
        .with_metadata(field.metadata.into_iter().collect()))
}

fn to_field_ref(field: ArrowField) -> Result<FieldRef, ArrowError> {
    Ok(Arc::new(to_field(field)?))
}

fn to_fields(fields: Vec<ArrowField>) -> Result<Fields, ArrowError> {
    fields.into_iter().map(to_field).collect()
}

fn to_data_type(arrow_type: ArrowType) -> Result<DataType, ArrowError> {
    Ok(match arrow_type {
        ArrowType::Null => DataType::Null,
        ArrowType::Boolean => DataType::Boolean,
        ArrowType::Int8 => DataType::Int8,
        ArrowType::Int16 => DataType::Int16,
        ArrowType::Int32 => DataType::Int32,
        ArrowType::Int64 => DataType::Int64,
        ArrowType::UInt8 => DataType::UInt8,
        ArrowType::UInt16 => DataType::UInt16,
        ArrowType::UInt32 => DataType::UInt32,
        ArrowType::UInt64 => DataType::UInt64,
        ArrowType::Float16 => DataType::Float16,
        ArrowType::Float32 => DataType::Float32,
        ArrowType::Float64 => DataType::Float64,
        ArrowType::Timestamp { unit, tz } => {
            DataType::Timestamp(to_time_unit(unit), tz.map(Arc::from))
        }
        ArrowType::Date32 => DataType::Date32,
        ArrowType::Date64 => DataType::Date64,
        ArrowType::Time32(unit) => DataType::Time32(to_time_unit(unit)),
        ArrowType::Time64(unit) => DataType::Time64(to_time_unit(unit)),
        ArrowType::Duration(unit) => DataType::Duration(to_time_unit(unit)),
        ArrowType::Interval(unit) => DataType::Interval(to_interval_unit(unit)),
        ArrowType::Binary => DataType::Binary,
        ArrowType::FixedSizeBinary(size) => DataType::FixedSizeBinary(size),
        ArrowType::LargeBinary => DataType::LargeBinary,
        ArrowType::BinaryView => DataType::BinaryView,
        ArrowType::Utf8 => DataType::Utf8,
        ArrowType::LargeUtf8 => DataType::LargeUtf8,
        ArrowType::Utf8View => DataType::Utf8View,
        ArrowType::List(field) => DataType::List(to_field_ref(*field)?),
        ArrowType::ListView(field) => DataType::ListView(to_field_ref(*field)?),
        ArrowType::FixedSizeList { field, size } => {
            DataType::FixedSizeList(to_field_ref(*field)?, size)
        }
        ArrowType::LargeList(field) => DataType::LargeList(to_field_ref(*field)?),
        ArrowType::LargeListView(field) => DataType::LargeListView(to_field_ref(*field)?),
        ArrowType::Struct(fields) => DataType::Struct(to_fields(fields)?),
        ArrowType::Union { mode, fields } => {
            let ids: Vec<i8> = fields.iter().map(|f| f.id).collect();
            let fields = fields
                .into_iter()
                .map(|f| to_field(f.field))
                .collect::<Result<Vec<_>, _>>()?;
            DataType::Union(UnionFields::try_new(ids, fields)?, to_union_mode(mode))
        }
        ArrowType::Dictionary { key, value } => DataType::Dictionary(
            Box::new(to_data_type(*key)?),
            Box::new(to_data_type(*value)?),
        ),
        ArrowType::Decimal32 { precision, scale } => DataType::Decimal32(precision, scale),
        ArrowType::Decimal64 { precision, scale } => DataType::Decimal64(precision, scale),
        ArrowType::Decimal128 { precision, scale } => DataType::Decimal128(precision, scale),
        ArrowType::Decimal256 { precision, scale } => DataType::Decimal256(precision, scale),
        ArrowType::Map {
            entries,
            keys_sorted,
        } => DataType::Map(to_field_ref(*entries)?, keys_sorted),
        ArrowType::RunEndEncoded { run_ends, values } => {
            DataType::RunEndEncoded(to_field_ref(*run_ends)?, to_field_ref(*values)?)
        }
        ArrowType::Unknown(value) => {
            return Err(ArrowError::SchemaError(format!("unknown type {value}")));
        }
    })
}

fn to_time_unit(unit: ArrowTimeUnit) -> TimeUnit {
    match unit {
        ArrowTimeUnit::Second => TimeUnit::Second,
        ArrowTimeUnit::Millisecond => TimeUnit::Millisecond,
        ArrowTimeUnit::Microsecond => TimeUnit::Microsecond,
        ArrowTimeUnit::Nanosecond => TimeUnit::Nanosecond,
    }
}

fn to_interval_unit(unit: ArrowIntervalUnit) -> IntervalUnit {
    match unit {
        ArrowIntervalUnit::YearMonth => IntervalUnit::YearMonth,
        ArrowIntervalUnit::DayTime => IntervalUnit::DayTime,
        ArrowIntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
    }
}

fn to_union_mode(mode: ArrowUnionMode) -> UnionMode {
    match mode {
        ArrowUnionMode::Sparse => UnionMode::Sparse,
        ArrowUnionMode::Dense => UnionMode::Dense,
    }
}
