//! Converts the columns of `bauplan-api` to an arrow schema, and back.

use std::sync::Arc;

use arrow::datatypes::{
    DataType, Field, FieldRef, Fields, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
};
use arrow::error::ArrowError;
use bauplan_api::arrow_schema::{
    ArrowField, ArrowIntervalUnit, ArrowTimeUnit, ArrowType, ArrowUnionField, ArrowUnionMode,
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

/// The columns that make the arrow `schema`.
pub(crate) fn from_arrow_schema(schema: &Schema) -> Vec<ArrowField> {
    from_fields(schema.fields())
}

fn from_field(field: &Field) -> ArrowField {
    ArrowField {
        name: field.name().clone(),
        data_type: from_data_type(field.data_type()),
        nullable: field.is_nullable(),
        dictionary_ordered: field.dict_is_ordered().unwrap_or_default(),
        metadata: field.metadata().clone().into_iter().collect(),
    }
}

fn from_field_ref(field: &FieldRef) -> Box<ArrowField> {
    Box::new(from_field(field))
}

fn from_fields(fields: &Fields) -> Vec<ArrowField> {
    fields.iter().map(|field| from_field(field)).collect()
}

fn from_data_type(data_type: &DataType) -> ArrowType {
    match data_type {
        DataType::Null => ArrowType::Null,
        DataType::Boolean => ArrowType::Boolean,
        DataType::Int8 => ArrowType::Int8,
        DataType::Int16 => ArrowType::Int16,
        DataType::Int32 => ArrowType::Int32,
        DataType::Int64 => ArrowType::Int64,
        DataType::UInt8 => ArrowType::UInt8,
        DataType::UInt16 => ArrowType::UInt16,
        DataType::UInt32 => ArrowType::UInt32,
        DataType::UInt64 => ArrowType::UInt64,
        DataType::Float16 => ArrowType::Float16,
        DataType::Float32 => ArrowType::Float32,
        DataType::Float64 => ArrowType::Float64,
        DataType::Timestamp(unit, tz) => ArrowType::Timestamp {
            unit: from_time_unit(*unit),
            tz: tz.as_deref().map(str::to_owned),
        },
        DataType::Date32 => ArrowType::Date32,
        DataType::Date64 => ArrowType::Date64,
        DataType::Time32(unit) => ArrowType::Time32(from_time_unit(*unit)),
        DataType::Time64(unit) => ArrowType::Time64(from_time_unit(*unit)),
        DataType::Duration(unit) => ArrowType::Duration(from_time_unit(*unit)),
        DataType::Interval(unit) => ArrowType::Interval(from_interval_unit(*unit)),
        DataType::Binary => ArrowType::Binary,
        DataType::FixedSizeBinary(size) => ArrowType::FixedSizeBinary(*size),
        DataType::LargeBinary => ArrowType::LargeBinary,
        DataType::BinaryView => ArrowType::BinaryView,
        DataType::Utf8 => ArrowType::Utf8,
        DataType::LargeUtf8 => ArrowType::LargeUtf8,
        DataType::Utf8View => ArrowType::Utf8View,
        DataType::List(field) => ArrowType::List(from_field_ref(field)),
        DataType::ListView(field) => ArrowType::ListView(from_field_ref(field)),
        DataType::FixedSizeList(field, size) => ArrowType::FixedSizeList {
            field: from_field_ref(field),
            size: *size,
        },
        DataType::LargeList(field) => ArrowType::LargeList(from_field_ref(field)),
        DataType::LargeListView(field) => ArrowType::LargeListView(from_field_ref(field)),
        DataType::Struct(fields) => ArrowType::Struct(from_fields(fields)),
        DataType::Union(fields, mode) => ArrowType::Union {
            mode: from_union_mode(*mode),
            fields: fields
                .iter()
                .map(|(id, field)| ArrowUnionField {
                    id,
                    field: from_field(field),
                })
                .collect(),
        },
        DataType::Dictionary(key, value) => ArrowType::Dictionary {
            key: Box::new(from_data_type(key)),
            value: Box::new(from_data_type(value)),
        },
        DataType::Decimal32(precision, scale) => ArrowType::Decimal32 {
            precision: *precision,
            scale: *scale,
        },
        DataType::Decimal64(precision, scale) => ArrowType::Decimal64 {
            precision: *precision,
            scale: *scale,
        },
        DataType::Decimal128(precision, scale) => ArrowType::Decimal128 {
            precision: *precision,
            scale: *scale,
        },
        DataType::Decimal256(precision, scale) => ArrowType::Decimal256 {
            precision: *precision,
            scale: *scale,
        },
        DataType::Map(entries, keys_sorted) => ArrowType::Map {
            entries: from_field_ref(entries),
            keys_sorted: *keys_sorted,
        },
        DataType::RunEndEncoded(run_ends, values) => ArrowType::RunEndEncoded {
            run_ends: from_field_ref(run_ends),
            values: from_field_ref(values),
        },
    }
}

fn from_time_unit(unit: TimeUnit) -> ArrowTimeUnit {
    match unit {
        TimeUnit::Second => ArrowTimeUnit::Second,
        TimeUnit::Millisecond => ArrowTimeUnit::Millisecond,
        TimeUnit::Microsecond => ArrowTimeUnit::Microsecond,
        TimeUnit::Nanosecond => ArrowTimeUnit::Nanosecond,
    }
}

fn from_interval_unit(unit: IntervalUnit) -> ArrowIntervalUnit {
    match unit {
        IntervalUnit::YearMonth => ArrowIntervalUnit::YearMonth,
        IntervalUnit::DayTime => ArrowIntervalUnit::DayTime,
        IntervalUnit::MonthDayNano => ArrowIntervalUnit::MonthDayNano,
    }
}

fn from_union_mode(mode: UnionMode) -> ArrowUnionMode {
    match mode {
        UnionMode::Sparse => ArrowUnionMode::Sparse,
        UnionMode::Dense => ArrowUnionMode::Dense,
    }
}
