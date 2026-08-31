"""Bauplan medallion pipeline for telemetry data.

DAG:
    [bauplan.telemetry_bronze] -> [signal_clean] -> [signal_summary]

Bronze -> Silver: parse, clean, deduplicate raw sensor readings.
Silver -> Gold: aggregate per-sensor hourly statistics.
"""

from typing import Annotated

import bauplan
import pyarrow

from bauplan import (
    Float64,
    Int64,
    Model,
    String,
    TableField,
    TableSchema,
    TimestampMicro,
    TimestampMicroUTC,
)


class BronzeColumns(TableSchema):
    """The projection of telemetry_bronze the Silver transformation reads."""

    dateTime: TimestampMicroUTC
    sensors: String
    value: Annotated[String, TableField(doc="Raw reading, still unparsed text.")]


class SignalCleanSchema(TableSchema):
    """Silver: one parsed reading per (signal, dateTime)."""

    dateTime: Annotated[
        TimestampMicro,
        TableField(
            doc="Reading time, shifted to UTC and stored without a timezone.",
        ),
    ]
    signal: Annotated[String, TableField(doc="Sensor name; the bronze `sensors` column.")]
    value: Annotated[Float64, TableField(doc="Reading parsed out of the raw text.")]
    value_original: Annotated[
        Float64, TableField(doc="The parsed reading before any downstream correction.")
    ]


@bauplan.model()
@bauplan.python("3.11", pip={"duckdb": "1.1.3"})
def signal_clean(
    bronze_data: Annotated[
        pyarrow.Table,
        Model("telemetry_bronze", projection_schema=BronzeColumns),
    ],
) -> Annotated[pyarrow.Table, SignalCleanSchema]:
    """Bronze -> Silver: clean and deduplicate raw telemetry readings.

    - Column mapping: sensors -> signal
    - Type casting: value (string) -> value (float)
    - Null removal
    - Deduplication by (signal, dateTime), keeping highest value

    | dateTime            | signal   | value | value_original |
    |---------------------|----------|-------|----------------|
    | 2026-02-07 05:27:44 | sensor_5 | 65.94 | 65.94          |
    """
    import duckdb

    con = duckdb.connect()
    con.register("bronze_raw", bronze_data)

    result = con.execute(
        """
        WITH parsed AS (
            SELECT
                dateTime AT TIME ZONE 'UTC' AS dateTime,
                sensors AS signal,
                TRY_CAST(value AS DOUBLE) AS value,
                TRY_CAST(value AS DOUBLE) AS value_original
            FROM bronze_raw
        ),
        filtered AS (
            SELECT dateTime, signal, value, value_original
            FROM parsed
            WHERE value IS NOT NULL
              AND dateTime IS NOT NULL
              AND signal IS NOT NULL
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY signal, dateTime
                    ORDER BY value DESC
                ) AS rn
            FROM filtered
        )
        SELECT dateTime, signal, value, value_original
        FROM ranked
        WHERE rn = 1
        """,
    ).arrow()

    return result


class SignalReadingColumns(TableSchema):
    """The projection of signal_clean the Gold aggregation reads."""

    dateTime: TimestampMicro
    signal: String
    value: Float64


class SignalSummarySchema(TableSchema):
    """Gold: hourly statistics per sensor."""

    hour: Annotated[
        TimestampMicro, TableField(doc="Reading time truncated to the hour.")
    ]
    signal: String
    reading_count: Annotated[
        Int64, TableField(doc="Number of readings for the sensor in the hour.")
    ]
    avg_value: Annotated[
        Float64, TableField(doc="Mean reading for the hour, rounded to 2 decimals.")
    ]
    min_value: Annotated[Float64, TableField(doc="Smallest reading in the hour.")]
    max_value: Annotated[Float64, TableField(doc="Largest reading in the hour.")]


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.11", pip={"polars": "1.38.1"})
def signal_summary(
    data: Annotated[
        pyarrow.Table,
        Model("signal_clean", projection_schema=SignalReadingColumns),
    ],
) -> Annotated[pyarrow.Table, SignalSummarySchema]:
    """Silver -> Gold: hourly statistics per sensor.

    Aggregates clean readings into per-sensor, per-hour summaries
    with count, mean, min, and max values.

    | hour                | signal   | reading_count | avg_value | min_value | max_value |
    |---------------------|----------|---------------|-----------|-----------|-----------|
    | 2026-02-07 05:00:00 | sensor_5 | 12            | 54.3      | 21.1      | 89.7      |
    """
    import polars as pl

    df = pl.from_arrow(data)

    result = (
        df.with_columns(pl.col("dateTime").dt.truncate("1h").alias("hour"))
        .group_by("hour", "signal")
        .agg(
            # `count()` is UInt32, which Iceberg has no type for: cast it to Int64.
            pl.col("value").count().cast(pl.Int64).alias("reading_count"),
            pl.col("value").mean().round(2).alias("avg_value"),
            pl.col("value").min().alias("min_value"),
            pl.col("value").max().alias("max_value"),
        )
        .sort("hour", "signal")
    )

    return result.to_arrow()
