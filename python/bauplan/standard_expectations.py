"""Standard expectations for data validation in Bauplan pipelines."""

import pyarrow as pa


def expect_column_equal_concatenation(
    table: pa.Table,
    target_column: str,
    columns: list,
    separator: str = "",
) -> bool:
    raise NotImplementedError("Only available at runtime in Bauplan")


def expect_column_mean_greater_than(
    table: pa.Table, column_name: str, value: float
) -> bool:
    raise NotImplementedError("Only available at runtime in Bauplan")


def expect_column_mean_greater_or_equal_than(
    table: pa.Table, column_name: str, value: float
) -> bool:
    raise NotImplementedError("Only available at runtime in Bauplan")


def expect_column_mean_smaller_than(
    table: pa.Table, column_name: str, value: float
) -> bool:
    raise NotImplementedError("Only available at runtime in Bauplan")


def expect_column_mean_smaller_or_equal_than(
    table: pa.Table, column_name: str, value: float
) -> bool:
    raise NotImplementedError("Only available at runtime in Bauplan")


def expect_column_some_null(table: pa.Table, column_name: str) -> bool:
    raise NotImplementedError("Only available at runtime in Bauplan")


def expect_column_no_nulls(table: pa.Table, column_name: str) -> bool:
    raise NotImplementedError("Only available at runtime in Bauplan")


def expect_column_all_null(table: pa.Table, column_name: str) -> bool:
    raise NotImplementedError("Only available at runtime in Bauplan")


def expect_column_all_unique(table: pa.Table, column_name: str) -> bool:
    raise NotImplementedError("Only available at runtime in Bauplan")


def expect_column_not_unique(table: pa.Table, column_name: str) -> bool:
    raise NotImplementedError("Only available at runtime in Bauplan")


def expect_column_accepted_values(
    table: pa.Table, column_name: str, accepted_values: list
) -> bool:
    raise NotImplementedError("Only available at runtime in Bauplan")
