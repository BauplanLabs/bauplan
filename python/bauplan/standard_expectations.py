"""
This module contains standard expectations that can be used to test data artifacts in a
Bauplan pipeline. Using these expectations instead of hand-made ones will make your
pipeline easier to maintain, and significantly faster and more memory-efficient.

Each function returns a boolean, so that the wrapping function can assert or
print out messages in case of failure.
"""

from typing import Any

import pyarrow as pa
import pyarrow.compute as pc


def _calculate_string_concatenation(
    table: pa.Table, columns: list, separator: str = ""
) -> Any:
    """
    Given a pyarrow table and a list of column names, concatenate the columns into a new column.
    The caller of the function can then use the column to compare it with an existing column or add it.

    The function does attempt type conversion to string if a column is not of type pa.string().

    """
    fields = []
    for column in columns:
        fields.append(
            pc.cast(table[column], pa.string())
            if table[column].type != pa.string()
            else table[column]
        )
    # last item needs to be the separator
    fields.append(separator)

    return pc.binary_join_element_wise(*fields)


def _calculate_column_mean(table: pa.Table, column_name: str) -> float:
    """
    Use built-in pyarrow compute functions to calculate the mean of a column.
    """
    return pc.mean(table[column_name]).as_py()


def expect_column_equal_concatenation(
    table: pa.Table,
    target_column: str,
    columns: list,
    separator: str = "",
) -> bool:
    """
    Expect the target column to be equal to the concatenation of the columns in the list.

    If the columns are not of type pa.string(), the function will attempt to convert them to string.
    If a custom separator is needed (default: the empty string), it can be passed as an argument.

    Parameters:
        table: the pyarrow table to test.
        target_column: the column to compare with the concatenation of the columns.
        columns: the list of columns to concatenate.
        separator: the separator to use when concatenating the columns.

    Returns:
        a boolean.

    ## Examples

    ```python
    import bauplan
    from bauplan.standard_expectations import expect_column_equal_concatenation

    @bauplan.expectation()
    @bauplan.python('3.11')
    def test_full_name_is_concat(
        data=bauplan.Model('customers'),
    ):
        assert expect_column_equal_concatenation(
            data,
            target_column='full_name',
            columns=['first_name', 'last_name'],
            separator=' ',
        )
        return True
    ```
    """
    # produce a new column that is the concatenation of the columns in the list
    # and compare the new column with the target column
    return pc.all(
        pc.equal(
            _calculate_string_concatenation(table, columns, separator),
            table[target_column],
        )
    ).as_py()


def expect_column_mean_greater_than(
    table: pa.Table, column_name: str, value: float
) -> bool:
    """
    Expect the mean of a column to be greater than the supplied value.

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to calculate the mean of.
        value: the value to compare the mean with.

    Returns:
        a boolean.

    ## Examples

    ```python
    import bauplan
    from bauplan.standard_expectations import expect_column_mean_greater_than

    @bauplan.expectation()
    @bauplan.python('3.11')
    def test_positive_avg_fare(
        data=bauplan.Model('normalized_taxi_trips'),
    ):
        assert expect_column_mean_greater_than(data, 'fare_amount', 0.0)
        return True
    ```
    """
    mean_ = _calculate_column_mean(table, column_name)
    return mean_ > value


def expect_column_mean_greater_or_equal_than(
    table: pa.Table, column_name: str, value: float
) -> bool:
    """
    Expect the mean of a column to be equal or greater than the supplied value.

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to calculate the mean of.
        value: the value to compare the mean with.

    Returns:
        a boolean.

    ## Examples

    ```python
    import bauplan
    from bauplan.standard_expectations import expect_column_mean_greater_or_equal_than

    @bauplan.expectation()
    @bauplan.python('3.11')
    def test_avg_rating_at_least_three(
        data=bauplan.Model('product_reviews'),
    ):
        assert expect_column_mean_greater_or_equal_than(data, 'rating', 3.0)
        return True
    ```
    """
    mean_ = _calculate_column_mean(table, column_name)
    return mean_ >= value


def expect_column_mean_smaller_than(
    table: pa.Table, column_name: str, value: float
) -> bool:
    """
    Expect the mean of a column to be smaller than the supplied value.

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to calculate the mean of.
        value: the value to compare the mean with.

    Returns:
        a boolean.

    ## Examples

    ```python
    import bauplan
    from bauplan.standard_expectations import expect_column_mean_smaller_than

    @bauplan.expectation()
    @bauplan.python('3.11')
    def test_error_rate_below_five_percent(
        data=bauplan.Model('request_logs'),
    ):
        assert expect_column_mean_smaller_than(data, 'error_rate', 0.05)
        return True
    ```
    """
    mean_ = _calculate_column_mean(table, column_name)
    return mean_ < value


def expect_column_mean_smaller_or_equal_than(
    table: pa.Table, column_name: str, value: float
) -> bool:
    """
    Expect the mean of a column to be equal or smaller than the supplied value.

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to calculate the mean of.
        value: the value to compare the mean with.

    Returns:
        a boolean.

    ## Examples

    ```python
    import bauplan
    from bauplan.standard_expectations import expect_column_mean_smaller_or_equal_than

    @bauplan.expectation()
    @bauplan.python('3.11')
    def test_avg_latency_within_slo(
        data=bauplan.Model('request_logs'),
    ):
        assert expect_column_mean_smaller_or_equal_than(data, 'latency_ms', 250.0)
        return True
    ```
    """
    mean_ = _calculate_column_mean(table, column_name)
    return mean_ <= value


def _column_nulls(table: pa.Table, column_name: str) -> int:
    return table[column_name].null_count


def expect_column_some_null(table: pa.Table, column_name: str) -> bool:
    """
    Expect the column to have at least one null.

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to test.

    Returns:
        a boolean.

    ## Examples

    ```python
    import bauplan
    from bauplan.standard_expectations import expect_column_some_null

    @bauplan.expectation()
    @bauplan.python('3.11')
    def test_optional_notes_has_nulls(
        data=bauplan.Model('customers'),
    ):
        assert expect_column_some_null(data, 'optional_notes')
        return True
    ```
    """
    return _column_nulls(table, column_name) > 0


def expect_column_no_nulls(table: pa.Table, column_name: str) -> bool:
    """
    Expect the column to have no null values.

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to test.

    Returns:
        a boolean.

    ## Examples

    ```python
    import bauplan
    from bauplan.standard_expectations import expect_column_no_nulls

    @bauplan.expectation()
    @bauplan.python('3.11')
    def test_no_null_pickup_datetime(
        data=bauplan.Model('normalized_taxi_trips'),
    ):
        column = 'pickup_datetime'
        ok = expect_column_no_nulls(data, column)
        assert ok, f'expected {column} to have no nulls'
        return ok
    ```
    """
    return _column_nulls(table, column_name) == 0


def expect_column_all_null(table: pa.Table, column_name: str) -> bool:
    """
    Expect the column to have all null values.

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to test.

    Returns:
        a boolean.

    ## Examples

    ```python
    import bauplan
    from bauplan.standard_expectations import expect_column_all_null

    @bauplan.expectation()
    @bauplan.python('3.11')
    def test_legacy_user_id_fully_blanked(
        data=bauplan.Model('customers'),
    ):
        assert expect_column_all_null(data, 'legacy_user_id')
        return True
    ```
    """
    return _column_nulls(table, column_name) == table[column_name].length()


def _column_unique(table: pa.Table, column_name: str) -> int:
    return len(pc.unique(table[column_name]))


def expect_column_all_unique(table: pa.Table, column_name: str) -> bool:
    """
    Expect the column to have all unique values (i.e. no duplicates).

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to test.

    Returns:
        a boolean.

    ## Examples

    ```python
    import bauplan
    from bauplan.standard_expectations import expect_column_all_unique

    @bauplan.expectation()
    @bauplan.python('3.11')
    def test_trip_id_is_unique(
        data=bauplan.Model('normalized_taxi_trips'),
    ):
        assert expect_column_all_unique(data, 'trip_id')
        return True
    ```
    """
    return _column_unique(table, column_name) == len(table[column_name])


def expect_column_not_unique(table: pa.Table, column_name: str) -> bool:
    """
    Expect the column to have at least one duplicate value.

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to test.

    Returns:
        a boolean.

    ## Examples

    ```python
    import bauplan
    from bauplan.standard_expectations import expect_column_not_unique

    @bauplan.expectation()
    @bauplan.python('3.11')
    def test_customer_id_repeats_in_orders(
        data=bauplan.Model('orders'),
    ):
        assert expect_column_not_unique(data, 'customer_id')
        return True
    ```
    """
    return _column_unique(table, column_name) < len(table[column_name])


def _column_accepted_values(
    table: pa.Table, column_name: str, accepted_values: list
) -> Any:
    return pc.all(pc.is_in(table[column_name], pa.array(accepted_values))).as_py()


def expect_column_accepted_values(
    table: pa.Table, column_name: str, accepted_values: list
) -> bool:
    """
    Expect all values in the column to come from the list of accepted values.

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to test.
        accepted_values: the list of accepted values.

    Returns:
        a boolean.

    ## Examples

    ```python
    import bauplan
    from bauplan.standard_expectations import expect_column_accepted_values

    @bauplan.expectation()
    @bauplan.python('3.11')
    def test_order_status_domain(
        data=bauplan.Model('orders'),
    ):
        assert expect_column_accepted_values(
            data,
            'order_status',
            ['pending', 'paid', 'shipped', 'cancelled'],
        )
        return True
    ```
    """

    return _column_accepted_values(table, column_name, accepted_values)
