import bauplan
from bauplan.standard_expectations import (
    expect_column_accepted_values,
    expect_column_all_unique,
    expect_column_mean_greater_than,
    expect_column_no_nulls,
)

_EXPECTED_REGIONS = ['AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'MIDDLE EAST']


@bauplan.expectation()
@bauplan.python('3.11')
def test_customers_by_region_and_nation_shape(
    data=bauplan.Model('customers_by_region_and_nation', columns=['region_name', 'nation_name', 'customer_count'])
):
    return (
        expect_column_no_nulls(data, 'region_name')
        and expect_column_no_nulls(data, 'nation_name')
        and expect_column_no_nulls(data, 'customer_count')
        and expect_column_accepted_values(data, 'region_name', accepted_values=_EXPECTED_REGIONS)
        and expect_column_mean_greater_than(data, 'customer_count', 0)
    )


@bauplan.expectation()
@bauplan.python('3.11')
def test_orders_by_region_shape(
    data=bauplan.Model('orders_by_region', columns=['region_name'])
):
    return (
        expect_column_no_nulls(data, 'region_name')
        and expect_column_all_unique(data, 'region_name')
        and expect_column_accepted_values(data, 'region_name', accepted_values=_EXPECTED_REGIONS)
    )


@bauplan.expectation()
@bauplan.python('3.11')
def test_orders_by_region_metrics(
    data=bauplan.Model('orders_by_region', columns=['total_orders', 'total_revenue', 'avg_order_value'])
):
    return (
        expect_column_mean_greater_than(data, 'total_orders', 0)
        and expect_column_mean_greater_than(data, 'total_revenue', 0)
        and expect_column_mean_greater_than(data, 'avg_order_value', 0)
    )
