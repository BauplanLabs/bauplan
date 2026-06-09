import bauplan
from bauplan.standard_expectations import (
    expect_column_accepted_values,
    expect_column_all_unique,
    expect_column_no_nulls,
)

_EXPECTED_REGIONS = ['AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'MIDDLE EAST']


@bauplan.expectation()
@bauplan.python('3.11')
def test_orders_keys(
    data=bauplan.Model('orders', columns=['order_key', 'customer_key'])
):
    return (
        expect_column_no_nulls(data, 'order_key')
        and expect_column_no_nulls(data, 'customer_key')
        and expect_column_all_unique(data, 'order_key')
    )


@bauplan.expectation()
@bauplan.python('3.11')
def test_customers_keys(
    data=bauplan.Model('customers', columns=['customer_key', 'nation_key'])
):
    return (
        expect_column_no_nulls(data, 'customer_key')
        and expect_column_no_nulls(data, 'nation_key')
        and expect_column_all_unique(data, 'customer_key')
    )


@bauplan.expectation()
@bauplan.python('3.11')
def test_nations_keys(
    data=bauplan.Model('nations', columns=['nation_key', 'nation_name'])
):
    return (
        expect_column_no_nulls(data, 'nation_key')
        and expect_column_no_nulls(data, 'nation_name')
        and expect_column_all_unique(data, 'nation_key')
    )


@bauplan.expectation()
@bauplan.python('3.11')
def test_regions_keys(
    data=bauplan.Model('regions', columns=['region_key', 'region_name'])
):
    return (
        expect_column_no_nulls(data, 'region_key')
        and expect_column_no_nulls(data, 'region_name')
        and expect_column_all_unique(data, 'region_key')
    )


@bauplan.expectation()
@bauplan.python('3.11')
def test_customers_enriched_region(
    data=bauplan.Model('customers_enriched', columns=['customer_key', 'region_name'])
):
    return (
        expect_column_no_nulls(data, 'customer_key')
        and expect_column_no_nulls(data, 'region_name')
        and expect_column_accepted_values(data, 'region_name', accepted_values=_EXPECTED_REGIONS)
    )
