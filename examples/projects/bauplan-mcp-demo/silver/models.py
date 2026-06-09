import bauplan


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11')
def orders(
    source=bauplan.Model(
        'tpch_1.orders',
        columns=['o_orderkey', 'o_custkey', 'o_totalprice'],
    )
):
    """
    Renames raw TPCH orders to snake_case column names.

    | order_key | customer_key | total_price |
    |-----------|--------------|-------------|
    | 1         | 370          | 172799.49   |
    """
    return (
        source
        .select(['o_orderkey', 'o_custkey', 'o_totalprice'])
        .rename_columns(['order_key', 'customer_key', 'total_price'])
    )


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11')
def customers(
    source=bauplan.Model(
        'tpch_1.customer',
        columns=['c_custkey', 'c_nationkey'],
    )
):
    """
    Renames raw TPCH customer to snake_case column names.

    | customer_key | nation_key |
    |--------------|------------|
    | 1            | 15         |
    """
    return (
        source
        .select(['c_custkey', 'c_nationkey'])
        .rename_columns(['customer_key', 'nation_key'])
    )


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11')
def nations(
    source=bauplan.Model(
        'tpch_1.nation',
        columns=['n_nationkey', 'n_name', 'n_regionkey'],
    )
):
    """
    Renames raw TPCH nation to snake_case column names.

    | nation_key | nation_name | region_key |
    |------------|-------------|------------|
    | 0          | ALGERIA     | 0          |
    """
    return (
        source
        .select(['n_nationkey', 'n_name', 'n_regionkey'])
        .rename_columns(['nation_key', 'nation_name', 'region_key'])
    )


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11')
def regions(
    source=bauplan.Model(
        'tpch_1.region',
        columns=['r_regionkey', 'r_name'],
    )
):
    """
    Renames raw TPCH region to snake_case column names.

    | region_key | region_name |
    |------------|-------------|
    | 0          | AFRICA      |
    """
    return (
        source
        .select(['r_regionkey', 'r_name'])
        .rename_columns(['region_key', 'region_name'])
    )


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11')
def customers_enriched(
    customers=bauplan.Model(
        'customers',
        columns=['customer_key', 'nation_key'],
    ),
    nations=bauplan.Model(
        'nations',
        columns=['nation_key', 'nation_name', 'region_key'],
    ),
    regions=bauplan.Model(
        'regions',
        columns=['region_key', 'region_name'],
    ),
):
    """
    Joins customers with their nation and region for use across gold models.

    | customer_key | nation_key | nation_name | region_key | region_name |
    |--------------|------------|-------------|------------|-------------|
    | 1            | 15         | MOROCCO     | 0          | AFRICA      |
    """
    with_nation = customers.join(nations, keys='nation_key', join_type='left outer')
    with_region = with_nation.join(regions, keys='region_key', join_type='left outer')
    return with_region.select(['customer_key', 'nation_key', 'nation_name', 'region_key', 'region_name'])
