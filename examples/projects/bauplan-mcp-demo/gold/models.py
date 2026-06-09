import bauplan


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11')
def customers_by_region_and_nation(
    customers_enriched=bauplan.Model(
        'silver.customers_enriched',
        columns=['customer_key', 'nation_name', 'region_name'],
    ),
):
    """
    Counts customers grouped by region and nation.

    | region_name | nation_name | customer_count |
    |-------------|-------------|----------------|
    | AFRICA      | MOROCCO     | 598            |
    """
    agg = customers_enriched.group_by(['region_name', 'nation_name']).aggregate([
        ('customer_key', 'count'),
    ])
    return agg.rename_columns(['region_name', 'nation_name', 'customer_count'])


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11')
def orders_by_region(
    orders=bauplan.Model(
        'silver.orders',
        columns=['order_key', 'customer_key', 'total_price'],
    ),
    customers_enriched=bauplan.Model(
        'silver.customers_enriched',
        columns=['customer_key', 'region_name'],
    ),
):
    """
    Aggregates order count, total revenue, and average order value per region.

    | region_name | total_orders | total_revenue  | avg_order_value |
    |-------------|--------------|----------------|-----------------|
    | AFRICA      | 300027       | 44996094406.09 | 149973.81       |
    """
    joined = orders.join(customers_enriched, keys='customer_key', join_type='left outer')
    agg = joined.group_by('region_name').aggregate([
        ('order_key', 'count'),
        ('total_price', 'sum'),
        ('total_price', 'mean'),
    ])
    return agg.rename_columns(['region_name', 'total_orders', 'total_revenue', 'avg_order_value'])
