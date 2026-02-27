import bauplan


def assert_no_parquet_field_ids(table):
    for field in table.schema:
        if field.metadata and b'PARQUET:field_id' in field.metadata:
            field_id = field.metadata[b'PARQUET:field_id']
            raise AssertionError(f"Field '{field.name}' has PARQUET:field_id metadata with value: {field_id}")


@bauplan.model()
@bauplan.python('3.11')
def parent(
    trips=bauplan.Model(
        'taxi_fhvhv',
        columns=[
            'pickup_datetime',
        ],
        filter="pickup_datetime >= '2022-01-01T00:00:00-05:00' AND pickup_datetime < '2023-01-01T01:00:00-05:00'",
    ),
):
    # the scan should not have field ids either
    assert_no_parquet_field_ids(trips)

    # now let's simulate as if the table somehow ended having field ids added by the user
    # (even though this will likely never happen in practice)

    import pyarrow as pa

    field1 = pa.field('col1', pa.int32(), metadata={b'PARQUET:field_id': b'3'})
    field2 = pa.field('col2', pa.string(), metadata={b'PARQUET:field_id': b'6'})
    field3 = pa.field('col3', pa.float64(), metadata={b'PARQUET:field_id': b'20'})
    schema = pa.schema([field1, field2, field3])
    data = {
        'col1': [1, 2, 3, 4, 5],
        'col2': ['a', 'b', 'c', 'd', 'e'],
        'col3': [1.1, 2.2, 3.3, 4.4, 5.5],
    }
    return pa.table(data, schema=schema)


@bauplan.model()
@bauplan.python('3.11')
def child(
    data=bauplan.Model(
        'parent',
    ),
):
    assert_no_parquet_field_ids(data)
    return data
