import bauplan
from bauplan.standard_expectations import expect_column_mean_greater_than


@bauplan.expectation()
@bauplan.python('3.11')
def test_trip_miles_mean(
    data=bauplan.Model(
        'query_model',
        columns=['trip_miles'],
    ),
):
    return expect_column_mean_greater_than(data, 'trip_miles', 0.0)
