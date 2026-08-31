import pyarrow
import bauplan

from typing import Annotated
from bauplan import (
    TableSchema,
    Model,
    Float64,
)


# BadBaseSchema inherits from an unrecognized base class.
# The parser only recognizes TableSchema.
class CustomBase: ...


class BadBaseSchema(CustomBase):
    """This schema has the wrong base class and should not be registered."""

    trip_miles: Float64


class TaxiPushdown(TableSchema):
    trip_miles: Float64


@bauplan.model()
def wrong_base_model(
    taxi_trips: Annotated[
        pyarrow.Table,
        Model(
            filter="PULocationID = 138",
            name="taxi_fhvhv",
            projection_schema=TaxiPushdown,
        ),
    ],
) -> Annotated[pyarrow.Table, BadBaseSchema]:
    return taxi_trips.slice(length=5)
