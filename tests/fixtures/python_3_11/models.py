import platform
from typing import Annotated

import bauplan
import pyarrow

from bauplan import (
    Int64,
    Model,
    TableSchema,
)


class LocationSchema(TableSchema):
    """The distinct pickup locations query_model selects from taxi_fhvhv."""

    location_id: Int64


@bauplan.model(
    materialization_strategy="NONE",
)
@bauplan.python()
def normalize_data(
    data: Annotated[pyarrow.Table, Model("query_model")],
) -> Annotated[pyarrow.Table, LocationSchema]:
    print(f"Running on python {platform.python_version()}")
    return data
