from typing import Annotated

import bauplan
import pyarrow

from bauplan import (
    Float64,
    Int64,
    Model,
    Parameter,
    TableField,
    TableSchema,
)


class TripMilesPushdown(TableSchema):
    """The single column the model reads from taxi_fhvhv."""

    trip_miles: Float64


class ConstantSchema(TableSchema):
    """A fixed table, returned so the model has an output at all."""

    y: Annotated[Int64, TableField(doc="A constant, unrelated to the input.")]


@bauplan.model(materialization_strategy="NONE")
@bauplan.python("3.11")
def params_are_cool_model(
    yayparams: Annotated[
        pyarrow.Table,
        Model(
            "taxi_fhvhv",
            projection_schema=TripMilesPushdown,
            filter="pickup_datetime >= '2023-01-01T00:00:00+00:00' AND pickup_datetime < '2023-01-01T01:00:00+00:00'",
        ),
    ],
    my_secret_value_1: Annotated[str, Parameter("my_secret_key_1")],
    my_secret_value_2: Annotated[str, Parameter("my_secret_key_2")],
    my_vault_string_us_value: Annotated[str, Parameter("my_vault_string_us")],
    my_vault_string_list_us_value: Annotated[str, Parameter("my_vault_string_list_us")],
    my_vault_secure_string_us_value: Annotated[
        str, Parameter("my_vault_secure_string_us")
    ],
    my_vault_override_us_with_eu_value: Annotated[
        str, Parameter("my_vault_override_us_with_eu")
    ],
    my_vault_string_eu_value: Annotated[str, Parameter("my_vault_string_eu")],
    my_vault_string_list_eu_value: Annotated[str, Parameter("my_vault_string_list_eu")],
    my_vault_secure_string_eu_value: Annotated[
        str, Parameter("my_vault_secure_string_eu")
    ],
    my_vault_override_eu_with_us_value: Annotated[
        str, Parameter("my_vault_override_eu_with_us")
    ],
) -> Annotated[pyarrow.Table, ConstantSchema]:
    print(f"my_secret_key_1={my_secret_value_1}")
    print(f"my_secret_key_2={my_secret_value_2}")
    print(f"my_vault_string_us={my_vault_string_us_value}")
    print(f"my_vault_string_list_us={my_vault_string_list_us_value}")
    print(f"my_vault_secure_string_us={my_vault_secure_string_us_value}")
    print(f"my_vault_override_us_with_eu={my_vault_override_us_with_eu_value}")
    print(f"my_vault_string_eu={my_vault_string_eu_value}")
    print(f"my_vault_string_list_eu={my_vault_string_list_eu_value}")
    print(f"my_vault_secure_string_eu={my_vault_secure_string_eu_value}")
    print(f"my_vault_override_eu_with_us={my_vault_override_eu_with_us_value}")

    return pyarrow.Table.from_pydict({"y": [1, 2, 3]})
