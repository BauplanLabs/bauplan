import time
from typing import Annotated

import bauplan
import pyarrow

from bauplan import (
    Model,
    Parameter,
    TableSchema,
    TimestampMicroUTC,
)


class PickupsSchema(TableSchema):
    """The single column read from taxi_fhvhv and passed down the DAG."""

    pickup_datetime: TimestampMicroUTC


# @bauplan.model(materialization_strategy='REPLACE')
@bauplan.model()
@bauplan.python("3.11")
def parent_1(
    trips: Annotated[
        pyarrow.Table,
        Model(
            "taxi_fhvhv",
            projection_schema=PickupsSchema,
            filter="pickup_datetime >= '2022-12-31T10:00:00-05:00' AND pickup_datetime < '2022-12-31T10:01:00-05:00'",
        ),
    ],
) -> Annotated[pyarrow.Table, PickupsSchema]:
    return trips


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.11")
def parallel_child_1(
    child_1_should_fail: Annotated[bool, Parameter("child_1_should_fail")],
    child_1_should_sleep: Annotated[bool, Parameter("child_1_should_sleep")],
    table_in: Annotated[pyarrow.Table, Model("parent_1")],
) -> Annotated[pyarrow.Table, PickupsSchema]:
    print("child 1 start with row count", table_in.num_rows)
    if child_1_should_sleep:
        print("child 1 sleep")
        time.sleep(1.5)

    if child_1_should_fail:
        raise Exception("child_1 threw exception")

    return table_in


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.11")
def parallel_child_2(
    child_2_should_fail: Annotated[bool, Parameter("child_2_should_fail")],
    child_2_should_sleep: Annotated[bool, Parameter("child_2_should_sleep")],
    table_in: Annotated[pyarrow.Table, Model("parent_1")],
) -> Annotated[pyarrow.Table, PickupsSchema]:
    print("child 2 start with row count", table_in.num_rows)
    if child_2_should_sleep:
        print("child 2 sleep")
        time.sleep(1.5)

    if child_2_should_fail:
        raise Exception("child_2 threw exception")

    return table_in


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.11")
def parallel_child_3(
    child_3_should_fail: Annotated[bool, Parameter("child_3_should_fail")],
    child_3_should_sleep: Annotated[bool, Parameter("child_3_should_sleep")],
    table_in: Annotated[pyarrow.Table, Model("parent_1")],
) -> Annotated[pyarrow.Table, PickupsSchema]:
    print("child 3 start with row count", table_in.num_rows)
    if child_3_should_sleep:
        print("child 3 sleep")
        time.sleep(1.5)

    if child_3_should_fail:
        raise Exception("child_3 threw exception")

    return table_in


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.11")
def parallel_grand_child_1(
    grand_child_1_should_fail: Annotated[bool, Parameter("grand_child_1_should_fail")],
    grand_child_1_should_sleep: Annotated[
        bool, Parameter("grand_child_1_should_sleep")
    ],
    table_in: Annotated[pyarrow.Table, Model("parallel_child_1")],
) -> Annotated[pyarrow.Table, PickupsSchema]:
    print("grand child 1 row count:", table_in.num_rows)
    if grand_child_1_should_sleep:
        print("grand child 1 sleep")
        time.sleep(1.5)

    if grand_child_1_should_fail:
        raise Exception("grand_child_1 threw exception")

    return table_in


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.11")
def parallel_great_grand_child_1(
    table_in: Annotated[pyarrow.Table, Model("parallel_grand_child_1")],
) -> Annotated[pyarrow.Table, PickupsSchema]:
    print("great grand child 1 row count:", table_in.num_rows)
    return table_in
