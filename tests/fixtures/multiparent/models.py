from typing import Annotated

import bauplan
import pyarrow

from bauplan import (
    Model,
    String,
    TableField,
    TableSchema,
)


class Model0Schema(TableSchema):
    """The single column model_0 selects."""

    col_0: String


class Model1Schema(TableSchema):
    """The single column model_1 selects."""

    col_1: String


class Model2Schema(TableSchema):
    """The values of col_0 and col_1, stacked into one column."""

    col_2: Annotated[
        String,
        TableField(doc="One row per parent: the col_0 value, then the col_1 value."),
    ]


class Model3Schema(TableSchema):
    """model_2 with its column renamed."""

    col_3: Annotated[String, TableField(lineage=Model2Schema["col_2"])]


class Model4Schema(TableSchema):
    """Every value seen across all four parents, stacked into one column."""

    col_4: Annotated[
        String,
        TableField(
            doc="col_0, col_1, both col_2 values and both col_3 values, in order."
        ),
    ]


@bauplan.model(materialization_strategy="NONE")
@bauplan.python("3.11")
def model_3(
    model_2: Annotated[pyarrow.Table, Model("model_2", projection_schema=Model2Schema)],
) -> Annotated[pyarrow.Table, Model3Schema]:
    return pyarrow.Table.from_pydict({"col_3": model_2["col_2"]})


@bauplan.model(materialization_strategy="NONE")
@bauplan.python("3.11", pip={"numpy": "2.4.2"})
def model_2(
    model_0: Annotated[pyarrow.Table, Model("model_0", projection_schema=Model0Schema)],
    model_1: Annotated[pyarrow.Table, Model("model_1", projection_schema=Model1Schema)],
) -> Annotated[pyarrow.Table, Model2Schema]:
    val_0 = model_0["col_0"].to_numpy()[0]
    val_1 = model_1["col_1"].to_numpy()[0]
    assert val_0 == "val_0"
    assert val_1 == "val_1"
    return pyarrow.Table.from_pydict({"col_2": pyarrow.array([val_0, val_1])})


@bauplan.model(materialization_strategy="NONE")
@bauplan.python("3.11", pip={"numpy": "2.4.2"})
def model_4(
    model_0: Annotated[pyarrow.Table, Model("model_0", projection_schema=Model0Schema)],
    model_1: Annotated[pyarrow.Table, Model("model_1", projection_schema=Model1Schema)],
    model_2: Annotated[pyarrow.Table, Model("model_2", projection_schema=Model2Schema)],
    model_3: Annotated[pyarrow.Table, Model("model_3", projection_schema=Model3Schema)],
) -> Annotated[pyarrow.Table, Model4Schema]:
    val_0 = model_0["col_0"].to_numpy()[0]
    val_1 = model_1["col_1"].to_numpy()[0]
    val_2_0 = model_2["col_2"].to_numpy()[0]
    val_2_1 = model_2["col_2"].to_numpy()[1]
    val_3_0 = model_3["col_3"].to_numpy()[0]
    val_3_1 = model_3["col_3"].to_numpy()[1]
    assert val_0 == "val_0"
    assert val_1 == "val_1"

    assert val_2_0 == "val_0"
    assert val_2_1 == "val_1"

    # model_3 renames col_2 to col_3, so values should match
    assert val_3_0 == "val_0"
    assert val_3_1 == "val_1"

    all_vals = [
        val_0,
        val_1,
        val_2_0,
        val_2_1,
        val_3_0,
        val_3_1,
    ]
    print(",".join(all_vals))

    return pyarrow.Table.from_pydict({"col_4": pyarrow.array(all_vals)})
