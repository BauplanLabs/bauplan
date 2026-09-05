from typing import Annotated

import bauplan
import pyarrow
from bauplan.standard_expectations import expect_column_all_unique


class SurvivalColumns(bauplan.TableSchema):
    """A projection of 2 columns for survival rate analysis."""

    Age: Annotated[
        bauplan.Float64,
        bauplan.TableField(
            doc="A passenger's age in years according to the solar calendar.",
            lineage="titanic['Age']",
        ),
    ]

    Survived: Annotated[
        bauplan.Int64,
        bauplan.TableField(
            doc="Indicator of passenger's survival; may be 1 (survived) or 0 (did not).",
            lineage="titanic['Survived']",
        ),
    ]


class SurvivalRateSchema(bauplan.TableSchema):
    """Analysis result of passenger survival rate by age (grouped by year)."""

    Age: Annotated[
        bauplan.Float64,
        bauplan.TableField(
            doc="Passenger age group by year.",
            lineage=SurvivalColumns["Age"],
        ),
    ]

    survival_rate: Annotated[
        bauplan.Float64,
        bauplan.TableField(
            doc="Likelihood of passenger survival for a given age group (by year).",
            lineage=SurvivalColumns["Survived"],
        ),
    ]


@bauplan.python("3.12", pip={"polars": "1.37"})
@bauplan.model()
def survival_rate_by_age(
    passengers: Annotated[
        pyarrow.Table,
        bauplan.Model(
            "titanic",
            projection_schema=SurvivalColumns,
            filter="Age IS NOT NULL",
        ),
    ],
) -> Annotated[pyarrow.Table, SurvivalRateSchema]:
    """
    Bins passengers by age and returns survival rate per bin.

    Returned table:
    |   Age    | survival_rate |
    |----------|---------------|
    | 0        | 1.0           |
    | ...      | ...           |
    | 19       | 0.3           |
    | 20       | 0.4           |
    """

    import polars as pl  # ty: ignore[unresolved-import]

    df = pl.DataFrame(passengers)
    return (
        df.group_by(pl.col("Age").floor())
        .agg(pl.col("Survived").mean().alias("survival_rate"))
        .sort("Age")
        .to_arrow()
    )


class PassengerAgeGroup(bauplan.TableSchema):
    """Projection schema for age group data."""

    Age: Annotated[
        bauplan.Float64,
        bauplan.TableField(lineage=SurvivalRateSchema["Age"]),
    ]


@bauplan.expectation()
@bauplan.python("3.12")
def test_age(
    data: Annotated[
        pyarrow.Table,
        bauplan.Model("survival_rate_by_age", projection_schema=PassengerAgeGroup),
    ],
) -> bool:
    """Validates that the Age bins are unique"""
    return expect_column_all_unique(data, "Age")
