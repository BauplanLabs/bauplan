import bauplan
from bauplan.standard_expectations import expect_column_all_unique


@bauplan.python("3.12", pip={"polars": "1.37"})
@bauplan.model()
def survival_rate_by_age(
    passengers=bauplan.Model(
        "titanic", columns=["Age", "Survived"], filter="Age IS NOT NULL"
    ),
):
    """
    Bins passengers by age and returns survival rate per bin.

    Returned table:
    | Age | survival_rate |
    |----------|---------------|
    | 0        | 1.0           |
    | ...      | ...           |
    | 19       | 0.3           |
    | 20       | 0.4           |
    """
    import polars as pl # noqa

    df = pl.from_arrow(passengers)
    return (
        df.group_by(pl.col("Age").floor())
        .agg(pl.col("Survived").mean().alias("survival_rate"))
        .sort("Age")
        .to_arrow()
    )


@bauplan.expectation()
@bauplan.python("3.12")
def test_age(data=bauplan.Model("survival_rate_by_age", columns=["Age"])):
    """Validates that the Age bins are unique"""
    return expect_column_all_unique(data, "Age")
