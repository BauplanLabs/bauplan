import bauplan


@bauplan.python("3.12", pip={"polars": "1.37"})
@bauplan.model(materialization_strategy="REPLACE")
def workshop_average_fares(
    data=bauplan.Model("bauplan.titanic", columns=["Pclass", "Fare", "Sex"]),
):
    """Compute the mean Titanic fare for each passenger class."""
    import polars as pl

    df = pl.from_arrow(data)

    return (
        df.group_by(pl.col("Pclass"), pl.col("Sex"))
        .agg(pl.col("Fare").mean().cast(float), pl.len().alias("n_passengers"))
        .to_arrow()
    )


@bauplan.expectation()
@bauplan.python("3.12", pip={"pyarrow": "23.0"})
def test_fare(data=bauplan.Model("workshop_average_fares", columns=["Fare"])):
    """Validates that the Fare is a float"""

    import pyarrow as pa

    is_valid = data.schema.field("Fare").type == pa.float32()
    assert is_valid, (
        f"Fare column has the wrong type. Expected {pa.float32()}, found {data.schema.field('Fare').type}"
    )
    return is_valid
