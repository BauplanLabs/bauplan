import bauplan


@bauplan.python("3.12", pip={"polars": "1.37"})
@bauplan.model(materialization_strategy="REPLACE")
def workshop_average_fares(
    data=bauplan.Model("titanic", columns=["Pclass", "Fare"]),
):
    """Compute the mean Titanic fare for each passenger class."""
    import polars as pl

    df = pl.from_arrow(data)

    return df.group_by(pl.col("Pclass")).agg(pl.col("Fare").mean()).to_arrow()
