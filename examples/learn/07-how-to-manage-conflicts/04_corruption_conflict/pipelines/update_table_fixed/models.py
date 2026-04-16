import bauplan


@bauplan.python("3.12", pip={"polars": "1.37"})
@bauplan.model(materialization_strategy="APPEND")
def workshop_fare_table(
    passengers_fare=bauplan.Model(
        "titanic",
        columns=["Name", "Fare"],
    ),
    year=bauplan.Parameter("year"),
    inflation_rate=bauplan.Parameter("inflation_rate"),
):
    """
    Append inflation-adjusted fares for the given year using the correct formula.

    The inflation multiplier is (inflation_rate + 1.0)^(year - 1912), which correctly
    compounds the rate over time and causes fares to grow year-on-year.
    """
    import polars as pl

    return (
        pl.from_arrow(passengers_fare)
        .with_columns(
            Fare=pl.col("Fare")
            * pl.lit(
                (
                    float(inflation_rate)
                    # Reinstating the 1.0 to fix the calculation
                    + 1.0
                )
            ).pow(year - 1912),
            fare_calculated_in=pl.lit(int(year)),
        )
        .sort("Name")
        .to_arrow()
    )
