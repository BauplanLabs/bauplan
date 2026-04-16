import bauplan


@bauplan.python("3.12", pip={"polars": "1.37"})
@bauplan.model(materialization_strategy="REPLACE")
def workshop_fare_table(
    workshop_passengers_fare=bauplan.Model(
        "titanic",
        columns=["Name", "Fare"],
    ),
):
    """
    Find fare for each passenger.

    Returned table:
    | Name                        | Fare  | fare_calculated_in |
    |-----------------------------|------|-------------------|
    | Braund, Mr. Owen Harris     | 7.25 | 1912              |
    | Heikkinen, Miss. Laina      | 26.0 | 1912              |
    | Allen, Mr. William Henry    | 13.88 | 1912              |
    | McCarthy, Mr. Timothy J     | 54.0 | 1912              |

    """
    import polars as pl

    return (
        pl.from_arrow(workshop_passengers_fare)
        .select("Name", "Fare")
        .sort("Name")
        .with_columns(
            fare_calculated_in=pl.lit(
                1912
            )  # Their fare was computed in 1912, we will adjust for inflation
        )
        .to_arrow()
    )
