import bauplan


@bauplan.python("3.12", pip={"polars": "1.37"})
@bauplan.model(materialization_strategy="REPLACE")
def workshop_average_fares(
    data=bauplan.Model(
        "bauplan.taxi_fhvhv",
        columns=["hvfhs_license_num", "base_passenger_fare", "on_scene_datetime"],
        filter="on_scene_datetime >= '2023-07-01'",
    ),
):
    """Compute the mean base passenger fare for each HVFHV license number, filtered to rides from July 2023 onwards."""
    import polars as pl

    df = pl.from_arrow(data)

    return (
        df.group_by(pl.col("hvfhs_license_num"))
        .agg(pl.col("base_passenger_fare").mean())
        .to_arrow()
    )
