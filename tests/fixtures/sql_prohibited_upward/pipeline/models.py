import bauplan


@bauplan.python("3.12", pip={"polars": "1.37"})
@bauplan.model()
def check_age_in_views(data=bauplan.Model("age")):

    import polars as pl  # noqa

    df = pl.from_arrow(data)
    assert df["n_nulls_age"][0] == 177

    return df.to_arrow()
