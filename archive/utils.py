import polars as pl
from scipy.stats import beta
import numpy as np


def query_disease_location_year(
    df_lazy: pl.LazyFrame, disease: str, location: str, year: int
):
    return (
        df_lazy.filter(
            pl.col("newspaper_date").dt.year() >= year,
            pl.col("newspaper_date").dt.year() <= year,
            pl.col("article_text").str.contains(location),
        )
        .with_columns(
            pl.col("article_text").str.contains(disease).alias("disease"),
        )
        .sort(pl.col("newspaper_date"))
        .with_columns(
            pl.col("newspaper_date").dt.year().alias("yr"),
            pl.col("newspaper_date").dt.month().alias("mo"),
        )
        .group_by(["yr", "mo"])
        .agg(
            pl.len().alias("n_location"),
            pl.col("disease").sum().alias("n_both"),
        )
        .collect()
    )


def compute_binomial_interval(
    successes: pl.Series, tries: pl.Series, alpha: float = 0.95
):
    "Function to compute Jeffrey's binomial interval for a series of successes and tries"
    return pl.DataFrame(
        np.stack(
            [
                beta(s + 0.5, t + 0.5).ppf([(1 - alpha) / 2, 1 - (1 - alpha) / 2])
                for s, t in zip(successes, tries)
            ]
        ),
        schema=["lower", "upper"],
    )
