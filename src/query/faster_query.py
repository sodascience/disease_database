import polars as pl
from pathlib import Path
from tqdm import tqdm
import plotnine as p9
from scipy.stats import beta
import numpy as np

COMBINED_DATA_FOLDER = Path("processed_data", "combined")


disease_query = r"ty(ph|f)(us|euz.)|febris.?typhoidea|kwaadaardige.*koorts"
location_query = r"amste[rl]da.*|amst\."


def collect_year(disease: str, location: str, year: int = 1830):
    DIS = "(?i)" + disease
    LOC = "(?i)" + location
    return (
        pl.scan_parquet(COMBINED_DATA_FOLDER / f"combined_{year}_{year + 1}.parquet")
        .filter(
            pl.col("newspaper_date").dt.year() >= year,
            pl.col("newspaper_date").dt.year() <= year,
        )
        .with_columns(
            pl.col("article_text").str.contains(DIS).alias("disease"),
            pl.col("article_text").str.contains(LOC).alias("location"),
        )
        .sort(pl.col("newspaper_date"))
        .with_columns(
            pl.col("newspaper_date").dt.year().alias("yr"),
            pl.col("newspaper_date").dt.month().alias("mo"),
        )
        .group_by(["yr", "mo"])
        .agg(
            pl.len().alias("n_total"),
            pl.col("disease").sum().alias("n_disease"),
            pl.col("location").sum().alias("n_location"),
            (pl.col("disease") & pl.col("location")).sum().alias("n_both"),
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


def query_fast(
    disease_query: str,
    location_query: str,
    start_year: int = 1830,
    end_year: int = 1940,
):
    df_list = []
    for yr in tqdm(range(start_year, end_year)):
        try:
            df_list.append(
                collect_year(disease=disease_query, location=location_query, year=yr)
            )
        except Exception as e:
            print(e)

    df = pl.concat(df_list)
    df = df.with_columns(
        compute_binomial_interval(df["n_both"], df["n_location"])
    )
    return df


df = query_fast(disease_query, location_query)

plt = (
    p9.ggplot(
        df.with_columns(
            pl.date(pl.col("yr"), pl.col("mo"), 1).alias("date"),
            (pl.col("n_both") / pl.col("n_location")).alias("y"),
        ),
        p9.aes(x="date", y="y"),
    )
    + p9.geom_ribbon(p9.aes(ymin="lower", ymax="upper"), fill="blue", alpha=0.5)
    + p9.geom_line(color="darkblue")
    + p9.scale_x_date(date_breaks="5 years", date_labels="%Y")
    + p9.theme_linedraw()
    + p9.theme(legend_position="none", axis_text_x=p9.element_text(rotation="vertical"))
    + p9.labs(
        title="Cholera in Amsterdam",
        y="Monthly normalized mentions",
    )
)

plt.show()
