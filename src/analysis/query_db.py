import polars as pl
import plotnine as p9
from pathlib import Path
from scipy import stats
import json


# parse the geojson to extract names and cbscodes for the different regions
with Path("processed_data/database/nl1869.geojson").open("rb") as file:
    geojson = json.load(file)

df_names = pl.DataFrame(
    [
        {
            "location": geom["properties"]["name"],
            "cbscode": geom["properties"]["cbscode"],
        }
        for geom in geojson["objects"]["nld"]["geometries"]
    ]
)

df = (
    pl.read_parquet("processed_data/database/disease_database_v1.2.parquet")
    .join(df_names, on="cbscode", how="left")
    .filter(
        pl.col("location").is_in(["Amsterdam", "Groningen", "Dordrecht"]),
        pl.col("disease").is_in(["cholera", "smallpox"]),
    )
    .with_columns(pl.date(pl.col("year"), pl.col("month"), 1).alias("date"))
)

# compute lower and upper using numpy and Jeffrey's CI for binomial distribution
dists = stats.beta(df["n_both"] + 0.5, df["n_location"] + 0.5)

df =  (
    df.with_columns(
        (pl.col("n_both") / pl.col("n_location")).alias("normalized_mentions")
    )
    .with_columns(lower=dists.ppf(0.025), upper=dists.ppf(0.975))
    .with_columns(
        pl.when(pl.col("n_both") == 0)
        .then(0)
        .otherwise(
            pl.when(pl.col("lower") > pl.col("normalized_mentions"))
            .then(pl.col("normalized_mentions"))
            .otherwise(pl.col("lower"))
        )
        .alias("lower"),
        pl.when(pl.col("normalized_mentions") == 1)
        .then(1)
        .otherwise(
            pl.when(pl.col("upper") < pl.col("normalized_mentions"))
            .then(pl.col("normalized_mentions"))
            .otherwise(pl.col("upper"))
        )
        .alias("upper"),
    )
)

plt = (
    p9.ggplot(
        df,
        p9.aes(
            x="date",
            y="mention_rate",
            ymin="lower",
            ymax="upper",
            color="disease",
            fill="disease",
        ),
    )
    + p9.geom_ribbon(alpha=0.4, color="none")
    + p9.geom_line()
    + p9.scale_x_date(date_breaks="10 years", date_labels="%Y")
    + p9.facet_grid(cols="location", rows="disease", scales="free_y")
    + p9.theme_linedraw()
    + p9.theme(axis_text_x=p9.element_text(rotation=90), legend_position="none")
    + p9.labs(
        title="Disease mentions in Amsterdam, Dordrecht, Groningen, 1830 - 1940",
        subtitle="Data from Delpher newspaper archive, Royal Library",
        x="Year",
        y="Normalized mentions",
    )
)

p9.ggsave(plt, "img/two_diseases_three_cities.png", width=12, height=6, dpi=300)
