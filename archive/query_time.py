import polars as pl
from tqdm import tqdm
from pathlib import Path
import plotnine as p9
from utils import query_disease_location_year, compute_binomial_interval

BASE_PATH = Path(".")
# optional if using external disk: BASE_PATH = Path("E:/", "disease_database")
COMBINED_DATA_FOLDER = Path("processed_data", "combined")


def query_time(
    disease_query: str,
    location_query: str,
    start_year: int = 1830,
    end_year: int = 1940,
):
    df_list = []
    for yr in tqdm(range(start_year, end_year)):
        text_df = pl.scan_parquet(
            BASE_PATH / COMBINED_DATA_FOLDER / f"combined_{yr}_{yr + 1}.parquet"
        )
        try:
            df_list.append(
                query_disease_location_year(
                    df_lazy=text_df,
                    disease="(?i)" + disease_query,
                    location="(?i)" + location_query,
                    year=yr,
                )
            )
        except Exception as e:
            print(e)

    df = pl.concat(df_list)
    df = df.with_columns(compute_binomial_interval(df["n_both"], df["n_location"]))
    return df


df = query_time(
    r"choler.*|krim.?koorts", r"graven.?hage|haag.*|s.?hage|grave\.", end_year=1870
)

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
        title="Cholera in The Hague",
        y="Monthly normalized mentions",
    )
)

plt.show()
