import polars as pl
from tqdm import tqdm
from pathlib import Path
import plotnine as p9
from src.query.utils import collect_year, compute_binomial_interval

COMBINED_DATA_FOLDER = Path("processed_data", "combined")

def query_time(
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

df = query_time(r"choler.*|krim.?koorts", r"graven.?hage|haag.*|s.?hage|grave\.")

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
