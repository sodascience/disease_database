import polars as pl
from tqdm import tqdm
from pathlib import Path
import plotnine as p9
from src.query.utils import collect_year, compute_binomial_interval

COMBINED_DATA_FOLDER = Path("processed_data", "combined")
LOCATION_EXCEL_FILE = Path("raw_data", "manual_input", "municipalities_1869.xlsx")


def query_map(
    disease_query: str,
    year: int,
):
    # iterate over each municipality
    muni_df = pl.read_excel(LOCATION_EXCEL_FILE)
    df_list = []
    for row in tqdm(muni_df.iter_rows(named=True), total=len(muni_df)):
        try:
            df_list.append(
                collect_year(disease=disease_query, location=row["Regex"], year=year).with_columns(pl.lit(row["cbscode"]).alias("cbscode"))
            )
        except Exception as e:
            print(e)
    df = pl.concat(df_list)
    df = df.with_columns(
        compute_binomial_interval(df["n_both"], df["n_location"])
    )
    return df

res = query_map(r"choler.*|krim.?koorts", 1866)

res.write_