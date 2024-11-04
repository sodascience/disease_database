import polars as pl
from tqdm import tqdm
from pathlib import Path
from src.query.utils import query_disease_location_year, compute_binomial_interval

BASE_PATH = Path(".")
# optional if using external disk: BASE_PATH = Path("E:/", "disease_database")
COMBINED_DATA_FOLDER = Path("processed_data", "combined")
LOCATION_EXCEL_FILE = Path("raw_data", "manual_input", "municipalities_1869.xlsx")


def query_space(disease_query: str, year: int):
    # iterate over each municipality
    muni_df = pl.read_excel(BASE_PATH / LOCATION_EXCEL_FILE)
    text_df = pl.scan_parquet(
        BASE_PATH / COMBINED_DATA_FOLDER / f"combined_{year}_{year + 1}.parquet"
    )
    df_list = []
    for row in tqdm(muni_df.iter_rows(named=True), total=len(muni_df)):
        try:
            df_list.append(
                query_disease_location_year(
                    df_lazy=text_df,
                    disease="(?i)" + disease_query,
                    location="(?i)" + row["Regex"],
                    year=year,
                ).with_columns(
                    pl.lit(row["cbscode"]).alias("cbscode"),
                    pl.lit(row["amsterdamcode"]).alias("amsterdamcode"),
                    pl.lit(row["Municipality"]).alias("Municipality"),
                )
            )
        except Exception as e:
            print(e)
    df = pl.concat(df_list)
    df = df.with_columns(compute_binomial_interval(df["n_both"], df["n_location"]))
    return df


res = query_space(r"choler.*|krim.?koorts", 1866)

res.sort(["Municipality", "yr", "mo"]).write_parquet(Path("processed_data", "cholera_1866.parquet"))
