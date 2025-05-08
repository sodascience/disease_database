# Database creation

This workflow transformed the harvested, extracted, and combined dataset into the final disease database. Note that we have run this workflow on a big machine with >200 cores and 1TB memory. Your mileage may vary as to how fast, how successful this will be on different hardware.

### Step 1: pre-processing / re-partitioning
To make our data processing much faster, we will now process all these files into a hive-partitioned parquet folder, with subfolders for each year. This is done using the following code

```sh
uv run src/create_database/preproc.py
```

After this, the folder `processed_data/partitioned` will contain differently organized parquet files, but they contain the exact same information.

### Step 2: database computation

> [!IMPORTANT]
> From this step onwards, we ran this on a linux (ubuntu server) machine with >200 cores and 1TB of memory

The next step is to create the actual database we are interested in. There are three inputs for this:

| Input | Description |
| :---- | :---------- |
| `raw_data/manual_input/disease_search_terms.xlsx` | Contains a list of diseases and their regex search definitions |
| `raw_data/manual_input/location_search_Terms.xlsx` | Contains a list of locations and their regex search definitions |
| `processed_data/partitioned/**/*.parquet` | Contains the texts of all articles from 1830-1940 |

The following command will take these inputs, perform the regex searches and output (many) `.parquet` files to `processed_data/database_flat`. On our big machine, this takes about 12 hours.

```sh
uv run src/create_database/main.py
```

It may be better to run this in the background without hangups:

```sh
nohup uv run src/create_database/main.py &
```

The resulting data looks approximately like this:

```py
import polars as pl
pl.scan_parquet("processed_data/database_flat/*.parquet").head().collect()
```

```
shape: (5, 8)
┌──────┬───────┬────────────┬────────┬────────────┬─────────┬───────────────┬─────────┐
│ year ┆ month ┆ n_location ┆ n_both ┆ location   ┆ cbscode ┆ amsterdamcode ┆ disease │
│ ---  ┆ ---   ┆ ---        ┆ ---    ┆ ---        ┆ ---     ┆ ---           ┆ ---     │
│ i32  ┆ i8    ┆ u32        ┆ u32    ┆ str        ┆ i32     ┆ i32           ┆ str     │
╞══════╪═══════╪════════════╪════════╪════════════╪═════════╪═══════════════╪═════════╡
│ 1834 ┆ 6     ┆ 1          ┆ 0      ┆ Aagtekerke ┆ 1000    ┆ 10531         ┆ typhus  │
│ 1833 ┆ 12    ┆ 3          ┆ 0      ┆ Aagtekerke ┆ 1000    ┆ 10531         ┆ typhus  │
│ 1834 ┆ 9     ┆ 1          ┆ 0      ┆ Aagtekerke ┆ 1000    ┆ 10531         ┆ typhus  │
│ 1832 ┆ 5     ┆ 1          ┆ 0      ┆ Aagtekerke ┆ 1000    ┆ 10531         ┆ typhus  │
│ 1831 ┆ 4     ┆ 2          ┆ 0      ┆ Aagtekerke ┆ 1000    ┆ 10531         ┆ typhus  │
└──────┴───────┴────────────┴────────┴────────────┴─────────┴───────────────┴─────────┘
```

In this format, the column `n_location` means the number of detected mentions of the location / municipality, and the column `n_both` represents the number of disease mentions within this set of articles mentioning the location.

### Step 3: post-processing

The third step is to organise the data (e.g., sorting by date), compute the mention rate, and store everything in a small single parquet file.

```sh
uv run src/create_database/postproc.py
```

The resulting data folder `processed_data/database` looks something like this:

```
database/
└── disease_database_v1.2.parquet
```

Now, for example, the typhus mentions in 1838 look like this:
```py
import polars as pl
lf = pl.scan_parquet("processed_data/database/disease_database_v1.2.parquet")
lf.filter(pl.col("disease") == "typhus", pl.col("year") == 1838).head().collect()
```
```
┌─────────┬──────┬───────┬─────────┬──────────────┬────────────┬────────┐
│ disease ┆ year ┆ month ┆ cbscode ┆ mention_rate ┆ n_location ┆ n_both │
│ ---     ┆ ---  ┆ ---   ┆ ---     ┆ ---          ┆ ---        ┆ ---    │
│ cat     ┆ i32  ┆ i8    ┆ i32     ┆ f64          ┆ u32        ┆ u32    │
╞═════════╪══════╪═══════╪═════════╪══════════════╪════════════╪════════╡
│ typhus  ┆ 1838 ┆ 1     ┆ 1       ┆ 0.0          ┆ 2          ┆ 0      │
│ typhus  ┆ 1838 ┆ 1     ┆ 2       ┆ 0.0          ┆ 3          ┆ 0      │
│ typhus  ┆ 1838 ┆ 1     ┆ 3       ┆ 0.0          ┆ 11         ┆ 0      │
│ typhus  ┆ 1838 ┆ 1     ┆ 4       ┆ 0.0          ┆ 2          ┆ 0      │
│ typhus  ┆ 1838 ┆ 1     ┆ 5       ┆ 0.0          ┆ 6          ┆ 0      │
└─────────┴──────┴───────┴─────────┴──────────────┴────────────┴────────┘
```


### Step 4: geographic information
Just to be sure, we also archive the geographic information from [NLGIS](https://nlgis.nl) alongside the parquet file. This is done through the following script:

```sh
uv run src/create_database/download_geojson.py
```

Now, finally, we have the full database:

```
database/
├── disease_database_v1.2.parquet
└── nl1869.geojson
```

The `disease_database_v1.2.parquet` file and `nl1869.geojson` can be joined using the `cbscode` unique identifier, to add information such as municipality name, "amsterdamcode", unique NLGIS id, and the actual geometric shape for producing maps.