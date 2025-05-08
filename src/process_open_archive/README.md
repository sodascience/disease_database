# Data extraction from open Delpher archive (1830-1879)
Between 1830 and 1879, Delpher historical news article data can be downloaded manually from [here](https://www.delpher.nl/over-delpher/delpher-open-krantenarchief/download-teksten-kranten-1618-1879#b1741).
The downloaded files, which are zip folders, take up lots of disk space because there is loads of data (also some we don't need!) which is stored in a relatively obscure `xml` format. 

The scripts in this folder efficiently process these zipped `xml` files and convert them to an small data format to create the delpher database.

## Step 1: Download zip files

Go to the delpher open archive [here](https://www.delpher.nl/over-delpher/delpher-open-krantenarchief/download-teksten-kranten-1618-1879#b1741) and download all the zip files. Put them in the folder `raw_data/open_archive` so that it looks like this:

```
📁 open_archive/
├── .gitignore
├── 📜 kranten_pd_183x.zip
├── 📜 kranten_pd_184x.zip
├── 📜 kranten_pd_1850-4.zip
├── 📜 kranten_pd_1855-9.zip
├── 📜 kranten_pd_1860-4.zip
├── 📜 kranten_pd_1865-9.zip
├── 📜 kranten_pd_1870-4.zip
├── 📜 kranten_pd_1875-6.zip
└── 📜 kranten_pd_1877-9.zip
```

## Step 2: Extract article content
The `extract_article_data.py` script extracts the titles and texts from the zip folder for each article. Run it like so:

```
uv run src/process_open_archive/extract_article_data.py
```

The harvested data will be saved under `processed_data/texts/open_archive/` as dataframes in parquet format. Three columns are included: `article_id`,  `article_title`, and `article_text`.

## Step 3: Extract article and newspaper metadata

With the `src/process_open_archive/extract_meta_data.py` script, we extract meta information about both the individual articles and the newspapers they appeared in. This results in two kinds of dataframes saved in parquet format under `processed_data/metadata/articles/open_archive/` and `processed_data/metadata/newspapers/open_archive/`, respectively.

```
uv run src/process_open_archive/extract_meta_data.py
```

Included columns in article metadata are `newspaper_id`, `article_id`, and `article_subject`. Included columns in newspaper metadata are `newspaper_id`, `newspaper_name`, `newspaper_location`, `newspaper_date`, `newspaper_years_digitalised`, `newspaper_years_issued`, `newspaper_language`, `newspaper_temporal`, `newspaper_publisher` and `newspaper_spatial`.

## Step 4: Combine and chunk data

In the last step, we join all three sources of data to create combined datafiles of one row per article. To run this part, run:

```
uv run src/process_open_archive/combine_and_chunk.py
```

This will create sets of data files in `processed_data/combined` with the following columns:

```
'newspaper_id'
'article_id'
'article_subject'
'article_title'
'article_text'
'newspaper_name'
'newspaper_location'
'newspaper_date'
'newspaper_years_digitalised'
'newspaper_years_issued'
'newspaper_language'
'newspaper_temporal'
'newspaper_publisher'
'newspaper_spatial'
```

This is still quite a lot of information, which will be further pruned in the `create_database` workflow.