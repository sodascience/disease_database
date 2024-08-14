# Disease database 

Creating a historical disease database (19th-20th century) for municipalities in the Netherlands. Work in progress...

![](img/cholera.png)

## Preparation

```
pip install tqdm polars requests matplotlib plotnine
```

## Data extraction
The downloaded delpher xml files are contained in a zip folder, which takes up a lot of storage space (11.8 G) 
and leads to slower and more cumbersome processing. 

The `extract_article_data.py` script extracts the titles and texts from the zip folder for each article.
Then, it stores all extracted data as a polars dataframe with three columns `file_name`, `title` and `text`.
Finally, it is saved as a parquet file (`article_data.parquet`), with a much smaller size of 844 MB. 

With the `extract_meta_ata.py` script, we extract meta information about both the newspapers and the individual articles.
This results in two separate polars dataframes saved in parquet format:

1) `article_meta_data.parquet` includes these columns: `newspaper_name`, `newspaper_location`,
   `newspaper_date`, `newspaper_years_digitalised`, `newspaper_years_issued`, `newspaper_language`, `newspaper_temporal`,
   `newspaper_publisher`, `newspaper_spatial`, and `pdf_link`.
2) `newspaper_meta_data.parquet` includes these columns: `newspaper_id`, `item_id`, `item_subject`, `item_filename`, and `item_type`.

Before you run the following script, make sure to specify the correct path to the delpher zip folder using `file_path`.

```
python extract_article_data.py
python extract_meta_data.py
```

## Data analysis
The script `data_analysis.py` does the following:

1. Merge the three parquet files from the previous step;
2. Filter articles by their titles and texts using our specified `search_string` (e.g., `r'(?i)cholera'`);
3. Filter articles by their `spatial` type (e.g., Regionaal/lokaal);
4. Count the number of remaining articles by publication date, and make a bar chart. 

By default, the search string is defined as `r'(?i)cholera'`, which matches any article title or text that contains the term "cholera", case-insensitive;
`spatial` is set at `'Regionaal/lokaal'`.

```
python data_analysis.py
```

You can also define your own search string and spatial:
```
python data_analysis.py --search-string <your search string> --spatial <spatial type>
```


## Contact

<img src="./img/soda_logo.png" alt="SoDa logo" width="250px"/>

This project is developed and maintained by the [ODISSEI Social Data
Science (SoDa)](https://odissei-soda.nl) team.

Do you have questions, suggestions, or remarks? File an issue in the
issue tracker or feel free to contact the team at [`odissei-soda.nl`](https://odissei-soda.nl)

