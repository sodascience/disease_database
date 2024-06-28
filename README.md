# Disease database 

Creating a historical disease database (19th-20th century) for municipalities in the Netherlands. Work in progress...

## Data extraction
The downloaded delpher xml files exist in a zip folder, which takes up a lot of storage space (11.8 G) 
and leads to slower and more cumbersome processing. 

With the  `extract_data.py` file, we can extract the relevant information from the zip folder for each article, 
including `file name`, `publication date`, `title` and `article content`. Then, we store all extracted data
as a polars dataframe and save it as a parquet file, which brings down the size down to 844 MB. 

Before you run the following script, make sure to specify the correct path to the delpher zip folder with `file_path`.

```
python extract_data.py
```

## Data analysis
Using the following script `data_analysis_py`, we can quickly search through all the extracted titles and article content
stored in the `delphers.parquet` file, match them against our specified `regex_pattern` (e.g., `r'(?i)amst'`), keep only
the matched rows/articles, count the number of remaining articles by publication date, and make a bar chart. 

Make sure to define your own `regex_pattern`.
```
python data_analysis.py
```

## Contact

<img src="./img/soda_logo.png" alt="SoDa logo" width="250px"/>

This project is developed and maintained by the [ODISSEI Social Data
Science (SoDa)](https://odissei-soda.nl) team.

Do you have questions, suggestions, or remarks? File an issue in the
issue tracker or feel free to contact the team at [`odissei-soda.nl`](https://odissei-soda.nl)

