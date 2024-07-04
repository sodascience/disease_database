import polars as pl
import matplotlib.pyplot as plt
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--search_string",
                        type=str,
                        default=r'(?i)cholera')
    parser.add_argument("--spatial",
                        type=str,
                        default='Regionaal/lokaal')

    args = parser.parse_args()
    search_string = args.search_string
    spatial = args.spatial

    # Load the parquet files into DataFrames
    newspaper_meta_data_df = pl.read_parquet('newspaper_meta_data.parquet')  # Columns: newspaper_id, publisher
    article_meta_data_df = pl.read_parquet('article_meta_data.parquet')  # Columns: newspaper_id, article_id, article_type
    article_data_df = pl.read_parquet('article_data.parquet')  # Columns: article_id, article_content

    # Join df1 with df2 on newspaper_id
    intermediate_df = article_meta_data_df.join(newspaper_meta_data_df, on='newspaper_id', how='inner')

    # Join the result with df3 on article_id
    final_df = intermediate_df.join(article_data_df, left_on='item_filename', right_on= 'file_name', how='inner')

    # Filter the DataFrame rows where any column contains the specific string pattern
    filtered_df = final_df.filter(
        (final_df['title'].str.contains(search_string)) |
        (final_df['text'].str.contains(search_string)) &
        (final_df['newspaper_spatial'] == spatial)
    )

    # Count number of matched articles per date
    count_per_category = filtered_df.group_by('newspaper_date').agg(pl.len())

    # Count the total number of matching articles
    matching_rows_count = filtered_df.shape[0]

    # Print
    print(count_per_category)
    print(f"Number of rows containing the string '{search_string}': {matching_rows_count}")

    # Make figure
    count_per_category_pd = count_per_category.to_pandas()
    plt.figure(figsize=(10, 6))
    plt.bar(count_per_category_pd['newspaper_date'], count_per_category_pd['len'])
    plt.xlabel('Date')
    plt.ylabel('Count')
    plt.title(f'Number of mentions of {search_string} by date')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    main()
