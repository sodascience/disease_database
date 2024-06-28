import polars as pl
import matplotlib.pyplot as plt

# Specify the pattern to search for
regex_pattern = r'(?i)amst'

# Read the Parquet file into a Polars DataFrame
df = pl.read_parquet("delpher.parquet")
df = df.with_columns(
    pl.col('date').str.to_date('%Y/%m/%d').alias('date')
)

# Filter the DataFrame rows where any column contains the specific string pattern
filtered_df = df.filter(
    df['title'].str.contains(regex_pattern) |
    df['paragraph'].str.contains(regex_pattern)
)

# Count number of matched articles per date
count_per_category = filtered_df.group_by('date').agg(pl.len())

# Count the total number of matching articles
matching_rows_count = filtered_df.shape[0]

# Print
print(count_per_category)
print(f"Number of rows containing the string '{regex_pattern}': {matching_rows_count}")

# Make figure
count_per_category_pd = count_per_category.to_pandas()
plt.figure(figsize=(10, 6))
plt.bar(count_per_category_pd['date'], count_per_category_pd['len'])
plt.xlabel('Date')
plt.ylabel('Count')
plt.title(f'Number of mentions of {regex_pattern} by date')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()