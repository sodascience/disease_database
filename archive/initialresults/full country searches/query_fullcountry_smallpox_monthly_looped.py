import os
import polars as pl
import pandas as pd

# Load the CSV file with municipality regex patterns and other columns
file_path = os.path.join(os.path.dirname(__file__), 'municipalities_1869.csv')
print("Loading municipality list...")
municipalities = pd.read_csv(file_path, delimiter=';')
print("Column names in the file:", municipalities.columns)

# Check if 'Regex' is in the CSV file
if 'Regex' not in municipalities.columns:
    print("Error: The file does not contain a 'Regex' column.")
    exit(1)

# Define the disease query for smallpox
disease_query = r"pokken|variola|pok.?ziekte"

# List of parquet files to read
parquet_files = [
    "C:\\github\\disease_database\\processed_data\\combined\\combined_1869_1870.parquet",
    "C:\\github\\disease_database\\processed_data\\combined\\combined_1870_1871.parquet",
    "C:\\github\\disease_database\\processed_data\\combined\\combined_1871_1872.parquet",
    "C:\\github\\disease_database\\processed_data\\combined\\combined_1872_1873.parquet"
]

# Function to query mentions for a single municipality across all parquet files
def query_for_municipality(disease: str, location_pattern: str, year: int = 1871, month: int = 5):
    n_total = 0
    n_both = 0

    # Loop through each parquet file
    for file_path in parquet_files:
        df = pl.scan_parquet(file_path)

        # Create the combined proximity query
        proximity_query = rf"(?i)(?:{disease}).{{0,40}}(?:{location_pattern})|(?:{location_pattern}).{{0,40}}(?:{disease})"

        # Perform the query for the specified year and month
        result = (
            df
            .filter((pl.col("date").dt.year() == year) & (pl.col("date").dt.month() == month))
            .with_columns(
                pl.col("text").str.contains(rf"(?i){location_pattern}").alias("location"),
                pl.col("text").str.contains(proximity_query).alias("both")  # Proximity check
            )
            .filter(pl.col("location"))  # Filter to only articles mentioning the location
            .group_by(pl.lit(True))  # Aggregate over filtered data
            .agg([
                pl.len().alias("n_total"),  # Total number of records mentioning the location
                pl.col("both").sum().alias("n_both")  # Number of records with disease and location within 40 characters
            ])
            .collect(streaming=True)
        )

        # Accumulate results
        if not result.is_empty():
            n_total += result['n_total'][0]
            n_both += result['n_both'][0]

    normalized_mentions = n_both / n_total if n_total > 0 else 0
    return n_both, n_total, normalized_mentions

# Loop over months from January to December
for month in range(5, 13):  # Months 1 to 12
    results = []
    print(f"Querying mentions for all municipalities for month {month} of 1871...")
    
    for index, row in municipalities.iterrows():
        location = row['Regex']
        name = row['name']
        cbscode = row['cbscode']
        amsterdamcode = row['amsterdamcode']
        print(f"Processing location with regex: {location}")
        
        n_both, n_total, normalized_mentions = query_for_municipality(disease_query, location, year=1871, month=month)
        results.append({
            'Location_Regex': location,
            'name': name,
            'cbscode': cbscode,
            'amsterdamcode': amsterdamcode,
            'n_both': n_both,
            'n_total': n_total,
            'Normalized_Mentions': normalized_mentions
        })

    # Convert results to DataFrame and save to CSV
    output_df = pd.DataFrame(results)

    output_csv_path = os.path.join(os.path.dirname(__file__), f'smallpox_mentions_{month:02d}_1871.csv')
    output_df.to_csv(output_csv_path, index=False)
    print(f"Results saved to {output_csv_path}")