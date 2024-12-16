import pandas as pd
import polars as pl
import os

# Load the CSV file with municipality regex patterns
file_path = os.path.join(os.path.dirname(__file__), 'municipalities_1869_test.csv')
print("Loading municipality list...")
municipalities = pd.read_csv(file_path)
print("Column names in the file:", municipalities.columns)

# Check if 'Regex' is in the CSV file
if 'Regex' not in municipalities.columns:
    print("Error: The file does not contain a 'Regex' column.")
    exit(1)

# Define the disease query for cholera
disease_query = r"choler.*|krim.?koorts"

# Function to query mentions for a single municipality in 1866
def query_for_municipality(disease: str, location_pattern: str, year: int = 1866):
    combined_query = rf"(?i)(({disease}).{{0,20}}({location_pattern})|({location_pattern}).{{0,20}}({disease}))"
    
    df = pl.scan_parquet("processed_data/combined/*.parquet")

    # Perform the query for the specified year and combined pattern
    result = (
        df
        .filter(pl.col("date").dt.year() == year)
        .with_columns(
            pl.col("text").str.contains(combined_query).alias("disease_location_near")
        )
        .group_by(pl.lit(True))  # Aggregate over entire data, since we want total counts
        .agg([
            pl.len().alias("n_total"),
            pl.col("disease_location_near").sum().alias("n_near_matches")
        ])
        .collect(streaming=True)
    )

    # Return normalized mentions or zero if no data found
    if result['n_total'][0] == 0:
        return 0
    
    return result['n_near_matches'][0] / result['n_total'][0]

# Loop over municipalities and store results in a list
results = []
print("Querying mentions for all municipalities in 1866...")
for index, row in municipalities.iterrows():
    location = row['Regex']
    print(f"Processing location with regex: {location}")
    
    normalized_mentions = query_for_municipality(disease_query, location)
    results.append({
        'Location_Regex': location,
        'Normalized_Mentions': normalized_mentions
    })

# Convert results to DataFrame and save to CSV
output_df = pd.DataFrame(results)
output_csv_path = os.path.join(os.path.dirname(__file__), 'cholera_mentions_1866.csv')
output_df.to_csv(output_csv_path, index=False)
print(f"Results saved to {output_csv_path}")
