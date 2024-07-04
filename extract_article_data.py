import zipfile
import polars as pl
import xml.etree.ElementTree as ET
from tqdm import tqdm

# Path to delpher's zip folder
file_path = 'C:/Users/fqixi/datasets/kranten_pd_1860-4.zip'

# Results object to write to
results = []

# Open the zip file
with zipfile.ZipFile(file_path, 'r') as zip_ref:
    # Iterate through each file in the zip archive
    for file_info in tqdm(zip_ref.infolist()):
        filename = file_info.filename
        publication_date = filename[:10]
        if filename.endswith('articletext.xml'):
            with zip_ref.open(file_info) as file:
                try:
                # Parse the XML content
                    content = file.read().decode('utf8')
                    root = ET.fromstring(content)
                    title_content = root.find('title').text
                    p_contents = [p.text for p in root.findall('p')]
                    p_contents = ' '.join(p_contents)
                    results.append({
                        'file_name': file_info.filename,
                        'date': publication_date,
                        'title': title_content,
                        'paragraph': p_contents,
                    })
                except Exception as e:
                    results.append({
                        'file_name': file_info.filename,
                        'date': publication_date,
                        'title': f"An error occurred: {str(e)}",
                        'paragraph': f"An error occurred: {str(e)}",
                    })

# Convert the results to a Polars DataFrame
df = pl.DataFrame(results)

# Save the DataFrame as a Parquet file
df.write_parquet('delpher.parquet')
