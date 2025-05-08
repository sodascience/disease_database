"""Script to download the 1869 map of the Netherlands as geojson from nlgis.nl"""

import requests
import datetime
from pathlib import Path

OUTPUT_FOLDER = Path("processed_data", "database")


def main():
    print(datetime.datetime.now(), "| Downloading and storing geojson.")
    out_path = OUTPUT_FOLDER / "nl1869.geojson"
    if out_path.exists():
        print(f"\n {out_path} already exists! Skipping...")
    response = requests.get("https://nlgis.nl/api/maps?year=1869")
    response.raise_for_status()
    with out_path.open("wb") as file:
        file.write(response.content)


if __name__ == "__main__":
    main()
