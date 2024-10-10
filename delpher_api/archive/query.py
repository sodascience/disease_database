"""
In this file:
Querying the search API of delpher
"""

import requests
from xml.etree import ElementTree
import matplotlib.pyplot as plt
import polars as pl
import datetime
import calendar

# first some namespace stuff for xml parsing
xmlns = {
    "srw": "http://www.loc.gov/zing/srw/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcx": "http://krait.kb.nl/coop/tel/handbook/telterms.html",
}

# NB: this file should contain the api key
with open("delpher_api/keys.txt") as f:
    apikey = f.readline()

# Search url is the base url with apikey appended
search_url = f"https://jsru.kb.nl/sru/sru/{apikey}"

# First, a basic query
# see https://www.loc.gov/standards/sru/cql/spec.html
q = """
cholera AND amst* AND type="artikel" AND date within "01-01-1860 31-01-1860"
"""

# useful / required parameters
params = {
    "operation": "searchRetrieve",
    "recordSchema": "dc",
    "x-collection": "DDD_artikel",
    "sortKeys": "date",
    "version": "1.1",
    "maximumRecords": 10,
    "startRecord": 1,
    "query": q,
}

# do the query and get a response
resp = requests.get(search_url, params=params)

# let's look at the url we sent!
resp.url

# parse the xml response
xml = ElementTree.fromstring(resp.text)

# make nice indentation and print function
ElementTree.indent(xml)


def printel(x: ElementTree.Element):
    "Print function for xml elements."
    print(ElementTree.tostring(x, encoding="unicode"))


# Extract articles
articles = xml.findall(".//srw:record", xmlns)


printel(articles[0])

# Now, let's do some more automated querying!
query = 'cholera AND amst* AND type="artikel"'

def add_query_date(q, year: int, month: int):
    "Function to append a 1 month date range to a query"
    _, lastday = calendar.monthrange(year, month)
    start = datetime.date(year, month, 1)
    end = datetime.date(year, month, lastday)
    return f'{q} AND date within "{start.isoformat()} {end.isoformat()}"'


def find_num_records(q: str, year: int = 1860, month: int = 1):
    "Function to find the number of records for a query in a month"
    q = add_query_date(q, year, month)
    print(q)
    params = {
        "operation": "searchRetrieve",
        "recordSchema": "dc",
        "x-collection": "DDD_artikel",
        "sortKeys": "date",
        "version": "1.1",
        "maximumRecords": 0,
        "startRecord": 1,
        "query": q,
    }
    resp = requests.get(search_url, params=params)
    xml = ElementTree.fromstring(resp.text)
    return int(xml.find("srw:numberOfRecords", xmlns).text)


# Collect data
tgt_file = "delpher_api/dataset_rotterdam.csv"
with open(tgt_file, "a+") as file:
    # write the header
    # file.write("year,month,count\n")

    # write the count for each month
    # to-do: async / fast / multithread?
    for y in range(1860, 1861):
        for m in range(1, 13):
            num = find_num_records(query, y, m)
            file.write(f"{y},{m},{num}\n")


# Create a plot of the data
df = pl.read_csv("delpher_api/dataset.csv").with_columns(
    pl.date(pl.col("year"), pl.col("month"), 1).alias("date"),
    pl.col("count").cast(pl.Int64),
    pl.col("count").rolling_mean(6).alias("count_6m"),
)

plt.plot(df["date"], df["count"])
plt.title("Cholera and Amsterdam mentions")
plt.xlabel("Date")
plt.ylabel("Mentions (6m rolling average)")
plt.show()