import json
from postgres_utils import PGConn

# Load JSON data from file
# with open("/Users/sparsh/Desktop/WebCrawler/GODREJPROP_pit_data.json", "r") as file:
#     data = json.load(file)

# data = data["total"]["data"]  # Extract relevant data

# # Extract symbols where series is "EQ"
# equity_data = [entry["symbol"] for entry in data if entry.get("series") == "EQ"]

# # Write output to symbols.json
# with open("symbols.json", "w") as file:
#     json.dump(equity_data, file)

# print("Symbols saved to symbols.json")

with open("/Users/sparsh/Desktop/WebCrawler/symbols.json") as f:
    data = json.load(f)

comp_set = set(data)

pgconn = PGConn({
                "database": "datadump",
                "host": "localhost",
                "port": "5432",
                "user": "sparsh",
                "password": "algobulls"
            })

