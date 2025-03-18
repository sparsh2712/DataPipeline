from nse_data_fetcher import NSEDataFetcher
import json 
from postgres_utils import PGConn
import os

if __name__ == "__main__":
    data_fetcher = NSEDataFetcher("/Users/sparsh/Desktop/DataPipeline/config")
    
    #Get company list --> 
    # endpoint = "corporate-governance-master"
    # params = {
    #     "index": "equities",
    #     "from_date": "01-01-2021",
    #     "to_date": "17-03-2025",
    #     "period_ended": "all"
    # }
    # referer_url = "https://www.nseindia.com/companies-listing/corporate-filings-governance"
    # comapny_data = data_fetcher._make_request(endpoint, params, referer_url)
    # with open("company_cg_data.json", "w") as file:
    #     json.dump(comapny_data, file, indent=4)
    
    #Get BOD -->
    if not os.path.exists("corporate_governance.json"):
        with open("corporate_governance.json", "w") as f:
            json.dump({}, f)
    
    existing_data = {}
    with open("corporate_governance.json", "r") as f:
        existing_data = json.load(f)

    psql_conn = PGConn({
                "database": "datadump",
                "host": "localhost",
                "port": "5432",
                "user": "sparsh",
                "password": "algobulls"
            })
    sql_query = """
        select 
            symbol, cg_record_id
        from 
            nse.metadata
    """
    results = psql_conn.execute(sql_query)
    metadata_dict = {entry[0]: entry[1] for entry in results}
    endpoint = "corporate-governance"
    referer_url = "https://www.nseindia.com/companies-listing/corporate-filings-governance"
    existing_symbols = set(existing_data.keys())
    print(f"Found {len(existing_symbols)} existing symbols in the JSON file")
    for symbol, id in metadata_dict.items():
        if symbol in existing_symbols:
            print(f"Skipping symbol: {symbol} (already exists)")
            continue

        print(f"processing symbol: {symbol}")
        params = {"recId": id}
        data_raw = data_fetcher._make_request(endpoint, params, referer_url)
        data = data_raw.get("cobod", [])[0].get("data",{})
        if len(data.get("CompositionBOD", [])) == 0 :
            with open("missing_data_symbols.log", "a") as log_file:
                log_file.write(f"{symbol}: BOD data missing\n")
            continue
        bod = []
        for data in data["CompositionBOD"]:
            director_name = f"{data["title"]}{data["directorName"]}"
            membership = data["membershipinCommofCompany"].split(",")
            bod.append({
                "director_name": director_name,
                "din": data["din"],
                "designation": data["category"],
                "tenure": data["tenure"],
                "membership": membership
            })
        
        com_list = data_raw.get("coc", [])
        if not com_list:
            with open("missing_data_symbols.log", "a") as log_file:
                log_file.write(f"{symbol}: Communities data missing\n")
            continue

        com_data = com_list[0].get("data", {})
        if not isinstance(com_data, dict) or not com_data:
            with open("missing_data_symbols.log", "a") as log_file:
                log_file.write(f"{symbol}: Communities data missing\n")
            continue

        communities = {
            c: [
                {
                    "name": entry.get("name", "Unknown"),
                    "designation": entry.get("category", "Unknown"),
                    "community_designation": entry.get("chairPersonMember", "Unknown")
                }
                for entry in v if isinstance(entry, dict)
            ]
            for c, v in com_data.items() if isinstance(v, list) and v
        }


        symbol_data = {
            "board_of_directors": bod,
            "communities": communities
        }
        with open("corporate_governance.json", "r") as f:
            current_data = json.load(f)

        current_data[symbol] = symbol_data
        with open("corporate_governance.json", "w") as f:
            json.dump(current_data, f, indent=4)
        
        print(f"Added data for symbol: {symbol}")
    

