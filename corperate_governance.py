from nse_data_fetcher import NSEDataFetcher
import json 


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
    endpoint = "corporate-governance"
    params = {
        "recId": "105459"
    }
    referer_url = "https://www.nseindia.com/companies-listing/corporate-filings-governance"
    data = data_fetcher._make_request(endpoint, params, referer_url)
    with open("temp.json", "w") as f:
        json.dump(data, f, indent=4)
    

