import requests
import json
from bs4 import BeautifulSoup

# Define headers to mimic a real browser request
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.nseindia.com",
}

# URL of the page
url = "https://www.nseindia.com/companies-listing/corporate-filings-regulation-29-promoters"

# Start session
session = requests.Session()

# Make a GET request
response = session.get(url, headers=headers)

# Check response
if response.status_code == 200:
    soup = BeautifulSoup(response.text, "html.parser")
    
    # Find the ul with id "leftNav"
    left_nav = soup.find("ul", id="leftNav")
    
    data_dict = {}

    if left_nav:
        # Get all li elements and extract data-name and href
        for li in left_nav.find_all("li"):
            for a_tag in li.find_all("a"):
                data_name = a_tag.get("data-name")
                href = a_tag.get("href")

                if data_name and href:  # Ensure both attributes exist
                    data_dict[data_name] = {
                        "endpoint": href,
                        "params": []
                    }

        # Write data_dict to api.json
        with open("api_new.json", "w", encoding="utf-8") as file:
            json.dump(data_dict, file, indent=4, ensure_ascii=False)

        print("Data successfully written to api.json")
    else:
        print("Could not find the leftNav element. The page might require JavaScript rendering.")
else:
    print(f"Failed to fetch page. Status Code: {response.status_code}")
