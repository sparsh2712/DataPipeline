import requests
import json
import argparse
import random
import time
import os
from typing import Dict, Any, Optional
import urllib.parse

def get_fresh_cookies(debug: bool = False, max_retries: int = 3) -> requests.Session:
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "DNT": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Sec-GPC": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache"
    }
    for attempt in range(max_retries):
        try:
            if debug:
                print(f"Attempt {attempt+1}/{max_retries}: Visiting NSE homepage to get initial cookies...")
            main_page_url = "https://www.nseindia.com/"
            response = session.get(main_page_url, headers=headers, timeout=15)
            response.raise_for_status()
            if debug:
                print(f"Initial cookies obtained: {len(session.cookies)} cookies")
                for cookie in session.cookies:
                    print(f"  {cookie.name}: {cookie.value[:20]}..." if len(cookie.value) > 20 else f"  {cookie.name}: {cookie.value}")
            if not session.cookies or 'nsit' not in [c.name for c in session.cookies]:
                if debug:
                    print("Missing essential cookies, retrying...")
                time.sleep(2)
                continue
            if debug:
                print("Visiting PIT page to get additional cookies...")
            pit_page_url = "https://www.nseindia.com/companies-listing/corporate-filings-insider-trading"
            response = session.get(pit_page_url, headers=headers, timeout=15)
            response.raise_for_status()
            if debug:
                print(f"Updated cookies: {len(session.cookies)} cookies")
                for cookie in session.cookies:
                    print(f"  {cookie.name}: {cookie.value[:20]}..." if len(cookie.value) > 20 else f"  {cookie.name}: {cookie.value}")
            if 'nseappid' in [c.name for c in session.cookies]:
                if debug:
                    print("Successfully obtained all necessary cookies!")
                return session
            else:
                if debug:
                    print("Missing 'nseappid' cookie, retrying...")
                time.sleep(2)
                continue
        except requests.exceptions.RequestException as e:
            print(f"Error getting fresh cookies (attempt {attempt+1}/{max_retries}): {e}")
            time.sleep(2)
    print("Warning: Could not obtain all necessary cookies after maximum retries.")
    return session

def fetch_corporate_pit(symbol: str, company_name: str = None, use_cookies: bool = True, use_hardcoded_cookies: bool = False, debug: bool = False, max_retries: int = 3) -> Optional[Dict[str, Any]]:
    base_url = "https://www.nseindia.com"
    if not company_name:
        company_name = symbol
    encoded_company_name = urllib.parse.quote(company_name)
    endpoint = f"/api/annual-reports?index=equities&symbol=RELIANCE"
    url = base_url + endpoint
    user_agents = [
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
    ]
    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-insider-trading",
        "DNT": "1",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Sec-GPC": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache"
    }
    session = None
    last_error = None
    for attempt in range(max_retries):
        try:
            if use_cookies:
                if session is None:
                    session = get_fresh_cookies(debug)
                if debug:
                    print(f"Attempt {attempt+1}/{max_retries}: Using freshly generated cookies")
                if use_hardcoded_cookies and 'nseappid' not in [c.name for c in session.cookies]:
                    if debug:
                        print("Fresh cookies missing 'nseappid', falling back to hardcoded cookies")
                    backup_session = requests.Session()
                    hardcoded_cookies = get_hardcoded_cookies()
                    for cookie_name, cookie_value in hardcoded_cookies.items():
                        backup_session.cookies.set(cookie_name, cookie_value, domain="nseindia.com")
                    session = backup_session
            else:
                if session is None:
                    session = requests.Session()
                if debug:
                    print(f"Attempt {attempt+1}/{max_retries}: Using a clean session without cookies")
            time.sleep(random.uniform(0.5, 2.0))
            response = session.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            if debug:
                print(f"Response status code: {response.status_code}")
                print(f"Response headers: {response.headers}")
                if 'br' in response.headers.get('Content-Encoding', ''):
                    print(f"Response is Brotli-compressed (showing first 100 bytes): {response.content[:100]}")
                elif 'gzip' in response.headers.get('Content-Encoding', ''):
                    print("Response is gzip-compressed")
                elif 'zstd' in response.headers.get('Content-Encoding', ''):
                    print("Response is zstd-compressed")
                else:
                    print(f"Response content (first 500 chars): {response.text[:500]}")
            if not response.content:
                print(f"Attempt {attempt+1}/{max_retries}: Empty response received")
                time.sleep(2)
                continue
            else:
                try:
                    result = response.json()
                    return result
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                    if debug:
                        print(f"Full response content: {response.text}")
                    if "<html" in response.text.lower():
                        print("Received HTML instead of JSON - might be a captcha or login page")
                    time.sleep(2)
                    continue
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt+1}/{max_retries}: Error during request: {e}")
            last_error = e
            time.sleep(2 * (attempt + 1))
            session = None
            continue
    print(f"Failed to fetch data after {max_retries} attempts. Last error: {last_error}")
    return None

def get_hardcoded_cookies():
    return {
        "abck": "0FDF97F6D2E2E2E2340A67EBE24AB912~0~YAAQ1TSVtF1OPmCVAQAA8/7lhQ0TMoRx/KhIdqB76sTwrXM87omTarDOqSYMVNwbZ3W91rngQwjKKYTCfJc5fiqxES/XBpkh7rk0dxMmG+F/WIeVKWUNIGrSz6xKNF81aDtoh9dVk6bncplhgu+IcBiJiF88OgcptOQpHp01x3uw8nqZPCKmoS+u8HQmzIIHlKvc5YRvK6/Avac+t1I1Ka0Zh8vkJOMYfkjeTX6/PkYBVrOQnJ7U03SGyVukjMVrM5PWVlvH0Tk3MwM2NcohWMNsN/6B3b7ZrrlQcJCdhWoH7EVvcEHi8g2BM6uDcqJ1w9PqFGD5BvJg6RRDtT0TtboeX75ryUFruXaOCI4K+VMzUlJSuKR85qhITXeL7jFPYD0+HtybuItXcL8KKRtMYezLuDLPD+BkzBJGpsWXYZpPpILak8nD95I5fRUxqovIYehg4d+hmsgl+T+h9zxlgkVwKTjRJaOxBkzhYOqOUnMNwnWOmAgk2lkch9Xcqpe9Eh64R7Ot+8AJ~-1~-1~-1",
        "ga_87M7PJ3R97": "GS1.1.1741705836.8.1.1741708353.50.0.0",
        "ga": "GA1.1.1710946848.1737299665",
        "ga_WM2NSQKJEK": "GS1.1.1741705836.8.1.1741708353.0.0.0",
        "nsit": "KrOJj0YkqaQdFrjZU16AEbpa",
        "AKA_A2": "A",
        "ak_bmsc": "5648DA9144EDE70391CE25C2C9DB4E30~000000000000000000000000000000~YAAQ1TSVtAZDPmCVAQAA6PjBhRtuFZPq/nHpsCEYoDoxaQNdKNDl30v6X+sCMURUSKn/U87ks6iPbj/SFd9D9ZJnwqaxZ91IqlH2jhugl4ILHz5DfjV75M0kQgJxEpZEXXBuSAiREBWWrXCXokOC34oqUGX262z2yBtHAYufSWD/Hk0rLcfalZl7+L88aU54yPxnjfzBuGzz/TFfa6CY9A0MhjqDsHGmBqCgcFkOL/PF3tRPFDMdqfU+bf0vAmaZ8Z4tXyvxQDg1K/lKJ3EPkScarOHsBgBcNC6uK3sAnLbd8pypdthGWbUxxaHkxAZ82MOQk69qUfbO/7RdpetKSN11wwcBrMhyM7zyIaSmeO0AxvTmEVY5bFm8qiwUhON8ydiGi7llw2q7Z22AltRBXjfaBL2c1ZaMrKn78aX1yGqKU1WcLlX6S07Yi6AWkewFoDvlCDdSaoLXWg7BN/E=",
        "bm_sz": "B97A2B2210A06C798D91369C2BB2EFFF~YAAQ1TSVtMlOPmCVAQAAjFvohRsX6proOzWlZ8MP/qYQK3KrD2ruWe4A6oei1v3ZGmA8ctesTaH+E/+Fk1kIMuAkFs2SMvH5q5a8bXO2YPMh4aOdZWIu8VTHYX02qDasUxhFBPjjiFAM4l2OqNHw4m1s76vKqfZG4caOwBOEiDdYmtPwq+AXUSgDt8PdBN2GD971CGDsJeDTOqM0y6CGm14TfRzw4vmNSnDQlZZBSrNREgqX7E324ueVMhYCB1eFA8kKcTxYY0rsDR8+9L7H0gJbzHbMN89umPq5jJ5P/SE6KKRKWo5OXNE/RMnONfyRerwNt4dLOzHjw+eknX3uR934mjz6WgtzQh5hZkQ4FSEZ/W60tm5uf1tHOgM/PK1MXvy07rwnIv2/hFKQYKb4y/65yd2z6qbeheNpXX03kdpQ5cGLXjH5B8fIn9uFvaeKPcsqe8eKRwm+pTMz7D1P4VM8Hw==~4535349~3551286",
        "RT": "z=1&dm=nseindia.com&si=ce2fc7a2-748e-487b-977c-7b7f086a565a&ss=m84o37cm&sl=3&se=8c&tt=3sf&bcn=%2F%2F684d0d46.akstat.io%2F&ld=3gb0",
        "bm_sv": "EC867FE13F7D88C7032D30AC6B659849~YAAQ1TSVtNtOPmCVAQAAoKzohRvAtTZ4vWHh6T6KeH58EIDWJy12OL4K3S/j3NSRutYAJaunFqf+Dvdfm/deEOwYAeaGnz9F9eNt8mxsYtlpmP7JO/y1S5JqdLOjTaWCzXi8+14dNhc80bKD+8t2pRFnSKg5ZWg24IYJ6U6ZZDLTtz9uXLwWFe9kF89LDjdDYWDJzIAYChv5mtmy886xNgyC1AcQwQpWAqbfCG22HGqdViOQ8IWeW79ZaZcfpNrPePoi0g==~1",
        "nseappid": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcGkubnNlIiwiYXVkIjoiYXBpLm5zZSIsImlhdCI6MTc0MTcwODM1MSwiZXhwIjoxNzQxNzE1NTUxfQ.7aqrW9JBthbBaQGCcm6WDo3iRVTo5eHh3Uxf8mBXuBY"
    }

def main():
    parser = argparse.ArgumentParser(description="Fetch Corporate PIT data from NSE India.")
    parser.add_argument("symbol", nargs="?", default="GODREJPROP", help="Stock symbol to fetch PIT data for (default: GODREJPROP)")
    parser.add_argument("--company", "-c", type=str, default="Godrej Properties Limited", help="Full company name (default: Godrej Properties Limited)")
    parser.add_argument("--save", action="store_true", help="Save the data to a JSON file")
    parser.add_argument("--output", "-o", type=str, help="Output file name (default: <symbol>_pit_data.json)")
    parser.add_argument("--no-cookies", action="store_true", help="Do not include cookies in the request")
    parser.add_argument("--use-hardcoded-cookies", action="store_true", help="Use hardcoded cookies instead of generating fresh ones")
    parser.add_argument("--debug", action="store_true", help="Print debug information")
    parser.add_argument("--raw", action="store_true", help="Save raw response to <symbol>_pit_response.raw file")
    parser.add_argument("--save-cookies", action="store_true", help="Save the generated cookies to a file")
    parser.add_argument("--retries", type=int, default=3, help="Number of retries for API requests (default: 3)")
    args = parser.parse_args()
    print(f"Fetching Corporate PIT data for {args.symbol} ({args.company})...")
    use_cookies = not args.no_cookies
    pit_data = fetch_corporate_pit(symbol=args.symbol, company_name=args.company, use_cookies=use_cookies, use_hardcoded_cookies=args.use_hardcoded_cookies, debug=args.debug, max_retries=args.retries)
    if args.save_cookies and use_cookies:
        try:
            session = get_fresh_cookies(args.debug)
            cookies_dict = {cookie.name: cookie.value for cookie in session.cookies}
            cookies_file = "nse_cookies.json"
            with open(cookies_file, "w") as f:
                json.dump(cookies_dict, f, indent=4)
            print(f"\nCookies saved to {cookies_file}")
        except Exception as e:
            print(f"Error saving cookies: {e}")
    if pit_data:
        print(json.dumps(pit_data, indent=4))
        if args.save:
            output_file = args.output if args.output else f"{args.symbol}_pit_data.json"
            with open(output_file, "w") as f:
                json.dump(pit_data, f, indent=4)
            print(f"\nData saved to {output_file}")
    else:
        print("Failed to fetch Corporate PIT data.")
        if args.raw:
            try:
                session = requests.Session()
                headers = {
                    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0",
                    "Accept": "*/*",
                    "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-insider-trading",
                }
                url = f"https://www.nseindia.com/api/live-analysis-stocksTraded"
                response = session.get(url, headers=headers)
                raw_dir = "raw_responses"
                if not os.path.exists(raw_dir):
                    os.makedirs(raw_dir)
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                raw_file = f"{raw_dir}/{args.symbol}_pit_response_{timestamp}.txt"
                with open(raw_file, "w", encoding="utf-8") as f:
                    f.write(response.text)
                print(f"Raw response saved to {raw_file} for inspection")
                try:
                    json_data = json.loads(response.text)
                    json_file = f"{raw_dir}/{args.symbol}_pit_response_{timestamp}.json"
                    with open(json_file, "w", encoding="utf-8") as f:
                        json.dump(json_data, f, indent=4)
                    print(f"Response also saved as formatted JSON to {json_file}")
                except json.JSONDecodeError:
                    print("Response is not valid JSON, skipping JSON formatting")
            except Exception as e:
                print(f"Error saving raw response: {e}")

if __name__ == "__main__":
    main()
