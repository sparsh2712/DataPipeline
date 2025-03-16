import requests
import json
import brotli
from typing import Dict, Any, Optional
import datetime
import time
import random
import os
from postgres_utils import PGConn

class NSEDataFetcher:
    def __init__(self, config_path, start_date='01-01-2000', end_date='01-03-2025', base_url="https://www.nseindia.com"):
        try:
            # Load configuration files
            with open(os.path.join(config_path, "api.json"), "r") as f:
                self.config_dict = json.load(f)
            with open(os.path.join(config_path, "headers.json"), "r") as f:
                self.headers = json.load(f)
            with open(os.path.join(config_path, "schema.json"), "r") as f:
                self.schema = json.load(f)
                
            self.base_url = base_url
            self.domain = "nseindia.com"
            self.start_date = start_date
            self.end_date = end_date
            self.request_count = 0
            self.requests_before_refresh = 20
            
            # Initialize PostgreSQL connection
            psql_config = {
                "database": "datadump",
                "host": "localhost",
                "port": "5432",
                "user": "sparsh",
                "password": "algobulls"
            }
            self.psql_conn = PGConn(psql_config)
            
            # Get fresh session with cookies instead of loading from file
            self.session = self.get_fresh_cookies()
            # Store current cookies for reference
            self.cookies = {cookie.name: cookie.value for cookie in self.session.cookies}
            
        except Exception as e:
            self._log_error(f"Initialization error: {str(e)}")
            raise

    def _log_error(self, error_message):
        """Log errors to errors.txt file"""
        try:
            with open("errors.txt", "a") as f:
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"[{timestamp}] {error_message}\n")
        except Exception as e:
            print(f"Error writing to error log: {e}")

    def get_fresh_cookies(self, max_retries: int = 3) -> requests.Session:
        session = requests.Session()
        # Use self.headers instead of hardcoded headers
        for attempt in range(max_retries):
            try:
                main_page_url = "https://www.nseindia.com/"
                response = session.get(main_page_url, headers=self.headers, timeout=15)
                response.raise_for_status()
                
                if not session.cookies or 'nsit' not in [c.name for c in session.cookies]:
                    time.sleep(2)
                    continue
                    
                pit_page_url = "https://www.nseindia.com/companies-listing/corporate-filings-insider-trading"
                response = session.get(pit_page_url, headers=self.headers, timeout=15)
                response.raise_for_status()
                
                if 'nseappid' in [c.name for c in session.cookies]:
                    return session
                else:
                    time.sleep(2)
                    continue
            except requests.exceptions.RequestException as e:
                print(f"Error getting fresh cookies (attempt {attempt+1}/{max_retries}): {e}")
                time.sleep(2)
        print("Warning: Could not obtain all necessary cookies after maximum retries.")
        return session

    def _refresh_session_if_needed(self):
        """Refresh cookies if request count reaches threshold"""
        self.request_count += 1
        if self.request_count >= self.requests_before_refresh:
            print("Refreshing cookies...")
            self.session = self.get_fresh_cookies()
            # Reset cookie counter
            self.request_count = 0
            # Update cookies dictionary with new values
            self.cookies = {cookie.name: cookie.value for cookie in self.session.cookies}

    def _get_date_ranges(self, start_date: str, end_date: str, freq_days: int):
        try:
            start_date = datetime.datetime.strptime(start_date, "%d-%m-%Y")
            end_date = datetime.datetime.strptime(end_date, "%d-%m-%Y")

            date_range = []
            current_end = end_date

            while current_end >= start_date:
                current_start = current_end - datetime.timedelta(days=freq_days)
                if current_start < start_date:
                    current_start = start_date
                
                date_range.append({
                    "from_date": current_start.strftime("%d-%m-%Y"),
                    "to_date": current_end.strftime("%d-%m-%Y")
                })

                if current_start <= start_date:
                    break

                current_end = current_start - datetime.timedelta(days=1)

            return date_range
        except Exception as e:
            self._log_error(f"Error generating date ranges: {str(e)}")
            return []
    
    def _make_request(self, endpoint, params, referer_url):
        # Check if we need to refresh cookies
        self._refresh_session_if_needed()
        
        url_extension = endpoint + "?" + "&".join([f"{key}={value}" for key, value in params.items()])
        url = self.base_url + "/api/" + url_extension
        
        try:
            self.session.get(referer_url, headers=self.headers, timeout=10)
            response = self.session.get(url, headers=self.headers, timeout=60)
            response.raise_for_status()

            if not response.content:
                self._log_error(f"Empty response received for URL: {url}")
                return None

            if 'br' in response.headers.get('Content-Encoding', ''):
                try:
                    decompressed_content = brotli.decompress(response.content)
                    json_text = decompressed_content.decode('utf-8')
                    return json.loads(json_text)
                except Exception as e:
                    self._log_error(f"Error decompressing/decoding JSON: {str(e)} for URL: {url}")
                    return None
            else:
                try:
                    return response.json()
                except json.JSONDecodeError as e:
                    self._log_error(f"Error decoding JSON: {str(e)} for URL: {url}")
                    return None
                
        except requests.exceptions.RequestException as e:
            self._log_error(f"Request error for URL {url}: {str(e)}")
            return None
        except Exception as e:
            self._log_error(f"Unexpected error for URL {url}: {str(e)}")
            return None

    def _generate_param_combinations(self, list_params):
        if not list_params:
            return [{}]
        
        result = []
        key, values = next(iter(list_params.items()))
        remaining = {k: v for k, v in list_params.items() if k != key}
        sub_combinations = self._generate_param_combinations(remaining)

        for value in values:
            for sub_combo in sub_combinations:
                result.append({**sub_combo, key: value})
        
        return result

    def _write_to_db(self, data_list, table_name, schema_map):
        """Write data to PostgreSQL database using f-string query"""
        try:
            if not data_list or not schema_map:
                return 0
            
            # Get column names from schema map
            columns = list(schema_map.keys())
            
            with open("debug.json", "w") as file:
                json.dump(data_list, file, indent=4)
            
            def format_value(val):
                if val is None or val == "":
                    return "NULL"
                if isinstance(val, str):
                    # Escape single quotes by doubling them
                    safe_val = val.replace("'", "''")
                    return f"'{safe_val}'"
                # If numeric or other type, convert directly
                return str(val)
            # Build VALUES part of the SQL statement
            values_list = []
            for item in data_list:
                value_row = []
                for col in columns:
                    api_key = schema_map[col]
                    value = item.get(api_key)
                    value_row.append(format_value(value))
                
                # print(value_row)
                # time.sleep(2)
                values_list.append(f"({', '.join(value_row)})")
            
            if not values_list:
                return 0
            
            # Build the complete SQL statement
            sql = f"""
            INSERT INTO nse.{table_name.lower()} 
            ({', '.join(columns)})
            VALUES {', '.join(values_list)};
            """

            with open ("sql.txt", "w") as f:
                f.write(sql)
            
            # Execute the SQL query
            self.psql_conn.execute(sql)
            
            return len(values_list)
        except Exception as e:
            self._log_error(f"Error writing to database table {table_name}: {str(e)}")
            return 0

    def fetch_data(self, endpoint_name, endpoint_config, freq_days=7):
        """Fetch data and write to database"""
        try:
            if endpoint_name not in self.schema:
                self._log_error(f"Schema not found for endpoint: {endpoint_name}")
                return 0
            
            schema_map = self.schema[endpoint_name]
            endpoint = endpoint_config.get("endpoint", "")
            if not endpoint:
                self._log_error(f"Endpoint URL not found for: {endpoint_name}")
                return 0
            
            referer_suffix = endpoint_config.get("referer", "corporate-filings-insider-trading")
            referer_url = f"{self.base_url}/companies-listing/{referer_suffix}"
            self.headers["Referer"] = referer_url
            
            params = endpoint_config.get("params", {})
            has_date_params = "from_date" in params and "to_date" in params
            
            date_ranges = []
            if has_date_params:
                from_date = params.get("from_date", self.start_date)
                to_date = params.get("to_date", self.end_date)
                if not from_date:
                    from_date = self.start_date
                if not to_date:
                    to_date = self.end_date
                
                date_ranges = self._get_date_ranges(from_date, to_date, freq_days)
                params_copy = params.copy()
                params_copy.pop("from_date", None)
                params_copy.pop("to_date", None)
                params = params_copy
            else:
                date_ranges = [{}]
            
            list_params = {}
            for key, value in list(params.items()):
                if isinstance(value, list):
                    list_params[key] = value
                    params.pop(key)
            
            total_rows_inserted = 0
            
            if list_params:
                param_combinations = self._generate_param_combinations(list_params)
                
                for param_combo in param_combinations:
                    current_params = {**params, **param_combo}
                    for date_range in date_ranges:
                        request_params = {**current_params, **date_range}
                        result = self._make_request(endpoint, request_params, referer_url)
                        
                        if result:
                            # Check if the result is directly a list or if it's nested
                            data_list = result.get("data", result)
                            if "symbol" in request_params.keys():
                                for entry in data_list:
                                    entry["symbol"] = request_params["symbol"]
                            if data_list and isinstance(data_list, list):

                                rows_inserted = self._write_to_db(data_list, endpoint_name, schema_map)
                                total_rows_inserted += rows_inserted
                                print(f"Inserted {rows_inserted} rows for {endpoint_name} with params {request_params}")
                            else:
                                self._log_error(f"Invalid data format for {endpoint_name}: {str(result)[:100]}...")
                        
                        # Add random delay to avoid rate limiting
                        random_time = random.uniform(0.5, 2.0)
                        time.sleep(random_time)
            else:
                for date_range in date_ranges:
                    request_params = {**params, **date_range}
                    result = self._make_request(endpoint, request_params, referer_url)
                    if result:
                        # Check if the result is directly a list or if it's nested
                        data_list = result
                        
                        if data_list and isinstance(data_list, list):
                            rows_inserted = self._write_to_db(data_list, endpoint_name, schema_map)
                            total_rows_inserted += rows_inserted
                            print(f"Inserted {rows_inserted} rows for {endpoint_name} with params {request_params}")
                        else:
                            self._log_error(f"Invalid data format for {endpoint_name}: {str(result)[:100]}...")
                    
                    # Add random delay to avoid rate limiting
                    random_time = random.uniform(0.5, 2.0)
                    time.sleep(random_time)
            
            return total_rows_inserted
        except Exception as e:
            self._log_error(f"Error fetching data for {endpoint_name}: {str(e)}")
            return 0

    def run(self):
        """Run the data fetcher for all endpoints in the config"""
        try:
            total_rows = 0
            for endpoint_name, endpoint_config in self.config_dict.items():
                if endpoint_name in self.schema:
                    print(f"Processing {endpoint_name}...")
                    rows = self.fetch_data(endpoint_name, endpoint_config)
                    total_rows += rows
                    print(f"Completed {endpoint_name}: {rows} rows inserted")
                else:
                    self._log_error(f"Schema not found for {endpoint_name}, skipping...")
            
            print(f"Total rows inserted: {total_rows}")
            return total_rows
        except Exception as e:
            self._log_error(f"Error in run method: {str(e)}")
            return 0

if __name__ == "__main__":
    # Config path
    CONFIG_PATH = "/Users/sparsh/Desktop/WebCrawler/config"
    
    # Initialize and run the data fetcher
    try:
        fetcher = NSEDataFetcher(CONFIG_PATH, start_date='01-01-2023', end_date='01-03-2025')
        total_rows = fetcher.run()
        print(f"Data fetching completed. Total rows inserted: {total_rows}")
    except Exception as e:
        print(f"Error running NSEDataFetcher: {str(e)}")
        with open("errors.txt", "a") as f:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"[{timestamp}] Fatal error: {str(e)}\n")