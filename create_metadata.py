from postgres_utils import PGConn
from datetime import datetime
import json 

psql_conn = PGConn({
                "database": "datadump",
                "host": "localhost",
                "port": "5432",
                "user": "sparsh",
                "password": "algobulls"
            })

def get_latest_entry_data(json_file_path):
    with open (json_file_path, "r") as file:
        data = json.load(file)

    records_by_symbol = {}
    data = data.get("data", [])
    for record in data:
        symbol = record.get('symbol')
        if not symbol:
            continue
            
        if symbol not in records_by_symbol:
            records_by_symbol[symbol] = []
            
        records_by_symbol[symbol].append(record)

    latest_records = {}
    for symbol, records in records_by_symbol.items():
        sorted_records = sorted(
            records,
            key=lambda x: datetime.strptime(x.get('submissionDate'), '%d-%b-%Y %H:%M') if x.get('submissionDate') else datetime(1970, 1, 1),
            reverse=True
        )
        
        if sorted_records:
            latest_records[symbol] = {"record_id": sorted_records[0].get('recordId'), "company_name": sorted_records[0].get('name'), "date": sorted_records[0].get('submissionDate')}
    
    return latest_records

def load_to_psql(data):
    value_list = []
    for symbol, values in data.items():
        company_name = values.get("company_name", "")
        company_name = values.get("company_name", "").replace("'", "")
        record_id = values.get("record_id", "000")
        date = datetime.strptime(values.get("date"), "%d-%b-%Y %H:%M").strftime("%Y-%m-%d %H:%M")
        value_list.append(f"('{company_name}', '{symbol}', '{record_id}', '{date}')")
    
    insert_data = ", ".join(value_list)
    sql_query = f"""
        insert into nse.metadata (company_name, symbol, cg_record_id, submission_date)
        values {insert_data}
    """
    psql_conn.execute(sql_query)


if __name__ == "__main__":
    json_file_path = "/Users/sparsh/Desktop/DataPipeline/company_cg_data.json"
    data = get_latest_entry_data(json_file_path)
    load_to_psql(data)


    


