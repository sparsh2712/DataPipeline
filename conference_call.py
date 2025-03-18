from postgres_utils import PGConn
import re
from difflib import SequenceMatcher

class ConferenceCall:
    def __init__(self, psql_conn):
        self.psql_conn = psql_conn
        self.company_name_to_symbol_map = self._company_name_to_symbol_map()

    def _company_name_to_symbol_map(self):
        sql = """select company_name, symbol from nse.metadata;"""
        result = self.psql_conn.execute(sql)
        return {row[0]: row[1] for row in result}

    def _extract_company_name(self, title):
        prefix_pattern = r'^(Earnings Call|Live:|Management Call|#|Q\dFY\d\d|Press Conference|Deleted on|Webinar:)\s+'
        clean_title = re.sub(prefix_pattern, '', title, flags=re.IGNORECASE)
        company_pattern = r'^(.*?)(?:\s+Earnings\s+Call|\s+Q&A|\s+:|\s+Investor\s+Call|\s+Conference\s+Call|\s+Webinar|\s+Business\s+Update|\s+Management\s+Call|\s+Press\s+Conference|\s+Analyst\s+Meeting|\s+IPO:|\s+\||\s+Capital\s+Markets\s+Day)'
        match = re.search(company_pattern, clean_title, re.IGNORECASE)
        if match:
            company_name = match.group(1).strip()
            company_name = re.sub(r'\s+(Ltd\.|Limited)(\s+for\s+Q)', r' \2', company_name)
            if "Call with" in company_name:
                company_name = company_name.split("Call with")[0].strip()
            elif "Call between" in company_name:
                company_name = company_name.split("Call between")[0].strip()
            company_name = re.sub(r'\s+\([A-Z]+\)$', '', company_name)
            company_name = re.sub(r'\s+Part\s+\d+:', '', company_name)
            return company_name
        
        return None
    
    def _update_conference_calls_table(self):
        sql = """
            select 
                distinct(video_id, video_title) 
            from 
                trendlyne.conference_calls 
            where 
                company_name is null or company_symbol is null;
            """
        result = self.psql_conn.execute(sql)
        print(f"Found {len(result)} videos to process")

        for row in result:
            combined_str = row[0]
            parts = combined_str.strip('()').split(',', 1)
            video_id = parts[0].strip('"\'')
            video_title = parts[1].strip('"\'')
            print(f"Processing video: {video_title}")
            extracted_name = self._extract_company_name(video_title)
            if extracted_name:
                mapped_name, company_symbol = self._get_company_from_name(extracted_name)
                if mapped_name and company_symbol:
                    escaped_extracted = extracted_name.replace("'", "''")
                    escaped_mapped = mapped_name.replace("'", "''")
                    sql_query = f"""
                    update trendlyne.conference_calls
                    set extracted_company_name = '{escaped_extracted}',
                        company_name = '{escaped_mapped}',
                        company_symbol = '{company_symbol}'
                    where video_id = '{video_id}';
                    """
                    self.psql_conn.execute(sql_query)
            else:
                sql_query = f"""
                update trendlyne.conference_calls
                set extracted_company_name = NULL,
                    company_name = NULL,
                    company_symbol = NULL
                where video_id = '{video_id}';
                """
                self.psql_conn.execute(sql_query)
    
    def _get_videos_with_no_company_name(self, file_path=None):
        sql = """select video_id, video_title from trendlyne.conference_calls where company_name is null;"""
        result = self.psql_conn.execute(sql)
        if file_path:
            with open(file_path, "w") as f:
                for row in result:
                    f.write(f"{row[0]}, {row[1]}\n")    
        else:
            return [(row[0], row[1]) for row in result]

    def _get_company_from_name(self, company_name):
        normalised_input = company_name.lower()
        for name, symbol in self.company_name_to_symbol_map.items():
            if normalised_input == name.lower():
                return name, symbol
        
        company_names = list(self.company_name_to_symbol_map.keys())
        best_match = self._get_best_match(normalised_input, company_names, threshold=0.6)

        if best_match:
            return best_match, self.company_name_to_symbol_map[best_match]
    
        return None, None

    def _get_best_match(self, input_string, string_list, threshold = 0.7):
        def similarity(a, b):
            return SequenceMatcher(None, a, b).ratio()
        scores = [(s, similarity(input_string, s)) for s in string_list]
        best_match, best_score = max(scores, key=lambda pair: pair[1], default=('', 0))
        return best_match if best_score >= threshold else ''

        

if __name__ == "__main__":
    pgconn = PGConn({
                "database": "datadump",
                "host": "localhost",
                "port": "5432",
                "user": "sparsh",
                "password": "algobulls"
            })
    cc = ConferenceCall(pgconn)
    cc._update_conference_calls_table()
    # cc.get_videos_with_no_company_name("videos_with_no_company_name.txt")