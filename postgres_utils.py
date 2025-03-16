import psycopg2
import threading

class PGConn:
    _expected_keys = ["database", "host", "port", "user", "password"]
    
    def __init__(self, psql_setup_details={}):
        self.details = psql_setup_details
        self.conn = None
        self.executing = False
        self.query = ""
        self.terminate_event = threading.Event()
        if sorted(self.details.keys()) != sorted(self._expected_keys):
            raise ValueError(f"Expected keys: {self._expected_keys}. Got: {self.details.keys()}")
        
    def connection(self):
        try:
            if self.conn is None:
                conn = psycopg2.connect(dbname=self.details["database"],
                                        host=self.details["host"],
                                        port=self.details["port"],
                                        user=self.details["user"],
                                        password=self.details["password"])
                self.conn = conn
                self.conn.autocommit = True
        except Exception as e:
            raise e
        finally:
            return self.conn
    
    def handle_termination(self, signal_received, frame):
        print("Termination signal received. Cleaning up...")
        try:
            if self.conn is not None:
                self.conn.cancel()
                print("Query cancelled on the database.")
                print(self.query)
        except Exception as e:
            print(f"Failed to cancel query: {e}")
        finally:
            exit(1)
            
    def cleanup(self):
        print("Termination signal received. Cleaning up...")
        self.terminate_event.set()
        try:
            if self.conn is not None:
                self.conn.cancel()
                print("Query cancelled on the database.")
                print(self.query)
        except Exception as e:
            print(f"Failed to cancel query: {e}")
        finally:
            exit(1)
        
    def run_psql_query(self,sql,with_desc=False):
        try:
            with self.connection().cursor() as curr:
                curr.execute(sql)
                try:
                    if with_desc:
                        self.result = (curr.fetchall(), curr.description)
                    else:
                        self.result = curr.fetchall()
                except:
                    self.result = None
        except Exception as e:
            print(f"Error executing query: {sql}")
            print(e)
            self.result = None
    
    def execute(self, sql, with_desc=False):
        if self.executing:
            print("Another query is already executing. Waiting for it to finish.")
        while self.executing:
            pass
        self.executing = True
        self.query = sql
        self.result = []
        
        query_thread = threading.Thread(target=self.run_psql_query,args=(sql,with_desc))
        query_thread.start()
        try:
            query_thread.join()
        except KeyboardInterrupt:
            self.cleanup() 
        
        self.executing = False
        self.query = ""
        return self.result

    def __del__(self):
        print("Destructor called for", self.conn)
        if self.conn is not None:
            self.conn.close()
        self.conn = None


if __name__ == "__main__":
    pgconn = PGConn({
                "database": "datadump",
                "host": "localhost",
                "port": "5432",
                "user": "sparsh",
                "password": "algobulls"
            })
