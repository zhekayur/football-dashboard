import boto3
import awswrangler as wr
import pandas as pd

def debug_data():
    region = "eu-north-1"
    database = "football_db"
    
    session = boto3.Session(region_name=region)
    print(f"Checking database: {database} in {region}")
    
    try:
        # List tables
        tables = wr.catalog.tables(database=database, boto3_session=session)
        fpl_tables = [t for t in tables['Table'].tolist() if t.startswith('raw_fpl_live_data')]
        
        if not fpl_tables:
            print("No 'raw_fpl_live_data' tables found.")
            return

        print(f"Found {len(fpl_tables)} tables. Checking row counts for each...")
        
        for table in fpl_tables:
            query = f'SELECT count(*) as count FROM "{table}"'
            try:
                df = wr.athena.read_sql_query(
                    sql=query,
                    database=database,
                    boto3_session=session
                )
                count = df['count'].iloc[0]
                print(f"Table: {table:<40} | Rows: {count}")
                
                if count > 0:
                     # If we find a table with data, print its columns to ensure compatibility
                     print(f"  -> Found data! Columns: {df.columns.tolist()}")
                     
            except Exception as e:
                print(f"Table: {table:<40} | Error querying: {e}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_data()
