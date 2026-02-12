import boto3
import awswrangler as wr

AWS_REGION = "eu-north-1"
DATABASE = "football_db"

session = boto3.Session(region_name=AWS_REGION)

print(f"Listing tables in {DATABASE}...")
try:
    tables = wr.catalog.tables(database=DATABASE, boto3_session=session)
    table_names = tables['Table'].tolist()
    
    for table in table_names:
        print(f"Checking table: {table}")
        try:
            query = f'SELECT count(*) as count FROM "{table}"'
            df = wr.athena.read_sql_query(
                sql=query,
                database=DATABASE,
                boto3_session=session,
                ctas_approach=False
            )
            count = df['count'].iloc[0]
            print(f"  -> Rows: {count}")
            
            # Check for non-null assists in a small sample if rows > 0
            if count > 0:
                 q_sample = f'SELECT count(assists) as filled_assists FROM "{table}" WHERE assists IS NOT NULL'
                 df_sample = wr.athena.read_sql_query(
                    sql=q_sample,
                    database=DATABASE,
                    boto3_session=session
                 )
                 filled = df_sample['filled_assists'].iloc[0]
                 print(f"  -> Non-null assists: {filled}")

        except Exception as e:
            print(f"  -> Error: {e}")
            
except Exception as e:
    print(e)
