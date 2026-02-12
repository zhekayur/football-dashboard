import boto3
import awswrangler as wr
import pandas as pd

AWS_REGION = "eu-north-1"
DATABASE = "football_db"
TABLE = "raw_portfolio_lake_yevhen_3991"

session = boto3.Session(region_name=AWS_REGION)

query = f"""
WITH RankedPlayers AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM "{DATABASE}"."{TABLE}"
)
SELECT 
    row_num,
    count(*) as total_rows,
    count(assists) as valid_assists,
    count(minutes) as valid_minutes,
    max(ingested_at) as ingestion_time
FROM RankedPlayers
WHERE row_num <= 3
GROUP BY row_num
ORDER BY row_num
"""

print("Querying Athena to compare data versions...")
try:
    df = wr.athena.read_sql_query(
        sql=query,
        database=DATABASE,
        boto3_session=session,
        ctas_approach=False
    )
    print(df)
    
except Exception as e:
    print(e)
