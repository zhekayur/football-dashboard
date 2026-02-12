import boto3
import awswrangler as wr
import pandas as pd

AWS_REGION = "eu-north-1"
DATABASE = "football_db"
TABLE = "raw_portfolio_lake_yevhen_3991"

session = boto3.Session(region_name=AWS_REGION)

# Exact query from app.py
query = f"""
        WITH RankedPlayers AS (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY id 
                    ORDER BY ingested_at DESC
                ) as row_num
            FROM "{DATABASE}"."{TABLE}"
            WHERE assists IS NOT NULL
        )
        SELECT *
        FROM RankedPlayers
        WHERE row_num = 1
        """

print("Testing fixed query...")
try:
    df = wr.athena.read_sql_query(
        sql=query,
        database=DATABASE,
        boto3_session=session,
        ctas_approach=False
    )
    
    print(f"Returned {len(df)} rows.")
    
    if len(df) > 0:
        print("Checking for nulls in stats columns...")
        stats_cols = ['assists', 'minutes', 'yellow_cards', 'red_cards']
        for col in stats_cols:
            null_count = df[col].isnull().sum()
            print(f"Column '{col}' null count: {null_count}")
            if null_count == 0:
                print(f"  -> {col} looks GOOD.")
            else:
                print(f"  -> {col} has NULLS!")
                
        # Check specific values
        print("\nSample Data:")
        print(df[['name', 'assists', 'minutes']].head())
    else:
        print("Query returned NO rows!")

except Exception as e:
    print(f"Error: {e}")
