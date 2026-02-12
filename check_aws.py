import boto3
import sys

def check_glue_tables():
    try:
        glue = boto3.client('glue', region_name='eu-north-1')
        database_name = 'football_db'
        
        response = glue.get_tables(DatabaseName=database_name)
        tables = [t['Name'] for t in response['TableList']]
        
        print(f"Tables in {database_name}:")
        for table in tables:
            print(f" - {table}")
            
        # Find latest raw_fpl_live_data table
        fpl_tables = [t for t in tables if t.startswith('raw_fpl_live_data')]
        if fpl_tables:
            latest_table = sorted(fpl_tables)[-1]
            print(f"\nLatest FPL table: {latest_table}")
        else:
            print("\nNo FPL tables found.")
            
    except Exception as e:
        print(f"Error connecting to AWS Glue: {e}")

if __name__ == "__main__":
    check_glue_tables()
