import dlt
import requests
import os
from dotenv import load_dotenv

load_dotenv()

READ_TOKEN = os.getenv('LOGFIRE_READ_TOKEN')
BASE_URL = 'https://logfire-us.pydantic.dev'

@dlt.resource(name="records", write_disposition="replace")
def logfire_records():
    query = "SELECT * FROM records ORDER BY start_timestamp DESC LIMIT 1000"
    
    response = requests.get(
        f'{BASE_URL}/v1/query',
        params={'sql': query},
        headers={
            'Authorization': f'Bearer {READ_TOKEN}',
            'Accept': 'application/json'
        }
    )
    
    data = response.json()
    rows = data.get('rows', [])
    columns = data.get('columns', [])
    
    print(f"📊 Found {len(rows)} rows with {len(columns)} columns")
    print(f"📋 Columns: {columns}")
    
    if rows:
        print(f"📝 First row sample: {rows[0]}")
    
    for row in rows:
        yield dict(zip(columns, row))

pipeline = dlt.pipeline(
    pipeline_name='logfire_pipeline_v2',  # Changed name
    destination='duckdb',
    dataset_name='agent_traces',
)

load_info = pipeline.run(logfire_records())

print("\n" + "="*50)
print("LOAD INFO:")
print(load_info)
print("\nRow counts:", load_info.row_counts)