import json
import os
from openai import OpenAI
from dotenv import load_dotenv
import clickhouse_connect  # <-- clickhouse driver

load_dotenv()

client = OpenAI()

LLM = os.environ.get("MODEL")

def create_clickhouse_connection():
    client = clickhouse_connect.get_client(
        host='localhost',    
        port=8123,
        username='admin',
        password='Matkhauchung1@',
        database='DWH'
    )
    return client

# Load schema từ file JSON
with open("schema.json", "r", encoding="utf-8") as file:
    schema = json.load(file)

# Hàm truy vấn
def extract_from_db(user_requirement):
    conn = create_clickhouse_connection()

    response = client.chat.completions.create(
        model=LLM,
        messages=[
            {'role': 'system', 'content': 'You are a very high-quality ClickHouse SQL writing assistant'},
            {'role': 'user', 'content': f"Here is the schema of the database {json.dumps(schema, indent=2)}"},
            {'role': 'user', 'content': f"Write only the SQL script to find the answer for the requirement: {user_requirement} (no need to explain)"}
        ],
    )

    query = response.choices[0].message.content
    query = query.split("```sql\n")[-1].split("\n```")[0]  # nếu có markdown

    print(query)

    result = conn.query(query)

    columns = result.column_names
    rows = result.result_rows

    result_json = [dict(zip(columns, row)) for row in rows]

    try:
        return json.dumps(result_json, indent=4, ensure_ascii=False, default=str)
    except Exception as e:
        print(e)
