import os
import psycopg2
import csv
from io import StringIO
from fastapi import FastAPI, Body
from fastapi.responses import StreamingResponse, Response
from typing import Annotated
from dotenv import load_dotenv

app = FastAPI()
load_dotenv()

conn = psycopg2.connect(
    dbname=os.getenv('DBNAME'),
    user = os.getenv('USER'),
    password = os.getenv('PASSWORD'),
    host = os.getenv('HOST'),
    port=os.getenv('PORT')
)

def serialize_data(cursor):
    columns = [col[0] for col in cursor.description]
    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    return query_data

def generate_csv(data: list[dict]):
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    output.seek(0)
    return output

@app.post("/")
def main(sql: Annotated[str, Body()], token: Annotated[str, Body()]) -> Response:
    if token != os.getenv('TOKEN'):
        return "Permisson denied"
    
    cursor = conn.cursor()
    cursor.execute(sql)
    result = serialize_data(cursor)
    
    response = StreamingResponse(generate_csv(result), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=data.csv"

    
    return response

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)