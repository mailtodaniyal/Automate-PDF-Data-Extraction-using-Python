from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json
import psycopg2
from google.cloud import vision
from google.cloud.vision_v1 import types

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "path/to/your/service-account.json"

DB_CONFIG = {
    "host": "your_db_host",
    "database": "your_database",
    "user": "your_username",
    "password": "your_password"
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'extract_text_from_pdfs',
    default_args=default_args,
    description='Extract text from scanned PDFs and store in database',
    schedule_interval=timedelta(days=1),
    catchup=False
)

def extract_text_from_pdf(pdf_path):
    client = vision.ImageAnnotatorClient()
    
    with open(pdf_path, "rb") as pdf_file:
        content = pdf_file.read()
    
    image = types.Image(content=content)
    response = client.document_text_detection(image=image)
    
    extracted_text = response.full_text_annotation.text if response.full_text_annotation else ""
    return extracted_text

def process_pdfs():
    input_folder = "/path/to/pdf/folder"
    output_folder = "/path/to/output/json"
    os.makedirs(output_folder, exist_ok=True)
    
    extracted_data = []
    for filename in os.listdir(input_folder):
        if filename.endswith(".pdf"):
            pdf_path = os.path.join(input_folder, filename)
            text = extract_text_from_pdf(pdf_path)
            
            result = {"filename": filename, "text": text}
            extracted_data.append(result)
            
            with open(os.path.join(output_folder, f"{filename}.json"), "w") as f:
                json.dump(result, f, indent=4)
    
    return extracted_data

def store_text_in_db():
    extracted_data = process_pdfs()
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS extracted_text (
        id SERIAL PRIMARY KEY,
        filename TEXT,
        extracted_text TEXT
    )"""
    cursor.execute(create_table_query)
    
    for data in extracted_data:
        cursor.execute("INSERT INTO extracted_text (filename, extracted_text) VALUES (%s, %s)", 
                       (data["filename"], data["text"]))
    
    conn.commit()
    cursor.close()
    conn.close()

task_extract = PythonOperator(
    task_id='extract_text',
    python_callable=process_pdfs,
    dag=dag,
)

task_store = PythonOperator(
    task_id='store_text_in_db',
    python_callable=store_text_in_db,
    dag=dag,
)

task_extract >> task_store
