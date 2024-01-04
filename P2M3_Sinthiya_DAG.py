'''
=================================================
Milestone 3

Nama  : Sinthiya Kusuma Nagari
Batch : FTDS-001-RMT

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Adapun dataset yang dipakai adalah dataset mengenai penjualan dari bisnis retail di US tahun 2011-2014
=================================================
'''
# import libraries
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from elasticsearch import Elasticsearch
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd

def ambil_data():
    # fetch data
    '''  Fungsi ini ditujukan untuk mengambil data dari PostgreSQL untuk selanjutnya dilakukan Data Cleaning.
    url: postgresql+psycopg2://airflow:airflow@postgres/airflow - lokasi PostgreSQL
    database: airflow - nama database dimana data disimpan
    table: table_m3 - nama table dimana data disimpan  
    '''
    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
    conn = engine.connect()

    df = pd.read_sql_query("select * from table_m3", conn)

    # save to .data/ path
    df.to_csv('/opt/airflow/dags/P2M3_Sinthiya_data_raw.csv', sep=',', index=False)

def processing():
    # load
    '''  Fungsi ini ditujukan untuk membersihkan data.
    output: P2M3_Sinthiya_data_clean.csv (data yang telah bersih)
    '''
    data = pd.read_csv("/opt/airflow/dags/P2M3_Sinthiya_data_raw.csv")

    # filter data
    data.dropna(inplace=True)
    data.drop_duplicates(inplace=True)
    data['Ship Date'] = pd.to_datetime(data['Ship Date'], format='%d-%m-%Y')
    data['Order Date'] = pd.to_datetime(data['Order Date'], format='%d-%m-%Y')
    for i in data.columns:
        i_lower = i.lower()
        i_cleaned = i_lower.replace(' ', '_').replace('-', '_')
        data.rename(columns={i: i_cleaned}, inplace=True)
    data.to_csv('/opt/airflow/dags/P2M3_Sinthiya_data_clean.csv', index=False)

def upload_to_elasticsearch():
    '''  Fungsi ini ditujukan untuk upload data ke elasticsearch.
    '''
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/P2M3_Sinthiya_data_clean.csv')
    
    for i, r in df.iterrows():
        doc = r.to_dict()  # Convert the row to a dictionary
        res = es.index(index="table_m3", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")


default_args = {
    'owner': 'Sintia',
    'start_date': datetime(2023, 12, 24, 12, 00)
}
# mendefine dag agar bisa dibaca di ariflow dengan penjadwalan setiap jam 6.30
with DAG(
    "P2M3_Sinthiya_DAG",
    description='Milestone_3',
    schedule_interval='30 6 * * *',
    default_args=default_args, 
    catchup=False
) as dag:
    # Task: 1
    '''  Fungsi ini ditujukan untuk menjalankan ambil data dari postgresql. '''
    fetching_data = PythonOperator(
        task_id='fetching_data',
        python_callable=ambil_data)

    # Task: 2
    '''  Fungsi ini ditujukan untuk menjalankan pembersihan data.'''
    edit_data = PythonOperator(
        task_id='edit_data',
        python_callable=processing)

    # Task: 3
    '''  Fungsi ini ditujukan untuk menjalankan upload data ke elasticsearch.'''
    upload_data = PythonOperator(
        task_id='upload_data_elastic',
        python_callable=upload_to_elasticsearch)

    # Urutan rangkaian perkerjaan dari task airflow
    with TaskGroup("processing_tasks") as processing_tasks:
        fetching_data >> edit_data >> upload_data