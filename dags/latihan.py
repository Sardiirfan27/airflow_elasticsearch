from airflow.models import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime
from sqlalchemy import create_engine #koneksi ke postgres
import pandas as pd

from elasticsearch import Elasticsearch

def csv_to_psg():
    db = 'airflow_m3'
    user = 'airflow_m3'
    password='airflow_m3'
    host='postgres'
    
    postgres_url= f'postgresql+psycopg2://{user}:{password}@{host}/{db}'
    engine= create_engine(postgres_url)
    conn= engine.connect()

    df= pd.read_csv('/opt/airflow/dags/irfan.csv') 
    df.to_sql('testing', conn, index=False, if_exists='replace')   


def psg_to_csv():
    db = 'airflow_m3'
    user = 'airflow_m3'
    password='airflow_m3'
    host='postgres'
    
    postgres_url= f'postgresql+psycopg2://{user}:{password}@{host}/{db}'
    engine= create_engine(postgres_url)
    conn= engine.connect()

    df = pd.read_sql_query("select * from testing", conn) #nama table sesuaikan sama nama table di postgres
    df.to_csv('/opt/airflow/dags/irfan_new.csv', sep=',', index=False)
    
def clean_csv():
    df= pd.read_csv('/opt/airflow/dags/irfan_new.csv')
    df.drop_duplicates(inplace=True)
    df= df.query('age>=0')
    df.to_csv('/opt/airflow/dags/irfan_clean.csv', index=False)

def upload_to_elasticsearch():
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/irfan_clean.csv')
    
    for i, r in df.iterrows():
        doc = r.to_dict()  # Convert the row to a dictionary
        res = es.index(index="testing_m3", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")

default_args = {
    'owner': 'irfan',
    'start_date': datetime(2022, 10, 10),
    # 'retries': 1,
    # 'retry_delay': dt.timedelta(minutes=5),
}

with DAG('Testing_M3',
         default_args=default_args,
         description='Milestone_3',
         schedule_interval='10 5 10 * *',      # '0 * * * *',
        #  cathup=False
         ) as dag:

    load_data_pg = PythonOperator(task_id='csv_psg',
                                 python_callable=csv_to_psg)
    
    load_data_csv = PythonOperator(task_id='psg_csv',
                                 python_callable=psg_to_csv)
    
    clean_data_csv = PythonOperator(task_id='clean_csv',
                                 python_callable=clean_csv)
    
    load_data_es = PythonOperator(task_id='load_data_es',
                                 python_callable=upload_to_elasticsearch)
    
    
load_data_pg >> load_data_csv >> clean_data_csv >> load_data_es
    