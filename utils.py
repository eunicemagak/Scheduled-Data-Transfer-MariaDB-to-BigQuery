import mysql.connector
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import os

load_dotenv()

def connect_to_mariadb():
    mariadb_conn = mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_DATABASE'))
    return mariadb_conn


def initialize_bigquery_client(project_id, service_account_key_file):
    credentials = service_account.Credentials.from_service_account_file(service_account_key_file)
    bigquery_client = bigquery.Client(project=project_id, credentials=credentials)
    return bigquery_client


def create_bigquery_dataset(bigquery_client, dataset_id):
    dataset_ref = bigquery_client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset = bigquery_client.create_dataset(dataset, exists_ok=True)
    return dataset


def retrieve_bigquery_table_schema(bigquery_client, dataset_id, table_id):
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery_client.get_table(table_ref)
    return table
