import mysql.connector
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
from my_logging import logger
import os

# Load environment variables from .env file
load_dotenv()

# Connect to the MariaDB database
def connect_to_mariadb():
    logger.info("Connecting to MariaDB...")
    # Establish a connection to MariaDB using environment variables
    mariadb_conn = mysql.connector.connect(
        host=os.getenv('DB_HOST'),         # Database host from environment
        port=os.getenv('DB_PORT'),         # Database port from environment
        user=os.getenv('DB_USER'),         # Database user from environment
        password=os.getenv('DB_PASSWORD'), # Database password from environment
        database=os.getenv('DB_DATABASE')  # Database name from environment
    )
    logger.info("Connected to MariaDB.")
    return mariadb_conn


# Initialize the BigQuery client using a service account key
def initialize_bigquery_client(project_id, service_account_key_file):
    logger.info("Initializing BigQuery client...")
    # Load credentials from the provided service account key file
    credentials = service_account.Credentials.from_service_account_file(service_account_key_file)
    
    # Create a BigQuery client with the specified project ID and credentials
    bigquery_client = bigquery.Client(project=project_id, credentials=credentials)
    logger.info("BigQuery client initialized.")
    return bigquery_client


# Create a BigQuery dataset if it doesn't exist
def create_bigquery_dataset(bigquery_client, dataset_id):
    logger.info("Creating BigQuery dataset...")
    # Define a reference to the dataset
    dataset_ref = bigquery_client.dataset(dataset_id)
    
    # Create a BigQuery dataset with the specified ID and allow existence check
    dataset = bigquery.Dataset(dataset_ref)
    dataset = bigquery_client.create_dataset(dataset, exists_ok=True)
    logger.info("BigQuery dataset created.")
    return dataset


# Retrieve the schema of a specific BigQuery table
def retrieve_bigquery_table_schema(bigquery_client, dataset_id, table_id):
    logger.info("Retrieving BigQuery table schema...")
    # Define references to the dataset and the table within it
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    # Get the schema of the specified BigQuery table
    table = bigquery_client.get_table(table_ref)
    logger.info("BigQuery table schema retrieved.")
    return table
