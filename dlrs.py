import time
from google.cloud import bigquery
from my_logging import logger
from utils import connect_to_mariadb, initialize_bigquery_client, create_bigquery_dataset, retrieve_bigquery_table_schema

# Configuration for BigQuery and Google Cloud Storage
project_id = 'rt-warehouse'          # The ID of the Google Cloud project
dataset_id = 'EmalifyMariaDB'               # The ID of the BigQuery dataset
table_id = 'dlrs'                  # The ID of the BigQuery table
service_account_key_file = '.credentials'     # Path to the service account key file

# Establish a connection to MariaDB
mariadb_conn = connect_to_mariadb()


""" Function to fetch schema for a given table from MariaDB """
def fetch_table_schema_from_mariadb(mariadb_conn, table_name):
    """
    Fetches the schema of a specified table from MariaDB.
    
    Args:
        mariadb_conn (connection): A connection to the MariaDB database.
        table_name (str): The name of the table.
        
    Returns:
        list: A list of tuples representing the table schema.
    """
    logger.info(f'Fetching schema for {table_name}')
    mariadb_cursor = mariadb_conn.cursor()
    table_schema_query = f"DESCRIBE {table_name}"
    mariadb_cursor.execute(table_schema_query)
    table_schema = mariadb_cursor.fetchall()
    logger.info(f'Schema for {table_name} is {table_schema}')
    return table_schema

    
# Function to convert MySQL schema to BigQuery schema
def convert_mysql_schema_to_bigquery_schema(table_schema):
    """
    Converts a MySQL table schema to a BigQuery-compatible schema.
    
    Args:
        table_schema (list): A list of tuples representing the MySQL table schema.
        
    Returns:
        list: A list of `bigquery.SchemaField` objects representing the BigQuery schema.
    """
    logger.info(f'Converting {table_schema} to bigquery')
    bq_schema = []
    for column in table_schema:
        column_name = column[0]
        column_type = column[1]
        bq_field = bigquery.SchemaField(name=column_name, field_type=column_type)
        bq_schema.append(bq_field)
        logger.info(f'Big Query Schema for {bq_schema}')
    return bq_schema


 # Function to retrieve the last ID from a BigQuery table
def get_bq_last_id(bigquery_client, table):
    """
    Retrieves the last ID from a specified BigQuery table.
    
    Args:
        bigquery_client (bigquery.Client): An initialized BigQuery client.
        table (bigquery.Table): The BigQuery table to retrieve the last ID from.
        
    Returns:
        int: The last ID value.
    """
    logger.info(f'Getting big query last id for {table}')
    # Retrieve the maximum ID from the BigQuery table
    max_id_query = f"SELECT MAX(id) FROM {table.dataset_id}.{table.table_id}"
    max_id_result = bigquery_client.query(max_id_query).result()

    for row in max_id_result:
        last_id = row[0]
    logger.info(f'Big Query last id is {last_id}')
    return last_id


# Function to fetch data from MariaDB for a given table
def fetch_data_from_mariadb(mariadb_cursor, table_name, last_id=None):
    """
    Fetches data from MariaDB for a specified table.
    
    Args:
        mariadb_cursor (cursor): A MariaDB cursor object.
        table_name (str): The name of the table.
        last_id (int, optional): The last ID value. Defaults to None.
        
    Returns:
        list: A list of rows retrieved from MariaDB.
    """
    logger.info(f'Fetching data from mariadb for {table_name}')
    if last_id is None:
        # Fetch all data from the MariaDB table
        mysql_query = f"SELECT * FROM {table_name} LIMIT 10000"
    else:
        # Fetch data from MySQL table with ID greater than the last ID in BigQuery
        mysql_query = f"SELECT * FROM {table_name} WHERE id > {last_id} LIMIT 10000"

    logger.info(f'MySQL Query: {mysql_query}') 

    mariadb_cursor.execute(mysql_query)
    rows = mariadb_cursor.fetchall()
    logger.info(f'All rows from {table_name} fetched. Row count: {len(rows)}')
    return rows


def insert_data_into_bigquery(bigquery_client, table, rows):
    """
    Inserts data into a specified BigQuery table.
    
    Args:
        bigquery_client (bigquery.Client): An initialized BigQuery client.
        table (bigquery.Table): The BigQuery table to insert data into.
        rows (list): A list of rows to insert.
    """
    logger.info(f'Inserting {len(rows)} rows into {table} ')  # Updated log message
    batch_size = 1000
    for i in range(0, len(rows), batch_size):
        batch_rows = rows[i:i + batch_size]

        # Insert batch rows into BigQuery table
        errors = bigquery_client.insert_rows(table, batch_rows, selected_fields=table.schema)

        if errors:
            logger.error(f"Failed to insert rows into BigQuery table: {errors}")  # Updated log message
        else:
            logger.info("Batch insertion into BigQuery table completed successfully!")  # Updated log message



def main():
    """
    Main function for orchestrating the data migration process.
    """
    # Initialize BigQuery client
    bigquery_client = initialize_bigquery_client(project_id, service_account_key_file)

    # Create BigQuery dataset if it doesn't exist
    dataset = create_bigquery_dataset(bigquery_client, dataset_id)

    # Fetch table schema from MariaDB
    table_name = 'dlrs'
    table_schema = fetch_table_schema_from_mariadb(mariadb_conn, table_name)

    # Convert MySQL schema to BigQuery schema
    bq_schema = convert_mysql_schema_to_bigquery_schema(table_schema)

    # Retrieve BigQuery table schema
    table = retrieve_bigquery_table_schema(bigquery_client, dataset_id, table_id)

    # Get the last ID from BigQuery table
    last_id = get_bq_last_id(bigquery_client, table)

    # Fetch data from MariaDB for the specified table
    rows = fetch_data_from_mariadb(mariadb_conn.cursor(), table_name, last_id)

    # Insert data into BigQuery
    insert_data_into_bigquery(bigquery_client, table, rows)

    # Close connections
    mariadb_conn.close()


"""ensures that the main function is run in an infinite loop at regular intervals (every 10 seconds) """
if __name__ == "__main__":
    while True:
        main()
        time.sleep(10*1)