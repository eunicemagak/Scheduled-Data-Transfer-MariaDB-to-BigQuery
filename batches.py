import time
from google.cloud import bigquery
from my_logging import logger
from utils import connect_to_mariadb, initialize_bigquery_client, create_bigquery_dataset, retrieve_bigquery_table_schema

# BigQuery client and dataset configuration
project_id = 'rt-warehouse'
dataset_id = 'EmalifyMariaDB'
table_id = 'batches'
service_account_key_file = '.credentials'

# Connect to MariaDB database
mariadb_conn = connect_to_mariadb()


def fetch_table_schema_from_mariadb(mariadb_conn, table_name):
    logger.info(f'Fetching schema for {table_name}')
    mariadb_cursor = mariadb_conn.cursor()
    table_schema_query = f"DESCRIBE {table_name}"
    mariadb_cursor.execute(table_schema_query)
    table_schema = mariadb_cursor.fetchall()
    logger.info(f'Schema for {table_name} is {table_schema}')
    return table_schema

    
def convert_mysql_schema_to_bigquery_schema(table_schema):
    logger.info(f'Converting {table_schema} to bigquery')
    bq_schema = []
    for column in table_schema:
        column_name = column[0]
        column_type = column[1]
        bq_field = bigquery.SchemaField(name=column_name, field_type=column_type)
        bq_schema.append(bq_field)
        logger.info(f'Big Query Schema for {bq_schema}')
    return bq_schema


def get_bq_last_id(bigquery_client, table):
    logger.info(f'Getting big query last id for {table}')
    max_id_query = f"SELECT MAX(id) FROM {table.dataset_id}.{table.table_id}"
    max_id_result = bigquery_client.query(max_id_query).result()
    
    for row in max_id_result:
        last_id = row[0]
    logger.info(f'Big Query last id is {last_id}')
    return last_id


def fetch_data_from_mariadb(mariadb_cursor, table_name, last_id=None):
    logger.info(f'Fetching data from mariadb for {table_name}')
    if last_id is None:
        # Fetch all data from the MariaDB table
        mysql_query = f"SELECT * FROM {table_name}"
    else:
        # Fetch data from MySQL table with ID greater than the last ID in BigQuery
        mysql_query = f"SELECT * FROM {table_name} WHERE id > {last_id}"

    logger.info(f'MySQL Query: {mysql_query}') 

    mariadb_cursor.execute(mysql_query)
    rows = mariadb_cursor.fetchall()
    logger.info(f'All rows from {table_name} fetched. Row count: {len(rows)}')
    return rows


def insert_data_into_bigquery(bigquery_client, table, rows):
    logger.info(f'Inserting {rows} into {table} ')
    batch_size = 1000
    for i in range(0, len(rows), batch_size):
        batch_rows = rows[i:i + batch_size]

        # Insert batch rows into BigQuery table
        errors = bigquery_client.insert_rows(table, batch_rows, selected_fields=table.schema)

        if errors:
            print(f"Failed to insert rows into BigQuery table: {errors}")
        else:
            print("Batch insertion into BigQuery table completed successfully!")


def main():
    # Initialize BigQuery client
    bigquery_client = initialize_bigquery_client(project_id, service_account_key_file)

    # Create BigQuery dataset if it doesn't exist
    dataset = create_bigquery_dataset(bigquery_client, dataset_id)

    # Fetch table schema from MariaDB
    table_name = 'batches'
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


if __name__ == "__main__":
    while True:
        main()
        time.sleep(3600*4)