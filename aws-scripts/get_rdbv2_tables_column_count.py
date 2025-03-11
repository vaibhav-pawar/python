import boto3
import time
import logging
from openpyxl import Workbook


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


athena_client = boto3.client('athena')
glue_client = boto3.client('glue')


def athena_query_engine(query):
    #logger.info(f"Executing query: {query}")
    query_process = athena_client.start_query_execution(QueryString=query)

    if not query_process or 'QueryExecutionId' not in query_process:
        logger.error("Failed to start Athena query execution")
        return []

    query_execution_id = query_process['QueryExecutionId']

    while True:
        query_execution = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_execution.get('QueryExecution', {}).get('Status', {}).get('State')
        state_change_reason = query_execution.get('QueryExecution', {}).get('Status', {}).get('StateChangeReason')

        if status == "SUCCEEDED":
            break
        elif status in ["QUEUED", "RUNNING"]:
            time.sleep(2)
        else:
            logger.error(f"Query failed: {state_change_reason}")
            return []

    collection_of_resources = []
    response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    collection_of_resources.extend(response['ResultSet']['Rows'])

    while 'NextToken' in response:
        response = athena_client.get_query_results(QueryExecutionId=query_execution_id, NextToken=response['NextToken'])
        collection_of_resources.extend(response['ResultSet']['Rows'])

    collected_result_from_query = [[d['VarCharValue'] for d in row['Data']] for row in collection_of_resources[1:]]

    return collected_result_from_query


def glue_collect_database_information():
    logger.info("Collecting Database Names from Glue.")
    paginator = glue_client.get_paginator('get_databases')
    databases = []
    for page in paginator.paginate():
        for database in page['DatabaseList']:
            databases.append(database['Name'])
    rdbv2_databases_collection = [name for name in databases if name.startswith("<database-name>__")]
    logger.info(f"List of rdb_v2 databases : {rdbv2_databases_collection}")
    return rdbv2_databases_collection


def get_all_glue_tables(database_name):
    paginator = glue_client.get_paginator('get_tables')
    all_tables = []
    for page in paginator.paginate(DatabaseName=database_name):
        for table in page['TableList']:
            all_tables.append(table['Name'])
    return all_tables


def store_results_in_excel(results, filename="rdbv2_tables_column_count.xlsx"):
    wb = Workbook()
    ws = wb.active
    ws.append(["Database Name", "Table Name", "Column Count"])

    for row in results:
        ws.append(row)

    wb.save(filename)
    logger.info(f"Results saved to {filename}")


if __name__ == "__main__":
    databases = glue_collect_database_information()
    final_results = []

    for db in databases:
        logger.info(f"Going through tables of {db}.")
        tables = get_all_glue_tables(db)
        for table in tables:
            query = f"""
            SELECT COUNT(column_name) AS column_count
            FROM information_schema.columns
            WHERE table_name = '{table}'
            AND table_schema = '{db}';
            """
            result = athena_query_engine(query)

            if result:
                final_results.append([db, table, result[0][0]])
                logger.info(f"{db}.{table} column count: {result[0][0]}")
            else:
                logger.error(f"Failed to fetch column count for {db}.{table}")

    store_results_in_excel(final_results)