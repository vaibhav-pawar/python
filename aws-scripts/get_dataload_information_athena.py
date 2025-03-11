import boto3
import time
from datetime import date
from datetime import datetime
import pandas as pd
import os

athena_client = boto3.client('athena')

def athena_query_engine(query, database_name):
    query_process = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database_name
        },
        ResultConfiguration={
            'OutputLocation': 's3://<bucket-name>/athena_queried_data_store/',
        }
    )
    query_execution_id = query_process['QueryExecutionId']

    if query_execution_id:
        while True:
            query_execution = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_execution.get('QueryExecution', {}).get('Status', {}).get('State')
            StateChangeReason = query_execution.get('QueryExecution', {}).get('Status', {}).get('StateChangeReason')
            if status == "SUCCEEDED":
                break
            elif status in ["QUEUED", "RUNNING"]:
                time.sleep(2)
            else:
                print(f"Error :",StateChangeReason)
                break

        Collection_of_resources = []
        response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        Collection_of_resources.extend(response['ResultSet']['Rows'])
        while 'NextToken' in response:
            response = athena_client.get_query_results(
                QueryExecutionId=query_execution_id,
                NextToken=response['NextToken']
            )
            Collection_of_resources.extend(response['ResultSet']['Rows'])
        remove_data_key = []
        for row in Collection_of_resources:
            remove_data_key.append(row['Data'])
        feteched_data_from_query = [[d['VarCharValue'] for d in row] for row in remove_data_key]
        feteched_data_from_query = [item[0] for item in feteched_data_from_query[1:]]
        return feteched_data_from_query


if __name__ == "__main__":
    database_name = "<database-name>"
    table_name = "<table-name>"
    total_batches_loaded_on_date = "20241023"
    store_result_in_file = "datalake_loads"
    final_result = []

    total_batches_loaded_in_DL_query = f"""
            SELECT distinct _loaddate FROM "{database_name}"."{table_name}" where _loaddate like '%{total_batches_loaded_on_date}%' order by _loaddate desc
            """

    total_batches_loaded_in_DL_response_from_query_engine = athena_query_engine(total_batches_loaded_in_DL_query, database_name)
    print(f"Count of total batches loaded in Datalake: {len(total_batches_loaded_in_DL_response_from_query_engine)}")

    for dl_loaddates in total_batches_loaded_in_DL_response_from_query_engine:
        load_of_which_date_query = f"""
                    SELECT distinct <column_name> FROM "{database_name}"."{table_name}" where _loaddate='{dl_loaddates}' limit 10;
                    """
        load_of_which_date_response_from_query_engine = athena_query_engine(load_of_which_date_query, database_name)
        load_of_which_date_response_from_query_engine = load_of_which_date_response_from_query_engine[0]
        custom_dict = {"Datalake Batch Loaded time": dl_loaddates, "Load of which date": load_of_which_date_response_from_query_engine}
        print(custom_dict)
        final_result.append(custom_dict)

    df = pd.DataFrame(final_result)
    df.to_excel(f'{store_result_in_file}_{total_batches_loaded_on_date}.xlsx', index=False)
    print(f"Data saved to {store_result_in_file}_{total_batches_loaded_on_date}.xlsx")