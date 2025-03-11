import boto3
from datetime import date
from datetime import datetime
import pandas as pd
import subprocess
import psycopg2
import json
import time
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

def get_creds_dbeaver():
    git_bash_path = r"C:\Program Files\Git\bin\bash.exe"

    get_tokens_command = 'aws sts assume-role --role-arn "<role-arn>" --role-session-name <session-name>'
    get_tokens_execute = subprocess.run([git_bash_path, '-c', get_tokens_command], capture_output=True, text=True)
    if get_tokens_execute.returncode == 0:
        # print("Token details have been collected.")
        get_tokens_response = json.loads(get_tokens_execute.stdout)
        credentials = get_tokens_response['Credentials']
        access_key_id = credentials['AccessKeyId']
        secret_access_key = credentials['SecretAccessKey']
        session_token = credentials['SessionToken']

        bash_commands = f'''
        export AWS_ACCESS_KEY_ID={access_key_id} &&
        export AWS_SECRET_ACCESS_KEY={secret_access_key} &&
        export AWS_SESSION_TOKEN={session_token} &&
        aws redshift-serverless get-credentials --workgroup-name <workgroup-name> --db-name <database-name>
        '''

        set_env_and_get_creds_execute = subprocess.run([git_bash_path, '-c', bash_commands], capture_output=True,
                                                       text=True)
        if set_env_and_get_creds_execute.returncode == 0:
            # print("Temporary credentials collected successfully.")
            try:
                get_temp_creds_response = json.loads(set_env_and_get_creds_execute.stdout)
                # print(json.dumps(get_temp_creds_response, indent=2))
                return get_temp_creds_response['dbUser'], get_temp_creds_response['dbPassword']
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON output: {e}")
        else:
            print("Error executing get-credentials command:\n", set_env_and_get_creds_execute.stderr)
    else:
        print("Error executing assume-role command:\n", get_tokens_execute.stderr)


def redshift_connection(username, password):
    host = '<hostname>'
    port = '5439'
    dbname = '<database-name>'
    user = username
    password = password

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        return conn
    except Exception as e:
        print(f"An error occurred: {e}")


def get_total_tables_list(input_file, sheet_name, column_name):
    df = pd.read_excel(input_file, sheet_name=sheet_name)
    table_names = df[f'{column_name}'].tolist()
    return table_names

if __name__ == "__main__":
    print("Collecting required information before starting the activity please wait...")
    database_name = "<database-name>"
    table_names = get_total_tables_list('input_file.xlsx', 'Sheet1', 'tables')
    filename_timestamp = "data_push_progress_"+datetime.now().strftime('%d-%m-%y_%H%M')
    username, password = get_creds_dbeaver()
    conn = redshift_connection(username, password)
    cursor = conn.cursor()
    final_result = []

    print("Monitoring Data Load Progress in Datalake and Lakehouse:")
    for table in table_names:
        get_total_count_of_batches_ingested_query = f"""
                SELECT count(distinct(_loaddate)) FROM "{database_name}"."{table}";
                """
        get_total_row_count_query = f"""
                SELECT count(_loaddate) FROM "{database_name}"."{table}";
                """
        try:
            datalake_get_total_count_of_batches_ingested_query_executed = athena_query_engine(get_total_count_of_batches_ingested_query, database_name)
            datalake_get_total_count_of_batches_ingested_response_from_query_engine = datalake_get_total_count_of_batches_ingested_query_executed[0]
            datalake_get_total_row_count_query_executed = athena_query_engine(get_total_row_count_query,database_name)
            datalake_get_total_row_count_response_from_query_engine = datalake_get_total_row_count_query_executed[0]
            cursor.execute(get_total_count_of_batches_ingested_query)
            lakehouse_total_count_of_batches_ingested_result = cursor.fetchone()
            cursor.execute(get_total_row_count_query)
            lakehouse_total_row_count_result = cursor.fetchone()
            datalake_custom_dict = {
                "Database_Name": database_name,
                "Table_Name": table,
                "DL Total Batches Ingested": datalake_get_total_count_of_batches_ingested_response_from_query_engine,
                "DL Total Rows Ingested": datalake_get_total_row_count_response_from_query_engine,
                "LH Total Batches Ingested": lakehouse_total_count_of_batches_ingested_result[0],
                "LH Total Rows Ingested": lakehouse_total_row_count_result[0]
            }
            print(datalake_custom_dict)
            final_result.append(datalake_custom_dict)
        except Exception as e:
            conn.rollback()
            print(f"Error occurred on {database_name}.{table}: {e}")

    cursor.close()
    conn.close()
    if final_result:
        df1 = pd.DataFrame(final_result)
        df1.to_excel(f'{filename_timestamp}.xlsx', index=False)
        print(f"Data saved to {filename_timestamp}.xlsx")