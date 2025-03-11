import boto3
import time
from datetime import date
from datetime import datetime
from openpyxl import Workbook
import os

athena_client = boto3.client('athena')
s3_client = boto3.client('s3')
sns = boto3.client('sns')

def handler(event, context):
    store_gathered_results_to_s3()

database_tags_are_missing_query = f"""
        WITH Tagg AS (
            SELECT "database", tag_key, NULLIF(tag_value, '') as tag_value
            FROM "<database_name>"."<table_name>" 
            ORDER BY "database" ASC
        )
        SELECT 
          DISTINCT "database",
          COALESCE(MAX(CASE WHEN tag_key = 'namespace' THEN tag_value END), 'NULL') AS namespace,
          COALESCE(MAX(CASE WHEN tag_key = 'namespace_data_group' THEN tag_value END), 'NULL') AS namespace_data_group,
          COALESCE(MAX(CASE WHEN tag_key = 'data_readiness_status' THEN tag_value END), 'NULL') AS data_readiness_status
        FROM Tagg
        GROUP BY "database"
        HAVING 
          MAX(CASE WHEN tag_key = 'namespace' THEN tag_value END) IS NULL OR 
          MAX(CASE WHEN tag_key = 'namespace_data_group' THEN tag_value END) IS NULL OR 
          MAX(CASE WHEN tag_key = 'data_readiness_status' THEN tag_value END) IS NULL;
        """

table_tags_are_missing_query = f"""
        WITH Tagg AS (
            SELECT "database", "table", tag_key, NULLIF(tag_value, '') as tag_value
            FROM "<database_name>"."<table_name>" 
            ORDER BY "database" ASC
        )
        SELECT 
          "database",
          "table",
          COALESCE(MAX(CASE WHEN tag_key = 'dataset_access_control' THEN tag_value END), 'NULL') AS dataset_access_control,
          COALESCE(MAX(CASE WHEN tag_key = 'dataset_internal_owner_team_id' THEN tag_value END), 'NULL') AS dataset_internal_owner_team_id
        FROM Tagg
        WHERE
          "table" NOT LIKE '_temp%' AND
          "table" NOT LIKE 'etleap_permission_validation_table_name%'
        GROUP BY "database", "table"
        HAVING 
          MAX(CASE WHEN tag_key = 'dataset_access_control' THEN tag_value END) IS NULL OR
          MAX(CASE WHEN tag_key = 'dataset_internal_owner_team_id' THEN tag_value END) IS NULL;
        """

def athena_query_engine(query):
    query_process = athena_client.start_query_execution(QueryString=query)
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
        Missing_Tags_data_list = [[d['VarCharValue'] for d in row] for row in remove_data_key]
        print(f"\tTotal number of rows fetched: {len(Missing_Tags_data_list)}")
        return Missing_Tags_data_list

def store_gathered_results_to_s3():
    print("\n\U000027A1 Collecting details of resources with missing tags...")

    excel_path = f"lakeformation-resource-missing-tags-{date.today()}.xlsx"
    wb = Workbook()
    ws1 = wb.active
    ws1.title = 'Database_missing_tags'
    Database_missing_tags_response_from_query_engine = athena_query_engine(database_tags_are_missing_query)
    for row in Database_missing_tags_response_from_query_engine:
        ws1.append(row)
    ws2 = wb.create_sheet(title='Table_missing_tags')
    Table_missing_tags_response_from_query_engine = athena_query_engine(table_tags_are_missing_query)
    for row in Table_missing_tags_response_from_query_engine:
        ws2.append(row)

    os.chdir('/tmp')
    cwd = os.getcwd()
    print("Saving Excel File in {0}".format(cwd))
    wb.save(excel_path)
    print("List of local files: ", os.listdir('/tmp'))

    print(f"\n\U000027A1 Uploading '{excel_path}' to S3...")
    s3_bucket = '<bucket-name>-' + os.environ.get('STACK_ENV')
    prefix = 'lakeformation-resource-missing-tags'
    s3_key = f'{prefix}/{date.today()}/{excel_path}'
    s3_client.upload_file(excel_path, s3_bucket, s3_key)
    s3_path = f"s3://{s3_bucket}/{s3_key}"

    print(f"\tFile '{excel_path}' is uploaded to S3")
    print(f"\tS3 PATH : {s3_path}")

    print("\n\U000027A1 Creating DLS ticket...")
    if os.environ.get('STACK_ENV') == 'prd':
        snstopic = "<sns-arn-to-create-dls-ticket>"
    else:
        snstopic = "<sns-arn-to-create-dls-ticket>-" + os.environ.get('STACK_ENV')

    response = sns.publish(
        TopicArn=snstopic,
        Subject='Lakeformation resources with missing tags on ' + os.environ.get('STACK_ENV'),
        Message=f'''
                    Kindly, download the excel file from below location. These records are fetched at {date.today()}
                    
                    S3 PATH: {s3_path}

                    Thank You
                ''',
        MessageAttributes={
            'Support': {'DataType': 'String',
                        'StringValue': 'DLS-Ticket'}
        }
    )
    print("Response: {}".format(response))