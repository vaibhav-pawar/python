import boto3
import time
import os
import re
import logging
import json
from datetime import date,datetime

env = os.environ.get('STACK_ENV')
today = date.today()
day = today.strftime("%Y-%m-%d")

client = boto3.client('sns')
client_athena = boto3.client('athena')
ssm_client = boto3.client('ssm')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

if env == "prd":
    resource_list = f"<ssm-resource-names>"
else:
    env = 'stg'
    resource_list = f"<ssm-resource-names>"

def handler(event, context):
    msck_checkup()

def get_s3inventory_list(resource_list):
    resource_names = []
    ssm_response = ssm_client.get_parameters(Names=[resource_list], WithDecryption=True)
    for param in ssm_response["Parameters"]:
        substrings = re.split(r',', param['Value'])
        for substring in substrings:
            parts = substring.split('.')
            if len(parts) == 2:
                database = parts[0]
                table = parts[1]
                resource_names.append({'Database_Name': database, 'Table_Name': table})
    return resource_names


def athena_query_engine(database,table):
    logger.info(f"MSCK REPAIR is running for {database}.{table}")

    query_process = client_athena.start_query_execution(
        QueryString=f'MSCK REPAIR TABLE {table}',
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': f's3://<bucket-name>-{env}/MSCK-REPAIR/{day}',
        }
    )
    query_execution_id = query_process['QueryExecutionId']

    if query_execution_id:
        while True:
            query_execution = client_athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_execution.get('QueryExecution', {}).get('Status', {}).get('State')
            if status == "SUCCEEDED":
                logger.info(f"MSCK Repair is {status} for {database}.{table}: {query_execution}")
                break
            elif status in ["QUEUED", "RUNNING"]:
                time.sleep(2)
            else:
                logger.warning(f"MSCK Repair is {status} for {database}.{table}: {query_execution}")
                body = {"AlarmName": f'{env}-MSCK Repair is unsuccessful for underlying table of {database}.{table}',
                        "NewStateValue": "ALARM",
                        "StateChangeTime": datetime.now().isoformat(timespec='microseconds') + 'Z',
                        "state_message": f'{env}-MSCK Repair is unsuccessful for underlying table of {database}.{table}',
                        "event_details": f'{env}-MSCK Repair is unsuccessful for underlying table of {database}.{table}'}
                if env == "prd":
                    sns_topic = "<sns-arn-trigger-vo-alert>"
                else:
                    sns_topic = "<sns-arn-trigger-email-alert>" + env
                response = client.publish(TopicArn=sns_topic, Subject=f'MSCK table repair status for - {day} | env:{env}', Message=json.dumps(body, indent=4))
                print("SNS_Response: {}".format(response))
                break

def msck_checkup():
    resource_collection = get_s3inventory_list(resource_list)

    logger.info(f"MSCK repair will be performed on following resources: {resource_collection}")

    for iterating_over_tables in resource_collection:
        msck_repair_query_engine = athena_query_engine(iterating_over_tables['Database_Name'],iterating_over_tables['Table_Name'])