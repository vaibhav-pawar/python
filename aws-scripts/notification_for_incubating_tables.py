import os
import time
from datetime import date
import boto3
from openpyxl import Workbook
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import logging


athena_client = boto3.client('athena')
s3_client = boto3.client('s3')
ses = boto3.client('ses')
ssm_client = boto3.client('ssm')


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    main()


def get_ssm_parameter():
    ssm_response = ssm_client.get_parameters(Names=['<ssm-source-email>'], WithDecryption=True)
    parameter_value = ssm_response['Parameters'][0]['Value']
    return parameter_value


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
        collected_result_from_query = [[d['VarCharValue'] for d in row] for row in remove_data_key]
        logger.info(f"Total number of rows fetched: {len(collected_result_from_query)}")
        return collected_result_from_query


def store_gathered_results_to_s3(env, day, query_results):
    excel_path = f"incubating-table-with-producer-details-{date.today()}.xlsx"
    wb = Workbook()
    ws1 = wb.active
    ws1.title = 'incubating-table-details'
    for row in query_results:
        ws1.append(row)
    os.chdir('/tmp')
    cwd = os.getcwd()
    logger.info("Saving Excel File in {0}".format(cwd))
    wb.save(excel_path)
    filepath = f'/tmp/{excel_path}'
    logger.info("List of local files: ", os.listdir('/tmp'))
    logger.info(f"Uploading '{excel_path}' to S3...")
    s3_bucket = f"<bucket-name>-{env}"
    s3_key = f"incubating-tables-older-than-six-months/{day}/{excel_path}"
    s3_client.upload_file(excel_path, s3_bucket, s3_key)
    s3_path = f"s3://{s3_bucket}/{s3_key}"
    logger.info(f"\tFile '{excel_path}' is uploaded to S3")
    logger.info(f"\tS3 PATH : {s3_path}")
    return filepath, excel_path


def send_email_with_attachment(sender, recipient, subject, body, attachment_path, attachment_filename):
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = recipient
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'html', 'utf-8'))

    try:
        with open(attachment_path, 'rb') as file:
            part = MIMEApplication(file.read(), Name=os.path.basename(attachment_filename))
            part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_filename)}"'
            msg.attach(part)
    except FileNotFoundError:
        logger.info(f"Attachment file not found: {attachment_path}")
        return

    try:
        response = ses.send_raw_email(
            Source=sender,
            Destinations=[recipient],
            RawMessage={'Data': msg.as_string()}
        )
        logger.info(f"Email sent! Message ID: {response['MessageId']}")
    except Exception as e:
        logger.warning(f"Email sending failed: {e}")


def main():
    today = date.today()
    day = today.strftime("%Y-%m-%d")
    env = os.environ.get("STACK_ENV")
    config = {
        'prd': {
            'base_database': '<resource-tags-info-database>',
            'base_table': '<resource-tags-info-table>',
            'producer_database': '<producer-info-database>',
            'producer_table': '<producer-info-table>',
        },
        'dev': {
            'base_database': '<resource-tags-info-database>',
            'base_table': '<resource-tags-info-table>',
            'producer_database': '<producer-info-database>',
            'producer_table': '<producer-info-table>',
        }
    }

    query = f'''
    SELECT DISTINCT 
        p.dataset_database_name,
        p.table_or_view_name,
        date_format(p.table_creation_time, '%Y-%m-%d %H:%i:%s') AS table_creation_time,
        CASE 
            WHEN p.table_or_view_name LIKE '<table-name>' 
                OR p.table_or_view_name LIKE '<table-name>'
                OR p.table_or_view_name LIKE '<table-name>'
                OR p.table_or_view_name LIKE '<table-name>' 
            THEN '<default-tag-name>'
            ELSE COALESCE(t.dataset_internal_owner_team_id, 'Not Found') 
        END AS Producer_information
    FROM 
        {config[env]['base_database']}.{config[env]['base_table']} p
    LEFT JOIN 
        "{config[env]['producer_database']}"."{config[env]['producer_table']}" t
        ON p.dataset_database_name = t."database_standard_name"
        AND p.table_or_view_name = t."table_or_view_standard_name"
        AND t.data_store = '<datastore-name>'
    WHERE 
        p.dataset_database_name LIKE '%incubating%'
        AND p.table_or_view_name NOT LIKE '_temp%'
        AND p.table_or_view_name NOT LIKE 'etleap_%'
        AND p.table_creation_time < DATE_ADD('month', -6, current_timestamp)
    '''

    logger.info('Performing following Query: %s', query)
    query_results = athena_query_engine(query)
    if len(query_results) > 1:
        file_location, file_name = store_gathered_results_to_s3(env, day, query_results)

        source_email = get_ssm_parameter()
        recipient_email = '<receiver-email>'
        subject = f'Incubating Tables older than six months | {env}'
        body = (
            "Dear Team,<br><br>"
            "Attached is an Excel file containing the details of incubating databases and tables, including their creation dates and producer information.<br><br>"
            "Thank You"
        )
        attachment_path = file_location
        attachment_filename = file_name

        send_email_with_attachment(source_email, recipient_email, subject, body, attachment_path, attachment_filename)
    else:
        logger.info("No records found")