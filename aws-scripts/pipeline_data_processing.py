import time
import boto3
from bs4 import BeautifulSoup
import requests
import os
import urllib.request
import json
from datetime import date, datetime
import logging


s3_client = boto3.client('s3')
ssm_client = boto3.client('ssm')
sns_client = boto3.client('sns')


env = os.environ.get("STACK_ENV")
threshold_days = 2
today_date = datetime.now().date()
timestamp_of_file_upload = datetime.utcnow().strftime('%Y_%m_%dT%H_%M_%SZ')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.info(f"Today Date: {today_date}")
    get_url = mic_web_urls()
    web_data = web_data_collector(get_url)


def mic_web_urls():
    access_key = f"<ssm-website-url>"
    ssm_response = ssm_client.get_parameters(Names=[access_key], WithDecryption=True)
    parameter_values = ssm_response['Parameters'][0]['Value']
    first_value, second_value = parameter_values.split(',')
    base_url = first_value.strip()
    web_page_url = second_value.strip()
    return base_url, web_page_url


def web_data_collector(get_url):
    logger.info(f"Accessing data of webpage: {get_url[1]}")
    response = requests.get(get_url[1])
    if response.status_code == 200:
        logger.info(f"Response {response.status_code} : {response.reason}")
        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find_all('table', class_='table')[0]
            rows = table.find_all('tr')
            mic_decision_maker(rows,get_url)
        except Exception as e:
            logger.info(f"MIC web scraping failed: {e}")
            vo_alert_trigger(f"MIC web scraping failed: {e}")
    else:
        logger.info(f"Response {response.status_code} : {response.reason}")
        vo_alert_trigger(f"MIC website {get_url[1]} is unreachable")


def vo_alert_trigger(response):
    body = {"AlarmName": response,
            "NewStateValue": "ALARM",
            "StateChangeTime": datetime.now().isoformat(timespec='microseconds') + 'Z',
            "state_message": response,
            "event_details": response}
    sns_topic = "<sns-arn-generate-vo-alert>"
    sns_response = sns_client.publish(TopicArn=sns_topic,
                                  Message=json.dumps(body, indent=4))
    logger.info(f"SNS_Response: {sns_response}")


def mic_pipeline_processing(data_file, manifest_file):
    logger.info(f"Uploading {data_file} & {manifest_file} file in dropzone.")
    schema_name = f'<schema-name>'
    dataset_name = '<dataset-name>'
    database_name = f'<database-name>'
    table_name = '<table_name>'
    s3_bucket_name = '<bucket-name>'
    file_names = [f'{data_file}', f'{manifest_file}']
    for file_name in file_names:
        s3_bucket = s3_bucket_name
        prefix = f'{schema_name}/datasets/{dataset_name}/{database_name}/{table_name}'
        s3_key = f'{prefix}/{timestamp_of_file_upload}/snapshot/{file_name}'
        s3_client.upload_file(file_name, s3_bucket, s3_key)
    logger.info(f"All files has been uploaded successfully in s3 path: {prefix + '/' + timestamp_of_file_upload + '/snapshot/'}")
    time.sleep(10)
    try:
        trace_file = prefix + '/' + timestamp_of_file_upload + '/snapshot/.ack.json'
        response = s3_client.get_object(Bucket=s3_bucket_name, Key=trace_file)
        file_content = response['Body'].read().decode('utf-8')
        json_data = json.loads(file_content)
        trace_id = json_data.get('trace_id')
        logger.info(f"Trace ID: {trace_id}")
    except Exception as e:
        logger.info(f"Error reading S3 file: {e}")


def mic_decision_maker(web_data,get_url):
    for row in web_data:
        columns = row.find_all('td')
        if columns:
            publication_date = columns[5].get_text().strip()
            logger.info(f"Publication Date : {publication_date}")
            publication_date_formatted = datetime.strptime(publication_date, "%d %B %Y")
            publication_date_formatted_date = publication_date_formatted.date()
            difference = (today_date - publication_date_formatted_date).days
            if difference > threshold_days:
                logger.info("MIC data is not released by official yet. Hence, stopping the script execution.")
            else:
                logger.info(f"MIC data is release by official on {publication_date_formatted_date}. Downloading Latest MIC data file...")
                os.chdir('/tmp')
                cwd = os.getcwd()
                logger.info(f"Current working directory : {cwd}")
                csv_link = get_url[0] + columns[2].find('a')['href']
                data_file = csv_link.split('/')[-1]
                try:
                    urllib.request.urlretrieve(csv_link, data_file)
                    file_size = os.path.getsize(data_file) / 1024
                    logger.info(f"File is downloaded successfully from {csv_link}. Total size of the file is {file_size} kb")
                except Exception as e:
                    logger.info(f"An error occurred: {e}")
                logger.info("Creating manifest file.")
                manifest_file = '.manifest.json'
                manifest_content = {
                    "files": [
                        {
                            "name": f"{data_file}"
                        }
                    ]
                }
                json_content = json.dumps(manifest_content, indent=4)
                logger.info(f"manifest file content: {json_content}")
                with open(manifest_file, 'w') as file:
                    file.write(json_content)
                logger.info("manifest file has been created.")
                logger.info(f"List of files in current working directory {cwd}: {os.listdir('/tmp')}")
                mic_pipeline_processing(data_file, manifest_file)