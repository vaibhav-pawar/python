import json
import os
import boto3
import logging
from datetime import date,datetime

client_ses = boto3.client("ses")
client_sns = boto3.client('sns')

env = os.environ.get("STACK_ENV")
now = datetime.now()
month = now.strftime("%B")
year = now.strftime("%Y")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    for record in event['Records']:
        message_body = json.loads(record['body'])
        content = message_body['Message']
        content_json = json.loads(content)
        logger.info(f"Received event: {content_json}")
        event_type = content_json['content']['attributes']['event-type']
        dataset_name = content_json['content']['attributes']['dataset']
        database_name = content_json['content']['attributes']['database']
        table_name = content_json['content']['attributes']['table']
        pipeline_name = "{" + dataset_name + "__" + database_name + "}" + "__" + "{" + table_name + "}"
        trace_id = content_json['event']['trace-id']
        event_type = content_json['event']['event-type']
        if event_type == '<successful-data-ingestion-event-name>':
            logger.info("Pipeline is loaded successfully")
            mail_body = f"""
                      <p>Hello Team,</p>
                      <br>
                      <p>MIC data has been uploaded in pipeline {pipeline_name} for {month}-{year}</p>
                      <p>Trace-id is {trace_id}</p>
                      <p>Kindly, check and confirm the data from your side as well.</p>
                      <br>
                      <p>Thanks & Regards,</p>
                      <p>DataLakeOps Team</p>
                      """
            to = ['<email-1>','<email-2>']
            logger.info(f"Sending email to {to}")
            response = client_ses.send_email(
                Destination={
                    "ToAddresses": to,
                },
                Message={
                    "Body": {
                        "Html": {
                            "Charset": "UTF-8",
                            "Data": f'{mail_body}',
                        },
                        "Text": {
                            "Charset": "UTF-8",
                            "Data": f'{mail_body}',
                        },
                    },
                    "Subject": {
                        "Charset": "UTF-8",
                        "Data": f"Upload MIC data for {month}-{year}",
                    },
                },
                Source="<source-email>",
            )
            logger.info("Email sent successfully")
        elif event_type == '<data-ingestion-failure-event>':
            logger.warning("Pipeline is failed to load. Sending Vo Alert")
            body = {"AlarmName": f'{env}-MIC data is failed to process for {month}-{year}',
                    "NewStateValue": "ALARM",
                    "StateChangeTime": datetime.now().isoformat(timespec='microseconds') + 'Z',
                    "state_message": f"MIC data is failed to process for {month}-{year}",
                    "event_details": f"Pipeline_Name: {pipeline_name} | event_type: {event_type} | {content_json['content']['error']}"}
            sns_topic = "<sns-arn-trigger-vo-alert>"
            response = client_sns.publish(TopicArn=sns_topic, Subject=f'{env}-MIC data is failed to process for {month}-{year}', Message=json.dumps(body, indent=4))
            logger.info(f"SNS_Response: {response}")