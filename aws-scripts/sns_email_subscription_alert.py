import boto3
import json
import os
import botocore
from datetime import date
from datetime import datetime

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
sns = boto3.client('sns')
BUCKET = f"<bucket-name>-{os.environ.get('STACK_ENV')}"
PREFIX = "SNS-Email-Subscription-Observed"

def handler1(event, context):
    return collectlogs(event)

def handler2(event, context):
    return main(event)

def collectlogs(event):

    #Check if Timestampfile is present or not
    try:
        s3_resource.Object(f'{BUCKET}', f"{PREFIX}/timestamp.json").load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("File does not exist in bucket. Creating a new Timestamp file.")
            converttostring = str(date.today())
            record_date = {"Previous_Date": converttostring}
            data_string = json.dumps(record_date)
            s3_client.put_object(
                Body=data_string,
                Bucket=f'{BUCKET}',
                Key=f"{PREFIX}/timestamp.json"
            )
            print("Data is added into Timestamp file.")
        else:
            raise
    else:
        print("Timestamp file is already exist in s3 bucket.")
        pass

    #Adding New email subscription with following folder/file structure S3-BUCKET > PREFIX > TODAY_DATE_Timestamp > FILE_Timestamp.
    print("New Subscription is Added:", event)
    data_string = json.dumps(event)
    s3_client.put_object(
        Body=data_string,
        Bucket=BUCKET,
        Key=f"{PREFIX}/{date.today()}/NewEmailSubscriptionAdded-{datetime.utcnow().strftime('%Y%m%d%H%M%SZ')}.json"
    )

def main(event):
    print("Reading content of Timestamp file.")
    data = s3_client.get_object(Bucket=BUCKET, Key=f'{PREFIX}/timestamp.json')
    loading_content_of_timestamp = json.loads(data['Body'].read().decode())
    print("Collecting Email Subscription Logs of date:", loading_content_of_timestamp)

    filename_collection = []
    response = s3_client.list_objects(Bucket=BUCKET, Prefix=f"{PREFIX}/{loading_content_of_timestamp['Previous_Date']}")
    for getting_list_of_files in response['Contents']:
        filename_collection.append(getting_list_of_files['Key'])
    print("Total File Names:", filename_collection)

    data_of_files = []
    for checking_content_of_each_file in filename_collection:
        data = s3_client.get_object(Bucket=BUCKET, Key=checking_content_of_each_file)
        loading_content_of_objects = json.loads(data['Body'].read().decode())
        data_of_files.append(loading_content_of_objects)

    print("Data of files:", data_of_files)

    new = ""
    for event in data_of_files:
        if event['detail']['eventName'] == "Subscribe" and event["detail"]["requestParameters"]["protocol"] == "email":
            customdict = f"""
                            Account ID: {event["account"]}
                            Region: {event["region"]}
                            Event Name: {event['detail']['eventName']}
                            Event time: {event['detail']["eventTime"]}
                            TopicName: {event['detail']["requestParameters"]["topicArn"][35:]}
                            TopicArn: {event['detail']["requestParameters"]["topicArn"]}
                            SubscriptionID: {event['detail']['responseElements']['subscriptionArn'][-36:]}
                            subscriptionArn: {event['detail']['responseElements']['subscriptionArn']}
                            Subscription Method: {event["detail"]["requestParameters"]["protocol"]}
                            Performed By:
                                AccountID: {event['detail']["userIdentity"]["accountId"]}
                                Type: {event['detail']["userIdentity"]["type"]}
                                PrincipleID: {event['detail']["userIdentity"]["principalId"]}
                                ARN: {event['detail']["userIdentity"]["arn"]}
                        """
            new += str(customdict) + "\n"

    print("Sending Mail to Admins...")
    if os.environ.get('STACK_ENV') == 'prd':
        snstopic = f"arn:aws:sns:" + os.environ.get('AWS_Region') + "<sns-topic-arn>"
    else:
        snstopic = f"arn:aws:sns:" + os.environ.get('AWS_Region') + "<sns-topic-arn>" + os.environ.get('STACK_ENV')
    response = sns.publish(
        TopicArn=snstopic,
        Message=f'''
                Hello Team,

                We observed following new Email subscriptions are added in SNS | Date: {loading_content_of_timestamp['Previous_Date']}
                {new}

                Thank You
                ''',
        Subject=f'''New Email subscriptions are added - SNS | Account : {event["account"]}''',
        MessageStructure='string',
        MessageAttributes={
            'Notification': {'DataType': 'String',
                             'StringValue': 'Email'},
            'Support': {'DataType': 'String',
                        'StringValue': 'DLS-Ticket'}
        }
    )
    print("Mail Sent Successfully...")

    print("Updating the timestamp file with today date.")
    current_date = str(date.today())
    record_date = {"Previous_Date": current_date}
    data_string = json.dumps(record_date)
    s3_client.put_object(
        Body=data_string,
        Bucket=BUCKET,
        Key=f"{PREFIX}/timestamp.json"
    )
    print("Timestamp file is updated.")