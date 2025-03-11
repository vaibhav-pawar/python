import requests
import json
import boto3
import os
from datetime import datetime, timezone, timedelta
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
from dateutil import parser


etleap_url = os.environ.get("ETLEAP_BASE_URL") + '/api/v2/pipelines?pageSize=0&timeout=300'
ssm_client = boto3.client('ssm')
sns = boto3.client('sns')
env = os.environ.get('STACK_ENV')


def get_etleap_creds():
    access_key_name = '<ssm-etleap-access-key>'
    secret_key_name = '<ssm-etleap-secret-key>'
    ssm_response = ssm_client.get_parameters(Names=[access_key_name, secret_key_name], WithDecryption=True)
    for param in ssm_response["Parameters"]:
        if param["Name"] == access_key_name:
            access_key = param["Value"]
        elif param["Name"] == secret_key_name:
            secret_key = param["Value"]
    return access_key, secret_key


def get_ignore_list():
    ignore_list_name = '<ssm-pipeline-ignore-list>'
    ssm_response = ssm_client.get_parameters(Names=[ignore_list_name], WithDecryption=True)
    return ssm_response["Parameters"][0]["Value"]


def stuck_pipelines():
    user, passwd = get_etleap_creds()
    response = requests.request("GET", etleap_url, auth=(user, passwd), verify=False)
    output = response.text
    json_data = json.loads(output)
    ignore_list = get_ignore_list()
    stuck_pipelines = []
    
    for pipeline_data in json_data["pipelines"]:
        pid = pipeline_data["id"]
        pname = pipeline_data["name"]
        if pid in ignore_list:
            print("Ignoring pipeline with id", pid)
        else:
            refresh_data = pipeline_data["destinations"][0].get("refreshVersion")
            dt = datetime.now(timezone.utc) - timedelta(minutes=30)
            createDate = parser.parse(pipeline_data["createDate"]).replace(tzinfo=timezone.utc)
            if pipeline_data.get('stopReason') == 'PAUSED':
                temp_pipeline = {"id": pid, "name": pname}
                print("Pipeline is in paused state, hence ignoring", temp_pipeline)
            elif ('latency' not in pipeline_data and pipeline_data["latestScriptVersion"] == 1 and pipeline_data["lastRefreshStartDate"] == None and pipeline_data["lastRefreshFinishDate"] == None):
                temp_pipeline = {"id": pid, "name": pname}
                print("Pipeline is in initial load state, hence ignoring", temp_pipeline)
            elif (refresh_data and parser.parse(pipeline_data["lastRefreshStartDate"]).replace(tzinfo=timezone.utc) < dt):
                temp_pipeline = {"id": pid, "name": pname,"lastRefreshStartDateUTC": pipeline_data["lastRefreshStartDate"]}
                stuck_pipelines.append(temp_pipeline)
            elif (not refresh_data and 'stopReason' in pipeline_data):
                temp_pipeline = {"id": pid, "name": pname, "stopReason": pipeline_data["stopReason"]}
                stuck_pipelines.append(temp_pipeline)
            elif (not refresh_data and 'latency' not in pipeline_data and (dt - createDate).days > 4 ):
                temp_pipeline = {"id": pid, "name": pname, "createDateUTC": pipeline_data["createDate"]}
                stuck_pipelines.append(temp_pipeline)

    if stuck_pipelines:
        print("Following are the details of stuck pipelines:")
        print(json.dumps(stuck_pipelines, indent=4, sort_keys=True))
        if env == 'prd':
            snstopic = "<sns-arn-to-trigger-vo-alert>"
            message = {"AlarmName": env + "Ingestion-Service-Etleap-Stuck-Pipelines","NewStateValue":"ALARM","StateChangeTime": datetime.now().isoformat(timespec='microseconds') + 'Z', "Pipelines": stuck_pipelines, "state_message": "There are " + str(len(stuck_pipelines)) + " pipelines which are stuck." }
        else:
            snstopic = "<sns-arn-to-trigger-email>" + env
            message = stuck_pipelines
        snspublish = sns.publish(TopicArn=snstopic,Subject='ETLeap Stuck Pipelines in ' + env,Message=json.dumps(message, indent=4, sort_keys=True))
        print(snspublish)
    else:
        print("There are no stuck pipelines.")


def handler(event, context):
    return stuck_pipelines()