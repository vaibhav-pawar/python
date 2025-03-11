import boto3
import requests
import json
import logging


ssm = boto3.client("ssm")


VO_Schedule_Details_url = "https://api.victorops.com/api-public/v2/team/<team-id>/oncall/schedule"
api_key = "<ssm-api-key>"
api_id = "<ssm-api-id>"
oncall_dl_ticket_params = "<ssm-jira-ticket-parameter>"


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    main()


def fetch_ssm_values(key):
    ssm_value = ssm.get_parameter(Name=f"{key}", WithDecryption=True)["Parameter"]["Value"]
    return ssm_value


def fetch_schedule_data(api_key_value, api_id_value):
    header = {
        "X-VO-Api-Key": api_key_value,
        "X-VO-Api-Id": api_id_value,
    }

    params = {
        "daysForward": 7,
        "daysSkip": 0,
    }

    response = requests.get(VO_Schedule_Details_url, headers=header, params=params)
    schedules_data = json.loads(response.content)
    current_on_call_details = []
    for checking in schedules_data["schedules"]:
        if checking["policy"]["name"] == "<policy-name>":
            for checking_rotation in checking["schedule"]:
                logger.info(checking_rotation)
                current_on_call = {
                    "shift_name": checking_rotation["shiftName"],
                    "weekly_oncall_start_datetime": checking_rotation["shiftRoll"],
                    "current_on_call": checking_rotation["rolls"][0]["onCallUser"]["username"],
                }
                current_on_call_details.append(current_on_call)
    return current_on_call_details


def jira_connect(Label, Shift_Name, weekly_oncall_roll_start, Current_Oncall_username):
    dlo_jira_sa_key = 'Bearer '+ssm.get_parameter(Name='<ssm-jira-key>', WithDecryption=True)['Parameter']['Value']
    headers = {'Authorization': dlo_jira_sa_key,'Content-Type': 'application/json'}

    dl_ticket_params = fetch_ssm_values(oncall_dl_ticket_params)
    url = f"<jira-api-url>"
    dictionary = eval(dl_ticket_params)
    payload = json.dumps(dictionary)
    logger.info(payload)
    response = requests.request(
        "POST",
        url,
        data=payload,
        headers=headers
    )
    if response.status_code == 201:
        issue_key = response.json()['key']
        logger.info(f"Issue created successfully: {issue_key}")
    else:
        logger.warning(f"Failed to create issue: {response.text}")


def main():
    api_key_value = fetch_ssm_values(api_key)
    api_id_value = fetch_ssm_values(api_id)
    schedule_data = fetch_schedule_data(api_key_value, api_id_value)

    for checking in schedule_data:
        if checking['shift_name'] == "<shift-name>":
            Label = ["<morning-shift-label>"]
        else:
            Label = ["<afternoon-shift-label>"]
        Shift_Name = checking["shift_name"]
        weekly_oncall_roll_start = checking["weekly_oncall_start_datetime"]
        Current_Oncall_username = checking["current_on_call"]
        jira_connect(Label, Shift_Name, weekly_oncall_roll_start, Current_Oncall_username)