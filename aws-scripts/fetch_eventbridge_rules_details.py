import boto3
import json
import pandas as pd

eventbridge_client = boto3.client('events')

def collect_eventbridge_rules():
    print("Collecting information. please wait for some time...")
    Suites_with_cron = []
    Suites_with_eventpattern = []
    paginator = eventbridge_client.get_paginator('list_rules')
    for page in paginator.paginate():
        for rule in page['Rules']:
            if 'ScheduleExpression' in rule:
                list_targets_from_each_rule_response = eventbridge_client.list_targets_by_rule(Rule=f'{rule["Name"]}')
                rule_details = {"Rule Name": rule['Name'], "Arn": rule['Arn'], "State": rule['State'], "ScheduleExpression": rule['ScheduleExpression'], "Total Targets Configured in Rule": len(list_targets_from_each_rule_response["Targets"])}
                if 'Description' in rule:
                    rule_details["Description"] = rule['Description']
                Suites_with_cron.append(rule_details)
            elif 'EventPattern' in rule:
                rule_details = {"Rule Name": rule['Name'], "Arn": rule['Arn'], "State": rule['State'], "EventPattern": rule['EventPattern']}
                if 'Description' in rule:
                    rule_details["Description"] = rule['Description']
                Suites_with_eventpattern.append(rule_details)

    print(f"Total count of eventbridge Cron based rules: {len(Suites_with_cron)}")
    print(f"Total count of eventbridge Eventpattern based rules: {len(Suites_with_eventpattern)}")

    df1 = pd.DataFrame(Suites_with_cron)
    df2 = pd.DataFrame(Suites_with_eventpattern)

    file_name = 'Eventbridge_Rules_Collection.xlsx'

    with pd.ExcelWriter(file_name, engine='openpyxl', mode='w') as writer:
        df1.to_excel(writer, sheet_name='Cron_Based_Rule', index=False)
        df2.to_excel(writer, sheet_name='Eventpattern_Based_Rule', index=False)

collect_eventbridge_rules()