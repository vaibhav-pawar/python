import boto3
import json
from prettytable import PrettyTable

eventbridge_client = boto3.client('events')

def find_ge_suite():
    ge_suite_names = input("Enter Ge suite names separated by comma: ")
    ge_suites_list = ge_suite_names.split(',')

    rule_names = []
    paginator = eventbridge_client.get_paginator('list_rules')
    for page in paginator.paginate():
        for rule in page['Rules']:
            if rule['Name'].startswith("<starting-name-of-ge-suite>"):
                rule_names.append(rule['Name'])

    suite_details = []
    print("Processing is started please wait for some time...")
    for going_through_each_suite in ge_suites_list:
        suite_found = False
        for going_through_each_rule in rule_names:
            list_targets_from_each_rule_response = eventbridge_client.list_targets_by_rule(Rule=f'{going_through_each_rule}')
            for index, checking_each_target in enumerate(list_targets_from_each_rule_response['Targets'], start=1):
                if 'Input' not in checking_each_target:
                    pass
                else:
                    result_converted_json = json.loads(checking_each_target['Input'])
                    if 'expectations' not in result_converted_json:
                        pass
                    else:
                        if going_through_each_suite in result_converted_json['expectations']:
                            suite_info = {"Suite_Name": going_through_each_suite, "Rule_Name": going_through_each_rule, "Target_No": f'Target{index}'}
                            existing_record_index = None
                            for i, existing_suite in enumerate(suite_details):
                                if existing_suite["Suite_Name"] == going_through_each_suite and existing_suite["Rule_Name"] == going_through_each_rule:
                                    existing_record_index = i
                                    break
                            if existing_record_index is not None:
                                suite_details[existing_record_index]["Target_No"] += f', Target{index}'
                            else:
                                suite_details.append(suite_info)
                                suite_found = True

        if not suite_found:
            suite_info = {"Suite_Name": going_through_each_suite, "Rule_Name": "Not configured in any rule", "Target_No": "N/A"}
            suite_details.append(suite_info)

    suite_details_table = PrettyTable()
    suite_details_table.field_names = ["Ge Suite Name", "Configured in rule", "Configured in target"]
    for suite_dict in suite_details:
        suite_details_table.add_row([suite_dict["Suite_Name"], suite_dict["Rule_Name"], suite_dict["Target_No"]])
    print(suite_details_table)

find_ge_suite()