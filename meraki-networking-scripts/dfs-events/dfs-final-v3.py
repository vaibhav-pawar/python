from ast import Pass
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import glob
import os
#from tkinter import N
import meraki
from datetime import datetime
import xlsxwriter
import argparse
import json
import time

event_list1 = []
event_list2 = []
event_list3 = []

def read(file):
    f = open(file, "r")
    content = f.read()
    f.close()
    return content

def main():

    cmd_parser = argparse.ArgumentParser(description="Extract events from the Dashboard")
    cmd_parser.add_argument('-k', '--api-key', help="Your API key. You can set the API_KEY global variable (not recommended !)") # API key (optionnal else set API_KEY global variable, not recommended)
    cmd_args = cmd_parser.parse_args()

    # Load organizations
    dashboard = meraki.DashboardAPI(
        api_key=cmd_args.api_key if cmd_args.api_key is not None else API_KEY,
        base_url='https://api.meraki.com/api/v1/',
        output_log=False,
        print_console=False
    )

    organizations = dashboard.organizations.getOrganizations()
    # Get organization ID
    for org in organizations:
        org_id = org['id']

    # Get network in organization
    networks = dashboard.organizations.getOrganizationNetworks(org_id, tags="0-Active")
    #for net in networks:
    #    print(json.dumps(net['name'],indent=4))

    for network in networks:
        net_id = network['id']
        print(f'\n\U000027A1 Analyzing network for List1 {network["name"]}')
        try:
            event = dashboard.networks.getNetworkEvents(networkId=net_id,productType="wireless",includedEventTypes="dfs_event")
            for old_log in event["events"]:
                if old_log in event_list1:
                    print('\n\U000027A1 This Data is Already Present in List1')
                    pass
                else:
                    print("New Data found updating List1")
                    print(json.dumps(old_log,indent=4))
                    old_log_config = {"Network": network["name"], "Description": old_log["description"], "OccuredAt": old_log["occurredAt"], "Device_Name": old_log["deviceName"],"Channel": old_log["eventData"]["channel"],"Radio": old_log["eventData"]["radio"] }
                    print(json.dumps(old_log_config,indent=4))
                    event_list1.append(old_log_config)
        except Exception:
            Pass
        
    #time.sleep(1200)
    print('\n\U000027A1 Wait for next 1 Hour')
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("Current Time =", current_time)
    time.sleep(3600)

    for network in networks:
        net_id = network['id']
        print(f'\n\U000027A1 Analyzing network for List2 {network["name"]}')
        try:
            event1 = dashboard.networks.getNetworkEvents(networkId=net_id,productType="wireless",includedEventTypes="dfs_event")
            for new_log in event1["events"]:
                if new_log in event_list2:
                    print('\n\U000027A1 This Data is Already Present in List1')
                    pass
                else:
                    print("New Data found updating List1")
                    print(json.dumps(old_log,indent=4))
                    new_log_config = {"Network": network["name"], "Description": new_log["description"], "OccuredAt": new_log["occurredAt"], "Device_Name": new_log["deviceName"],"Channel": new_log["eventData"]["channel"],"Radio": new_log["eventData"]["radio"] }
                    print(json.dumps(new_log_config,indent=4))
                    event_list2.append(new_log_config)
        except Exception:
            Pass

    print('\n\U000027A1 Comparing both Lists & Checking if there is any Change')
    for element in event_list2:
        if element not in event_list1:
            event_list3.append(element)
        else:
            pass
    
    print(json.dumps(event_list3,indent=4))

    if event_list3:
        print(json.dumps(f"New Data: {event_list3}",indent=4))
        APname = ""
        for item in event_list3:
            APname = APname + f"DFS is detected on Network: {item['Network']} - AP: {item['Device_Name']} at interval: {item['OccuredAt']} | Channel: {item['Channel']} & Radio: {item['Radio']}\n"
        print('\n\U000027A1 Sending Mail...')
        fromaddr = "automation.meraki@gmail.com"
        toaddr = "servier.network.in@capgemini.com"
        msg = MIMEMultipart()
        msg['From'] = fromaddr
        msg['To'] = toaddr
        msg['Subject'] = "DFS Alert is detected"
        body = \
        f"""<html>
            <body>
                Dear team,
                <br>
                New Alert is generated on following Networks [Note: Mentioned Timezone is GMT]
                <pre>{APname}</pre>
                <br>
                Thank you
            </body>
        </html>"""
        print(body)
        msg.attach(MIMEText(body, 'html'))
        p = MIMEBase('application', 'octet-stream')
        #msg.attach(p)
        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.starttls()
        s.login(fromaddr, read("/home/ubuntu/private/myseckey.txt"))
        text = msg.as_string()
        s.sendmail(fromaddr, toaddr, text)
        s.quit()
        print('\n\U000027A1 Mail Sent.')
        event_list3.clear()
    else:
        print('\n\U000027A1 No Data found in List3')

while True:
    main()
