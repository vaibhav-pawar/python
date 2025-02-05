from gettext import Catalog
from hashlib import new
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import glob
import os
import meraki
from datetime import datetime
import xlsxwriter
import argparse
import json
import time

event_list1 = []
event_list2 = []

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
    networks = dashboard.organizations.getOrganizationNetworks(org_id, tags="3-SA1")
    for network in networks:
        net_id = network['id']
    
    events1 = dashboard.networks.getNetworkEvents(networkId=net_id,productType="wireless",includedEventTypes="dfs_event",perPage=5)
    for old in events1['events']:
        old_data = {"Occured_At": old["occurredAt"], "Device_Name": old["deviceName"], "Channel": old["eventData"]["channel"], "Radio": old["eventData"]["radio"]}
        #print(json.dumps(old_data,indent=4))
        if old_data in event_list1:
            print("ignore - This Entry is Already Present in List1")
        else:    
            event_list1.append(old_data)
    print('\n\U000027A1 Adding data into first List...')
    for eve1 in event_list1:    
        print(json.dumps(eve1["Occured_At"],indent=4))
    print('\n\U000027A1 Data is Added into first List. Kindly, Wait for 2 Minutes')
    time.sleep(120)
 
    events2 = dashboard.networks.getNetworkEvents(networkId=net_id,productType="wireless",includedEventTypes="dfs_event",perPage=5)
    for new in events2['events']:
        new_data = {"Occured_At": new["occurredAt"], "Device_Name": new["deviceName"], "Channel": new["eventData"]["channel"], "Radio": new["eventData"]["radio"]}
        #print(json.dumps(new_data,indent=4))
        if new_data in event_list2:
            print("ignore - This Entry Already Present in List2")
        else:
            event_list2.append(new_data)
    print('\n\U000027A1 Adding data into seond List...')
    for eve1 in event_list2:    
        print(json.dumps(eve1["Occured_At"],indent=4))
    print('\n\U000027A1 Data is Added into second List.')
    time.sleep(10)

    print('\n\U000027A1 Comparing both Lists & Checking if there is any new data')
    for element in event_list1:
        if element not in event_list2:
            fromaddr = "automation.meraki@gmail.com"
            toaddr = "vaibhav.e.pawar@capgemini.com"
            msg = MIMEMultipart()
            msg['From'] = fromaddr
            msg['To'] = toaddr
            msg['Subject'] = "DFS Alert is detected"
            body = f"""
                <html>
                <body>
                    <h4>Hello Team,</h4>
                    
                    <h4>DFS is detected on AP: {element['Device_Name']} at interval: {element['Occured_At']}</h4>
                    <h4>Channel: {element['Channel']} | Radio: {element['Radio']}</h4>
                    
                    <h4>Regards,</h4>
                    <h4>Meraki Automation</h4>
                </body>
                </html>
                """
            msg.attach(MIMEText(body, 'html'))
            p = MIMEBase('application', 'octet-stream')
            #msg.attach(p)
            s = smtplib.SMTP('smtp.gmail.com', 587)
            s.starttls()
            s.login(fromaddr, read("/home/ubuntu/private/myseckey.txt"))
            text = msg.as_string()
            s.sendmail(fromaddr, toaddr, text)
            s.quit()
            print(element)
        else:
            print("No Changes in Data")
            
    for element in event_list2:
        if element not in event_list1:
            fromaddr = "automation.meraki@gmail.com"
            toaddr = "vaibhav.e.pawar@capgemini.com"
            msg = MIMEMultipart()
            msg['From'] = fromaddr
            msg['To'] = toaddr
            msg['Subject'] = "DFS Alert is detected"
            body = f"""
                <html>
                <body>
                    <h4>Hello Team,</h4>
                
                    <h4>DFS is detected on AP: {element['Device_Name']} at interval: {element['Occured_At']}</h4>
                    <h4>Channel: {element['Channel']} | Radio: {element['Radio']}</h4>

                    <h4>Regards,</h4>
                    <h4>Meraki Automation</h4>
                </body>
                </html>
                """
            msg.attach(MIMEText(body, 'html'))
            p = MIMEBase('application', 'octet-stream')
            #msg.attach(p)
            s = smtplib.SMTP('smtp.gmail.com', 587)
            s.starttls()
            s.login(fromaddr, read("/home/ubuntu/private/myseckey.txt"))
            text = msg.as_string()
            s.sendmail(fromaddr, toaddr, text)
            s.quit()
            print(element)
        else:
            print("No Changes in Data")

    #event_list1.clear()
    #event_list2.clear()
    print('\n\U000027A1 Task Completed')

while True:
    main()
