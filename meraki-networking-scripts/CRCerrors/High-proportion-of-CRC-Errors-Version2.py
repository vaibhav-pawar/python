from email.policy import default
from typing import Any
import meraki
import argparse
import json
from datetime import datetime
import xlsxwriter

def main():

    cmd_parser = argparse.ArgumentParser(description="Extract events from the Dashboard events logs")
    cmd_parser.add_argument('-k', '--api-key', help="Your API key. You can set the API_KEY global variable (not recommended !)") # API key (optionnal else set API_KEY global variable, not recommended)
    cmd_args = cmd_parser.parse_args()

    #Load API
    dashboard = meraki.DashboardAPI(
        api_key=cmd_args.api_key if cmd_args.api_key is not None else API_KEY,
        base_url='https://api.meraki.com/api/v1/',
        output_log=False,
        print_console=False,
    )

    #Load Organization
    organizations = dashboard.organizations.getOrganizations()
    for org in organizations:
        org_id = org['id']

    #Load Organization Networks
    networks = dashboard.organizations.getOrganizationNetworks(org_id, tags=["0-Active"])

    #Port is having High CRC errors
    HighProportionOfCRC = []

    for network in networks:
        net_id = network['id']
        print(f'\n\U000027A1 Analyzing network {network["name"]}')
        devices = dashboard.networks.getNetworkDevices(net_id)
        switchesList = []
        for device in devices:
            if 'MS' in device['model']:
                SwitchDetails = {"Name": device['name'],"Serial": device['serial']}
                switchesList.append(SwitchDetails)
        
        for switchSerial in switchesList:
            allPortsOfASwitch = dashboard.switch.getDeviceSwitchPortsStatusesPackets(serial=switchSerial['Serial'],timespan=86400)
            #print(json.dumps(allPortsOfASwitch,indent=4))
            for port in allPortsOfASwitch:
                try:
                    if port['packets'][3]['total'] >= 100 or port['packets'][4]['total'] >= 50:
                        CRCports = {"Network": network['name'],"SwitchName": switchSerial['Name'], "Switchport": port['portId'], "CRC align errors" : port['packets'][3]['total'], "Fragments" : port['packets'][4]['total']}
                        print(json.dumps(CRCports,indent=4))
                        HighProportionOfCRC.append(CRCports)
                except Exception:
                    pass
    
    row = 0
    column = 0
    # Create Excel file and name sheet as High Proportion of CRC Errors
    excel_file = xlsxwriter.Workbook(f'{datetime.now().strftime("%d-%m-%Y-%H%M")}-CRCErrors[0-Active].xlsx')
    excel_sheet = excel_file.add_worksheet("High Proportion of CRC Errors")
    cell_format = excel_file.add_format()
    cell_format.set_bold()
    cell_format.set_bg_color('#00A300')
    # Write header of CSV
    header = ["Network Name", "Switch Name", "Port ID", "CRC align errors", "Fragments"]
    for item in header:
        excel_sheet.write(row, column, item, cell_format)
        column +=1
    row = 1
    column = 0
    for port in HighProportionOfCRC:
        for data in port:
            excel_sheet.write(row, column, port[data])
            column += 1
        row += 1
        column = 0
    excel_file.close()

main()