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
    networks = dashboard.organizations.getOrganizationNetworks(org_id)

    #Port is having High CRC errors
    SwitchportList = []

    for network in networks:
        net_id = network['id']
        print(f'\n\U000027A1 Analyzing network {network["name"]}')
        devices = dashboard.networks.getNetworkDevices(net_id)
        switchesList = []
        try:
            for device in devices:
                if 'MS' in device['model']:
                    SwitchDetails = {"Name": device['name'],"Serial": device['serial']}
                    switchesList.append(SwitchDetails)
        except Exception:
                    pass
        
        for switchSerial in switchesList:
            allPortsOfASwitch = dashboard.switch.getDeviceSwitchPorts(serial=switchSerial['Serial'])
            #print(json.dumps(allPortsOfASwitch,indent=4))
            for port in allPortsOfASwitch:
                #print(json.dumps(port,indent=4))
                try:
                    if "WIreless" in port['tags'] or "recently-added" in port['tags'] or "Offcei" in port['tags'] or "Meeting-room" in port['tags'] or "MeetingRooms" in port['tags'] or "LAN" in port['tags'] or "CORE" in port['tags']:
                        SwitchportDetails = {"Network": network['name'],"Switch_Name": switchSerial['Name'], "Port": port['portId'], "Tags" : str(port['tags'])}
                        print(json.dumps(SwitchportDetails,indent=4))
                        SwitchportList.append(SwitchportDetails)
                except Exception:
                    pass
           
    row = 0
    column = 0
    # Create Excel file and name sheet as High Proportion of CRC Errors
    excel_file = xlsxwriter.Workbook(f'{datetime.now().strftime("%d-%m-%Y-%H%M")}-TagFilteringOnSwitchportsGlobal.xlsx')
    excel_sheet = excel_file.add_worksheet("Tag Filtering on switchports")
    cell_format = excel_file.add_format()
    cell_format.set_bold()
    cell_format.set_bg_color('#00A300')
    # Write header of CSV
    header = ["Network Name", "Switch Name", "Port", "Tags"]
    for item in header:
        excel_sheet.write(row, column, item, cell_format)
        column +=1
    row = 1
    column = 0
    for port in SwitchportList:
        for data in port:
            excel_sheet.write(row, column, port[data])
            column += 1
        row += 1
        column = 0
    excel_file.close()

main()