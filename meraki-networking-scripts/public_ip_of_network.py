import meraki
import argparse
import json
from datetime import datetime
import xlsxwriter

def main():

    cmd_parser = argparse.ArgumentParser(description="Extract events from the Dashboard events logs")
    cmd_parser.add_argument('-k', '--api-key', help="Your API key. You can set the API_KEY global variable (not recommended !)") # API key (optionnal else set API_KEY global variable, not recommended)
    cmd_args = cmd_parser.parse_args()

    # Load API
    dashboard = meraki.DashboardAPI(
        api_key=cmd_args.api_key if cmd_args.api_key is not None else API_KEY,
        base_url='https://api.meraki.com/api/v1',
        output_log=False,
        print_console=False
    )

    #Load Organization
    organizations = dashboard.organizations.getOrganizations()
    for org in organizations:
        org_id = org['id']
    
    #Load Organization Networks
    networks = dashboard.organizations.getOrganizationNetworks(org_id)

    publicipofnetworks = []
    oneswitchofeachnetwork = []

    for network in networks:
        net_id = network['id']
        print(f'\n\U000027A1 Analyzing network {network["name"]}')
        switchesList = []
        devices = dashboard.networks.getNetworkDevices(networkId=net_id)
        for device in devices:
            if 'MS' in device['model'] and device['lanIp'] is not None:
                SwitchDetails = {"Name": device['name'],"Serial": device['serial']}
                #print(json.dumps(SwitchDetails,indent=4))
                switchesList.append(SwitchDetails)
        
        oneswitchofeachnetwork = []
        try:
            SwitchListChooseOne = {"Switch": switchesList[0]}
            oneswitchofeachnetwork.append(SwitchListChooseOne)
        except Exception:
            pass

        for switchSerial in oneswitchofeachnetwork:
                allPortsOfASwitch = dashboard.organizations.getOrganizationDevicesStatuses(networkId=net_id,organizationId=org_id,serials=switchSerial["Switch"]["Serial"])
                #print(json.dumps(allPortsOfASwitch,indent=4))
                try:
                    if allPortsOfASwitch[0]["publicIp"] is not None:
                        DeviceCollection = {"Network": network['name'], "PublicIP": allPortsOfASwitch[0]["publicIp"]}
                        print(json.dumps(DeviceCollection,indent=4))
                        publicipofnetworks.append(DeviceCollection)
                except Exception:
                    pass

    row = 0
    column = 0
    # Create Excel file and name sheet as AP name
    excel_file = xlsxwriter.Workbook(f'{datetime.now().strftime("%d-%m-%Y")}-Global-Network_Publicip.xlsx')
    excel_sheet = excel_file.add_worksheet("Global Network Public IP")
    cell_format = excel_file.add_format()
    cell_format.set_bold()
    cell_format.set_bg_color('#00A300')
    # Write header of CSV
    header = ["Network Name", "Public IP"]
    for item in header:
        excel_sheet.write(row, column, item, cell_format)
        column +=1
    row = 1
    column = 0
    for port in publicipofnetworks:
        for data in port:
            excel_sheet.write(row, column, port[data])
            column += 1
        row += 1
        column = 0
    excel_file.close()

main()