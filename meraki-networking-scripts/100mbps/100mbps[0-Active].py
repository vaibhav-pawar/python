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
        base_url='https://api.meraki.com/api/v1/',
        output_log=False,
        print_console=False
    )

    #Load Organization
    organizations = dashboard.organizations.getOrganizations()
    for org in organizations:
        org_id = org['id']
    
    #Load Organization Networks
    networks = dashboard.organizations.getOrganizationNetworks(org_id, tags=["0-Active"])

    listOfPortsAt100Mb = []

    # Scan each Network
    for network in networks:
        net_id = network['id']
        print(f'\n\U000027A1 Analyzing network {network["name"]}')
        switchesList = []
        devices = dashboard.networks.getNetworkDevices(net_id)
        for device in devices:
            if 'MS' in device['model']:
                switchesList.append(device['serial'])

        for switchSerial in switchesList:
            #print("Serial: ", switchSerial)
            allPortsOfASwitch = dashboard.switch.getDeviceSwitchPortsStatuses(serial=switchSerial)
            for port in allPortsOfASwitch:
                try:
                    if port["speed"] == "100 Mbps" and "MR" in port["cdp"]["platform"]:
                        dictofFaultyPort = {"Network": network['name'], "neighborLLDP": port["lldp"]["systemName"], "Negotiating": port["speed"], "switchSerial": switchSerial, "portId": port["portId"]}
                        listOfPortsAt100Mb.append(dictofFaultyPort)
                except Exception:
                    pass

    row = 0
    column = 0
    # Create Excel file and name sheet as AP name
    excel_file = xlsxwriter.Workbook(f'{datetime.now().strftime("%d-%m-%Y-%H%M")}-100mbps[0-Active].xlsx')
    excel_sheet = excel_file.add_worksheet("100mbps")
    # Write header of CSV
    header = ["Network Name", "Access Point", "Negotiating", "Connected to Switch", "Port ID"]
    for item in header:
        excel_sheet.write(row, column, item)
        column +=1
    row = 1
    column = 0
    for port in listOfPortsAt100Mb:
        for data in port:
            excel_sheet.write(row, column, port[data])
            column += 1
        row += 1
        column = 0
    excel_file.close()
        
main()

###
# listOfNetwork = ['FR1', 'IE1', 'IN1']
# for network in listOFNetwork:
#   excel_sheet = excel_file.add_worksheet(network)
#
###
