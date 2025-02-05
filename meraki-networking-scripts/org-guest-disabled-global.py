from operator import truediv
from tokenize import Number
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
    networks = dashboard.organizations.getOrganizationNetworks(org_id)
    
    GuestSSIDDisabledNetworks = []

    # Scan each Network
    for network in networks:
        try:
            net_id = network['id']
            print(f'\n\U000027A1 Analyzing network {network["name"]}')
            Guest_SSID = dashboard.wireless.getNetworkWirelessSsid(networkId=net_id,number=1)
            if Guest_SSID["enabled"] == False:
                DisabledNetworks = {"Network": network['name'], "SSID": Guest_SSID['name']}
                print(json.dumps(DisabledNetworks,indent=4))
                GuestSSIDDisabledNetworks.append(DisabledNetworks)
        except Exception:
            pass

    row = 0
    column = 0
    # Create Excel file and name sheet as AP name
    excel_file = xlsxwriter.Workbook(f'{datetime.now().strftime("%d-%m-%Y")}-[Global]-ServierGuest_Disabled_Networks.xlsx')
    excel_sheet = excel_file.add_worksheet("ServierGuest Disabled Networks")
    cell_format = excel_file.add_format()
    cell_format.set_bold()
    cell_format.set_bg_color('#00A300')
    # Write header of CSV
    header = ["Network Name", "SSID Name"]
    for item in header:
        excel_sheet.write(row, column, item, cell_format)
        column +=1
    row = 1
    column = 0
    for port in GuestSSIDDisabledNetworks:
        for data in port:
            excel_sheet.write(row, column, port[data])
            column += 1
        row += 1
        column = 0
    excel_file.close()


main()