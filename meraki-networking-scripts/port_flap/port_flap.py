from time import sleep
import meraki
from datetime import datetime
import argparse
import os

def main():

    cmd_parser = argparse.ArgumentParser(description="Extract events from the Dashboard")
    cmd_parser.add_argument('-n', '--network', help="The site code in the form SRV-XXn-Country") # Site code
    cmd_parser.add_argument('-k', '--api-key', help="Your API key") # API key
    cmd_parser.add_argument('-s', '--switch-tags', help="List of switch tags", nargs="*")
    cmd_parser.add_argument('-c', '--count', help="Number of flaps to be generated", type=int, default=1)
    cmd_args = cmd_parser.parse_args()

    # If network name argument is specified, split the name into sub-strings to be used as tags
    if cmd_args.network is not None:
        net_arg = cmd_args.network.split("-") # net_arg[0] = "SRV" ; net_arg[1] = SITE CODE ; net_arg[2] = COUNTRY CODE
        tag = [f'3-{net_arg[1].upper()}', f'{net_arg[2]}'] # Set tag format for network

    device_tags = cmd_args.switch_tags if cmd_args.switch_tags is not None else ""
    if cmd_args.count is not None:
        count = cmd_args.count
    else:
        count = True


    # Load organizations
    try:
        dashboard = meraki.DashboardAPI(
            api_key=cmd_args.api_key if cmd_args.api_key is not None else os.getenv('MERAKI_DASHBOARD_API_KEY'),
            base_url='https://api.meraki.com/api/v1/',
            output_log=False,
            print_console=False,
            retry_4xx_error=True,
            retry_4xx_error_wait_time=120
        )
        organizations = dashboard.organizations.getOrganizations()
    except meraki.APIError:
        raiseError(meraki.APIError)

    # Get organization ID
    for org in organizations:
        org_id = org['id']

    # Get network in organization
    try:
        if cmd_args.network is not None:
            networks = dashboard.organizations.getOrganizationNetworks(org_id, tags=tag)
        else:
            networks = dashboard.organizations.getOrganizationNetworks(org_id, tags=["0-Lab"])
    except Exception:
        raiseError(Exception)

    # Loop through networks
    for network in networks:
        net_id = network['id']
        print(f'\n\U000027A1 Analyzing network {network["name"]}')
        devices = dashboard.organizations.getOrganizationDevices(
            organizationId=org_id,
            networkIds=net_id,
            tags=device_tags if device_tags is not None else ""
            ) # Get devices in current network
        for device in devices:
            if count == True:
                i=True
            else: i=1
                
            # Exclude devices that aren't MR access points
            if "MS" not in device["model"]:
                continue
            # Exclude devices with no name (prevent script error)
            if "name" not in device:
                continue

            while i <= count:
                try:
                        flapped_ports = dashboard.switch.cycleDeviceSwitchPorts(
                            serial=device["serial"],
                            ports=["2-32"]
                        )
                        print(f'\n\U0001F503 Iteration {i}/{count} \U00002796 Flapping {device["name"]} ports {flapped_ports["ports"]}')
                except meraki.APIError:
                    raiseError(meraki.APIError)
                #sleep(1)
                if i == True:
                    continue
                else:
                    i = i+1
    #return(0)

def raiseError(exception):
    if exception == meraki.APIError:
        print(f'Meraki API error: {exception}')
        return()
    elif exception == KeyboardInterrupt:
        print("\nExecution interrupted by user.")
    else:
        print(f'Error: {exception}')
        return()
        
if __name__ == '__main__':
    start_time = datetime.now()
    try:
        main()
    except KeyboardInterrupt:
        raiseError(KeyboardInterrupt)
    end_time = datetime.now()
    print(f'\nScript complete, total runtime {end_time - start_time}')