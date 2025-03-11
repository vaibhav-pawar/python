import boto3
import pandas as pd
import openpyxl
from openpyxl.styles import PatternFill, Alignment
from concurrent.futures import ThreadPoolExecutor

glue_client = boto3.client('glue')

def glue_collect_database_information():
    print("\n\U000027A1 Collecting Database Names from Glue.")
    paginator = glue_client.get_paginator('get_databases')
    databases = []

    for page in paginator.paginate():
        for database in page['DatabaseList']:
            databases.append(database['Name'])

    return databases

databases = glue_collect_database_information()
print("Total Number of Databases:", len(databases))

print("\n\U000027A1 Collecting Table Names from Database Names.")
def get_all_glue_tables(database_name):
    paginator = glue_client.get_paginator('get_tables')
    all_tables = []

    for page in paginator.paginate(DatabaseName=database_name):
        all_tables.extend(page['TableList'])

    return all_tables

with ThreadPoolExecutor() as executor:
    result = list(executor.map(get_all_glue_tables, databases))
print("Table Names Collected successfully from Glue.")

# print("Result", result)
all_collected_tables = [table for sublist in result for table in sublist]
all_database_table_names_list = []
for sorting_data in all_collected_tables:
    database_table_name_dict = {"Database_Name" : sorting_data['DatabaseName'], "Table_Name" : sorting_data['Name']}
    all_database_table_names_list.append(database_table_name_dict)

print("\n\U000027A1 Exporting Database & Table Names in excel File.")
df = pd.DataFrame(all_database_table_names_list)
excel_file_path = 'Glue_database_table_names.xlsx'
writer = pd.ExcelWriter(excel_file_path, engine='openpyxl')
df.to_excel(writer, index=False, sheet_name='Glue_database_table_names')
workbook = writer.book
worksheet = writer.sheets['Glue_database_table_names']
for cell in worksheet['1']:
    cell.fill = PatternFill(start_color='4CBB17', end_color='4CBB17', fill_type='solid')
    cell.alignment = Alignment(horizontal='center', vertical='center')
writer.save()
print(f"\033[32mFile saved as {excel_file_path}\033[0m")