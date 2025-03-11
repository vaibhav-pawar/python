import boto3
import re
import os
import logging
from prettytable import PrettyTable
from datetime import date, datetime, timedelta


client_glue = boto3.client("glue")
client_dynamodb = boto3.resource("dynamodb")
client_ses = boto3.client("ses")


dynamo_mapping = {
    "dev": "dev-config",
    "qa": "dev-config",
    "stg": "stage2-config",
    "prd": "prod2-config",
}


logger = logging.getLogger()
logger.setLevel(logging.INFO)


env = os.environ.get("STACK_ENV")
snapshot_pattern = r"snapshot_id=\d\d\d\d\d\d\d\dT\d\d\d\d\d\dZ"
current_date_minus_4_obj = datetime.now() - timedelta(days=4)
current_date_minus_4 = current_date_minus_4_obj.strftime("%Y%m%dT%H%M%SZ")


def handler(event, context):
    start_time = datetime.now()
    failed_to_process, ingestion_failed, partition_issue = audit()
    to, data, qc_status, dls_body = generate_report_data(failed_to_process, ingestion_failed, partition_issue)
    send_mail_ses(to, data, qc_status, dls_body)
    logger.info(f"Time Taken for execution: {datetime.now() - start_time}")


def get_item_dynamo(table_name, key):
    table = client_dynamodb.Table(table_name)
    response = table.get_item(Key={"id": f"{key}"})
    return response.get("Item", {})


def get_all_glue_tables(database_name):
    all_tables = []
    paginator = client_glue.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=database_name):
        all_tables.extend(page.get("TableList", list()))
    return all_tables


def audit():
    failed_to_process = []
    ingestion_failed = []
    partition_issue = []

    rdb_databases = get_item_dynamo(dynamo_mapping[env], "<dynamo-database-list>")["db_list"]
    ct_tables = (
        get_item_dynamo(dynamo_mapping[env], "<dynamo-exclude-base-tables>").get("table", {}).get("<database-short-name>", {})
    )
    table_exclude_list = get_item_dynamo(dynamo_mapping[env], "<dynamo-exclude-additional-tables-list>").get("tb_list", list())

    logger.info(f"Rdb databases: {rdb_databases}")
    logger.info(f"Raw ct tables: {ct_tables}")
    logger.info(f"Raw table exclude list: {table_exclude_list}")

    for db, tb_list in ct_tables.items():
        dl_db = "<databse-short-name>__" + db.lower()
        for tb in tb_list:
            table_exclude_list.append(f"{dl_db}.{tb.lower()}")

    logger.info(f"Finalized exclude list: {table_exclude_list}")

    for db in rdb_databases:
        logger.info(f"Processing database: {db}")
        table_list = get_all_glue_tables(db)

        for tb_details in table_list:
            tb_name = tb_details["Name"]
            obj_name = f"{db}.{tb_name}"
            logger.info(f"Processing table: {obj_name}")

            if obj_name not in table_exclude_list:
                try:
                    resp = validate_table(tb_details)
                    if resp:
                        failure_type, detail = resp
                        if failure_type == "ingestion_failed":
                            ingestion_failure = {"table_name": obj_name, "load_date": detail}
                            ingestion_failed.append(ingestion_failure)
                        else:
                            partition_issue.append(obj_name)
                except Exception:
                    logger.warning(f"Table failed to process: {obj_name}", exc_info=True)
                    failed_to_process.append(obj_name)
            else:
                logger.info(f"Table found in exclude list: {obj_name}")

    logger.info(f"Failed to process: {failed_to_process}")
    logger.info(f"Ingestion Delayed: {ingestion_failed}")
    logger.info(f"Partition Issue: {partition_issue}")

    return failed_to_process, ingestion_failed, partition_issue


def validate_table(table_details):
    db_name = table_details["DatabaseName"]
    tb_name = table_details["Name"]

    storage_descriptor = table_details.get("StorageDescriptor", {})
    location = storage_descriptor.get("Location")

    table_columns_detailed = storage_descriptor.get("Columns", list())
    cols = [col_details["Name"] for col_details in table_columns_detailed]

    partition_cols_detailed = table_details.get("PartitionKeys", list())
    partition_cols = [col_details["Name"] for col_details in partition_cols_detailed]
    logger.info(f"Location: {location} || Columns: {cols} || Partition_Columns:{partition_cols}")

    if not storage_descriptor or not location:
        logger.info(f"Table does not have storage_descriptor or location : {db_name}.{tb_name}")
    elif "dl_loaddate" in cols + partition_cols:
        location_stripped = location.strip("/")
        snapshot_id = location_stripped.rsplit("/", 1)[1]

        if re.match(snapshot_pattern, snapshot_id):
            snapshot_date = snapshot_id.replace("snapshot_id=", "")

            if snapshot_date < current_date_minus_4:
                logger.info(f"Ingestion Failed for Table: {db_name}.{tb_name} | snapshot_date: {snapshot_date}")
                return "ingestion_failed", snapshot_date

            table_partitions = client_glue.get_partitions(DatabaseName=db_name, TableName=tb_name)
            partition_count = len(table_partitions.get("Partitions", list()))
            if partition_count != 1:
                logger.warning(
                    f"Invalid partition count found for Table: {db_name}.{tb_name} | count: {partition_count}"
                )
                return "partition_issue", partition_count
        else:
            logger.warning(f"Invalid snapshot date found for table: {db_name}.{tb_name}")
            return "ingestion_failed", "invalid_snapshot_date"


def generate_report_data(failed_to_process, ingestion_delayed, partition_issue):
    qc_receivers = get_item_dynamo(dynamo_mapping[env], "<dynamo-receiver-email-addresses>")
    default_receivers = qc_receivers["default_receiver_list"]
    dls_receivers = qc_receivers["dls_receiver_list"]

    dls_body = f"""
                Tables failed to process = {failed_to_process}

                Ingestion Delayed for following Tables = {ingestion_delayed}

                Tables with partition issue = {partition_issue}

                To resolve follow the steps mentioned here: <how-to-resolve-wiki-link>
               """
    success_message = """
                      <p>Hello Team,</p>
                      <p>rdb QC passed all checks successfully.</p>
                      <p>Thank You</p>
                      """
    if not failed_to_process and not ingestion_delayed and not partition_issue:
        return default_receivers, success_message, "SUCCESS", dls_body

    default_receivers.extend(dls_receivers)
    action = "<p>To resolve follow the steps mentioned here: <a href='<how-to-resolve-wiki-link>'>Runbook URL</a></p>"

    overview = PrettyTable()
    overview.field_names = ["status", "count"]

    overview.add_row(["Failed to process", len(failed_to_process)])
    overview.add_row(["Ingestion delayed", len(ingestion_delayed)])
    overview.add_row(["Partition issue", len(partition_issue)])

    overview_html = overview.get_html_string(attributes={"border": "1", "bordercolor": "green", "bgcolor": "white"})
    email_table_output = "<h2>Overview:</h2>" + overview_html + "<br>"

    if failed_to_process:
        email_table_output += "<h2>Failed to process:</h2>"
        email_table_output += generate_failure_details(failed_to_process, ["table_name"])

    if ingestion_delayed:
        email_table_output += "<h2>Ingestion delayed:</h2>"
        email_table_output += generate_failure_details(ingestion_delayed, ["table_name", "load_date"])

    if partition_issue:
        email_table_output += "<h2>Partition issues:</h2>"
        email_table_output += generate_failure_details(partition_issue, ["table_name"])

    return default_receivers, email_table_output + action, "FAILED", dls_body


def generate_failure_details(content, field_names):
    content_table = PrettyTable()
    content_table.field_names = field_names

    for c in content:
        if len(field_names) == 2:
            content_table.add_row([c[field_names[0]], c[field_names[1]]])
        else:
            content_table.add_row([c])

    content_table_html = content_table.get_html_string(
        attributes={"border": "1", "bordercolor": "green", "bgcolor": "white"}
    )

    return content_table_html


def send_mail_ses(to, data, qc_status, dls_data):
    response = client_ses.send_email(
        Destination={
            "ToAddresses": to,
        },
        Message={
            "Body": {
                "Html": {
                    "Charset": "UTF-8",
                    "Data": data,
                },
                "Text": {
                    "Charset": "UTF-8",
                    "Data": dls_data,
                },
            },
            "Subject": {
                "Charset": "UTF-8",
                "Data": f"rdb QC report status for {date.today()}: {qc_status}",
            },
        },
        Source="<source-email-id",
    )
    logger.info(f"Email Sent successfully to {to}")