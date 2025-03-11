import boto3
from botocore.exceptions import NoCredentialsError
from datetime import datetime, timedelta
import os
import logging


s3 = boto3.client('s3')


def download_s3_files_in_date_range(bucket_name, prefix, start_date_str, end_date_str, local_dir):
    start_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"log_{start_time}.log"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file)
    console_handler = logging.StreamHandler()

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    end_date += timedelta(days=1)
    logger.info(f"Script started for date range {start_date_str} to {end_date_str}")

    dates_with_more_than_seven_files = []

    current_date = start_date
    while current_date < end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        date_str_without_hyphen = current_date.strftime("%Y%m%d")
        date_prefix = f"{prefix}datadate={date_str}/"
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=date_prefix)
        logger.info(f"Listing objects for prefix: {date_prefix}")
        if 'Contents' in response:
            local_date_dir = os.path.join(local_dir, f"data_date={date_str_without_hyphen}")
            if not os.path.exists(local_date_dir):
                os.makedirs(local_date_dir)
                logger.info(f"Created directory: {local_date_dir}")
            num_files = len(response['Contents'])
            for obj in response['Contents']:
                file_key = obj['Key']
                file_name = file_key.split('/')[-1]
                local_file_path = os.path.join(local_date_dir, file_name)
                try:
                    logger.info(f"Downloading {file_key} to {local_file_path}")
                    s3.download_file(bucket_name, file_key, local_file_path)
                except Exception as e:
                    logger.error(f"Error downloading {file_key}: {e}")
            if num_files > 7:
                dates_with_more_than_seven_files.append(date_str)
        current_date += timedelta(days=1)
    if dates_with_more_than_seven_files:
        with open('greaterthansevenfiles.txt', 'w') as f:
            for date in dates_with_more_than_seven_files:
                f.write(f"datadate={date}\n")
        logger.warning(f"Dates with more than 7 files written to greaterthansevenfiles.txt")

    logger.info("Script finished successfully.")

if __name__ == "__main__":
    bucket_name = '<source-bucket-name>'
    prefix = '<bucket-prefix>'
    start_date = '2019-08-30'
    end_date = '2019-08-30'
    local_directory = 'C:\\<folder-name>\\<sub-folder>'

    download_s3_files_in_date_range(bucket_name, prefix, start_date, end_date, local_directory)