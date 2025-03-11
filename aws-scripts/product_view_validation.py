import boto3
import pandas as pd


s3_client = boto3.client('s3')


def check_dates_in_destination_s3(source_date_file, bucket_name, key_prefix, output_file):
    df = pd.read_excel(source_date_file, header=None)
    source_file_total_dates = df[0].tolist()
    print("Source file total dates:", len(source_file_total_dates))

    s3_get_objects_list_response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=key_prefix)
    if 'Contents' not in s3_get_objects_list_response:
        print("No objects found under the specified prefix.")
        return

    s3_keys = [obj['Key'] for obj in s3_get_objects_list_response['Contents']]
    print("Total recursive files in destination bucket:", len(s3_keys))

    destination_bucket_total_folders = list(set(key.split('/')[3] for key in s3_keys))
    print("Total date folders in destination bucket:", len(destination_bucket_total_folders))

    missing_dates = []
    for source_date in source_file_total_dates:
        if source_date in destination_bucket_total_folders:
            print(f"Present: {source_date} is present in the destination bucket.")
        else:
            print(f"Missing: {source_date} is NOT present in the destination bucket.")
            missing_dates.append(source_date)

    print("Total Missing Dates:", len(missing_dates))
    if missing_dates:
        pd.DataFrame(missing_dates, columns=["Missing Dates"]).to_excel(output_file, index=False)
        print(f"Missing dates saved to {output_file}.")
    else:
        print("All dates from the source file are present in the destination bucket.")


if __name__ == "__main__":
    source_date_file = "ice_instrument_prices_total_dates.xlsx"
    destination_bucket_name = '<destination-bucket-name>'
    destination_key_prefix = '<destination-path>'
    output_file = "prices_missing_dates.xlsx"

    check_dates_in_destination_s3(source_date_file, destination_bucket_name, destination_key_prefix, output_file)