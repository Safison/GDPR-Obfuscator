import boto3
from utils import (
    parse_input_json,
    read_csv_from_s3,
    obfuscate_pii,
    write_csv_obfuscated_file_to_s3,
    write_parquet_obfuscated_file_to_s3,
    read_parquet_from_s3,
    write_json_obfuscated_file_to_s3,
    read_json_from_s3,
)

s3_client = boto3.client("s3")

def lambda_handler(event, context):
    """This function is triggered by an even bridge, step machine, etc.
    files to obfuscate and pii fields are passed as an event arguments
    It reads the CSV file, obfuscates sensitive data,
    and saves the obfuscated file back to the S3 bucket."""
    bucket_name, file_key, pii_fields = parse_input_json(event)
    if file_key.endswith(".csv"):
        df_csv = read_csv_from_s3(bucket_name, file_key, s3_client)
        df_obfuscate = obfuscate_pii(df_csv, pii_fields)
        obfus_file_key = write_csv_obfuscated_file_to_s3(bucket_name, file_key, df_obfuscate,s3_client)
        return {
        "statusCode": 200,
        "body": f"{file_key} Obfuscated version written to {bucket_name}/{obfus_file_key} successfully.",
        }
        
    elif file_key.endswith(".parquet"):
        df_parquet = read_parquet_from_s3(bucket_name, file_key,s3_client)
        df_obfuscate = obfuscate_pii(df_parquet, pii_fields)
        obfus_file_key = write_parquet_obfuscated_file_to_s3(bucket_name, file_key, df_obfuscate,s3_client)
        return {
        "statusCode": 200,
        "body": f"{file_key} Obfuscated version written to {bucket_name}/{obfus_file_key} successfully.",
        }
    elif file_key.endswith(".json"):
        df_json = read_json_from_s3(bucket_name, file_key,s3_client)
        obfuscated_df = obfuscate_pii(df_json, pii_fields)
        obfus_file_key = write_json_obfuscated_file_to_s3(bucket_name, file_key, obfuscated_df,s3_client)
        #print(f"{file_key} Obfuscated version written to {bucket_name}{obfus_file_key} successfully.")
        return {
        "statusCode": 200,
        "body": f"{file_key} Obfuscated version written to {bucket_name}/{obfus_file_key} successfully.",
        }
###################################
# Local Testing Only
# if __name__ == "__main__":
#     lambda_handler({
#          "file_to_obfuscate": "s3://ans-gdpr-bucket/students.json",
#          "pii_fields": ["student_id", "email_address"]
#      }, None)