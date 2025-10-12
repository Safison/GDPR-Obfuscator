import boto3
import pandas as pd
import base64
from utils import (
    parse_input_json,
    read_csv_from_s3,
    obfuscate_pii,
    write_csv_obfuscated_file_to_s3,
    write_parquet_obfuscated_file_to_s3,
    read_parquet_from_s3,
    write_json_obfuscated_file_to_s3,
    read_json_from_s3,
    csv_bytestream_for_boto3_put,
    parquet_bytestream_for_boto3_put,
    json_bytestream_for_boto3_put,
)

s3_client = boto3.client("s3")


def lambda_handler(event, context, s3_client=None):
    """This function is triggered by an event bridge, step machine, etc.
    the function obfuscates sensitive data in files stored in S3 bucket.
    file to obfuscate is passed as s3 uri,
    along with the fields to be obfuscated
    as a json string.e.g:
    {
         "file_to_obfuscate": "s3://my_bucket/file_key.csv",
         "pii_fields": ["field_1", "field_2"]
    }

    It reads the file from s3 bucket, check its type by checking its extension,
    obfuscates sensitive data in the specified fields in the file,
    saves the obfuscated file back to S3 bucket, and returns the
    S3 URI of the obfuscated file. Supported file types are
    CSV, Parquet, and JSON."""
    """For GDPR compliance:
    - The obfuscation tool performs irreversible anonymization.
    - No lookup tables or re-identification keys are retained.
    - Logs and debug output never capture original data."""
    try:
        s3_client = s3_client or boto3.client("s3")
        bucket_name, file_key, pii_fields = parse_input_json(event)
        if bucket_name == "Input JSON is empty.":
            return {
                "statusCode": 400,
                "body": "Input JSON is empty.",
            }
        if bucket_name == "bucket or file key is missing":
            return {
                "statusCode": 400,
                "body": "No bucket name or file key provided",
            }
        if bucket_name == "Invalid S3 URI format":
            return {
                "statusCode": 400,
                "body":
                    ("Invalid S3 URI format. "
                        "Expected format: s3://bucket_name/file_key"),
            }
        if file_key == "Unsupported file type":
            return {
                "statusCode": 400,
                "body":
                    ("Unsupported file type. Only CSV,"
                        "Parquet, and JSON files are supported."),
            }
        if pii_fields == ["No pii fields provided"]:
            return {
                "statusCode": 400,
                "body": "No PII fields provided for obfuscation.",
            }
        # CSV file obfuscation
        if file_key.endswith(".csv"):
            df_csv = read_csv_from_s3(bucket_name, file_key, s3_client)
            if (not isinstance(df_csv, (pd.DataFrame)) and
                    df_csv.startswith("Error")):
                return {
                    "statusCode": 400,
                    "body":
                        ("Error, no such file, specified key does not exist"),
                }

            df_obfuscate = obfuscate_pii(df_csv, pii_fields)
            csv_bytes = csv_bytestream_for_boto3_put(df_obfuscate)
            print(csv_bytes)
            obfus_file_key = write_csv_obfuscated_file_to_s3(
                    bucket_name,
                    file_key,
                    df_obfuscate,
                    s3_client)
            if obfus_file_key.startswith("Error"):
                return {
                    "statusCode": 400,
                    "body": "Error writing obfuscated file to S3",
                }
            return {
                "statusCode": 200,
                # "body": f"s3://{bucket_name}/{obfus_file_key}",
                "body": csv_bytes
            }
        # Parquet file obfuscation
        elif file_key.endswith(".parquet"):
            df_parquet = read_parquet_from_s3(bucket_name, file_key, s3_client)
            if (not isinstance(df_parquet, (pd.DataFrame)) and
                    df_parquet.startswith("Error")):
                return {
                    "statusCode": 400,
                    "body":
                        ("Error, no such file, specified key does not exist"),
                }

            df_obfuscate = obfuscate_pii(df_parquet, pii_fields)
            parq_bytes = parquet_bytestream_for_boto3_put(df_obfuscate)

            obfus_file_key = write_parquet_obfuscated_file_to_s3(
                    bucket_name,
                    file_key,
                    df_obfuscate,
                    s3_client)
            if obfus_file_key.startswith("Error"):
                return {
                    "statusCode": 400,
                    "body": "Error writing obfuscated file to S3",
                }

            return {
                "statusCode": 200,
                # "body": f"s3://{bucket_name}/{obfus_file_key}",
                "body": base64.b64encode(parq_bytes).decode("utf-8")
            }
        # JSON file obfuscation
        elif file_key.endswith(".json"):
            df_json = read_json_from_s3(bucket_name, file_key, s3_client)
            if (
                    not isinstance(df_json, (pd.DataFrame)) and
                    df_json.startswith("Error")):
                return {
                    "statusCode": 400,
                    "body":
                    ("Error, no such file, specified key does not exist"),
                }

            obfuscated_df = obfuscate_pii(df_json, pii_fields)
            json_bytes = json_bytestream_for_boto3_put(obfuscated_df)
            obfus_file_key = write_json_obfuscated_file_to_s3(
                    bucket_name,
                    file_key,
                    obfuscated_df,
                    s3_client)
            if obfus_file_key.startswith("Error"):
                return {
                    "statusCode": 400,
                    "body": "Error writing obfuscated file to S3",
                }

            return {
                "statusCode": 200,
                "body": json_bytes,
                    }
    except Exception as e:
        print(f"Internal server error: {e}")
        return {
            "statusCode": 500,
            "body": f"Internal server error: {e}",
        }

###################################
# Local Testing Only
if __name__ == "__main__":
    response = lambda_handler({
         "file_to_obfuscate": "s3://ans-gdpr-bucket/students.parquet",
         "pii_fields": ["name", "email_address"]
     }, None)
    print(response)
###################################