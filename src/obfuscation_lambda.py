from utils import (
    parse_input_json,
    read_csv_from_s3,
    obfuscate_pii,
    write_csv_obfuscated_file_to_s3,
    write_parquet_obfuscated_file_to_s3,
    read_parquet_from_s3,
)


def lambda_handler(event, context):
    """This function is triggered by an even bridge, step machine, etc.
    files to obfuscate and pii fields are passed as an event arguments
    It reads the CSV file, obfuscates sensitive data,
    and saves the obfuscated file back to the S3 bucket."""
    bucket_name, file_key, pii_fields = parse_input_json(event)
    if file_key.endswith(".csv"):
        df_csv = read_csv_from_s3(bucket_name, file_key)
        df_obfuscate = obfuscate_pii(df_csv, pii_fields)
        write_csv_obfuscated_file_to_s3(bucket_name, file_key, df_obfuscate)
        return {
        "statusCode": 200,
        "body": f"{file_key} Obfuscation completed successfully.",
        }
        
    elif file_key.endswith(".parquet"):
        df_parquet = read_parquet_from_s3(bucket_name, file_key)
        df_obfuscate = obfuscate_pii(df_parquet, pii_fields)
        write_parquet_obfuscated_file_to_s3(bucket_name, file_key, df_obfuscate)
        return {
        "statusCode": 200,
        "body": "Parquet Obfuscation completed successfully.",
        }

###################################
# Local Testing
if __name__ == "__main__":
    lambda_handler({
         "file_to_obfuscate": "s3://ans-gdpr-bucket/students.csv",
         "pii_fields": ["student_id", "email_address"]
     }, None)