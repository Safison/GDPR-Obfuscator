from utils import (
    parse_input_json,
    read_csv_from_s3,
    obfuscate_pii,
    write_obfuscated_file_to_s3,
)


def lambda_handler(event, context):
    """This function is triggered by an even bridge, step machine, etc.
    files to obfuscate and pii fields are passed as an event arguments
    It reads the CSV file, obfuscates sensitive data,
    and saves the obfuscated file back to the S3 bucket."""
    bucket_name, file_key, pii_fields = parse_input_json(event)
    df_csv = read_csv_from_s3(bucket_name, file_key)
    obfuscate_pii(df_csv, pii_fields)
    write_obfuscated_file_to_s3(bucket_name, file_key, df_csv)

    return {
        "statusCode": 200,
        "body": "Obfuscation completed successfully.",
    }

