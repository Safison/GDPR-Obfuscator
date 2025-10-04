import boto3
import csv
import io
from io import StringIO
import pandas as pd



def parse_input_json(input_json):
    """Parses the input JSON to extract bucket name,
    file key, and PII fields"""
    if input_json == {}:
        return "", "", []
    file_to_obfuscate = input_json["file_to_obfuscate"]
    if not file_to_obfuscate.startswith("s3://") or file_to_obfuscate.count("/") < 2:
        return "", "", []
    else:
        bucket_name = file_to_obfuscate[5 : file_to_obfuscate.index("/", 5)]
        file_key = file_to_obfuscate[file_to_obfuscate.index("/", 5) + 1 :]
        if input_json.get("pii_fields") is None:
            pii_fields = []
        else:
            pii_fields = input_json["pii_fields"]
        return bucket_name, file_key, pii_fields


def read_csv_from_s3(bucket_name, file_key):
    """Reads a CSV file from an S3 bucket and returns
    its content as a csv file"""
    s3 = boto3.client("s3")
    bucket_name = bucket_name
    file_key = file_key
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    csv_data = obj["Body"].read().decode("utf-8")
    csv_io = StringIO(csv_data)
    df_csv = pd.read_csv(csv_io)  # reads the CSV data into a pandas DataFrame
    return df_csv


def obfuscate_pii(df, pii_fields):
    """Obfuscates PII fields in the dataframe"""
    for field in pii_fields:
        df[field] = df[field].apply(lambda x: "***" if pd.notnull(x) else x)
    return df


def write_obfuscated_file_to_s3(bucket_name, file_key, df):
    """Writes the obfuscated dataframe back to an S3 bucket as a CSV file"""
    obfuscated_file_key = file_key.replace(".csv", "_obfuscated.csv")
    file_key = obfuscated_file_key
    s3 = boto3.client("s3")
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())
    print(f"Obfuscated file written to s3://{bucket_name}/{file_key}")


##################

# if __name__ == "__main__":
#     bucket_name, file_key,pii_fields = parse_input_json({
#         "file_to_obfuscate": "s3://ans-gdpr-bucket/students.csv",
#         "pii_fields": ["student_id", "email_address"]
#     })
#     df_csv = read_csv_from_s3(bucket_name, file_key)
#     obfuscate_pii(df_csv, pii_fields)
#    write_obfuscated_file_to_s3(bucket_name, file_key, df_csv)
