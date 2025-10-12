import boto3
import csv
import io
from io import StringIO
import pandas as pd


def parse_input_json(input_json):
    """Parses the input JSON to extract bucket name,
    file key, and PII fields"""
    try:
        if input_json == {}:
            return "Input JSON is empty.", "", []
        else:
            file_to_obfuscate = input_json["file_to_obfuscate"]
            if file_to_obfuscate is None or file_to_obfuscate == "":
                return "bucket or file key is missing", "", []
            if (file_to_obfuscate and
                    not file_to_obfuscate.startswith("s3://") or
                    file_to_obfuscate.count("/") < 2):
                return "Invalid S3 URI format", "", []
            if (
                not (file_to_obfuscate.endswith(".csv") or
                     file_to_obfuscate.endswith(".parquet") or
                     file_to_obfuscate.endswith(".json"))):
                return "", "Unsupported file type", []
            else:
                bucket_name = file_to_obfuscate[
                                5: file_to_obfuscate.index("/", 5)]
                file_key = file_to_obfuscate[
                            file_to_obfuscate.index("/", 5) + 1:]
                if input_json.get("pii_fields") is None:
                    pii_fields = ["No pii fields provided"]
                    return bucket_name, file_key, pii_fields
                elif (
                    not isinstance(input_json["pii_fields"], list) or
                        len(input_json["pii_fields"]) <= 1):
                    pii_fields = ["No pii fields provided"]
                    return bucket_name, file_key, pii_fields

                else:
                    pii_fields = input_json["pii_fields"]
                return bucket_name, file_key, pii_fields
    except Exception as e:
        return "", "", []


def obfuscate_pii(df, pii_fields):
    """Obfuscates PII fields in the dataframe"""
    if not pii_fields:
        return "no pii fields provided"
    if not isinstance(df, pd.DataFrame):
        return "no data frame provided"
    try:
        for field in pii_fields:
            df[field] = df[field].apply(
                        lambda x: "***" if pd.notnull(x) else x)
        return df
    except Exception as e:
        print(f"Error obfuscating PII fields: {e}")
        return df


###############
# csv file processing
###############
def read_csv_from_s3(bucket_name, file_key, s3):
    """Reads a CSV file from an S3 bucket and returns
    its content as a pandas DataFrame"""
    # bucket_name = bucket_name
    # file_key = file_key
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        csv_data = obj["Body"].read().decode("utf-8")
        csv_io = StringIO(csv_data)
        df_csv = pd.read_csv(csv_io)
        return df_csv
    except Exception as e:
        return (f"Error reading CSV from S3: {e}")


def check_s3_file_df_valid(bucket_name, file_key, df):
    """Checks if a bucket name, file key, and df are valid """
    if file_key is None or file_key == "":
        return "No file key provided"
    if not isinstance(df, pd.DataFrame):
        return "No valid dataframe provided"
    if bucket_name is None or bucket_name == "":
        return "No bucket name provided"
    return "pass"


def write_csv_obfuscated_file_to_s3(bucket_name, file_key, df, s3):
    """ Writes the obfuscated dataframe back to an
        S3 bucket as a CSV file and returns file key"""
    try:
        check_params = check_s3_file_df_valid(bucket_name, file_key, df)
        if file_key and not file_key.endswith(".csv"):
            return "File key must have a .csv extension"
        if check_params == "pass":
            timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
            obfuscated_file_key = file_key.replace(".csv", "_obfuscated.csv")
            file_key = f"csv_files/{timestamp}_{obfuscated_file_key}"
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            s3.put_object(
                Bucket=bucket_name,
                Key=file_key,
                Body=csv_buffer.getvalue())
            return file_key
        else:
            return check_params
    except Exception as e:
        return (f"Error writing obfuscated file to S3:{e}")

#######################
# parquet file processing
#######################
# read parquet file from s3


def read_parquet_from_s3(bucket_name, file_key, s3):
        """ Reads a Parquet file from an S3 bucket and returns
            its content as a pandas DataFrame"""
        try:
            obj = s3.get_object(Bucket=bucket_name, Key=file_key)
            parquet_data = obj['Body'].read()
            df_parquet = pd.read_parquet(io.BytesIO(parquet_data))
            return df_parquet
        except Exception as e:
            return (f"Error reading parquet from S3: {e}")


def write_parquet_obfuscated_file_to_s3(bucket_name, file_key, df, s3):
    """Writes the obfuscated dataframe back to an S3 bucket as a parquet file
        and returns file key"""
    try:
        check_params = check_s3_file_df_valid(bucket_name, file_key, df)
        if file_key and not file_key.endswith(".parquet"):
            return "File key must have a .parquet extension"
        if check_params == "pass":
            obfuscated_file_key = file_key.replace(
                                    ".parquet",
                                    "_obfuscated.parquet")
            file_key = obfuscated_file_key
            parq_buffer = io.BytesIO()
            df.to_parquet(parq_buffer, engine="pyarrow", index=False)
            parq_buffer.seek(0)
            time_stamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
            parq_file_key = f"parq_files/{time_stamp}_{file_key}"
            s3.put_object(
                Bucket=bucket_name,
                Key=parq_file_key,
                Body=parq_buffer.getvalue())
            return parq_file_key
    except Exception as e:
        return (f"Error writing obfuscated parquet file to s3: {e}")


######################
# json file processing
######################

def read_json_from_s3(bucket_name, file_key, s3):
    """Reads a JSON file from an S3 bucket and returns
    its content as a pandas DataFrame"""
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        json_data = obj["Body"].read().decode("utf-8")
        df_json = pd.read_json(StringIO(json_data))
        return df_json
    except Exception as e:
        return (f"Error reading json from S3: {e}")


def write_json_obfuscated_file_to_s3(bucket_name, file_key, df, s3):
    """Writes the obfuscated dataframe back to an S3 bucket as a JSON file
    and returns file key"""
    try:
        check_params = check_s3_file_df_valid(bucket_name, file_key, df)
        if file_key and not file_key.endswith(".json"):
            return "File key must have a .parquet extension"
        if check_params == "pass":
            timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
            obfuscated_file_key = file_key.replace(".json", "_obfuscated.json")
            file_key = f"json_files/{timestamp}_{obfuscated_file_key}"
            json_buffer = StringIO()
            df.to_json(json_buffer, orient="records", lines=True)
            s3.put_object(
                Bucket=bucket_name,
                Key=file_key,
                Body=json_buffer.getvalue())
            return file_key
    except Exception as e:
        return (f"Error writing obfuscated file to S3: {e}")


######################
# Function developed For testign purposes only
def convert_csv_to_parquet():
    """For testing puposes, the function
    converts a CSV file to a Parquet file"""
    try:
        df = pd.read_csv("students.csv")
        df.to_parquet("students.parquet", index=False)
        print(f"CSV file converted to Parquet and saved")
    except Exception as e:
        print(f"Error converting CSV to Parquet: {e}")
        raise e
