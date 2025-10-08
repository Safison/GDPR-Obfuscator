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
    except Exception as e:
        print(f"Error parsing input JSON: {e}")
        return "", "", []


def obfuscate_pii(df, pii_fields):
    """Obfuscates PII fields in the dataframe"""
    if not pii_fields:
        return "no pii fields provided"
    if not isinstance(df, pd.DataFrame):
        return "no data frame provided"
    try:
        for field in pii_fields:
            df[field] = df[field].apply(lambda x: "***" if pd.notnull(x) else x)
        return df
    except Exception as e:
        print(f"Error obfuscating PII fields: {e}")
        return df


###############
#csv file processing
###############
def read_csv_from_s3(bucket_name, file_key,s3):
    """Reads a CSV file from an S3 bucket and returns
    its content as a pandas DataFrame"""
    #s3 = boto3.client("s3")
    bucket_name = bucket_name
    file_key = file_key
    if bucket_name == "" or file_key == "":
        return "no bucket name or file key provided"  
    elif not file_key.endswith(".csv"):
        return "file is not a csv file"  
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        csv_data = obj["Body"].read().decode("utf-8")
        csv_io = StringIO(csv_data)
        df_csv = pd.read_csv(csv_io)  
        return df_csv
    except Exception as e:
        print(f"Error reading CSV from S3: {e}")
        raise e
    
 
def write_csv_obfuscated_file_to_s3(bucket_name, file_key, df,s3):
    """Writes the obfuscated dataframe back to an S3 bucket as a CSV file"""
    if bucket_name == "":
        return ("No bucket name provided")
    if file_key == "":
        return ("No file key provided")    

    if not isinstance(df, pd.DataFrame):
        return ("No valid dataframe provided")
        
    if not file_key.endswith(".csv"):
        return ("File key must end with .csv")
    try:
        timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
        obfuscated_file_key = file_key.replace(".csv", "_obfuscated.csv")
        file_key = f"csv_files/{timestamp}_{obfuscated_file_key}"
        #s3 = boto3.client("s3")
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())
        #print(f"CSV Obfuscated file written to s3://{bucket_name}/{file_key}")
        return file_key
    except Exception as e:
        print(f"Error writing obfuscated file to S3: {e}")
        raise e

#######################
#parquet file processing
#######################
#read parquet file from s3

def read_parquet_from_s3(bucket_name, file_key,s3):
        """Reads a Parquet file from an S3 bucket and returns
        its content as a pandas DataFrame"""
        #s3 = boto3.client("s3")
        bucket_name = bucket_name
        file_key = file_key
        if bucket_name == "" or file_key == "":
            return "no bucket n]ame or file key provided"  
        elif not file_key.endswith(".parquet"):
            return "file is not a parquet file"
        try:
            obj = s3.get_object(Bucket=bucket_name, Key=file_key)
            parquet_data = obj['Body'].read()
            df_parquet = pd.read_parquet(io.BytesIO(parquet_data))
            return df_parquet
        except Exception as e:
            print(f"Error reading Parquet from S3: {e}")
            raise e


def write_parquet_obfuscated_file_to_s3(bucket_name, file_key, df,s3):
    """Writes the obfuscated dataframe back to an S3 bucket as a parquet file"""
    if bucket_name == "":
        return ("No bucket name provided")
    if file_key == "":
        return ("No file key provided")    

    if not isinstance(df, pd.DataFrame):
        return ("No valid dataframe provided")
        
    if not file_key.endswith(".parquet"):
        return ("File key must end with .parquet")
    try:
        obfuscated_file_key = file_key.replace(".parquet", "_obfuscated.parquet")
        file_key = obfuscated_file_key
        #s3 = boto3.client("s3")
        parq_buffer = io.BytesIO()
        df.to_parquet(parq_buffer, engine="pyarrow", index=False)
        parq_buffer.seek(0)
        time_stamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
        parq_file_key = f"parq_files/{time_stamp}_{file_key}"
        s3.put_object(Bucket=bucket_name, Key= parq_file_key, Body=parq_buffer.getvalue())
        #print(f"Obfuscated file written to s3://{bucket_name}/{parq_file_key}")
        return parq_file_key
    except Exception as e:
        print(f"Error writing obfuscated file to S3: {e}")
        raise e


######################
# json file processing
######################
def read_json_from_s3(bucket_name, file_key,s3):
    """Reads a JSON file from an S3 bucket and returns
    its content as a pandas DataFrame"""
    #s3 = boto3.client("s3")
    bucket_name = bucket_name
    file_key = file_key
    if bucket_name == "" or file_key == "":
        return "no bucket name or file key provided"  
    elif not file_key.endswith(".json"):
        return "file is not a json file"  
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        json_data = obj["Body"].read().decode("utf-8")
        df_json = pd.read_json(StringIO(json_data))  
        return df_json
    except Exception as e:
        print(f"Error reading JSON from S3: {e}")
        raise e
    
def write_json_obfuscated_file_to_s3(bucket_name, file_key, df,s3):
    """Writes the obfuscated dataframe back to an S3 bucket as a JSON file"""
    if bucket_name == "":
        return ("No bucket name provided")
    if file_key == "":
        return ("No file key provided")    

    if not isinstance(df, pd.DataFrame):
        return ("No valid dataframe provided")
        
    if not file_key.endswith(".json"):
        return ("File key must end with .json")
    try:
        timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
        obfuscated_file_key = file_key.replace(".json", "_obfuscated.json")
        file_key = f"json_files/{timestamp}_{obfuscated_file_key}"
        #s3 = boto3.client("s3")
        json_buffer = StringIO()
        df.to_json(json_buffer, orient="records", lines=True)
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=json_buffer.getvalue())
        #print(f"JSON Obfuscated file written to s3://{bucket_name}/{file_key}")
        return file_key
    except Exception as e:
        print(f"Error writing obfuscated file to S3: {e}")
        raise e


######################
# Function developed For testign purposes only
######################
def convert_csv_to_parquet():
    """For testing puposes, the function converts a CSV file to a Parquet file"""
    try:
        df = pd.read_csv("students.csv")
        df.to_parquet("students.parquet", index=False)
        print(f"CSV file converted to Parquet and saved")
    except Exception as e:
        print(f"Error converting CSV to Parquet: {e}")
        raise e

#########################################
# Local Testing Only
# if __name__ == "__main__":
#     s3 = boto3.client("s3")
#     bucket_name, file_key,pii_fields = parse_input_json({
#          "file_to_obfuscate": "s3://ans-gdpr-bucket/students.json",
#          "pii_fields": ["name", "email_address"]
#       })
#     if file_key.endswith(".csv"):
#         df_csv = read_csv_from_s3(bucket_name, file_key,s3)
#         obfuscated_df = obfuscate_pii(df_csv, pii_fields)
#         write_csv_obfuscated_file_to_s3(bucket_name, file_key, obfuscated_df,s3)
#     elif file_key.endswith(".parquet"):
#         df_parquet = read_parquet_from_s3(bucket_name, file_key,s3)
#         obfuscated_df = obfuscate_pii(df_parquet, pii_fields)
#         write_parquet_obfuscated_file_to_s3(bucket_name, file_key, obfuscated_df,s3)

#     elif file_key.endswith(".json"):
#         df_json = read_json_from_s3(bucket_name, file_key,s3)
#         obfuscated_df = obfuscate_pii(df_json, pii_fields)
#         write_json_obfuscated_file_to_s3(bucket_name, file_key, obfuscated_df,s3)
##############################################
     
