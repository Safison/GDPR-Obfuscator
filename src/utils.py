import boto3
import csv
import io
import pandas as pd


#BUCKET_NAME = 'ans-gdpr-bucket'

def parse_input_json(input_json):
    """Parses the input JSON to extract bucket name, file key, and PII fields"""
    file_to_obfuscate = input_json['file_to_obfuscate']
    
    bucket_name = file_to_obfuscate[5:file_to_obfuscate.index('/', 5)]
    file_key = file_to_obfuscate[file_to_obfuscate.index('/', 5)+1:]
    pii_fields = input_json['pii_fields']
    
    return bucket_name, file_key, pii_fields

def read_csv_from_s3(bucket_name, file_key):
    """Reads a CSV file from an S3 bucket and returns its content as a csv file"""
    s3 = boto3.client('s3') 
    bucket_name = bucket_name
    file_key = file_key
    obj = s3.get_object(Bucket=bucket_name, Key=file_key) 
    csv_data = obj['Body'].read().decode('utf-8') 
    csv_file = io.StringIO(csv_data) #converts the CSV data into a csv file-like object
        
    return csv_file
    

def read_csv_columns(csv_file,pii_fields):
    """Reads specific columns from a CSV file using pandas dateframe"""
    df = pd.read_csv(csv_file, usecols=pii_fields) #reads only the specified columns from the CSV file into a pandas dataframe
    print(df.head())
    
    


##################
if __name__ == "__main__":
    bucket_name, file_key,pii_fields = parse_input_json({
        "file_to_obfuscate": "s3://ans-gdpr-bucket/students.csv",
        "pii_fields": ["name", "email_address"]
    })
    csv_file = read_csv_from_s3(bucket_name, file_key)
    read_csv_columns(csv_file, pii_fields)