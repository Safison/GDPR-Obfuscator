import boto3
import csv
import io

BUCKET_NAME = 'ans-gdpr-bucket'


def read_csv_from_s3(bucket_name, file_key):
    """Reads a CSV file from an S3 bucket and returns its content as a csv file"""
    s3 = boto3.client('s3')
    bucket_name = bucket_name
    file_key = file_key
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    csv_data = obj['Body'].read().decode('utf-8')
    csv_reader = csv.reader(io.StringIO(csv_data))
    # for row in csv_reader:
    #     print(row)
    # dict_reader = csv.DictReader(io.StringIO(csv_data))
    # for row in dict_reader:
    #     print(row) 
    return csv_reader
    

