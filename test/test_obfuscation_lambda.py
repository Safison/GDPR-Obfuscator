import boto3
import pandas as pd
import json
import os
import pytest
from moto import mock_aws
import sys
from io import StringIO

#sys.path.append(os.path.dirname(os.path.abspath("src/utils")))
sys.path.append("src")
from obfuscation_lambda import lambda_handler

@pytest.fixture
def s3_client():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        yield s3


def test_lambda_handler_csv(s3_client):
    #with mock_aws():
    bucket_name = "test-bucket"
    file_key = "test.csv"
    s3_client.create_bucket(Bucket=bucket_name)
    
    csv_data = "name,email_address,age,cohort\nAnas,anas@example.com,22,2023\nBob,bob@example.com,21,2024\n"
    
    s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_data)
    
    input_event = {
        "file_to_obfuscate": f"s3://{bucket_name}/{file_key}",
        "pii_fields": ["name", "email_address"]
    }
    response = lambda_handler(input_event, None, s3_client=s3_client)
    print(response)
    #print(response["body"])
    assert response["statusCode"] == 200
    
    obfus_file_key = response["body"].replace(f"s3://{bucket_name}/", "")
    #print(obfus_file_key)
    obj = s3_client.get_object(Bucket=bucket_name, Key=obfus_file_key)
    csv_data = obj["Body"].read().decode("utf-8")
    csv_io = StringIO(csv_data)
    df_obfuscated = pd.read_csv(csv_io)
    print(df_obfuscated)
    assert all(df_obfuscated["name"] == "***")
    assert all(df_obfuscated["email_address"] == "***")
    assert all(df_obfuscated["age"] == [22, 21])
    assert all(df_obfuscated["cohort"] == [2023, 2024])
    
