import pytest
import pandas as pd
from src.utils import (
    parse_input_json,
    read_csv_from_s3,
    obfuscate_pii,
    write_csv_obfuscated_file_to_s3,
    read_parquet_from_s3,
    write_parquet_obfuscated_file_to_s3,
    read_json_from_s3,
    write_json_obfuscated_file_to_s3,
)
import moto
import boto3
from moto import mock_aws
import io
from io import StringIO
import csv
import json

s3_client = boto3.client("s3", region_name="us-east-1")
@pytest.fixture
def s3_client():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        yield s3


def test_parse_input_json():
    input_json = {
        "file_to_obfuscate": "s3://ans-gdpr-bucket/students.csv",
        "pii_fields": ["name", "email_address"],
    }
    bucket_name, file_key, pii_fields = parse_input_json(input_json)
    assert bucket_name == "ans-gdpr-bucket"
    assert file_key == "students.csv"
    assert pii_fields == ["name", "email_address"]


def test_parse_input_json_different_path():
    input_json = {
        "file_to_obfuscate": "s3://my-bucket/data/files/info.csv",
        "pii_fields": ["id", "phone_number"],
    }
    bucket_name, file_key, pii_fields = parse_input_json(input_json)
    assert bucket_name == "my-bucket"
    assert file_key == "data/files/info.csv"
    assert pii_fields == ["id", "phone_number"]


def test_parse_input_json_empty():
    input_json = {}
    bucket_name, file_key, pii_fields = parse_input_json(input_json)
    assert bucket_name == "Input JSON is empty."
    assert file_key == ""
    assert pii_fields == []


def test_parse_input_json_no_pii_fields():
    input_json = {"file_to_obfuscate": "s3://ans-gdpr-bucket/students.csv"}
    bucket_name, file_key, pii_fields = parse_input_json(input_json)
    assert bucket_name == "ans-gdpr-bucket"
    assert file_key == "students.csv"
    assert pii_fields == ['No pii fields provided']


def test_parse_input_json_invalid_path():
    input_json = {
        "file_to_obfuscate": "http://ans-gdpr-bucket/students.csv",
        "pii_fields": ["name", "email_address"],
    }
    bucket_name, file_key, pii_fields = parse_input_json(input_json)
    assert bucket_name == "Invalid S3 URI format"
    assert file_key == ""
    assert pii_fields == []


@mock_aws
def test_read_csv_from_s3(s3_client):
    # Create a mock S3 bucket and upload a test CSV file
    bucket_name = "test-bucket"
    file_key = "test.csv"
    csv_content = "name,email_address\nAnas,anas@example.com\n" "Bob,bob@example.com"
    s3_client.create_bucket(Bucket=bucket_name)
    s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_content)
    df = read_csv_from_s3(bucket_name, file_key,s3_client)
    assert not df.empty
    assert list(df.columns) == ["name", "email_address"]
    assert df["name"].tolist() == ["Anas", "Bob"]
    assert df["email_address"].tolist() == ["anas@example.com", "bob@example.com"]


def test_read_csv_from_s3_no_file(s3_client):
    bucket_name = "nonexistent-bucket"
    file_key = "nonexistent-file.csv"
    df_csv = read_csv_from_s3(bucket_name,file_key,s3_client)
    assert df_csv.startswith("Error reading CSV from S3:")  
    

def test_obfuscate_pii():
    data = {
        "name": ["Anas", "Bob"],
        "email_address": ["anas@example.com", "bob@example.com"],
        "age": [22, 21],
        "cohort": [2023, 2024],
    }
    df = pd.DataFrame(data)
    pii_fields = ["name", "email_address"]
    obfuscated_df = obfuscate_pii(df, pii_fields)
    assert obfuscated_df["name"].tolist() == ["***", "***"]
    assert obfuscated_df["email_address"].tolist() == ["***", "***"]
    assert obfuscated_df["age"].tolist() == [22, 21]
    assert obfuscated_df["cohort"].tolist() == [2023, 2024]


def test_obfuscate_pii_no_pii_fields():
    data = {
        "name": ["Anas", "Bob"],
        "email_address": ["anas@example.com", "bob@example.com"],
        "age": [22, 21],
        "cohort": [2023, 2024],
    }
    df = pd.DataFrame(data)
    pii_fields = []
    obfuscated_df = obfuscate_pii(df, pii_fields)
    assert obfuscated_df == "no pii fields provided"


def test_obfuscate_no_df():
    df = "no df"
    pii_fields = ["name", "email_address"]
    obfuscated_df = obfuscate_pii(df, pii_fields)
    assert obfuscated_df == "no data frame provided"


@mock_aws
def test_write_csv_obfuscated_file_to_s3(s3_client):
    # Create a mock S3 bucket
    bucket = "test-bucket"
    file_key = "test.csv"
    s3_client.create_bucket(Bucket=bucket)
    data = {
        "name": ["Anas", "Bob"],
        "email_address": ["anas@example.com", "bob@example.com"],
        "age": [22, 21],
        "cohort": [2023, 2024],
    }
    df = pd.DataFrame(data)
    df_obfuscate = obfuscate_pii(df, ["name", "email_address"])
    csv_file_key = write_csv_obfuscated_file_to_s3(bucket, file_key, df_obfuscate,s3_client)
    response = s3_client.get_object(Bucket=bucket, Key=csv_file_key)
    content = response["Body"].read().decode("utf-8")
    expected_content = (
        "name,email_address,age,cohort\r\n"
        "***,***,22,2023"
        "\r\n***,***,21,2024\r\n"
    )
    assert content.replace('\r\n', '\n') == expected_content.replace('\r\n', '\n')


def test_write_csv_obfuscated_file_to_s3_invalid_bucket():
    bucket_name = "nonexistent-bucket"
    file_key = "test.csv"
    data = {
        "name": ["Anas", "Bob"],
        "email_address": ["anas@example.com", "bob@example.com"],
        "age": [22, 21],
        "cohort": [2023, 2024],
    }
    df = pd.DataFrame(data)
    result = write_csv_obfuscated_file_to_s3(bucket_name, file_key, df,s3_client)
    assert result.startswith("Error writing obfuscated file to S3:")
    # with pytest.raises(Exception):
    #     write_csv_obfuscated_file_to_s3(bucket_name, file_key, df,s3_client)
    

def test_write_csv_obfuscated_file_no_file_key():
    bucket_name = "test-bucket"
    file_key = ""
    data = {
        "name": ["Anas", "Bob"],
        "email_address": ["anas@example.com", "bob@example.com"],
        "age": [22, 21],
        "cohort": [2023, 2024],
    }   
    df = pd.DataFrame(data)
    assert write_csv_obfuscated_file_to_s3(bucket_name, file_key, df,s3_client) == ("No file key provided")


def test_write_csv_obfuscated_file_to_s3_no_df():
    bucket_name = "test-bucket"
    file_key = "test.csv"
    df = "no df"
    assert write_csv_obfuscated_file_to_s3(bucket_name, file_key, df,s3_client) == ("No valid dataframe provided")


def test_write_csv_obfuscated_file_to_s3_no_csv_extension():
    bucket_name = "test-bucket"
    file_key = "test.txt"
    data = {
        "name": ["Anas", "Bob"],
        "email_address": ["anas.example.com", "bob.example.com"],
        "age": [22, 21],
        "cohort": [2023, 2024],
    }
    df = pd.DataFrame(data)
    assert write_csv_obfuscated_file_to_s3(bucket_name, file_key, df,s3_client) == ("File key must have a .csv extension")


def test_write_csv_obfuscated_file_to_s3_no_bucket():
    bucket_name = ""
    file_key = "test.csv"
    data = {
        "name": ["Anas", "Bob"],
        "email_address": ["anas@example.com", "bob@example.com"],
        "age": [22, 21],
        "cohort": [2023, 2024],
    }
    df = pd.DataFrame(data)
    assert write_csv_obfuscated_file_to_s3(bucket_name, file_key, df,s3_client) == ("No bucket name provided")


@mock_aws
def test_read_parquet_from_s3(s3_client):
    # Create a mock S3 bucket and upload a test parquet file
    bucket_name = "test-bucket"
    file_key = "students.parquet"
    s3_client.create_bucket(Bucket=bucket_name)
    s3_client.upload_file(file_key, bucket_name, file_key)
    df = read_parquet_from_s3(bucket_name, file_key,s3_client)
    assert not df.empty
    assert list(df.columns) == ["student_id","name","course","cohort","graduation_date","email_address"]
    assert df["name"].tolist() == ["John_Smith"]
    assert df["email_address"].tolist() == ["j.smith@email.com"]


@mock_aws
def test_write_parquet_obfuscated_file_to_s3(s3_client):
    bucket_name = "test-bucket"
    file_key = "students.parquet"
    s3_client.create_bucket(Bucket=bucket_name)
    data = {
        "name": ["Anas", "Bob"],
        "email_address": ["anas@example.com", "bob@example.com"],
        "age": [22, 21],
        "cohort": [2023, 2024],
    }
    
    df = pd.DataFrame(data)
    df_obfuscate = obfuscate_pii(df, ["name", "email_address"])
    parquet_file_key = write_parquet_obfuscated_file_to_s3(bucket_name, file_key, df_obfuscate,s3_client)
    response = s3_client.get_object(Bucket=bucket_name, Key=parquet_file_key)
    df = pd.read_parquet(io.BytesIO(response['Body'].read()))
    assert not df.empty
    assert list(df.columns) == ["name", "email_address", "age", "cohort"]
    assert df["name"].tolist() == ["***", "***"]
    assert df["email_address"].tolist() == ["***", "***"]
   
    
@mock_aws
def test_read_json_from_s3(s3_client):
    # Creates a mock S3 bucket and upload a test parquet file
    bucket_name = "test-bucket"
    file_key = "test.json"
    data = {
        "student_id": [1, 2],
        "name": ["Anas", "Bob"],
        "course": ["Software", "DE"],
        "cohort": [2023, 2024],
        "graduation_date": ["2024-05-15", "2025-06-20"],
        "email_address": ["anas@example.com", "bob@example.com"],
            }
    json_file = json.dumps(data)
    s3_client.create_bucket(Bucket=bucket_name)
    s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=json_file)
    df = read_json_from_s3(bucket_name, file_key,s3_client)
    assert not df.empty
    assert list(df.columns) == ["student_id","name","course","cohort","graduation_date","email_address"]
    assert df["name"].tolist() == ["Anas", "Bob"]
    assert df["email_address"].tolist() == ["anas@example.com", "bob@example.com"] 


@mock_aws
def test_write_json_obfuscated_file_to_s3(s3_client):
    bucket_name = "test-bucket"
    file_key = "test.json"
    s3_client.create_bucket(Bucket=bucket_name)
    data = {
        "name": ["Anas", "Bob"],
        "email_address": ["anas@example.com", "bob@example.com"],
        "age": [22, 21],
        "cohort": [2023, 2024],
    }
    df = pd.DataFrame(data)
    df_obfuscate = obfuscate_pii(df, ["name", "email_address"])
    json_file_key = write_json_obfuscated_file_to_s3(bucket_name, file_key, df_obfuscate,s3_client)
    response = s3_client.get_object(Bucket=bucket_name, Key=json_file_key)  
    content = response["Body"].read().decode("utf-8")
    expected_content = (
        '{"name":"***","email_address":"***","age":22,"cohort":2023}\n'
        '{"name":"***","email_address":"***","age":21,"cohort":2024}\n'
    )
    assert content == expected_content
     

