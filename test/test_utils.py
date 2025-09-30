import pytest
import pandas as pd
from src.utils import (
    parse_input_json,
    read_csv_from_s3,
    obfuscate_pii,
    write_obfuscated_file_to_s3,
)
import moto
import boto3
from moto import mock_aws
import io
from io import StringIO


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
    assert bucket_name == ""
    assert file_key == ""
    assert pii_fields == []


def test_parse_input_json_no_pii_fields():
    input_json = {"file_to_obfuscate": "s3://ans-gdpr-bucket/students.csv"}
    bucket_name, file_key, pii_fields = parse_input_json(input_json)
    assert bucket_name == "ans-gdpr-bucket"
    assert file_key == "students.csv"
    assert pii_fields == []


def test_parse_input_json_invalid_path():
    input_json = {
        "file_to_obfuscate": "http://ans-gdpr-bucket/students.csv",
        "pii_fields": ["name", "email_address"],
    }
    bucket_name, file_key, pii_fields = parse_input_json(input_json)
    assert bucket_name == ""
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
    df = read_csv_from_s3(bucket_name, file_key)
    assert not df.empty
    assert list(df.columns) == ["name", "email_address"]


def test_read_csv_from_s3_no_file():
    bucket_name = "nonexistent-bucket"
    file_key = "nonexistent-file.csv"
    with pytest.raises(Exception):
        read_csv_from_s3(bucket_name, file_key)


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
    assert obfuscated_df.equals(df)


@mock_aws
def test_write_obfuscated_file_to_s3(s3_client):
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
    write_obfuscated_file_to_s3(bucket, file_key, df)
    response = s3_client.get_object(Bucket=bucket, Key="test_obfuscated.csv")
    content = response["Body"].read().decode("utf-8")
    expected_content = (
        "name,email_address,age,cohort\r\n"
        "Anas,anas@example.com,22,2023"
        "\r\nBob,bob@example.com,21,2024\r\n"
    )
    assert content == expected_content


def test_write_obfuscated_file_to_s3_no_bucket():
    bucket_name = "nonexistent-bucket"
    file_key = "test.csv"
    data = {
        "name": ["Anas", "Bob"],
        "email_address": ["anas@example.com", "bob@example.com"],
        "age": [22, 21],
        "cohort": [2023, 2024],
    }
    df = pd.DataFrame(data)
    with pytest.raises(Exception):
        write_obfuscated_file_to_s3(bucket_name, file_key, df)
