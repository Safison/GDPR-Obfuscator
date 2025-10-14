import boto3
import pandas as pd
import json
import os
import pytest
from moto import mock_aws
import sys
from io import StringIO, BytesIO
import base64

sys.path.append("src/")
from obfuscation_lambda import lambda_handler


# Creates Boto3 s3 mock client
@pytest.fixture
def s3_client():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        yield s3


# Tests for csv methods
class TestCSV:
    def test_lambda_handler_csv_returns_bytestream(self, s3_client):
        # Tests lambda handler returns bytestream for obfuscated csv file

        bucket_name = "test-bucket"
        file_key = "test.csv"
        s3_client.create_bucket(Bucket=bucket_name)

        csv_data = ("name,email_address,age,cohort\n"
                    "Anas,anas@example.com,22,2023\n"
                    "Bob,bob@example.com,21,2024\n")
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_data)

        input_event = {
            "file_to_obfuscate": f"s3://{bucket_name}/{file_key}",
            "pii_fields": ["name", "email_address"]
        }
        response = lambda_handler(input_event, None, s3_client=s3_client)
        assert response["statusCode"] == 200
        csv_bytestream = response["body"]
        df_obfuscated = pd.read_csv(BytesIO(csv_bytestream))
        assert isinstance(csv_bytestream, bytes)
        assert all(df_obfuscated["name"] == "***")
        assert all(df_obfuscated["email_address"] == "***")
        assert all(df_obfuscated["age"] == [22, 21])
        assert all(df_obfuscated["cohort"] == [2023, 2024])

    def test_lambda_handler_csv_returns_s3_uri(self, s3_client):
        # Tests lambda handler returns s3 uri for obfuscated csv file

        bucket_name = "test-bucket"
        file_key = "test.csv"
        s3_client.create_bucket(Bucket=bucket_name)

        csv_data = ("name,email_address,age,cohort\n"
                    "Anas,anas@example.com,22,2023\n"
                    "Bob,bob@example.com,21,2024\n")
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_data)

        input_event = {
            "file_to_obfuscate": f"s3://{bucket_name}/{file_key}",
            "pii_fields": ["name", "email_address"]
        }
        response = lambda_handler(input_event, None, s3_client=s3_client)
        assert response["statusCode"] == 200
        obfus_file_key = response["file_key"].replace(
            f"s3://{bucket_name}/", ""
            )
        obj = s3_client.get_object(Bucket=bucket_name, Key=obfus_file_key)
        csv_data = obj["Body"].read().decode("utf-8")
        csv_io = StringIO(csv_data)
        df_obfuscated = pd.read_csv(csv_io)
        assert obfus_file_key.endswith("_obfuscated.csv")
        assert all(df_obfuscated["name"] == "***")
        assert all(df_obfuscated["email_address"] == "***")
        assert all(df_obfuscated["age"] == [22, 21])
        assert all(df_obfuscated["cohort"] == [2023, 2024])


# Tests for lambda faulty scenarios
class TestFaultScenarios:
    def test_lambda_handler_no_file(self, s3_client):
        # Tests lambda handler when no file exists

        bucket_name = "test-bucket"
        file_key = "nonexistent.csv"
        s3_client.create_bucket(Bucket=bucket_name)

        input_event = {
            "file_to_obfuscate": f"s3://{bucket_name}/{file_key}",
            "pii_fields": ["name", "email_address"]
        }
        response = lambda_handler(input_event, None, s3_client=s3_client)
        assert response["statusCode"] == 400
        assert "no such file" in response["body"].lower()

    def test_lambda_handler_no_pii_fields(self, s3_client):
        # Tests lambda handler when no pii fields provided

        bucket_name = "test-bucket"
        file_key = "test.csv"
        s3_client.create_bucket(Bucket=bucket_name)

        csv_data = ("name,email_address,age,cohort\n"
                    "Anas,anas@example.com,22,2023\n"
                    "Bob,bob@example.com,21,2024\n")
        input_event = {
            "file_to_obfuscate": f"s3://{bucket_name}/{file_key}",
            "pii_fields": [""]
        }
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_data)
        response = lambda_handler(input_event, None, s3_client=s3_client)
        assert response["statusCode"] == 400
        assert "no pii fields" in response["body"].lower()

    def test_lambda_invalid_file_extension(self, s3_client):
        """ Test lambda handler when file type is not one
            one of the supported types. supported types
            include .csv, .json, .parquet only"""

        bucket_name = "test-bucket"
        file_key = "test.txt"
        s3_client.create_bucket(Bucket=bucket_name)

        txt_data = "This is a test text file."
        input_event = {
            "file_to_obfuscate": f"s3://{bucket_name}/{file_key}",
            "pii_fields": ["name", "email_address"]
        }
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=txt_data)
        response = lambda_handler(input_event, None, s3_client=s3_client)
        assert response["statusCode"] == 400
        assert "unsupported file type" in response["body"].lower()

    def test_lambda_handler_empty_event(self, s3_client):
        # Tests lambda handler with empty event

        input_event = {}
        response = lambda_handler(input_event, None, s3_client=s3_client)
        assert response["statusCode"] == 400
        assert "input json is empty" in response["body"].lower()

    def test_lambda_handler_no_s3_uri(self, s3_client):
        # Tests lambda handler with invalid s3 uri

        input_event = {
            "file_to_obfuscate": "not_a_valid_s3_uri",
            "pii_fields": ["name", "email_address"]
        }
        response = lambda_handler(input_event, None, s3_client=s3_client)
        assert response["statusCode"] == 400
        assert "invalid s3 uri" in response["body"].lower()


# Tests lambda handler with parquet file
class TestParquet:
    def test_lambda_handler_parquet_returns_bytestream(self, s3_client):
        # Tests lambda handler returns bytestream for obfuscated parquet file

        bucket_name = "test-bucket"
        file_key = "test.parquet"
        s3_client.create_bucket(Bucket=bucket_name)

        data = {
                "name": ["Anas", "Bob"],
                "email_address": ["anas@example.com", "bob@example.com"],
                "age": [22, 21],
                "cohort": [2023, 2024],
            }
        parq_df = pd.DataFrame(data)
        parq_buffer = BytesIO()
        parq_df.to_parquet(parq_buffer, index=False)
        parq_buffer.seek(0)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=parq_buffer.getvalue())
        input_event = {
            "file_to_obfuscate": f"s3://{bucket_name}/{file_key}",
            "pii_fields": ["name", "email_address"]
        }
        response = lambda_handler(input_event, None, s3_client=s3_client)
        assert response["statusCode"] == 200

        parq_bytestream = BytesIO(base64.b64decode(response["body"]))
        parq_data_bytes = parq_bytestream.getvalue()
        df_obfuscated = pd.read_parquet(parq_bytestream)

        assert isinstance(parq_data_bytes, bytes)
        assert all(df_obfuscated["name"] == "***")
        assert all(df_obfuscated["email_address"] == "***")
        assert all(df_obfuscated["age"] == [22, 21])
        assert all(df_obfuscated["cohort"] == [2023, 2024])

    def test_lambda_handler_parquet_returns_s3_uri(self, s3_client):
        # Tests lambda handler returns s3 uri for obfuscated parquet file

        bucket_name = "test-bucket"
        file_key = "test.parquet"
        s3_client.create_bucket(Bucket=bucket_name)

        data = {
                "name": ["Anas", "Bob"],
                "email_address": ["anas@example.com", "bob@example.com"],
                "age": [22, 21],
                "cohort": [2023, 2024],
            }
        parq_df = pd.DataFrame(data)
        parq_buffer = BytesIO()
        parq_df.to_parquet(parq_buffer, index=False)
        parq_buffer.seek(0)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=parq_buffer.getvalue())
        input_event = {
            "file_to_obfuscate": f"s3://{bucket_name}/{file_key}",
            "pii_fields": ["name", "email_address"]
        }
        response = lambda_handler(input_event, None, s3_client=s3_client)
        assert response["statusCode"] == 200
        obfus_file_key = response["file_key"].replace(
            f"s3://{bucket_name}/", "")
        obj = s3_client.get_object(Bucket=bucket_name, Key=obfus_file_key)
        parq_data = obj["Body"].read()
        parq_io = BytesIO(parq_data)
        df_obfuscated = pd.read_parquet(parq_io)
        assert obfus_file_key.endswith("_obfuscated.parquet")
        assert all(df_obfuscated["name"] == "***")
        assert all(df_obfuscated["email_address"] == "***")
        assert all(df_obfuscated["age"] == [22, 21])
        assert all(df_obfuscated["cohort"] == [2023, 2024])


# Tests for lambda handler with json file
class TestJson:
    def test_lambda_handler_json_returns_bytestream(self, s3_client):
        # Tests lambda handler returns bytestream for obfuscated json file

        bucket_name = "test-bucket"
        file_key = "test.json"
        s3_client.create_bucket(Bucket=bucket_name)
        data = {
            "name": ["Anas", "Bob"],
            "email_address": ["anas@example.com", "bob@example.com"],
            "age": [22, 21],
            "cohort": [2023, 2024],
                }
        json_file = json.dumps(data)
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=json_file)
        input_event = {
            "file_to_obfuscate": f"s3://{bucket_name}/{file_key}",
            "pii_fields": ["name", "email_address"]
        }
        response = lambda_handler(input_event, None, s3_client=s3_client)
        assert response["statusCode"] == 200
        json_bytestream = response["body"]
        df_obfuscated = pd.read_json(BytesIO(json_bytestream))
        assert isinstance(json_bytestream, bytes)
        assert all(df_obfuscated["name"] == "***")
        assert all(df_obfuscated["name"] == "***")
        assert all(df_obfuscated["email_address"] == "***")
        assert all(df_obfuscated["age"] == [22, 21])
        assert all(df_obfuscated["cohort"] == [2023, 2024])

    def test_lambda_handler_json_returns_s3_uri(self, s3_client):
        # Tests lambda handler returns s3 uri for obfuscated json file

        bucket_name = "test-bucket"
        file_key = "test.json"
        s3_client.create_bucket(Bucket=bucket_name)
        data = {
            "name": ["Anas", "Bob"],
            "email_address": ["anas@example.com", "bob@example.com"],
            "age": [22, 21],
            "cohort": [2023, 2024],
                }
        json_file = json.dumps(data)
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=json_file)
        input_event = {
            "file_to_obfuscate": f"s3://{bucket_name}/{file_key}",
            "pii_fields": ["name", "email_address"]
        }
        response = lambda_handler(input_event, None, s3_client=s3_client)
        assert response["statusCode"] == 200
        obfus_file_key = response["file_key"].replace(
            f"s3://{bucket_name}/", ""
            )
        obj = s3_client.get_object(Bucket=bucket_name, Key=obfus_file_key)
        json_data = obj["Body"].read().decode("utf-8")
        df_obfuscated = pd.read_json(StringIO(json_data), lines=True)
        
        assert obfus_file_key.endswith("_obfuscated.json")
        assert all(df_obfuscated["name"] == "***")
        assert all(df_obfuscated["name"] == "***")
        assert all(df_obfuscated["email_address"] == "***")
        assert all(df_obfuscated["age"] == [22, 21])
        assert all(df_obfuscated["cohort"] == [2023, 2024])
