import pytest
import pandas as pd
from src.utils import parse_input_json, read_csv_from_s3, obfuscate_pii, write_obfuscated_file_to_s3

def test_parse_input_json():
    input_json = {
        "file_to_obfuscate": "s3://ans-gdpr-bucket/students.csv",
        "pii_fields": ["name", "email_address"]
    }
    bucket_name, file_key, pii_fields = parse_input_json(input_json)
    assert bucket_name == "ans-gdpr-bucket"
    assert file_key == "students.csv"
    assert pii_fields == ["name", "email_address"]

def test_parse_input_json_different_path():
    input_json = {
        "file_to_obfuscate": "s3://my-bucket/data/files/info.csv",
        "pii_fields": ["id", "phone_number"]
    }
    bucket_name, file_key, pii_fields = parse_input_json(input_json)
    assert bucket_name == "my-bucket"
    assert file_key == "data/files/info.csv"
    assert pii_fields == ["id", "phone_number"]

def test_parse_input_json_empty():
    input_json = {
        
    }
    bucket_name, file_key, pii_fields = parse_input_json(input_json)
    assert bucket_name == ""
    assert file_key == ""
    assert pii_fields == []

def test_parse_input_json_no_pii_fields():
    input_json = {
        "file_to_obfuscate": "s3://ans-gdpr-bucket/students.csv"
    }
    bucket_name, file_key, pii_fields = parse_input_json(input_json)
    assert bucket_name == "ans-gdpr-bucket"
    assert file_key == "students.csv"
    assert pii_fields == [] 

def test_parse_input_json_invalid_path():
    input_json = {
        "file_to_obfuscate": "http://ans-gdpr-bucket/students.csv",
        "pii_fields": ["name", "email_address"]
    }
    bucket_name, file_key, pii_fields = parse_input_json(input_json)
    assert bucket_name == ""
    assert file_key == ""
    assert pii_fields == []

