import boto3
from config import S3_BUCKET
from flask import session
import csv

def get_bucket():
    resource = boto3.resource('s3')
    return resource.Bucket(S3_BUCKET)

def get_dynamo_client():
    return boto3.client('dynamodb')

def get_records_count(file_name):
    client = get_dynamo_client()
    table_name = get_table_name_from_filename(file_name)
    
    if table_name != '':
        response = client.scan(
            TableName=table_name)
        return response[0]['Count']
    else:
        return 0

def get_table_name_from_filename(file_name):
    table = ''
    if file_name.endswith("Casualty.csv"):
        table = 'table_casualty'
    elif file_name.endswith("Units.csv"):
        table = 'table_units'
    elif file_name.endswith("Crash.csv"):
        table = 'table_crash'
    return table

def csv_get_dict_records(filename):    
    client = boto3.client('s3')

    bucket_name = S3_BUCKET
    object_key = filename

    csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
    body = csv_obj['Body']

    csv_string = body.read().decode('utf-8-sig')
    file_content = csv_string.split("\n")

    return  csv.DictReader(file_content, delimiter=',', quotechar='"')
