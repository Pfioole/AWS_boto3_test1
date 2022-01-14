import boto3
import pandas

import json
import os

filepath = "./serverparams.json"
with open(filepath) as json_file:
    serverparamsJSON = json.load(json_file)
access_key = serverparamsJSON["aws_access_key_id"]
secret_key = serverparamsJSON["aws_secret_access_key"]
base_region = serverparamsJSON["base_region"]

# Creating the low level functional client kkk

client = boto3.client(
    's3',
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key,
    region_name = base_region
)

#client = boto3.client(
#    's3',
#    aws_access_key_id = 'AKIATBDHBTXPJXFHIXL2',
#    aws_secret_access_key = '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',
#    region_name = base_region
#)

# Creating the high level object oriented interface

resource = boto3.resource(
    's3',
    aws_access_key_id= access_key,
    aws_secret_access_key= secret_key,
    region_name='eu-central-1'
)

#resource = boto3.resource(
#   's3',
#    aws_access_key_id='AKIATBDHBTXPJXFHIXL2',
#    aws_secret_access_key='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',
#    region_name='eu-central-1'
#)

# Creating a bucket in AWS S3
def Create_New_Bucket(bucket_name, selected_region):
    location = {'LocationConstraint': selected_region}
    client.create_bucket(
        Bucket= bucket_name,
        CreateBucketConfiguration = location
    )
    return ('Bucket created')

# Creating a bucket in AWS S3
message = Create_New_Bucket("study-1", base_region)
print(message)

clientResponse = client.list_buckets()

#Print the bucket names one by one
print('Printing bucket names...')
for bucket in clientResponse['Buckets']:
    print(f'Bucket Name: {bucket["Name"]}')

# Read a csv object
obj = client.get_object(
    Bucket = 'study-1',
    Key = 'testtabel.csv'
)
data = pandas.read_csv(obj['Body'])

# Print the dataframe

print('Printing the dataframe...')
print(data)

#Fetch the list of existing buckets

