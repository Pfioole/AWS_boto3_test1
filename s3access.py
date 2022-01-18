import boto3
import pandas as pd
import json
#import os
import random


filepath = "./serverparams.json"
with open(filepath) as json_file:
    serverparamsJSON = json.load(json_file)
access_key = serverparamsJSON["aws_access_key_id"]
secret_key = serverparamsJSON["aws_secret_access_key"]
base_region = serverparamsJSON["base_region"]

# Creating the low level functional client

client = boto3.client(
    's3',
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key,
    region_name = base_region
)

# Creating the high level object oriented interface

resource = boto3.resource(
    's3',
    aws_access_key_id= access_key,
    aws_secret_access_key= secret_key,
    region_name='eu-central-1'
)


# Function to create a bucket in AWS S3
def create_new_bucket(bucket_name, selected_region):
    location = {'LocationConstraint': selected_region}
    client.create_bucket(
        Bucket= bucket_name,
        CreateBucketConfiguration = location
    )
    return ('Bucket created')


# Creating a bucket in AWS S3
studyName = "study" + str(random.randint(1,1000))
message = create_new_bucket(studyName, base_region)
print(message)



clientResponse = client.list_buckets()
#Print the bucket names one by one
print('Printing bucket names...')
for bucket in clientResponse['Buckets']:
    print(f'Bucket Name: {bucket["Name"]}')

# Read a sas object
obj = client.get_object(
    Bucket = 'study-5',
    Key = 'ae.sas7bdat'
)
df_domain = pd.read_sas(obj['Body'], format='sas7bdat', encoding='iso-8859-1')
# data = pandas.read_csv(obj['Body'])

# Print the dataframe

print('Printing the dataframe...')
print(df_domain)

#Fetch the list of existing buckets

