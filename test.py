
import boto3
import pandas as pd
import numpy as np 
from io import BytesIO as StringIO
import json


with open('password.json') as json_file:  
    data = json.load(json_file)


dic = {'something':[1,2,3,4]}
for_s3 =pd.DataFrame(dic)

DESTINATION = 'aflgainspython'

def _write_dataframe_to_csv_on_s3(dataframe, filename):

	session = boto3.Session(
    aws_access_key_id=data['aws_access_key_id'],
    aws_secret_access_key=data['aws_secret_access_key'],
    region_name=data['region_name'])


	""" Write a dataframe to a CSV on S3 """
	print("Writing {} records to {}".format(len(dataframe), filename))
	# Create buffer
	csv_buffer = StringIO()
	# Write dataframe to buffer
	dataframe.to_csv(csv_buffer, index=False)
	# Create S3 object
	s3_resource = session.resource("s3")
	# Write buffer to S3 object
	s3_resource.Object(DESTINATION, filename).put(Body=csv_buffer.getvalue())

match_data_existing = pd.read_csv('https://s3.amazonaws.com/aflgainspython/afl_data_match.csv')

_write_dataframe_to_csv_on_s3(match_data_existing,'something')