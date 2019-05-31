from scrape_afl_data import *
import boto3
import pandas as pd
import numpy as np 
from io import BytesIO as StringIO
import json




def _write_dataframe_to_csv_on_s3(dataframe, filename):

	############## Get our credentials ###############
	with open('password.json') as json_file:  
	    data = json.load(json_file)

	# Define our session
	DESTINATION = 'aflgainspython'

	session = boto3.Session(
    aws_access_key_id=data['aws_access_key_id'],
    aws_secret_access_key=data['aws_secret_access_key'],
    region_name=data['region_name'])

	""" Write a dataframe to a CSV on S3 """
	logger.info("Writing {} records to {}".format(len(dataframe), filename))
	# Create buffer
	csv_buffer = StringIO()
	# Write dataframe to buffer
	dataframe.to_csv(csv_buffer, index=False)
	# Create S3 object
	s3_resource = session.resource("s3")
	# Write buffer to S3 object
	s3_resource.Object(DESTINATION, filename).put(Body=csv_buffer.getvalue(), ACL='public-read')


def scrape_main(event,context):
	###### Set the logger ##########
	logger = logging.getLogger(__name__)
	logger.setLevel(logging.DEBUG)

	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		
	# create console handler and set level to debug
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)
	ch.setFormatter(formatter)
	# add ch to logger
	logger.addHandler(ch)


	####### Function starts here
	logger.info('Started scraping')
	logger.info('Downloading from s3')
	# Load your data from s3
	match_data_existing = pd.read_csv('https://s3.amazonaws.com/aflgainspython/afl_data_match.csv')
	player_data_existing = pd.read_csv('https://s3.amazonaws.com/aflgainspython/afl_data_player.csv')

	# Obtain the url list
	url_list = player_data_existing['url'].unique()

	# Get  today's date
	now = datetime.datetime.now()

	# Scrape the data
	player_data = []
	match_data = []
	logger.info("Scraping years:" +str(list(range(2000,now.year+1)) ) )
	for year in list(range(2000,np.min([now.year+1,now.year+1]))):
		seas = afl_season(year,game_not_to_scrape= url_list)
		seas.extract_season_data(verbose = False,n_sec = 15)

		if not len(seas.player_data)==0:
			# Concatenate it all together
			player_data.append(pd.concat(seas.player_data, ignore_index=True))
			match_data.append(pd.concat(seas.match_data, ignore_index = True))

		time.sleep(5)

	# Don't do anything if we haven't scraped anything
	if len(player_data)==0:
		logger.info('No Data Scraped')
		return

	# ... Otherwise concatenate the data frames together
	player_for_print = pd.concat(player_data, ignore_index=True)
	match_for_print = pd.concat(match_data, ignore_index=True)

	# If the URL list is not empty, then append the new results with the existing ones
	if not len(url_list)==0:
		player_for_print = player_data_existing.append(player_for_print)
		match_for_print= match_data_existing.append(match_for_print)

	# Print them to s3
	logger.info('Loading data back to s3')

	_write_dataframe_to_csv_on_s3(player_for_print, 'afl_data_player.csv')
	_write_dataframe_to_csv_on_s3(match_for_print, 'afl_data_match.csv')

if __name__=="__main__":
	scrape_main(1,1)
