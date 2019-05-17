"""AFL stats web scraper
-----------------------
Author: AFLgains
Email: AFLgains@gmail.com
Github: https://github.com/AFLgains
Develeoped on Python 2.7

This script allows the user to scrape AFL tables for AFL data for specified 
seasons greater than the year 2000.

The script takes in optional arguments with the following usage details: 

usage: scrape_afl_data.py [-h] [-from_year fy] [-to_year ty]
                          [-prev_player_data_fileloc player_filepath]
                          [-prev_match_data_fileloc match_filepath]
                          [-verbose verbose]

optional arguments:
  -h, --help            show this help message and exit
  -from_year fy         Starting year to scrape
  -to_year ty           End year to scrape
  -prev_player_data_fileloc player_filepath
                        Path to previous player data file location
  -prev_match_data_fileloc match_filepath
                        Path to previous match data file location
  -verbose verbose      Verbose print during scraping


If no arguments are supplied, data from Season 2000 to now will be scraped

The script requires that `pandas` be installed within the Python
environment you are running this script in.

This file can also be imported as a module and contains the following
classes:

	* afl_season - A class to represent and AFL season 
	* afl_match - A class to represent an AFL match

...and functions

    * main - the main function of the script


Example 1 - Scrape data from Season 2000 onwards
-----------------------------------------------
python scrape_afl_data.py



Example 2 - Scrape data from Season 2010 to 2015
-----------------------------------------------
python scrape_afl_data.py -from_year 2010 -to_year 2015



Example 3 - Scrape data from Season 2010 to 2015 specifying previously 
downloaded data path. This is important if you don't want to re-scrape
data you have already scraped and speed up the process. 
-----------------------------------------------
python scrape_afl_data.py -from_year 2010 -to_year 2015 -prev_player_data_fileloc ./afl_player_data.csv -prev_match_data_fileloc ./afl_match_data.csv



FOR DOCKER USERS:
----------------
This script is shipped with a docker file you can use to run. For basic use:

sudo docker build -t "afl_scraper" .
sudo docker run afl_scraper

The dockerfile also had a volume in it which store the data at /usr/src/app/data
You can access the volume by creating a bind mounted volume at run time which maps a path 
in your host filesystem to a path in the Docker's container. That is:

docker run -v /path/to/where/your/data/needs/to/save:/usr/src/app/data afl_scraper


"""

# Importing libraries
import argparse
import requests
from bs4 import BeautifulSoup
import urllib	
import numpy as np 
import matplotlib.pyplot as plt
import pandas as pd
import lxml
import re
import bcrypt
import datetime
import os
import logging
import time


# Creating a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

# Adding a file handler
fh = logging.FileHandler(r'scrape.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)
logger.addHandler(fh)

class afl_season():

	"""
	A class used to represent an AFL season
	...

    Attributes
    ----------
    year : int
        Year to scrape

    game_not_to_scrape : list of str, optional
    	A list of urls to not scrape. Can speed up scraping. 

    Methods
    -------
    extract_season_data(verbose=False)
        Extracts season's data from AFLtables.com and stores it in a dataframe
    """

	def __init__(self,year, game_not_to_scrape = []):

		"""
        Parameters
        ----------
	    year : int
	        Year to scrape

	    game_not_to_scrape : list of str, optional
	    	A list of urls to not scrape. Can speed up scraping. 

	    url_list:

	    player_data:

	    match_data:


        """

		# Get the current time
		now = datetime.datetime.now()

		# Assertions
		assert type(year) == int, "Year input must be an integer"
		assert year >= 2000, "Seasons must be greater than 1999" 
		assert year <= now.year, "Seasons must be less than " + now.year

		# Define the year
		self.year = year

		# Get the URL list which represents all the matches
		url = "https://afltables.com/afl/seas/"+ str(self.year)+".html#10"
		response = requests.get(url)
		soup = BeautifulSoup(response.text, "html.parser")
		x = soup.findAll('a')
		self.url_list = ["http://afltables.com/afl/stats/"+e['href'].replace("../stats/","") for e in x if "../stats/games" in str(e)]

		# Cut the URL list short if we have already scraped it
		self.url_list = [e for e in self.url_list if e not in game_not_to_scrape]


	def extract_season_data(self,verbose = False,n_sec = 5):

		"""Extracts season's data from AFLtables.com and stores it in a dataframe

		Parameters
        ----------
        verbose: bool, optional
        	Print detailed information of each match scraped 
        """

		self.player_data = []
		self.match_data = []

		# Where the URL list is empty
		if len(self.url_list)==0:
			logger.info("No games to scrape for season "+ str(self.year))
			return 

		# Otherwise enter into the scraping code
		for url in self.url_list:
			# Define a match object
			match = afl_match(url = url,season = self.year)

			# Get all the data
			match.get_meta_data()		
			match.get_match_data()
			match.get_player_profiles()

			# Store the data
			# Create Match Json
			match.create_match_info_csv()
			match.create_player_csv()

			# Store into pandas
			self.player_data.append(match.player)
			self.match_data.append(match.match)

			# Print out user
			if verbose == True:
				logger.info('Date:{0}, Attendance: {1}, Round:, {2}, Home: {3}, Away: {4}, ID: {5}'.format(match.date,match.attendance,match.round,match.home_team,match.away_team,match.match_id))
			else:
				logger.info('Season:{0}, Home: {1}, Away: {2}'.format(self.year,match.home_team,match.away_team))

			# Pause fo x seconds to reduce load on the server 
			time.sleep(n_sec)



class afl_match():
	"""
	A class used to represent an AFL match
	...

    Attributes
    ----------
    url : str
        AFLtables url to scrape

    season : int
    	Season from which to scrape. 

    Methods
    -------
    get_meta_data()
        Retrieve general match data (date, venue, teams, etc.)

    get_match_data()
    	Retrieve player stats for the match

    get_player_profiles()
    	Retrieve player profiles (ages, games played etc.)

    get_score_progression()
    	Retrieve the score progression through time (NOT AVAILIABLE)

    create_player_csv()
    	Transform player stats data into dataframe ready for CSV export

    create_match_info_csv()
    	Transform match data into dataframe ready for CSV export

    process_player_data()
    	Extract player and match statistics from html data

    extract_elements()
    	Extract match meta data from html table
    """

	# Initialise
	def __init__(self,url,season):
		self.url = url
		self.tables = pd.read_html(url)
		self.season = season

	# Extract the meta data
	def get_meta_data(self):
		self.home_team = self.tables[0].iloc[1,1]
		self.away_team = self.tables[0].iloc[2,1]
		self.umpires = self.tables[0].iloc[5,2]
		self.str = self.tables[0].iloc[0,1]
		self.date = self.extract_elements(self.str,"Date")
		self.venue = self.extract_elements(self.str,"Venue")
		self.round = self.extract_elements(self.str,"Round")
		self.attendance = self.extract_elements(self.str,"Attendance")
		self.match_id =bcrypt.hashpw(self.str.encode('utf8'),bcrypt.gensalt())
		self.home_score= int(self.tables[0].iloc[1,5].split('.')[2])
		self.away_score = int(self.tables[0].iloc[2,5].split('.')[2])
		self.margin = self.home_score - self.away_score 


	def get_match_data(self):
		self.home_match_stats,self.away_match_stats, = self.process_player_data("Match Statistics",self.tables)


	def get_player_profiles(self):
		self.home_player_profile,self.away_player_profile, = self.process_player_data("Player Details",self.tables)


	def get_score_progression(self):
		pass

	def create_player_csv(self):
		home = self.home_match_stats
		home['status'] = "home"

		away = self.away_match_stats
		away['status'] = "away"

		player = home.append(away)
		player['url'] = self.url

		self.player = player.iloc[:,1:]

	def create_match_info_csv(self):
		d = {
			'home_team' : [self.home_team],
			'away_team' : [self.away_team],
			'umpires' : [self.umpires],
			'round' :[self.round],
			'attendance':  [self.attendance],
			'venue':[self.venue],
			'date':[self.date],
			'season': [self.season],
			'margin': [self.margin],
			'home_score': [self.home_score],
			'away_score':[self.away_score],
			'url':[self.url]
			}
		self.match = pd.DataFrame(data = d)


	def process_player_data(self,string, table_list):
		""" Processes player data"

		Parameters
	    ----------
	    table_list: list dataframes 
	    	List of dataframes from AFLtables.com specifying match and player details 
	    """

		# Assertion
		assert string in ["Match Statistics","Player Details"], "Input must either be Match Statistics or Player Details"

		# Get the index of all of the match stats data
		stats_tables_idx = [(i) for i,e in enumerate(table_list) if string in str(e.columns.get_level_values(0)[0])]

		# Warning if we can't find 2 tables (corresponding to the home and away)
		if not len(stats_tables_idx)==2:
			home = []
			away  = []
			logger.info("Warning: Could not find two tables")
			return home, away

		# Only take the 22 players
		home = table_list[stats_tables_idx[0] ][0:22]
		away = table_list[stats_tables_idx[1] ][0:22]

		# And drop the level 0 index (since its a multiindex array)
		home.columns = home.columns.droplevel(0)
		away.columns = away.columns.droplevel(0)

		# Return output
		return home, away


	def extract_elements(self,string,element): 

	    met_data_titles = ["Round:","Venue:","Date:","Attendance:",""]

	    # Check that the element is one of the above, otherwise return Invalid element
	    if not element+":" in met_data_titles or element == "":
	        return "Invalid Element"
	    
	    # Get indexes of the elements
	    id_met = [i for i,e in enumerate(met_data_titles) if element in e] # The index of the element (0,1,2,or 3)
	    
	    # Build the string to search
	    s1 = met_data_titles[id_met[0] ]
	    s2 = met_data_titles[id_met[0] + 1]
	    
	    # Search
	    result = re.search(s1+'(.*)'+s2, string)
	    
	    #Return result
	    if result is None:
	        return "Not Found"
	    
	    return result.group(1).strip()



def main():
	logger.info('Started')

	# Arguments to pass in
	parser = argparse.ArgumentParser(description='Scrape years of data')
	parser.add_argument('-from_year', metavar='fy', type=int, default = 2000,
                    help='Starting year to scrape')
	parser.add_argument('-to_year', metavar='ty', type=int, default = datetime.datetime.now().year,
                    help='End year to scrape', choices = range(2001, datetime.datetime.now().year+1))
	parser.add_argument('-prev_player_data_fileloc', metavar='player_filepath', type=str, default = "/afl_data_player.csv",
                    help='Path to previous player data file location')
	parser.add_argument('-prev_match_data_fileloc', metavar='match_filepath', type=str, default = "/afl_data_match.csv",
                    help='Path to previous match data file location')
	parser.add_argument('-verbose', metavar='verbose', type=bool, default = False,
                    help='Verbose print during scraping')
	args = parser.parse_args()


	url_list = []
	if os.path.exists('.'+args.prev_player_data_fileloc) and os.path.exists('.'+args.prev_match_data_fileloc):
		logger.info('Previous Data found')
		player_data_existing = pd.read_csv('.'+args.prev_player_data_fileloc)
		match_data_existing = pd.read_csv('.'+args.prev_match_data_fileloc)
		url_list = player_data_existing['url'].unique()
	else:
		logger.info("Previous data could not be found")
		
	# Measure the current time
	now = datetime.datetime.now()

	# Scrape the data
	player_data = []
	match_data = []
	logger.info("Scraping years:" +str(list(range(args.from_year,args.to_year+1)) ) )
	for year in list(range(args.from_year,np.min([now.year+1,args.to_year+1]))):
		seas = afl_season(year,game_not_to_scrape= url_list)
		seas.extract_season_data(verbose = args.verbose)

		if not len(seas.player_data)==0:
			# Concatenate it all together
			player_data.append(pd.concat(seas.player_data, ignore_index=True))
			match_data.append(pd.concat(seas.match_data, ignore_index = True))

		time.sleep(5)

	# Don't do anything if we haven't scraped anything
	if len(player_data)==0:
		return

	# ... Otherwise concatenate the data frames together
	player_for_print = pd.concat(player_data, ignore_index=True)
	match_for_print = pd.concat(match_data, ignore_index=True)

	# If the URL list is not empty, then append the new results with the existing ones
	if not len(url_list)==0:
		player_for_print = player_data_existing.append(player_for_print)
		match_for_print= match_data_existing.append(match_for_print)

	# Print them out to the directory
	print "Printing data to /data"+args.prev_player_data_fileloc
	
	player_for_print.to_csv("."+args.prev_player_data_fileloc,index = False)
	match_for_print.to_csv("."+args.prev_match_data_fileloc,index = False)


if __name__ == "__main__":
	main()

