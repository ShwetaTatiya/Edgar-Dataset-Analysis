import pandas as pd
import requests
from bs4 import BeautifulSoup
import sys
import urllib.request
import urllib
import csv
import os
import zipfile
from zipfile import ZipFile
from urllib.request import urlopen
from io import BytesIO
import glob
import numpy as np
import logging
import datetime
import time
import boto
import boto.s3
from boto.s3.key import Key 

## Creating the Loggers
logger = logging.getLogger("Problem1")
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler("Prog2_Log.log")
file_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)



def summary_metrics(csv1):

	cs1 = pd.DataFrame()
	cs1 = csv1

	# Getting total no. of files as per time
	summary2 = cs1.groupby('time')['cik'].count()
	describe2 = pd.DataFrame(summary2)
	describe2.to_csv(str(os.getcwd()) + "/total_no_of_files_as_per_time.csv")

	# Getting no. of files accessible as per status code
	summary3 = cs1['code'].value_counts()
	#print(summary3)
	describe3 = pd.DataFrame(summary3)
	describe3.to_csv(str(os.getcwd()) + "/total_no_of_files_accessible_as_per_status_code.csv")

	# dividing size into quartiles and getting total no. of files as per the size range 
	cs1 = cs1.groupby(pd.cut(cs1['size'], np.percentile(cs1['size'], [0, 25, 75, 90, 100]))).count()
	cs1 = cs1.rename(columns={'size':'old_size'})
	cs1 = cs1.reset_index()
	summary1 = cs1.groupby('size')['cik'].sum()
	describe1 = pd.DataFrame(summary1)
	describe1.to_csv(str(os.getcwd()) + "/total_files_as_per_size-quartiles.csv")

	logger.info("CSVs for Summary Metrics generated")


try:
	def replacing_missing_values(csv1):
		new_data = pd.DataFrame()
		csv1['ip'].dropna(inplace = True)
		csv1['cik'].dropna(inplace = True)
		csv1['accession'].dropna(inplace = True)
		csv1['date'].fillna(method = 'bfill' , inplace = True)
		csv1['time'].fillna(method = 'ffill' , inplace = True)
		csv1['zone'].fillna(method = 'ffill' , inplace = True) 
		csv1 = csv1.reset_index(drop = True)
		#csv1.drop('index', axis = 1, inplace=True)
		index = csv1.index[csv1["extention"] == ".txt"]
		csv1.set_value(index, "extention", (csv1['accession'].map(str) + csv1['extention']))
		csv1 = csv1.set_index("extention")
		csv1['code'].fillna(value= 0 , inplace = True)
		csv1['size'] = csv1['size'].fillna(value= 0)
		csv1['size'] = csv1['size'].astype('int64')
		csv1['idx'].fillna(value= 0 , inplace = True)
		csv1['norefer'].fillna(value= 0 , inplace = True)
		csv1['noagent'].fillna(value= 0 , inplace = True)
		csv1['find'].fillna(value= 0 , inplace = True)
		#csv1['crawler'][(csv1['noagent'] is '0') or (csv1['code'] is '404')] = 1
		#csv1.loc[csv1['noagent'] == 1,"crawler"] = csv1['crawler'].replace(np.nan,0)
		default = "win"
		csv1['browser'] = csv1['browser'].replace(np.nan,default)
		#csv1.loc[csv1['noagent'] == 0,"browser"] = csv1['browser'].replace(np.nan,default)
		#csv1.loc[csv1['noagent'] == 1,"browser"] = csv1['browser'].replace(np.nan,"not defined")
		new_data = csv1
		return new_data
	logger.info("Missing Data Analysis was done for all the columns")
except Exception as e:
	logger.error(str(e))
	exit()


try:
	def changing_datatypes(csvdata):
		#if(csvdata.values() != ''):
		csvdata['date'] = pd.to_datetime(csvdata['date'])
		csvdata['zone'] = csvdata['zone'].astype('int64')
		csvdata['cik'] = csvdata['cik'].astype('int64')
		#csvdata['code'] = csvdata['code'].astype('int64')
	##	csvdata['size'] = csvdata['size'].astype('int64')
		#csvdata['idx'] = csvdata['idx'].astype('int64')
		#csvdata['norefer'] = csvdata['norefer'].astype('int64')
		#csvdata['noagent'] = csvdata['noagent'].astype('int64')
		#csvdata['find'] = csvdata['find'].astype('int64')
		#csvdata['crawler'] = csvdata['crawler'].astype('int64')
		#print("replacing values")
		newdata = replacing_missing_values(csvdata)
		#print("Summary metrics")
		summary_metrics(newdata)
		newdata.to_csv("combined_data.csv",encoding='utf-8')
		return 0
	logger.info("Data types were Changed")	
except Exception as e:
	logger.error(str(e))
	exit()


def create_frames(path):
	allFiles = glob.glob( path + "/log*.csv")
	frame = pd.DataFrame()
	list_ = []
	for file_ in allFiles:
	    df = pd.read_csv(file_ ,index_col=None, header=0)
	    list_.append(df)
	frame = pd.concat(list_)
	changing_datatypes(frame)
logger.info("Converting all CSVs into Dataframes")

def generate_log_file(link,year):
	foldername = str(year)
	path = str(os.getcwd()) + "/" + foldername

	for file in link:
		print(file)
		filename = file.split('/')[-1]
		filename1 = filename.split('.')[0]
		with urlopen(file) as zipresp:
			with ZipFile(BytesIO(zipresp.read())) as zfile:
				zfile.extractall(path)
				create_frames(path)
	logger.info("Extracting Log files for First day of each month was created ")


def generate_url(url,year):
	link = []
	year_a = year
	#if (map(int,year) < 2003):
	soup = BeautifulSoup((urllib.request.urlopen(url)),"html.parser")
	list_links = soup.select('ul li')
	for list_link in list_links:
		for li in list_link.find_all('a'):
			log_string = str(li.text).split('.')[0]
			if ("01" in log_string[9:11]):
				href = li['href']	
				link.append(href)
	generate_log_file(link,year_a)
logger.info("Getting zip files of every month")
#else:
#	print("Invalid Year")
#	exit()			




	
def zipping(path, ziph):
    # ziph is zipfile handle
    ziph.write(os.path.join("combined_data.csv"))
    ziph.write(os.path.join("total_no_of_files_as_per_time.csv"))
    ziph.write(os.path.join("total_files_as_per_size-quartiles.csv"))
    ziph.write(os.path.join("total_no_of_files_accessible_as_per_status_code.csv"))
    ziph.write(os.path.join('Prog2_Log.log'))   
logger.info("CSV files and the Log file were zipped in a single folder")

def main():
	#AWS_ACCESS_KEY_ID = str(sys.argv[2])
	#AWS_SECRET_ACCESS_KEY = str(sys.argv[3])
	year = input("Enter the year ")
	AWS_ACCESS_KEY_ID = input("AWS_ACCESS_KEY_ID =")
	AWS_SECRET_ACCESS_KEY = input("AWS_SECRET_ACCESS_KEY =")
	inputLocation = input("Bucket Loc = ")
	#inputLocation = (sys.argv[4])
	#year = str(sys.argv[1])

	baseURL = "https://www.sec.gov/"
	year_input = int(year)
	if(year_input >= 2003):
		if(year_input <= 2016):
			url = baseURL + "files/edgar"+ year +".html"
			generate_url(url,year)
			logger.info("Files for the year %s ... ", year)
	if(year_input == 2017):
		year1 = year +"_1"
		url = baseURL + "files/edgar"+ year1 +".html"
		logger.info("Files for the year %s ... ", year)
		generate_url(url,year1)
	else:
		print("Data doesn't exist")
		logger.warning("Data does not exist")
	if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
	   	logger.warning('Access Key and Secret Access Key not provided')
	   	print('Access Key and Secret Access Key not provided')
	   	exit()
	logger.info("URL %s was created ", url)


	zf = zipfile.ZipFile('%s.zip' %year, 'w')
	zipping('/', zf)
	zf.close()


	#Creating a S3 bucket
	#try:
		#Creating a S3 bucket
	conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
	logger.info("Connected to S3")
	print("Connected to S3")
	'''
	except:
		logger.info("Amazon keys are invalid")
		print("Amazon keys are invalid")
		exit()
	'''

	#Location for the region
	loc = ''
	if inputLocation == 'APNortheast':
	    loc=boto.s3.connection.Location.APNortheast
	elif inputLocation == 'APSoutheast':
	    loc=boto.s3.connection.Location.APSoutheast
	elif inputLocation == 'APSoutheast2':
	    loc=boto.s3.connection.Location.APSoutheast2
	elif inputLocation == 'CNNorth1':
	    loc=boto.s3.connection.Location.CNNorth1
	elif inputLocation == 'EUCentral1':
	    loc=boto.s3.connection.Location.EUCentral1
	elif inputLocation == 'EUWest2':
	    loc=boto.s3.connection.Location.EU
	elif inputLocation == 'SAEast':
	    loc=boto.s3.connection.Location.SAEast
	elif inputLocation == 'USWest':
	    loc=boto.s3.connection.Location.USWest
	elif inputLocation == 'USWest2':
	    loc=boto.s3.connection.Location.USWest2

	try:
		ts = time.time()
		st = datetime.datetime.fromtimestamp(ts)    
		bucket_name = AWS_ACCESS_KEY_ID.lower()+str(st).replace(" ", "").replace("-", "").replace(":","").replace(".","")
		bucket = conn.create_bucket(bucket_name, location=loc)
		filename = ('%s.zip' %year)
		logger.info("S3 bucket successfully created")


		#Uploading files to the Bucket
		def percent_cb(complete, total):
			sys.stdout.write('.')
			sys.stdout.flush()

		k = Key(bucket)
		k.key = 'Problem2'
		k.set_contents_from_filename(filename, cb=percent_cb, num_cb=10)

		logger.info("Zip file successfully uploaded to S3")
		print("Zip File successfully uploaded to S3")
	except Exception as e:
		logger.error(str(e))
		exit()



	logger.info("-----FINISH------")
	print("-----FINISH------")

if __name__ == "__main__":
    main()
	















