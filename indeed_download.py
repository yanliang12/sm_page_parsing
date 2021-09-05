##############indeed_download.py##################

'''

docker run -it ^
-v "E:\dcd_data":/dcd_data/ ^
yanliang12/yan_dcd:1.0.1 


####indeed_download.sh####
while true; do
   python3 indeed_download.py
   sleep $[60 * 60]
done
####indeed_download.sh####

bash indeed_download.sh &

'''

#######

import os
import re
import pandas
import pytz
import datetime
import jessica_es
import indeed_parsing
import yan_web_page_download
import yan_web_page_batch_download
from os import listdir
from os.path import isfile, join, exists

#######

import pyspark
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

sc = SparkContext("local")
sqlContext = SparkSession.builder.getOrCreate()

#######

first_page_url = 'https://ae.indeed.com/jobs?l=Abu+Dhabi&sort=date'

'''
for daily job
'''

today = datetime.datetime.now(pytz.timezone('Asia/Dubai'))
today = today.strftime("%Y%m%d")

today_folder_job_page = '/dcd_data/indeed/job_page/source=date%s'%(today)
today_folder_job_list_page = '/dcd_data/indeed/job_list_page/source=date%s'%(today)

try:
	os.makedirs(today_folder_job_page)
except Exception as e:
	print(e)

try:
	os.makedirs(today_folder_job_list_page)
except Exception as e:
	print(e)

'''
get the last 100 jobs
'''

list_page_urls = [
{'page_url':first_page_url,}
]

for i in range(10,110,10):
	list_page_url = '%s&start=%d'%(first_page_url,i)
	list_page_urls.append({'page_url':list_page_url,})

list_page_urls_df = pandas.DataFrame(list_page_urls)
list_page_urls_df.to_json(
	'/dcd_data/indeed/indeed_job_list_page_url.json',
	orient = 'records',
	lines = True,
	)

'''
download today's list page
'''

yan_web_page_batch_download.args.input_json = '/dcd_data/indeed/indeed_job_list_page_url.json'
yan_web_page_batch_download.args.local_path = today_folder_job_list_page
yan_web_page_batch_download.args.sleep_second_per_page = '10'
yan_web_page_batch_download.args.page_regex = 'DOCTYPE'
yan_web_page_batch_download.args.overwrite = 'true'
yan_web_page_batch_download.main()

'''
parsing the job page url 
'''

parsing_from_list_to_url = udf(
	indeed_parsing.parsing_from_list_to_url,
	ArrayType(MapType(StringType(), StringType())))

sqlContext.read.json(today_folder_job_list_page)\
	.withColumn(
	'parsed',
	parsing_from_list_to_url(
		'page_html',
		'page_url')
	).drop('page_html').write.mode('Overwrite').parquet('list_page_parsed')

sqlContext.read.parquet('list_page_parsed').registerTempTable('parsed')

sqlContext.sql(u"""
	SELECT DISTINCT parsed.page_url
	FROM (
	SELECT EXPLODE(parsed) AS parsed
	FROM parsed
	) AS temp
	""").write.mode('Overwrite').json('today_page_url')
today_page_url = sqlContext.read.json('today_page_url')
today_page_url.count()

'''
download the job pages
'''
yan_web_page_batch_download.args.input_json = 'today_page_url'
yan_web_page_batch_download.args.local_path = today_folder_job_page
yan_web_page_batch_download.args.sleep_second_per_page = '5'
yan_web_page_batch_download.args.page_regex = 'DOCTYPE'
yan_web_page_batch_download.args.overwrite = None
yan_web_page_batch_download.main()

##############indeed_download.py##################