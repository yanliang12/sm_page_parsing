#######propertyfinder_rent_download.py#######
'''

docker run -it ^
-v "E:\dcd_data":/dcd_data/ ^
yanliang12/yan_sm_download:1.0.1

bash propertyfinder_rent_download.sh &


####propertyfinder_rent_download.sh####
while true; do
   python3 propertyfinder_rent_download.py &
   sleep $[60 * 120]
done
####propertyfinder_rent_download.sh####


'''

import os
import re
import pandas
import pytz
import datetime
import jessica_es
import yan_web_page_download
import yan_web_page_batch_download
from os import listdir
from os.path import isfile, join, exists

import propertyfinder_parsing

#######

import pyspark
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

sc = SparkContext("local")
sqlContext = SparkSession.builder.getOrCreate()

#######

today = datetime.datetime.now(pytz.timezone('Asia/Dubai'))
today = today.strftime("date%Y%m")

today_folder_page_html = '/dcd_data/propertyfinder/page_html/source=%s'%(today)
today_folder_page_list_html = '/dcd_data/propertyfinder/page_list_html/source=%s'%(today)

try:
	os.makedirs(today_folder_page_html)
except Exception as e:
	print(e)

try:
	os.makedirs(today_folder_page_list_html)
except Exception as e:
	print(e)

#######

first_page_url = 'https://www.propertyfinder.ae/en/search?c=2&fu=0&l=6&ob=nd&page=1&rp=y'

list_page_urls = [
{'page_url':first_page_url,}
]

for i in range(2,100):
	list_page_url = 'https://www.propertyfinder.ae/en/search?c=2&fu=0&l=6&ob=nd&page={}&rp=y'.format(i)
	list_page_urls.append({'page_url':list_page_url,})

list_page_urls_df = pandas.DataFrame(list_page_urls)
list_page_urls_df.to_json(
	'list_page_url.json',
	orient = 'records',
	lines = True,
	)

#######

'''
download today's list page
'''

yan_web_page_batch_download.args.input_json = 'list_page_url.json'
yan_web_page_batch_download.args.local_path = today_folder_page_list_html
yan_web_page_batch_download.args.sleep_second_per_page = None
yan_web_page_batch_download.args.page_regex = 'DOCTYPE'
yan_web_page_batch_download.args.overwrite = 'true'
yan_web_page_batch_download.main()


'''
parsing list page to get the page url
'''

parsing_from_list_to_url = udf(
	propertyfinder_parsing.parsing_from_list_to_url,
	ArrayType(MapType(StringType(), StringType())))

sqlContext.read.json(today_folder_page_list_html)\
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
today_page_url.show(100, False)
today_page_url.count()

'''
download the job pages
'''
yan_web_page_batch_download.args.input_json = 'today_page_url'
yan_web_page_batch_download.args.local_path = today_folder_page_html
yan_web_page_batch_download.args.sleep_second_per_page = None
yan_web_page_batch_download.args.page_regex = 'DOCTYPE'
yan_web_page_batch_download.args.overwrite = None
yan_web_page_batch_download.main()

#######propertyfinder_rent_download.py#######