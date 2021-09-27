###########dubizzle_download.py#############
'''

docker run -it ^
-v "E:\dcd_data":/dcd_data/ ^
yanliang12/yan_dcd_downloader:1.0.1

bash dubizzle_download.sh &



####dubizzle_download.sh####
while true; do
   python3 dubizzle_download.py
   sleep $[60 * 60]
done
####dubizzle_download.sh####


'''

#######

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

#######

import pyspark
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

sc = SparkContext("local")
sqlContext = SparkSession.builder.getOrCreate()

#######

import dubizzle_parsing

#######

page_list_urls = []

for i in range(0,8):
	page_list_url = 'https://abudhabi.dubizzle.com/en/property-for-rent/residential/?sort=newest&page=%d'%(i)
	page_list_urls.append({
		'page_url':page_list_url
		})

page_list_urls = pandas.DataFrame(page_list_urls)
page_list_urls.to_json(
	'dubizzle_property_rent_list_page_url.json',
	orient = 'records',
	lines = True,
	)

'''
create today's folders
'''

today = datetime.datetime.now(pytz.timezone('Asia/Dubai'))
today = today.strftime("date%Y%m")

today_folder_property_page = '/dcd_data/dubizzle/property_page/source=%s'%(today)
today_folder_property_list_page = '/dcd_data/dubizzle/property_list_page/source=%s'%(today)

try:
	os.makedirs(today_folder_property_page)
except Exception as e:
	print(e)

try:
	os.makedirs(today_folder_property_list_page)
except Exception as e:
	print(e)

'''
download today's list page
'''
yan_web_page_batch_download.args.input_json = 'dubizzle_property_rent_list_page_url.json'
yan_web_page_batch_download.args.local_path = today_folder_property_list_page
yan_web_page_batch_download.args.curl_file = '/dcd_data/dubizzle_list_curl.sh'
yan_web_page_batch_download.args.overwrite = 'true'
yan_web_page_batch_download.args.page_regex = 'doctype'
yan_web_page_batch_download.args.sleep_second_per_page = '10'
yan_web_page_batch_download.main()

'''
parsing the property page url 
'''

#property_list_page = sqlContext.read.json('/dcd_data/dubizzle/property_list_page')
property_list_page = sqlContext.read.json(today_folder_property_list_page)

udf_parsing_from_list_to_url = udf(
	dubizzle_parsing.parsing_from_list_to_url,
	ArrayType(MapType(StringType(), StringType())))

property_list_page.withColumn(
	'parsed',
	udf_parsing_from_list_to_url(
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
today_page_url.show(20, False)
today_page_url.registerTempTable('today_page_url')

sqlContext.sql(u"""
	SELECT COUNT(*)
	FROM today_page_url
	""").show()

#305|

'''
download the property pages
'''

yan_web_page_batch_download.args.input_json = 'today_page_url'
yan_web_page_batch_download.args.local_path = today_folder_property_page
yan_web_page_batch_download.args.curl_file = '/dcd_data/dubizzle_page_curl.sh'
yan_web_page_batch_download.args.sleep_second_per_page = '10'
yan_web_page_batch_download.args.page_regex = 'doctype'
yan_web_page_batch_download.args.overwrite = None
yan_web_page_batch_download.main()

###########dubizzle_download.py#############