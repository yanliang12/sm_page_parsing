###########dubizzle_car_car_download.py#############
'''

docker run -it ^
-v "E:\dcd_data":/dcd_data/ ^
yanliang12/yan_sm_download:1.0.1 

bash dubizzle_download.sh &



####dubizzle_download.sh####
while true; do
   python3 dubizzle_download.py
   sleep $[50 * 60]
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


first_page = {'page_url':'https://abudhabi.dubizzle.com/search/'}

page_list_urls = []


for i in range(1,11):
	page_list_url = 'https://abudhabi.dubizzle.com/search/?page={}&keywords=&is_basic_search_widget=1&is_search=1'.format(i)
	page_list_urls.append({
		'page_url':page_list_url
		})

page_list_urls = pandas.DataFrame(page_list_urls)
page_list_urls.to_json(
	'page_list_urls.json',
	orient = 'records',
	lines = True,
	)

#######

today = datetime.datetime.now(pytz.timezone('Asia/Dubai'))
today = today.strftime("date%Y%m%d")

today_folder_property_page = '/dcd_data/dubizzle/page_html/source=%s'%(today)
today_folder_property_list_page = '/dcd_data/dubizzle/page_list_html/source=%s'%(today)

try:
	os.makedirs(today_folder_property_page)
except Exception as e:
	print(e)

try:
	os.makedirs(today_folder_property_list_page)
except Exception as e:
	print(e)

#######

yan_web_page_batch_download.args.input_json = 'page_list_urls.json'
yan_web_page_batch_download.args.local_path = today_folder_property_list_page
yan_web_page_batch_download.args.curl_file = '/dcd_data/dubizzle_list_page.sh'
yan_web_page_batch_download.args.overwrite = 'true'
yan_web_page_batch_download.args.page_regex = 'DOCTYPE'
yan_web_page_batch_download.args.sleep_second_per_page = "5"
yan_web_page_batch_download.main()

##########

'''
page = pandas.read_json(
	'/dcd_data/dubizzle/page_list_html/source=date202110/4d0e35d8f7933c4b8bd6410cbb975dd6.json',
	lines = True, 
	orient = 'records',
	)

page_html = page['page_html'][0]
page_url = page['page_url'][0]

parsing_from_list_to_url(
	page_html,
	page_url,
	)
'''

re_page_url = re.compile(r'\<a href\=\"(?P<page_url>[^\"]*?)\?back\=[^\=\&]*?\&pos\=\d+\" class\=\"lpv\-link\-item', flags=re.DOTALL)
page_url_prefix = ''

def parsing_from_list_to_url(
	page_html,
	page_url,
	):
	output = []
	for m in re.finditer(
		re_page_url,
		page_html):
		page_url1 = m.group('page_url')
		page_url1 = '%s%s'%(page_url_prefix, page_url1)
		output.append({'page_url':page_url1})
	return output

property_list_page = sqlContext.read.json(today_folder_property_list_page)

udf_parsing_from_list_to_url = udf(
	parsing_from_list_to_url,
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


#########

yan_web_page_batch_download.args.input_json = 'today_page_url'
yan_web_page_batch_download.args.local_path = today_folder_property_page
yan_web_page_batch_download.args.curl_file = '/dcd_data/dubizzle_page.sh'
yan_web_page_batch_download.args.sleep_second_per_page = "5"
yan_web_page_batch_download.args.redicrete = None
yan_web_page_batch_download.args.page_regex = '(doctype|DOCTYPE)'
yan_web_page_batch_download.args.overwrite = None
yan_web_page_batch_download.main()
###########dubizzle_download.py#############