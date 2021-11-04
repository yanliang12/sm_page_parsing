#######khaleejtimes_download.py#######
'''

docker run -it ^
-v "E:\dcd_data":/dcd_data/ ^
yanliang12/yan_sm_download:1.0.1

python3 khaleejtimes_download.py &



####khaleejtimes_download.sh####
while true; do
   python3 khaleejtimes_download.py
   sleep $[60 * 60]
done
####khaleejtimes_download.sh####

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

#######

import pyspark
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

sc = SparkContext("local")
sqlContext = SparkSession.builder.getOrCreate()

#######

'''


https://gulfnews.com/search?query=abu+dhabi
today = 'dcd'
'''

today = datetime.datetime.now(pytz.timezone('Asia/Dubai'))
today = today.strftime("date%Y")

today_folder_page_html = '/dcd_data/khaleejtimes/page_html/source=%s'%(today)
today_folder_page_list_html = '/dcd_data/khaleejtimes/page_list_html/source=%s'%(today)

try:
	os.makedirs(today_folder_page_html)
except Exception as e:
	print(e)

try:
	os.makedirs(today_folder_page_list_html)
except Exception as e:
	print(e)

#######

'''
first_page_url = 'https://www.khaleejtimes.com/search?text=%22Department+of+Community+Development%22&x=40&y=31'

first_page_url = 'https://www.khaleejtimes.com/uae/abu-dhabi&pagenumber=2'

{'page_url':first_page_url,}

'''

list_page_urls = []

for i in range(1,5):
	list_page_url = 'https://www.khaleejtimes.com/business/property?pagenr={}'.format(i)
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
yan_web_page_batch_download.args.redirect = None
yan_web_page_batch_download.args.curl_file = None
yan_web_page_batch_download.args.sleep_second_per_page = None
yan_web_page_batch_download.args.page_regex = 'DOCTYPE'
yan_web_page_batch_download.args.overwrite = 'true'
yan_web_page_batch_download.main()


'''
parse the list page to get the page url
'''

re_page_url = [
	#re.compile(r'href\="(?P<page_url>\/news\/[^\"]*?)"\>', flags=re.DOTALL),
	re.compile(r'post\-thumbnail"\>\<a href\="(?P<page_url>https\:\/\/www\.khaleejtimes\.com\/[^\\/]*?\/[^\\/]*?)"\>', flags=re.DOTALL),
	]

page_url_prefix = 'https://www.khaleejtimes.com'
page_url_prefix = ''

def parsing_from_list_to_url(
	page_html,
	page_url,
	):
	output = []
	for r in re_page_url:
		for m in re.finditer(
			r,
			page_html):
			page_url1 = m.group('page_url')
			page_url1 = '%s%s'%(page_url_prefix, page_url1)
			output.append({'page_url':page_url1})
	return output

parsing_from_list_to_url = udf(
	parsing_from_list_to_url,
	ArrayType(MapType(StringType(), StringType())))

###

list_page_html = sqlContext.read.json(today_folder_page_list_html)

list_page_html.withColumn(
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

#90

'''
download the job pages
'''
yan_web_page_batch_download.args.input_json = 'today_page_url'
yan_web_page_batch_download.args.local_path = today_folder_page_html
yan_web_page_batch_download.args.redirect = None
yan_web_page_batch_download.args.curl_file = None
yan_web_page_batch_download.args.sleep_second_per_page = None
yan_web_page_batch_download.args.page_regex = 'DOCTYPE'
yan_web_page_batch_download.args.overwrite = None
yan_web_page_batch_download.main()

#######khaleejtimes_download.py#######