#######emirates247_download.py#######
'''

docker run -it ^
-v "E:\dcd_data":/dcd_data/ ^
yanliang12/yan_dcd:1.0.1

python3 emirates247_download.py &

####emirates247_download.sh####
while true; do
   python3 emirates247_download.py &
   sleep $[60 * 60]
done
####emirates247_download.sh####

bash emirates247_download.sh &

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

import emirates247_parsing

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


https://emirates247.com/search?query=abu+dhabi
'''

today = datetime.datetime.now(pytz.timezone('Asia/Dubai'))
today = today.strftime("%Y%m")

today_folder_page_html = '/dcd_data/emirates247/page_html/source=date%s'%(today)
today_folder_page_list_html = '/dcd_data/emirates247/page_list_html/source=date%s'%(today)

try:
	os.makedirs(today_folder_page_html)
except Exception as e:
	print(e)

try:
	os.makedirs(today_folder_page_list_html)
except Exception as e:
	print(e)

#######

first_page_url = 'https://www.emirates247.com/search-7.117?t=b&q=abu+dhabi&s=d'

list_page_urls = [
{'page_url':first_page_url,}
]

for i in range(1,10):
	list_page_url = 'https://www.emirates247.com/search-7.117?t=b&q=abu+dhabi&s=d&p={}'.format(i)
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

parsing_from_list_to_url = udf(
	emirates247_parsing.parsing_from_list_to_url,
	ArrayType(MapType(StringType(), StringType())))

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
#84

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

#######emirates247_download.py#######