#########bayut_download.py#########

'''

docker run -it ^
-v "E:\dcd_data":/dcd_data/ ^
yanliang12/yan_dcd:1.0.1

bash bayut_download.sh &



####bayut_download.sh####
while true; do
   python3 bayut_download.py &
   sleep $[60 * 60]
done
####bayut_download.sh####


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

today = datetime.datetime.now(pytz.timezone('Asia/Dubai'))
today = today.strftime("data%Y%m")

today_folder_page_html = '/dcd_data/bayut/page_html/source=%s'%(today)
today_folder_page_list_html = '/dcd_data/bayut/page_list_html/source=%s'%(today)

try:
	os.makedirs(today_folder_page_html)
except Exception as e:
	print(e)

try:
	os.makedirs(today_folder_page_list_html)
except Exception as e:
	print(e)

#######

first_page_url = 'https://www.bayut.com/to-rent/property/abu-dhabi/?sort=date_desc'

list_page_urls = [
	{'page_url':first_page_url,}
]

for i in range(2,10):
	list_page_url = 'https://www.bayut.com/to-rent/property/abu-dhabi/page-{}/?sort=date_desc'.format(i)
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

#########bayut_download.py#########