###########opensooq_download.py###########

'''

docker run -it ^
-v "E:\dcd_data":/dcd_data/ ^
yanliang12/yan_sm_download:1.0.1

bash opensooq_download.sh &


####opensooq_download.sh####
while true; do
   python3 opensooq_download.py &
   sleep $[1 * 60]
done
####opensooq_download.sh####

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

today = datetime.datetime.now(pytz.timezone('Asia/Dubai'))
today = today.strftime("date%Y%m")

today_folder_page_html = '/dcd_data/opensooq/page_html/source=%s'%(today)
today_folder_page_list_html = '/dcd_data/opensooq/page_list_html/source=date%s'%(today)

try:
	os.makedirs(today_folder_page_html)
except Exception as e:
	print(e)

try:
	os.makedirs(today_folder_page_list_html)
except Exception as e:
	print(e)

#######

list_page_urls = []

for i in range(1,20):
	list_page_url = 'https://ae.opensooq.com/en/find?have_images=&allposts=&onlyPremiumAds=&onlyDonation=&onlyPrice=&onlyUrgent=&onlyShops=&onlyMemberships=&onlyBuynow=&memberId=&sort=record_posted_date.desc&term=&cat_id=&scid=17&city=Abu+Dhabi&page={}&per-page=30'.format(i)
	list_page_urls.append({'page_url':list_page_url,})

list_page_urls_df = pandas.DataFrame(list_page_urls)
list_page_urls_df.to_json(
	'list_page_url.json',
	orient = 'records',
	lines = True,
	)

#######


yan_web_page_batch_download.args.input_json = 'list_page_url.json'
yan_web_page_batch_download.args.local_path = today_folder_page_list_html
yan_web_page_batch_download.args.sleep_second_per_page = None
yan_web_page_batch_download.args.redicrete = 'true'
yan_web_page_batch_download.args.page_regex = 'DOCTYPE'
yan_web_page_batch_download.args.overwrite = 'true'
yan_web_page_batch_download.main()


###########

re_page_url = re.compile(r'href\=\"(?P<page_url>\/en\/search\/\d+\/[^\\\/]*?)\"', flags=re.DOTALL)

page_url_prefix = 'https://ae.opensooq.com'

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

'''
page = pandas.read_json(
	'/dcd_data/opensooq/page_list_html/source=datedate202110/598c237adb3842de16faa8f4edea6f79.json',
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

parsing_from_list_to_url = udf(
	parsing_from_list_to_url,
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
today_page_url.show(10, False)
today_page_url.count()

###########

yan_web_page_batch_download.args.input_json = 'today_page_url'
yan_web_page_batch_download.args.local_path = today_folder_page_html
yan_web_page_batch_download.args.sleep_second_per_page = None
yan_web_page_batch_download.args.redicrete = 'true'
yan_web_page_batch_download.args.page_regex = 'DOCTYPE'
yan_web_page_batch_download.args.overwrite = None
yan_web_page_batch_download.main()

###########opensooq_download.py###########