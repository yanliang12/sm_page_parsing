############news_processing.py################
'''

docker run -it ^
-p 0.0.0.0:6371:6371 ^
-p 0.0.0.0:6399:6399 ^
-v "E:\dcd_data":/dcd_data/ ^
yanliang12/yan_sm_download:1.0.1 

python3 news_processing.py &

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

####################

'''

rm -r /dcd_data/es/elasticsearch_khaleejtimes

mv /jessica/elasticsearch-6.7.1 /jessica/elasticsearch_news
cp -r /jessica/elasticsearch_news /dcd_data/es/

'''

es_session = jessica_es.start_es(
	es_path = "/dcd_data/es/elasticsearch_news",
	es_port_number = "6371")

jessica_es.start_kibana(
	kibana_path = '/jessica/kibana-6.7.1-linux-x86_64',
	kibana_port_number = "6399",
	es_port_number = "6371",
	)

'''

localhost:6399

DELETE news

PUT news
{
  "mappings": {
  "doc":{
	    "properties": {
	      "geo_point": {"type": "geo_point"},
	      "crawling_date": {"type": "date"},
	      "news_date": {"type": "date"}
	    }
   }
  }
}

'''

####################

today = datetime.datetime.now(pytz.timezone('Asia/Dubai'))
today = today - datetime.timedelta(days=1)
today = today.strftime("date%Y")

#################################################


'''
parsing the pages
'''

import gulfnews_parsing
import khaleejtimes_parsing
import thenationalnews_parsing

####khaleejtimes#####

today_folder_page_html = '/dcd_data/khaleejtimes/page_html/source=%s'%(today)
parsed_json_path = 'news_parsed/website=www.khaleejtimes.com'

print('load the pages of {}'.format(today_folder_page_html))

khaleejtimes_page_html = sqlContext.read.json(today_folder_page_html)
khaleejtimes_page_html.write.mode('Overwrite').parquet('khaleejtimes_page_html/source=%s'%(today))

print('processing the pages of {}'.format(today_folder_page_html))

sqlContext.read.parquet('khaleejtimes_page_html').registerTempTable('khaleejtimes_page_html')

#####

sqlContext.udf.register(
	"page_parsing", 
	khaleejtimes_parsing.page_parsing,
	ArrayType(MapType(StringType(), StringType())))

sqlContext.sql(u"""
	select *,
	page_parsing(page_html, page_url) as parsed
	from khaleejtimes_page_html
	""").drop('page_html').write.mode('Overwrite').parquet('khaleejtimes_page_html_parsed')

sqlContext.read.parquet('khaleejtimes_page_html_parsed').registerTempTable('khaleejtimes_page_html_parsed')

sqlContext.sql(u"""
	select * from khaleejtimes_page_html_parsed where parsed is not null
	""").write.mode('Overwrite').json(parsed_json_path)

print('processing of {} is completed'.format(today_folder_page_html))


####gulfnews#####

today_folder_page_html = '/dcd_data/gulfnews/page_html/source=%s'%(today)
parsed_json_path = 'news_parsed/website=gulfnews.com'

print('load the pages of {}'.format(today_folder_page_html))

gulfnews_page_html = sqlContext.read.json(today_folder_page_html)
gulfnews_page_html.write.mode('Overwrite').parquet('gulfnews_page_html/source=%s'%(today))

print('processing the pages of {}'.format(today_folder_page_html))

sqlContext.read.parquet('gulfnews_page_html').registerTempTable('gulfnews_page_html')

#####

sqlContext.udf.register(
	"page_parsing", 
	gulfnews_parsing.page_parsing,
	ArrayType(MapType(StringType(), StringType())))

sqlContext.sql(u"""
	select *,
	page_parsing(page_html, page_url) as parsed
	from gulfnews_page_html
	""").drop('page_html').write.mode('Overwrite').parquet('gulfnews_page_html_parsed')

sqlContext.read.parquet('gulfnews_page_html_parsed').registerTempTable('gulfnews_page_html_parsed')

sqlContext.sql(u"""
	select * from gulfnews_page_html_parsed where parsed is not null
	""").write.mode('Overwrite').json(parsed_json_path)

print('processing of {} is completed'.format(today_folder_page_html))

####thenationalnews#####

today_folder_page_html = '/dcd_data/thenationalnews/page_html/source=%s'%(today)
parsed_json_path = 'news_parsed/website=www.thenationalnews.com'

print('load the pages of {}'.format(today_folder_page_html))

thenationalnews_page_html = sqlContext.read.json(today_folder_page_html)
thenationalnews_page_html.write.mode('Overwrite').parquet('thenationalnews_page_html/source=%s'%(today))

print('processing the pages of {}'.format(today_folder_page_html))

sqlContext.read.parquet('thenationalnews_page_html').registerTempTable('thenationalnews_page_html')

#####

sqlContext.udf.register(
	"page_parsing", 
	page_parsing,
	ArrayType(MapType(StringType(), StringType())))

sqlContext.sql(u"""
	select *,
	page_parsing(page_html, page_url) as parsed
	from thenationalnews_page_html
	""").drop('page_html').write.mode('Overwrite').parquet('thenationalnews_page_html_parsed')

sqlContext.read.parquet('thenationalnews_page_html_parsed').registerTempTable('thenationalnews_page_html_parsed')

sqlContext.sql(u"""
	select * from thenationalnews_page_html_parsed where parsed is not null
	""").write.mode('Overwrite').json(parsed_json_path)

print('processing of {} is completed'.format(today_folder_page_html))

#################################################

'''
extract the numbers and geo-points, and date
'''

print('enriching the parsed pages')

parsed_json_path = 'news_parsed'
sqlContext.read.json(parsed_json_path).registerTempTable('page_parsed')

'''
produce the geo_point
'''

sqlContext.sql(u"""
	select website, count(*)
	from page_parsed
	group by website
	""").show()


#################################################

'''
build the index data
'''

news_es_data = 'news_es_data'

print('attaching the attributes to the main table')

sqlContext.sql(u"""
	SELECT p.*
	from page_parsed as p
	""").write.mode('Overwrite').json(news_es_data)

print('enrichment is complete and the es data is ready')

#############

print('ingesting data to es')

files = [join(news_es_data, f) 
	for f in listdir(news_es_data) 
	if isfile(join(news_es_data, f))
	and bool(re.search(r'.+\.json$', f))]

for f in files:
	try:
		print('ingesting %s'%(f))
		df = jessica_es.ingest_json_to_es_index(
			json_file = f,
			es_index = "news",
			es_session = es_session,
			document_id_feild = 'page_url_hash',
			)
		print(df)
	except Exception as e:
		print(e)

############news_processing.py################