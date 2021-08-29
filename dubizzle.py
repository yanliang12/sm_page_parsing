'''
https://abudhabi.dubizzle.com/en/property-for-rent/residential/?sort=newest&page=500

docker run -it ^
-p 0.0.0.0:9377:9377 ^
-p 0.0.0.0:5794:5794 ^
-v "E:\dcd_data":/dcd_data/ ^
yanliang12/yan_dcd:1.0.1 

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

'''

ingest the data to es to build the dashboard

mv /jessica/elasticsearch-6.7.1 /jessica/elasticsearch_property_rent
cp -r /jessica/elasticsearch_property_rent /dcd_data/es/

'''

es_session = jessica_es.start_es(
	es_path = "/dcd_data/es/elasticsearch_property_rent",
	es_port_number = "9377")

jessica_es.start_kibana(
	kibana_path = '/jessica/kibana-6.7.1-linux-x86_64',
	kibana_port_number = "5794",
	es_port_number = "9377",
	)

'''
http://localhost:5794/app/kibana#/dashboard/84bd18c0-fdc2-11eb-bd1a-8f30bb208bae

DELETE property_rent

PUT property_rent
{
  "mappings": {
  "doc":{
	    "properties": {
	      "property__property_geo_location__geo_point": {"type": "geo_point"}
	    }
   }
  }
}

'''

#######

page_list_urls = []

for i in range(0,100):
	page_list_url = 'https://abudhabi.dubizzle.com/en/property-for-rent/residential/?sort=newest&page=%d'%(i)
	page_list_urls.append({
		'page_url':page_list_url
		})

page_list_urls = pandas.DataFrame(page_list_urls)
page_list_urls.to_json(
	'/dcd_data/dubizzle/dubizzle_property_rent_list_page_url.json',
	orient = 'records',
	lines = True,
	)

'''
create today's folders
'''

today = datetime.datetime.now(pytz.timezone('Asia/Dubai'))
today = today.strftime("%Y%m%d")

today_folder_property_page = '/dcd_data/dubizzle/property_page/source=date%s'%(today)
today_folder_property_list_page = '/dcd_data/dubizzle/property_list_page/source=date%s'%(today)

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
yan_web_page_batch_download.args.input_json = '/dcd_data/dubizzle/dubizzle_property_rent_list_page_url.json'
yan_web_page_batch_download.args.local_path = today_folder_property_list_page
yan_web_page_batch_download.args.curl_file = '/dcd_data/dubizzle/dubizzle_list_curl.sh'
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
today_page_url.count()
'''
2243
'''

'''
download the property pages
'''

yan_web_page_batch_download.args.input_json = 'today_page_url'
yan_web_page_batch_download.args.local_path = today_folder_property_page
yan_web_page_batch_download.args.curl_file = '/dcd_data/dubizzle/dubizzle_page_curl.sh'
yan_web_page_batch_download.args.sleep_second_per_page = '10'
yan_web_page_batch_download.args.page_regex = 'doctype'
yan_web_page_batch_download.args.overwrite = None
yan_web_page_batch_download.main()

'''
parsing the page of propertys

today_folder_property_page = '/dcd_data/dubizzle/property_page'

'''


dubizzle_property_page = sqlContext.read.json(today_folder_property_page)

parsed_json_path = '/dcd_data/temp/dubizzle_property_page_parsed'

udf_property_page_parsing = udf(
	dubizzle_parsing.page_parsing,
	ArrayType(MapType(StringType(), StringType())))

dubizzle_property_page.withColumn(
	'parsed',
	udf_property_page_parsing(
		'page_html',
		'page_url'
		)
	).drop('page_html')\
	.write.mode('Overwrite').json(parsed_json_path)

sqlContext.read.json(parsed_json_path).registerTempTable('dubizzle_property_page_parsed')

sqlContext.sql(u"""
	SELECT * FROM dubizzle_property_page_parsed
	""").printSchema()

'''
produce the geo_point
'''
sqlContext.read.json(parsed_json_path).registerTempTable('dubizzle_property_page_parsed')
sqlContext.sql(u"""
	SELECT DISTINCT page_url_hash,
	parsed.property__property_geo_location__geo_point
	FROM (
	SELECT page_url_hash,
	EXPLODE(parsed) AS parsed
	FROM dubizzle_property_page_parsed
	) AS temp
	WHERE parsed.property__property_geo_location__geo_point IS NOT NULL 
	AND page_url_hash IS NOT NULL
	""").write.mode('Overwrite').parquet('property__property_geo_location__geo_point')


'''
find the post_data 
'''
sqlContext.read.json(parsed_json_path).registerTempTable('dubizzle_property_page_parsed')
sqlContext.sql(u"""
	SELECT DISTINCT page_url_hash,
	parsed.property__property_post_date__post_date
	FROM (
	SELECT page_url_hash,
	EXPLODE(parsed) AS parsed
	FROM dubizzle_property_page_parsed
	) AS temp
	WHERE parsed.property__property_post_date__post_date IS NOT NULL 
	AND page_url_hash IS NOT NULL
	""").write.mode('Overwrite').parquet('property__property_post_date__post_date')

'''
rent price 
'''
sqlContext.read.json(parsed_json_path).registerTempTable('dubizzle_property_page_parsed')
sqlContext.sql(u"""
	SELECT DISTINCT page_url_hash,
	FLOAT(parsed.property__property_rental_price_amount__number) AS property__property_rental_price_amount__number
	FROM (
	SELECT page_url_hash,
	EXPLODE(parsed) AS parsed
	FROM dubizzle_property_page_parsed
	) AS temp
	WHERE parsed.property__property_rental_price_amount__number IS NOT NULL 
	AND page_url_hash IS NOT NULL
	""").write.mode('Overwrite').parquet('property__property_rental_price_amount__number')

'''
property size
'''
udf_property_size_amount_extraction = udf(
	dubizzle_parsing.property_size_amount_extraction,
	FloatType(),
	)

udf_property_size_unit_extraction = udf(
	dubizzle_parsing.property_size_unit_extraction,
	StringType(),
	)

sqlContext.read.json(parsed_json_path).registerTempTable('dubizzle_property_page_parsed')
sqlContext.sql(u"""
	SELECT DISTINCT page_url_hash,
	parsed.property__property_size__size
	FROM (
	SELECT page_url_hash,
	EXPLODE(parsed) AS parsed
	FROM dubizzle_property_page_parsed
	) AS temp
	WHERE parsed.property__property_size__size IS NOT NULL 
	AND page_url_hash IS NOT NULL
	""")\
	.withColumn(
	'property_size_amount', 
	udf_property_size_amount_extraction('property__property_size__size'))\
	.withColumn(
	'property_size_unit', 
	udf_property_size_unit_extraction('property__property_size__size'))\
	.write.mode('Overwrite').parquet('property__property_size__size')


'''
bed room number
'''

udf_bedroom_amount_extraction = udf(
	dubizzle_parsing.property_size_amount_extraction,
	FloatType(),
	)

sqlContext.read.json(parsed_json_path).registerTempTable('dubizzle_property_page_parsed')
sqlContext.sql(u"""
	SELECT DISTINCT page_url_hash,
	parsed.property__property_bedroom_number__bedroom_number
	FROM (
	SELECT page_url_hash,
	EXPLODE(parsed) AS parsed
	FROM dubizzle_property_page_parsed
	) AS temp
	WHERE parsed.property__property_bedroom_number__bedroom_number IS NOT NULL 
	AND page_url_hash IS NOT NULL
	""")\
	.withColumn(
	'property_bedroom_number', 
	udf_property_size_amount_extraction('property__property_bedroom_number__bedroom_number'))\
	.write.mode('Overwrite').parquet('property__property_bedroom_number__bedroom_number')

'''
build the index data
'''
sqlContext.read.parquet('property__property_post_date__post_date').registerTempTable('property__property_post_date__post_date')
sqlContext.read.parquet('property__property_geo_location__geo_point').registerTempTable('property__property_geo_location__geo_point')
sqlContext.read.parquet('property__property_rental_price_amount__number').registerTempTable('property__property_rental_price_amount__number')
sqlContext.read.parquet('property__property_size__size').registerTempTable('property__property_size__size')
sqlContext.read.parquet('property__property_bedroom_number__bedroom_number').registerTempTable('property__property_bedroom_number__bedroom_number')

sqlContext.read.json(parsed_json_path).registerTempTable('dubizzle_property_page_parsed')

sqlContext.sql(u"""
	SELECT j.page_url_hash AS partition_id,
	j.*,
	g.property__property_geo_location__geo_point,
	d.property__property_post_date__post_date,
	p.property__property_rental_price_amount__number,
	s.property_size_amount,
	s.property_size_unit,
	b.property_bedroom_number
	FROM dubizzle_property_page_parsed AS j
	LEFT JOIN property__property_geo_location__geo_point AS g ON g.page_url_hash = j.page_url_hash
	LEFT JOIN property__property_post_date__post_date AS d ON d.page_url_hash = j.page_url_hash
	LEFT JOIN property__property_rental_price_amount__number AS p ON p.page_url_hash = j.page_url_hash
	LEFT JOIN property__property_size__size AS s ON s.page_url_hash = j.page_url_hash
	LEFT JOIN property__property_bedroom_number__bedroom_number AS b ON b.page_url_hash = j.page_url_hash
	""").write.mode('Overwrite').json('es_data')

'''
ingest to es
'''

json_folder = 'es_data'
files = [join(json_folder, f) 
	for f in listdir(json_folder) 
	if isfile(join(json_folder, f))
	and bool(re.search(r'.+\.json$', f))]


for f in files:
	try:
		print('ingesting %s'%(f))
		df = jessica_es.ingest_json_to_es_index(
			json_file = f,
			es_index = "property_rent",
			es_session = es_session,
			document_id_feild = 'page_url_hash',
			)
		print(df)
	except Exception as e:
		print(e)

