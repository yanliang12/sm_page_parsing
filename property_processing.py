############property_processing.py################
'''

docker run -it ^
-v "E:\dcd_data":/dcd_data/ ^
yanliang12/yan_sm_download:1.0.1

python3 property_processing.py

python3 property_dashboard.py &

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

import dubizzle_parsing
import propertyfinder_parsing
import bayut_parsing

#######

import pyspark
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

sc = SparkContext("local")
sqlContext = SparkSession.builder.getOrCreate()

#####

today = datetime.datetime.now(pytz.timezone('Asia/Dubai'))
today = today.strftime("date%Y%m%d")

#################################################


'''
parsing the pages
'''

####dubizzle#####

today_folder_page_html = '/dcd_data/dubizzle/page_html/source=%s'%(today)
parsed_json_path = '/dcd_data/temp/property_parsed/website=abudhabi.dubizzle.com'

print('load the pages of {}'.format(today_folder_page_html))

dubizzle_page_html = sqlContext.read.json(today_folder_page_html)
dubizzle_page_html.write.mode('Overwrite').parquet('dubizzle_page_html/source=%s'%(today))

print('processing the pages of {}'.format(today_folder_page_html))

sqlContext.read.parquet('dubizzle_page_html').registerTempTable('dubizzle_page_html')

#####

sqlContext.udf.register(
	"page_parsing", 
	dubizzle_parsing.property_page_parsing,
	ArrayType(MapType(StringType(), StringType())))

sqlContext.sql(u"""
	select *,
	page_parsing(page_html, page_url) as parsed
	from dubizzle_page_html
	""").drop('page_html').write.mode('Overwrite').parquet('dubizzle_page_html_parsed')

sqlContext.read.parquet('dubizzle_page_html_parsed').registerTempTable('dubizzle_page_html_parsed')

sqlContext.sql(u"""
	select * from dubizzle_page_html_parsed where parsed is not null
	""").write.mode('Overwrite').json(parsed_json_path)


print('processing of {} is completed'.format(today_folder_page_html))



#####propertyfinder####

today_folder_page_html = '/dcd_data/propertyfinder/page_html/source=%s'%(today)
parsed_json_path = '/dcd_data/temp/property_parsed/website=www.propertyfinder.ae'

print('load the pages of {}'.format(today_folder_page_html))

sqlContext.read.json(today_folder_page_html).write.mode('Overwrite').parquet('propertyfinder_page_html/source=%s'%(today))

print('processing the pages of {}'.format(today_folder_page_html))

sqlContext.read.parquet('propertyfinder_page_html').registerTempTable('propertyfinder_page_html')

sqlContext.udf.register(
	"page_parsing", 
	propertyfinder_parsing.page_parsing,
	ArrayType(MapType(StringType(), StringType())))

sqlContext.sql(u"""
	select *,
	page_parsing(page_html, page_url) as parsed
	from propertyfinder_page_html
	""").drop('page_html').write.mode('Overwrite').json(parsed_json_path)

print('processing of {} is completed'.format(today_folder_page_html))

#####bayut####

today_folder_page_html = '/dcd_data/bayut/page_html/source=%s'%(today)
parsed_json_path = '/dcd_data/temp/property_parsed/website=www.bayut.com'

print('load the pages of {}'.format(today_folder_page_html))

sqlContext.read.json(today_folder_page_html).write.mode('Overwrite').parquet('bayut_page_html/source=%s'%(today))

print('processing the pages of {}'.format(today_folder_page_html))

sqlContext.read.parquet('bayut_page_html').registerTempTable('bayut_page_html')

sqlContext.udf.register(
	"page_parsing", 
	bayut_parsing.page_parsing,
	ArrayType(MapType(StringType(), StringType())))

sqlContext.sql(u"""
	select *,
	page_parsing(page_html, page_url) as parsed
	from bayut_page_html
	""").drop('page_html').write.mode('Overwrite').json(parsed_json_path)

print('processing of {} is completed'.format(today_folder_page_html))


#################################################

'''
extract the numbers and geo-points, and date
'''

print('enriching the parsed pages')

parsed_json_path = '/dcd_data/temp/property_parsed'
sqlContext.read.json(parsed_json_path).registerTempTable('page_parsed')


'''
produce the geo_point
'''
sqlContext.sql(u"""
	SELECT DISTINCT page_url_hash,
	parsed.property__property_geo_location__geo_point
	FROM (
	SELECT page_url_hash,
	EXPLODE(parsed) AS parsed
	FROM page_parsed
	) AS temp
	WHERE parsed.property__property_geo_location__geo_point IS NOT NULL 
	AND page_url_hash IS NOT NULL
	""").write.mode('Overwrite').parquet('property__property_geo_location__geo_point')

'''
find the post_data 
'''

month_mapping = {
	"january":1,
	"february":2,
	"march":3,
	"april":4,
	"may":5,
	"june":6,
	"july":7,
	"august":8,
	"september":9,
	"october":10,
	"november":11,
	"december":12,
	}

def date_processing(input):
	try:
		return re.search(r'^\d{4}\-\d{1,2}\-\d{1,2}$', input).group()
	except:
		pass
	try:
		date = re.search(r'^(?P<month>[A-z]+) (?P<day>\d+)[a-z]{2}\, (?P<year>\d{4})$', input).groupdict()
		month = month_mapping[date['month'].lower()]
		day = int(date['day'])
		year = int(date['year'])
		return '%04d-%02d-%02d'%(year, month, day)
	except:
		pass
	try:
		date = re.search(r'^(?P<month>[A-z]+) (?P<day>\d+)\, (?P<year>\d{4})$', input).groupdict()
		month = month_mapping[date['month'].lower()]
		day = int(date['day'])
		year = int(date['year'])
		return '%04d-%02d-%02d'%(year, month, day)
	except:
		pass
	return None

'''
input = 'October 7th, 2021'
input = 'October 7, 2021'
input = '2021-05-20'
date_processing(input)
'''

sqlContext.udf.register(
	"date_processing", 
	date_processing,
	StringType())

sqlContext.sql(u"""
	SELECT DISTINCT page_url_hash,
	date_processing(parsed.property__property_post_date__post_date) as property__property_post_date__post_date
	FROM (
	SELECT page_url_hash,
	EXPLODE(parsed) AS parsed
	FROM page_parsed
	) AS temp
	WHERE parsed.property__property_post_date__post_date IS NOT NULL 
	AND page_url_hash IS NOT NULL
	""").write.mode('Overwrite').parquet('property__property_post_date__post_date')

#######post date#######


def amount_extraction(
	size_str,
	):
	try:
		size = re.search(r'[\d\,\.]+', size_str).group()
		size = re.sub(r'[\,]+', '', size)
		size = float(size)
		return size
	except:
		return None

sqlContext.udf.register(
	"amount_extraction", 
	amount_extraction,
	FloatType())

###

def unit_extraction(
	size_str,
	):
	try:
		return re.search(r'[A-z]+', size_str).group()
	except:
		return None


sqlContext.udf.register(
	"unit_extraction", 
	unit_extraction,
	StringType())

###

sqlContext.sql(u"""
	SELECT DISTINCT 
	page_url_hash,
	amount_extraction(parsed.property__property_rental_price_amount__number) AS property__property_rental_price_amount__number
	FROM (
	SELECT page_url_hash,
	EXPLODE(parsed) AS parsed
	FROM page_parsed
	) AS temp
	WHERE parsed.property__property_rental_price_amount__number IS NOT NULL 
	AND page_url_hash IS NOT NULL
	""").write.mode('Overwrite').parquet('property__property_rental_price_amount__number')

sqlContext.sql(u"""
	SELECT DISTINCT 
	page_url_hash,
	amount_extraction(parsed.property__property_sale_price_amount__number) AS property__property_sale_price_amount__number
	FROM (
	SELECT page_url_hash,
	EXPLODE(parsed) AS parsed
	FROM page_parsed
	) AS temp
	WHERE parsed.property__property_sale_price_amount__number IS NOT NULL 
	AND page_url_hash IS NOT NULL
	""").write.mode('Overwrite').parquet('property__property_sale_price_amount__number')

sqlContext.sql(u"""
	SELECT DISTINCT 
	page_url_hash,
	amount_extraction(parsed.property__property_size__size) AS property__property_size__size_amount,
	unit_extraction(parsed.property__property_size__size) AS property__property_size__size_unit
	FROM (
	SELECT page_url_hash,
	EXPLODE(parsed) AS parsed
	FROM page_parsed
	) AS temp
	WHERE parsed.property__property_size__size IS NOT NULL 
	AND page_url_hash IS NOT NULL
	""").write.mode('Overwrite').parquet('property__property_size__size')

sqlContext.sql(u"""
	SELECT DISTINCT 
	page_url_hash,
	amount_extraction(parsed.property__property_bedroom_number__bedroom_number) AS property__property_bedroom_number__bedroom_number
	FROM (
	SELECT page_url_hash,
	EXPLODE(parsed) AS parsed
	FROM page_parsed
	) AS temp
	WHERE parsed.property__property_bedroom_number__bedroom_number IS NOT NULL 
	AND page_url_hash IS NOT NULL
	""").write.mode('Overwrite').parquet('property__property_bedroom_number__bedroom_number')


sqlContext.sql(u"""
	SELECT DISTINCT 
	page_url_hash,
	amount_extraction(parsed.property__property_bath_number__bath_number) AS property__property_bath_number__bath_number
	FROM (
	SELECT page_url_hash,
	EXPLODE(parsed) AS parsed
	FROM page_parsed
	) AS temp
	WHERE parsed.property__property_bath_number__bath_number IS NOT NULL 
	AND page_url_hash IS NOT NULL
	""").write.mode('Overwrite').parquet('property__property_bath_number__bath_number')


#################################################

'''
build the index data
'''
sqlContext.read.parquet('property__property_geo_location__geo_point').registerTempTable('property__property_geo_location__geo_point')
sqlContext.read.parquet('property__property_post_date__post_date').registerTempTable('property__property_post_date__post_date')
sqlContext.read.parquet('property__property_rental_price_amount__number').registerTempTable('property__property_rental_price_amount__number')
sqlContext.read.parquet('property__property_sale_price_amount__number').registerTempTable('property__property_sale_price_amount__number')
sqlContext.read.parquet('property__property_size__size').registerTempTable('property__property_size__size')
sqlContext.read.parquet('property__property_bedroom_number__bedroom_number').registerTempTable('property__property_bedroom_number__bedroom_number')
sqlContext.read.parquet('property__property_bath_number__bath_number').registerTempTable('property__property_bath_number__bath_number')

sqlContext.sql(u"""
	SELECT j.*,
	g.property__property_geo_location__geo_point,
	d.property__property_post_date__post_date,
	p.property__property_rental_price_amount__number,
	q.property__property_sale_price_amount__number,
	s.property__property_size__size_amount,
	s.property__property_size__size_unit,
	b.property__property_bedroom_number__bedroom_number,
	f.property__property_bath_number__bath_number
	FROM page_parsed AS j
	LEFT JOIN property__property_geo_location__geo_point AS g ON g.page_url_hash = j.page_url_hash
	LEFT JOIN property__property_post_date__post_date AS d ON d.page_url_hash = j.page_url_hash
	LEFT JOIN property__property_rental_price_amount__number AS p ON p.page_url_hash = j.page_url_hash
	LEFT JOIN property__property_sale_price_amount__number AS q ON q.page_url_hash = j.page_url_hash
	LEFT JOIN property__property_size__size AS s ON s.page_url_hash = j.page_url_hash
	LEFT JOIN property__property_bedroom_number__bedroom_number AS b ON b.page_url_hash = j.page_url_hash
	LEFT JOIN property__property_bath_number__bath_number AS f ON f.page_url_hash = j.page_url_hash
	""").write.mode('Overwrite').json('/dcd_data/temp/property_es_data')

print('enrichment is complete and the es data is ready')

############property_processing.py################