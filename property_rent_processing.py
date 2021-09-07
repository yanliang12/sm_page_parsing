'''

docker run -it ^
-v "E:\dcd_data":/dcd_data/ ^
yanliang12/yan_dcd:1.0.1 

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

#######

import pyspark
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

sc = SparkContext("local")
sqlContext = SparkSession.builder.getOrCreate()


'''
parsing the page
'''

#########

today_folder_page_html = '/dcd_data/propertyfinder/page_html'
parsed_json_path = '/dcd_data/temp/property_parsed/website=www.propertyfinder.ae'

page_html = sqlContext.read.json(today_folder_page_html)

udf_page_parsing = udf(
	propertyfinder_parsing.page_parsing,
	ArrayType(MapType(StringType(), StringType())))

page_html.withColumn(
	'parsed',
	udf_page_parsing(
		'page_html',
		'page_url'
		)
	).drop('page_html')\
	.write.mode('Overwrite').json(parsed_json_path)

#########

today_folder_page_html = '/dcd_data/dubizzle/property_page'
parsed_json_path = '/dcd_data/temp/property_parsed/website=website=abudhabi.dubizzle.com'

page_html = sqlContext.read.json(today_folder_page_html)

udf_property_page_parsing = udf(
	dubizzle_parsing.page_parsing,
	ArrayType(MapType(StringType(), StringType())))

page_html.withColumn(
	'parsed',
	udf_property_page_parsing(
		'page_html',
		'page_url'
		)
	).drop('page_html')\
	.write.mode('Overwrite').json(parsed_json_path)


############################################


parsed_json_path = '/dcd_data/temp/property_parsed'
sqlContext.read.json(parsed_json_path).registerTempTable('dubizzle_property_page_parsed')

'''
produce the geo_point
'''
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
