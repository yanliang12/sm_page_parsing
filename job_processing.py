'''

docker run -it ^
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
import bayt_parsing
import indeed_parsing
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

############

page_html_json = '/dcd_data/indeed/job_page'
page_html = sqlContext.read.json(page_html_json)

udf_job_page_parsing = udf(
	indeed_parsing.job_page_parsing,
	ArrayType(MapType(StringType(), StringType())))

page_html.withColumn(
	'parsed',
	udf_job_page_parsing(
		'page_html',
		'page_url'
		)
	).drop('page_html')\
	.write.mode('Overwrite').json('/dcd_data/temp/job_parsed/website=ae.indeed.com')

############

page_html_json = '/dcd_data/bayt/page_html'
page_html = sqlContext.read.json(page_html_json)

udf_job_page_parsing = udf(
	bayt_parsing.page_parsing,
	ArrayType(MapType(StringType(), StringType())))

page_html.withColumn(
	'parsed',
	udf_job_page_parsing(
		'page_html',
		'page_url'
		)
	).drop('page_html')\
	.write.mode('Overwrite').json('/dcd_data/temp/job_parsed/website=www.bayt.com')

############

indeed_job_page_parsed = sqlContext.read.json('/dcd_data/temp/job_parsed')
indeed_job_page_parsed.registerTempTable('indeed_job_page_parsed')

'''
prepare the geo-point lookup table
'''

def geo_point_processing(
	input,
	):
	try:
		return re.sub(r'[^\d\,\.\-]+', '', input)
	except:
		return None

sqlContext.udf.register("geo_point_processing", geo_point_processing, StringType())

schema = StructType()\
	.add("company",StringType(),True)\
	.add("geo_point",StringType(),True)

sqlContext.read\
	.option('header', False)\
	.schema(schema)\
	.csv('/dcd_data/indeed/company_geo_point.csv/*.csv')\
	.registerTempTable('company_geo_location')

sqlContext.sql(u"""
	SELECT company, COLLECT_SET(
	geo_point_processing(geo_point))[0] AS geo_point
	FROM company_geo_location
	WHERE company IS NOT NULL AND geo_point IS NOT NULL
	GROUP BY company
	""").registerTempTable('company_geo_location')


'''
find the geo-location of employers
'''
sqlContext.sql(u"""
	SELECT 
	page_url_hash,
	crawling_date,
	parsed.job__job_company_name__company_name 
	FROM (
		SELECT 
		page_url_hash,
		crawling_date,
		EXPLODE(parsed) AS parsed
		FROM indeed_job_page_parsed
	) AS temp
	WHERE parsed.job__job_company_name__company_name IS NOT NULL
	""").write.mode('Overwrite').parquet('job__job_company_name__company_name')
sqlContext.read.parquet('job__job_company_name__company_name').registerTempTable('job__job_company_name__company_name')

sqlContext.sql(u"""
	SELECT DISTINCT 
	n.page_url_hash, 
	c.geo_point AS job__job_company_geo_point__geo_point
	FROM job__job_company_name__company_name as n,
	company_geo_location AS c
	WHERE c.company = n.job__job_company_name__company_name
	AND c.geo_point IS NOT NULL
	""").write.mode('Overwrite').parquet('job__job_company_geo_point__geo_point')

'''
find the contract duration from the parsed data
'''
udf_contract_number = udf(
	indeed_parsing.extract_contract_duration_number,
	ArrayType(IntegerType()))

udf_contract_unit = udf(
	indeed_parsing.extract_contract_duration_unit,
	ArrayType(StringType()))

sqlContext.sql(u"""
	SELECT DISTINCT
	page_url_hash,
	parsed.job__job_contract_length__contract_length 
	FROM (
	SELECT 
	page_url_hash,
	crawling_date,
	EXPLODE(parsed) AS parsed
	FROM indeed_job_page_parsed
	) AS temp
	WHERE parsed.job__job_contract_length__contract_length IS NOT NULL
	""")\
	.withColumn(
	'contract_length_number',
	udf_contract_number(
		'job__job_contract_length__contract_length'
		)
	)\
	.withColumn(
	'contract_length_unit',
	udf_contract_unit(
		'job__job_contract_length__contract_length'
		)
	).registerTempTable('job__job_contract_length__contract_length')

sqlContext.sql(u"""
	SELECT DISTINCT page_url_hash, 
	job__job_contract_length__contract_length,
	contract_length_number,
	contract_length_unit
	FROM job__job_contract_length__contract_length
	WHERE page_url_hash IS NOT NULL 
	AND (
		contract_length_number IS NOT NULL
		OR contract_length_unit IS NOT NULL
	)
	""").write.mode('Overwrite').parquet('job__job_contract_length__contract_length')


'''
indeed: find the post data from crawling data and post duration
'''

udf_post_date = udf(
	indeed_parsing.generate_post_date_from_crawling_date_and_post_duration,
	StringType())

sqlContext.sql(u"""
	SELECT 
	page_url_hash,
	crawling_date,
	parsed.job__job_post_duration__duration 
	FROM (
	SELECT 
	page_url_hash,
	crawling_date,
	EXPLODE(parsed) AS parsed
	FROM indeed_job_page_parsed
	) AS temp
	WHERE parsed.job__job_post_duration__duration IS NOT NULL
	""").withColumn(
	'job__job_post_date__date',
	udf_post_date(
		'crawling_date',
		'job__job_post_duration__duration'
		)
	).registerTempTable('job__job_post_date__date')

sqlContext.sql(u"""
	SELECT DISTINCT page_url_hash, job__job_post_date__date
	FROM job__job_post_date__date
	WHERE page_url_hash IS NOT NULL AND job__job_post_date__date IS NOT NULL
	""").write.mode('Overwrite').parquet('job__job_post_date__date_indeed')


'''
byat: crawling date to post date
'''

udf_generate_post_date = udf(
	bayt_parsing.generate_post_date,
	StringType())

sqlContext.sql(u"""
	SELECT 
	page_url_hash,
	crawling_date,
	parsed.job__job_post_month__month,
	parsed.job__job_post_day__day 
	FROM (
		SELECT 
		page_url_hash,
		crawling_date,
		EXPLODE(parsed) AS parsed
		FROM indeed_job_page_parsed
	) AS temp
	WHERE parsed.job__job_post_month__month IS NOT NULL
	OR parsed.job__job_post_day__day IS NOT NULL
	""").registerTempTable('job__job_post_month__month')

sqlContext.sql(u"""
	SELECT page_url_hash,
	COLLECT_SET(crawling_date)[0] AS crawling_date,
	COLLECT_SET(job__job_post_month__month)[0] AS job__job_post_month__month,
	COLLECT_SET(job__job_post_day__day)[0] AS job__job_post_day__day
	FROM job__job_post_month__month
	GROUP BY page_url_hash
	""").write.mode('Overwrite').parquet('job__job_post_day__day')

sqlContext.read.parquet('job__job_post_day__day')\
	.withColumn(
	'job__job_post_date__date',
	udf_generate_post_date(
		'job__job_post_month__month',
		'job__job_post_day__day',
		'crawling_date'
		)
	).registerTempTable('job__job_post_date__date1')

sqlContext.sql(u"""
	SELECT DISTINCT page_url_hash, job__job_post_date__date
	FROM job__job_post_date__date1
	WHERE page_url_hash IS NOT NULL AND job__job_post_date__date IS NOT NULL
	""").write.mode('Overwrite').parquet('job__job_post_date__date_bayt')


'''
calculate the salary of reach job
'''

udf_salary_amount = udf(
	indeed_parsing.extract_salary_amount_from_salary_description,
	ArrayType(FloatType())
	)

udf_salary_frequency = udf(
	indeed_parsing.extract_salary_frequency_from_salary_description,
	ArrayType(StringType())
	)

sqlContext.sql(u"""
	SELECT 
	page_url_hash,
	crawling_date,
	parsed.job__job_salary__salary 
	FROM (
	SELECT 
	page_url_hash,
	crawling_date,
	EXPLODE(parsed) AS parsed
	FROM indeed_job_page_parsed
	) AS temp
	WHERE parsed.job__job_salary__salary IS NOT NULL
	""").write.mode('Overwrite').parquet('job__job_salary__salary')

job__job_salary__salary = sqlContext.read.parquet('job__job_salary__salary')
job__job_salary__salary = job__job_salary__salary\
	.withColumn('salary_amount',
	udf_salary_amount('job__job_salary__salary')
	)\
	.withColumn('salary_frequency',
	udf_salary_frequency('job__job_salary__salary')
	)
job__job_salary__salary.write.mode('Overwrite').parquet('salary_amount')

sqlContext.read.parquet('salary_amount').registerTempTable('salary_amount')
sqlContext.sql(u"""
	SELECT DISTINCT page_url_hash, salary_amount, salary_frequency
	FROM salary_amount
	WHERE salary_amount IS NOT NULL OR salary_frequency IS NOT NULL
	""").write.mode('Overwrite').parquet('salary_amount1')

'''
merger all to a json folder
'''

sqlContext.read.parquet('job__job_post_date__date_bayt').registerTempTable('job__job_post_date__date_bayt')
sqlContext.read.parquet('job__job_post_date__date_indeed').registerTempTable('job__job_post_date__date_indeed')
sqlContext.read.parquet('job__job_contract_length__contract_length').registerTempTable('job__job_contract_length__contract_length')
sqlContext.read.parquet('job__job_company_geo_point__geo_point').registerTempTable('job__job_company_geo_point__geo_point')
sqlContext.read.parquet('salary_amount1').registerTempTable('salary_amount1')

sqlContext.sql(u"""
	SELECT j.page_url_hash AS partition_id,
	j.*,
	CASE 
		WHEN p.job__job_post_date__date IS NOT NULL THEN p.job__job_post_date__date
		WHEN i.job__job_post_date__date IS NOT NULL THEN i.job__job_post_date__date
		ELSE NULL
	END AS job__job_post_date__date,
	g.job__job_company_geo_point__geo_point,
	s.salary_amount,
	s.salary_frequency,
	c.job__job_contract_length__contract_length,
	c.contract_length_number,
	c.contract_length_unit
	FROM indeed_job_page_parsed AS j
	LEFT JOIN job__job_post_date__date_bayt AS p ON p.page_url_hash = j.page_url_hash
	LEFT JOIN job__job_post_date__date_indeed AS i ON i.page_url_hash = j.page_url_hash
	LEFT JOIN job__job_company_geo_point__geo_point AS g ON g.page_url_hash = j.page_url_hash
	LEFT JOIN salary_amount1 AS s ON s.page_url_hash = j.page_url_hash
	LEFT JOIN job__job_contract_length__contract_length AS c ON c.page_url_hash = j.page_url_hash
	""").write.mode('Overwrite').json('/dcd_data/temp/job_es_data')
