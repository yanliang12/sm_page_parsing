'''
https://www.indeed.com/uae/jobs/city/abu-dhabi

docker run -it ^
-p 0.0.0.0:9364:9364 ^
-p 0.0.0.0:5311:5311 ^
-v "E:\dcd_data":/dcd_data/ ^
yanliang12/yan_dcd:1.0.1 


####indeed_schedule.sh####
while true; do
   python3 indeed.py
   sleep $[60 * 30]
done
####indeed_schedule.sh####

bash indeed_schedule.sh &

'''

#######

import os
import re
import pandas
import pytz
import datetime
import jessica_es
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

#######

'''

ingest the data to es to build the dashboard

mv /jessica/elasticsearch-6.7.1 /jessica/elasticsearch_job
cp -r /jessica/elasticsearch_job /dcd_data/es/

'''

es_session = jessica_es.start_es(
	es_path = "/dcd_data/es/elasticsearch_job",
	es_port_number = "9364")

'''
http://localhost:9364
'''

jessica_es.start_kibana(
	kibana_path = '/jessica/kibana-6.7.1-linux-x86_64',
	kibana_port_number = "5311",
	es_port_number = "9364",
	)


'''
http://localhost:5311

DELETE job

PUT job
{
  "mappings": {
  "doc":{
	    "properties": {
	      "job__job_company_geo_point__geo_point": {"type": "geo_point"}
	    }
   }
  }
}

'''

#######


first_page_url = 'https://ae.indeed.com/jobs?l=Abu+Dhabi&sort=date'
#first_page_url = 'https://ae.indeed.com/jobs?q=khalifa+city&l=Abu+Dhabi&sort=date'

'''
create today's folders
'''

'''
for demo

https://www.md5hashgenerator.com/

today_folder_job_list_page = '/dcd_data/indeed/demo_list_page'
today_folder_job_page = '/dcd_data/indeed/demo_page'

'''


'''
for daily job
'''

today = datetime.datetime.now(pytz.timezone('Asia/Dubai'))
today = today.strftime("%Y%m%d")

today_folder_job_page = '/dcd_data/indeed/job_page/source=date%s'%(today)
today_folder_job_list_page = '/dcd_data/indeed/job_list_page/source=date%s'%(today)

try:
	os.makedirs(today_folder_job_page)
except Exception as e:
	print(e)

try:
	os.makedirs(today_folder_job_list_page)
except Exception as e:
	print(e)

'''
get the last 100 jobs
'''

list_page_urls = [
{'page_url':first_page_url,}
]

for i in range(10,110,10):
	list_page_url = '%s&start=%d'%(first_page_url,i)
	list_page_urls.append({'page_url':list_page_url,})

list_page_urls_df = pandas.DataFrame(list_page_urls)
list_page_urls_df.to_json(
	'/dcd_data/indeed/indeed_job_list_page_url.json',
	orient = 'records',
	lines = True,
	)

'''
download today's list page
'''

yan_web_page_batch_download.args.input_json = '/dcd_data/indeed/indeed_job_list_page_url.json'
yan_web_page_batch_download.args.local_path = today_folder_job_list_page
yan_web_page_batch_download.args.sleep_second_per_page = '10'
yan_web_page_batch_download.args.page_regex = 'DOCTYPE'
yan_web_page_batch_download.args.overwrite = 'true'
yan_web_page_batch_download.main()

'''
parsing the job page url 
'''

parsing_from_list_to_url = udf(
	indeed_parsing.parsing_from_list_to_url,
	ArrayType(MapType(StringType(), StringType())))

sqlContext.read.json(today_folder_job_list_page)\
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
today_page_url.count()

'''
download the job pages
'''
yan_web_page_batch_download.args.input_json = 'today_page_url'
yan_web_page_batch_download.args.local_path = today_folder_job_page
yan_web_page_batch_download.args.sleep_second_per_page = '5'
yan_web_page_batch_download.args.page_regex = 'DOCTYPE'
yan_web_page_batch_download.args.overwrite = None
yan_web_page_batch_download.main()

'''
parsing the page of jobs
'''

#indeed_job_page = sqlContext.read.json('/dcd_data/indeed/job_page')
indeed_job_page = sqlContext.read.json(today_folder_job_page)

udf_job_page_parsing = udf(
	indeed_parsing.job_page_parsing,
	ArrayType(MapType(StringType(), StringType())))

indeed_job_page.withColumn(
	'parsed',
	udf_job_page_parsing(
		'page_html',
		'page_url'
		)
	).drop('page_html')\
	.write.mode('Overwrite').json('indeed_job_page_parsed')

'''
cp -r indeed_job_page_parsed /dcd_data/temp/
cp -r /dcd_data/temp/indeed_job_page_parsed ./
'''

'''
find the geo-location of employers
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
sqlContext.sql(u"""
	SELECT * FROM company_geo_location
	WHERE company = "CIMS Medical Recruitment"
	""").show()
'''

sqlContext.read.json('indeed_job_page_parsed').registerTempTable('indeed_job_page_parsed')
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
	SELECT DISTINCT n.page_url_hash, 
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

sqlContext.read.json('indeed_job_page_parsed').registerTempTable('indeed_job_page_parsed')

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
find the post data from crawling data and post duration
'''

udf_post_date = udf(
	indeed_parsing.generate_post_date_from_crawling_date_and_post_duration,
	StringType())

sqlContext.read.json('indeed_job_page_parsed').registerTempTable('indeed_job_page_parsed')
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
	""").write.mode('Overwrite').parquet('job__job_post_date__date')

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

sqlContext.read.json('indeed_job_page_parsed').registerTempTable('indeed_job_page_parsed')
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

sqlContext.read.parquet('job__job_contract_length__contract_length').registerTempTable('job__job_contract_length__contract_length')
sqlContext.read.parquet('job__job_post_date__date').registerTempTable('job__job_post_date__date')
sqlContext.read.parquet('job__job_company_geo_point__geo_point').registerTempTable('job__job_company_geo_point__geo_point')
sqlContext.read.parquet('salary_amount1').registerTempTable('salary_amount1')

sqlContext.read.json('indeed_job_page_parsed').registerTempTable('indeed_job_page_parsed')

sqlContext.sql(u"""
	SELECT j.page_url_hash AS partition_id,
	j.*,
	p.job__job_post_date__date,
	g.job__job_company_geo_point__geo_point,
	s.salary_amount,
	s.salary_frequency,
	c.job__job_contract_length__contract_length,
	c.contract_length_number,
	c.contract_length_unit
	FROM indeed_job_page_parsed AS j
	LEFT JOIN job__job_post_date__date AS p ON p.page_url_hash = j.page_url_hash
	LEFT JOIN job__job_company_geo_point__geo_point AS g ON g.page_url_hash = j.page_url_hash
	LEFT JOIN salary_amount1 AS s ON s.page_url_hash = j.page_url_hash
	LEFT JOIN job__job_contract_length__contract_length AS c ON c.page_url_hash = j.page_url_hash
	""").write.mode('Overwrite').json('es_data')

'''

sqlContext.read.json('es_data').registerTempTable('es_data')

sqlContext.sql(u"""
	SELECT count(*),
	count(distinct page_url_hash)
	FROM es_data
	""").show()


sqlContext.sql(u"""
	SELECT count(*),
	count(distinct page_url_hash)
	FROM es_data
	""").show()


+--------+-----------------------------+
|count(1)|count(DISTINCT page_url_hash)|
+--------+-----------------------------+
|     954|                          426|
+--------+-----------------------------+

{
  "exists": {
    "field": "job__job_company_geo_point__geo_point"
  }
}

'''

json_folder = 'es_data'
files = [join(json_folder, f) 
	for f in listdir(json_folder) 
	if isfile(join(json_folder, f))
	and bool(re.search(r'.+\.json$', f))]


for f in files:
	try:
		print('ingesting %s'%(f))
		df = ingest_json_to_es_index(
			json_file = f,
			es_index = "job",
			es_session = es_session,
			document_id_feild = 'page_url_hash',
			)
		print(df)
	except Exception as e:
		print(e)

'''
http://localhost:9364/job/_search?pretty=true
'''