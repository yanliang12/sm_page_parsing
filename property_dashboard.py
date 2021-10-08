##########property_dashboard.py###########

'''

docker run -it ^
-p 0.0.0.0:6794:6794 ^
-p 0.0.0.0:3974:3974 ^
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

import bayut_parsing
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


############################################

'''

mv /jessica/elasticsearch-6.7.1 /jessica/elasticsearch_property
cp -r /jessica/elasticsearch_property /dcd_data/es/

'''

es_session = jessica_es.start_es(
	es_path = "/dcd_data/es/elasticsearch_property",
	es_port_number = "6794")

jessica_es.start_kibana(
	kibana_path = '/jessica/kibana-6.7.1-linux-x86_64',
	kibana_port_number = "3974",
	es_port_number = "6794",
	)

'''
http://localhost:3974/app/kibana#/dashboard/84bd18c0-fdc2-11eb-bd1a-8f30bb208bae

DELETE property

PUT property
{
  "mappings": {
  "doc":{
	    "properties": {
	      "property__property_geo_location__geo_point": {"type": "geo_point"},
	      "parsed.property__property_post_date__post_date": {"type": "text"},
	      "property__property_post_date__post_date": {"type": "date"}
	    }
   }
  }
}

'''

json_folder = '/dcd_data/temp/property_es_data'

files = [join(json_folder, f) 
	for f in listdir(json_folder) 
	if isfile(join(json_folder, f))
	and bool(re.search(r'.+\.json$', f))]

for f in files:
	try:
		print('ingesting %s'%(f))
		df = jessica_es.ingest_json_to_es_index(
			json_file = f,
			es_index = "property",
			es_session = es_session,
			document_id_feild = 'page_url_hash',
			)
		print(df)
	except Exception as e:
		print(e)


##########property_dashboard.py###########