###########job_dashboard.py###########
'''

docker run -it ^
-p 0.0.0.0:9364:9364 ^
-p 0.0.0.0:5311:5311 ^
-v "E:\dcd_data":/dcd_data/ ^
yanliang12/yan_sm_download:1.0.1 

python3 job_processing.py 

python3 job_dashboard.py & 


'''

#######

import os
import re
import time

import jessica_es
from os import listdir
from os.path import isfile, join, exists

#######

'''
mv /jessica/elasticsearch-6.7.1 /jessica/elasticsearch_job
cp -r /jessica/elasticsearch_job /dcd_data/es/
'''

es_session = jessica_es.start_es(
	es_path = "/dcd_data/es/elasticsearch_job",
	es_port_number = "9364")

'''
http://localhost:9364
'''

while True:
	jessica_es.start_kibana(
		kibana_path = '/jessica/kibana-6.7.1-linux-x86_64',
		kibana_port_number = "5311",
		es_port_number = "9364",
		)
	time.sleep(60*60)


'''
http://localhost:5311
http://localhost:9364/job/_search?pretty=true

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


{
  "exists": {
    "field": "job__job_company_geo_point__geo_point"
  }
}

'''

json_folder = '/dcd_data/temp/job_es_data'
files = [join(json_folder, f) 
	for f in listdir(json_folder) 
	if isfile(join(json_folder, f))
	and bool(re.search(r'.+\.json$', f))]

for f in files:
	try:
		print('ingesting %s'%(f))
		df = jessica_es.ingest_json_to_es_index(
			json_file = f,
			es_index = "job",
			es_session = es_session,
			document_id_feild = 'page_url_hash',
			)
		print(df)
	except Exception as e:
		print(e)


###########job_dashboard.py###########