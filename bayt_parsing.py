###########bayt_parsing.py###########
import re
import datetime

###########

re_page_url = re.compile(r'\<a data\-js\-aid\=\"jobID\" data\-js\-link href\=\"(?P<page_url>\/en\/uae\/jobs\/[^\\\/]*?\/)\" data\-job\-id', flags=re.DOTALL)
page_url_prefix = 'https://www.bayt.com'

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

###########

'''

import pandas

page = pandas.read_json(
	'/dcd_data/bayt/page_list_html/source=date20210904/69b55bc8f0286a8990108a08c56390a9.json',
	orient = 'records',
	lines = True,
	)

page_html = page['page_html'][0]
page_url = page['page_url'][0]

for e in parsing_from_list_to_url(
	page_html,
	page_url,
	):
	print(e)

print(page_url)

'''
###########bayt_parsing.py###########