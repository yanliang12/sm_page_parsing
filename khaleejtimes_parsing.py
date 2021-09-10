###########khaleejtimes_parsing.py###########
import re
import datetime

###########

re_page_url = re.compile(r'\<h2\>\<a href\=\"(?P<page_url>[^\"]*?)\"\>', flags=re.DOTALL)
page_url_prefix = 'https://www.khaleejtimes.com'

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


###########khaleejtimes_parsing.py###########