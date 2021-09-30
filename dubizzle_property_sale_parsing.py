##############dubizzle_property_sale_parsing.py##############
import re
import datetime

###########

re_page_url = re.compile(
	r'\"(?P<page_url>https\:\/\/abudhabi\.dubizzle\.com\/property\-for\-sale\/[^\\\/]*?\/[^\\\/]*?\/\d+\/\d+\/\d+\/[^\\\/]*?\/)\"',
	flags=re.DOTALL)

page_url_prefix = ''

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

'''
import pandas

page = pandas.read_json(
	'/dcd_data/dubizzle/property_list_page/download_date=20210814/67496a5ee4510da70ef147179c2a532c.json',
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




##############dubizzle_property_sale_parsing.py##############