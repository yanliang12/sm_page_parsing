#########crunchbase_parsing.py#########

import re

re_page_url_link = [
	re.compile(r'class\=\"yuRUbf\"\>\<a href\=\"(?P<page_url>[^\"]*?crunchbase[^\"]*?)\" data', flags=re.DOTALL),
	]

def parsing_from_list_to_url(
	page_html,
	page_url,
	):
	output = []
	for r in re_page_url_link:
		for m in re.finditer(r,
			page_html):
			page_url1 = m.group('page_url')
			output.append({'page_url':page_url1})
	return output


'''
import pandas

page = pandas.read_json(
	'/kg_data/crunchbase/page_html/source=tsi/8d4a6370e43ae1a92948b26439bd4de8.json',
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
'''

re_page_url = [
	re.compile(r'\/person\/(?P<people__people_id__people_id>[^\\\/]*?)(\/|$)', flags=re.DOTALL),
]

re_profile = [
	re.compile(r'Gender\<.{0,800}title\=\"(?P<people__people_gender__gender>[^\"]*?)\"\>', flags=re.DOTALL),
	re.compile(r' aria\-label\=\"(?P<people__people_name__people_name>[^\"]*?)\" href\=\"\/person\/', flags=re.DOTALL),
	re.compile(r'Primary Organization.{0,1500}aria\-label\=\"(?P<organization__organization_name__organization_name>[^\"]*?)\" href\=\"\/organization\/(?P<organization__organization_id__organization_id>[^\"\/]*?)\"\>', flags=re.DOTALL),
	re.compile(r'Primary Job Title.{0,1000}ng\-star\-inserted\" title\=\"[^\"]*?\"\>(?P<people__people_primary_job_title__job_title>[^\<\>]*?)\</span\>\<\/field\-formatter\>', flags=re.DOTALL),
	]


def page_parsing(
	page_html,
	page_url,
	):
	output = []
	output.append({"people__people_page_url__url":
		page_url})
	######
	for r in re_page_url:
		for m in re.finditer(r, page_url):
			output.append(m.groupdict())
	######
	for r in re_profile:
		for m in re.finditer(r,
			page_html):
			output.append(m.groupdict())
	for e in output:
		if 'people__people_id__people_id' in e:
			output.append({"people":"cb:%s"%(e["people__people_id__people_id"])})
		if bool(re.search(r'^organization__', list(e.keys())[0])):
			organization = ' '.join([e[k] for k in e])			
			e['organization'] = organization
			output.append({'people__people_primary_organization__organization':organization})
	return output

'''

import pandas

page = pandas.read_json(
	'/kg_data/crunchbase/page_html/source=tsi/8d4a6370e43ae1a92948b26439bd4de8.json',
	orient = 'records',
	lines = True,
	)

page_html = page['page_html'][0]
page_url = page['page_url'][0]


output = page_parsing(
	page_html,
	page_url,
	)

for e in output:
	print(e)


print(page_url)

for m in re.finditer(
	r'Gender.*?Male.{0,100}', 
	page_html,
	):
	print(m.group())

'''

#########crunchbase_parsing.py#########