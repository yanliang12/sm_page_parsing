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

###########

re_page_url_attributes = [
	re.compile(r'jobs\/(?P<job__job_indeed_id__job_id>[^\\\/]*?)\/', flags=re.DOTALL),
]

attribute_name_needs_clearning = [
'job__job_description__text',
'job__job_required_license_certification__license_certification',
]

re_residence_blcok = re.compile(r'\<dt\>Residence Location\<\/dt\>\s*\<dd\>[^\<\>]*?\<\/dd\>', flags=re.DOTALL)

re_residence_attributes = [
	re.compile(r'\>(?P<job__job_required_residence_location__residence_location>[^\<\>\;]*?)\;', flags=re.DOTALL),
	re.compile(r'\; (?P<job__job_required_residence_location__residence_location>[^\<\>\;]*?)\;', flags=re.DOTALL),
	re.compile(r'\; (?P<job__job_required_residence_location__residence_location>[^\<\>\;]*?)\<', flags=re.DOTALL),
]


re_nationality_blcok = re.compile(r'\<dt\>Nationality\<\/dt\>\s*\<dd\>[^\<\>]*?\<\/dd\>', flags=re.DOTALL)

re_nationality_attributes = [
	re.compile(r'\>(?P<job__job_required_nationality__nationality>[^\<\>\;]*?)\;', flags=re.DOTALL),
	re.compile(r'\; (?P<job__job_required_nationality__nationality>[^\<\>\;]*?)\;', flags=re.DOTALL),
	re.compile(r'\; (?P<job__job_required_nationality__nationality>[^\<\>\;]*?)\<', flags=re.DOTALL),
]

re_skill_blcok = re.compile(r'\<b\>Skills\<\\\/b\>\<\\/p\>[^\"]*?\"\,', flags=re.DOTALL)

re_skill_attributes = [
	re.compile(r'\<p\>(?P<job__job_required_skill__skill>[^\<\>\;]*?)\<\\\/p\>', flags=re.DOTALL),
]

re_job_attributes = [
	re.compile(r'\<h2 class\=\"h5\"\>Job Description\<\/h2\>\s*\<div\>(?P<job__job_description__text>.*?)\<\/div\>', flags=re.DOTALL),
	re.compile(r'\<h2 class=\"h5\"\>Education\<\/h2\>\s*\<p\>(?P<job__job_required_education_field__field>[^\<\>]*?)\<\/p\>', flags=re.DOTALL),
	re.compile(r'\<dt\>Age\<\/dt\>\s*\<dd\>(?P<job__job_required_age__age>[^\<\>]*?)\<\/dd\>', flags=re.DOTALL),
	re.compile(r'\<dt\>Degree\<\/dt\>\s*\<dd\>(?P<job__job_required_education__education>[^\<\>]*?)\<\/dd\>', flags=re.DOTALL),
	re.compile(r'\<dt\>Gender\<\/dt\>\s*\<dd\>(?P<job__job_required_gender__gender>[^\<\>]*?)\<\/dd\>', flags=re.DOTALL),
	re.compile(r'\<dt\>Years of Experience\<\/dt\>\s*\<dd\>(?P<job__job_required_experience__experience>[^\<\>]*?)\<\/dd\>', flags=re.DOTALL),
	re.compile(r'\<dt\>Career Level\<\/dt\>\s*\<dd\>(?P<job__job_career_level__career_level>[^\<\>]*?)\<\/dd\>', flags=re.DOTALL),
	re.compile(r'\<dt\>Number of Vacancies\<\/dt\>\s*\<dd\>(?P<job__job_vacancy_number__vacancy_number>[^\<\>]*?)\<\/dd\>', flags=re.DOTALL),
	re.compile(r'\<dt\>Monthly Salary Range\<\/dt\>\s*\<dd\>(?P<job__job_salary__salary>[^\<\>]*?)\<\/dd\>', flags=re.DOTALL),
	re.compile(r'\<dt\>Employment Type\<\/dt\>\s*\<dd\>(?P<job__job_type__job_type>[^\<\>]*?)\<\/dd\>', flags=re.DOTALL),
	re.compile(r'\<dt\>Job Role\<\/dt\>\s*\<dd\>(?P<job__job_category__job_category>[^\<\>]*?)\<\/dd\>', flags=re.DOTALL),
	re.compile(r'\<dt\>Company Type\<\/dt\>\s*\<dd\>(?P<job__job_company_type__company_type>[^\<\>]*?)\<\/dd\>', flags=re.DOTALL),
	re.compile(r'\<dt\>Company Industry\<\/dt\>\s*\<dd\>(?P<job__job_company_industry__industry>[^\<\>]*?)\<\/dd\>\s*\<\/div\>', flags=re.DOTALL),
	re.compile(r'\<a class\=\"is\-black\" href\=\"\/en\/[a-z]+\/jobs\/jobs\-in\-[^\\\/]*?\/\"\>\<span\>(?P<location__location_city__city>[^\<\>]*?)\<\/span\>\<\/a\>\, \<a class\=\"is\-black\" href\=\"\/en\/[a-z]*?\/jobs\/\"\>\<span\>[^\<\>]*?\<\/span\>\<\/a\>', flags=re.DOTALL),
	re.compile(r'\<a class\=\"is\-black\" href\=\"\/en\/[a-z]+\/jobs\/jobs\-in\-[^\\\/]*?\/\"\>\<span\>[^\<\>]*?\<\/span\>\<\/a\>\, \<a class\=\"is\-black\" href\=\"\/en\/[a-z]*?\/jobs\/\"\>\<span\>(?P<location__location_country__country>[^\<\>]*?)\<\/span\>\<\/a\>', flags=re.DOTALL),
	re.compile(r'\"JobPosting\"\,\"title\"\:\"(?P<job__job_name__job_name>[^\"]*?)\"\,', flags=re.DOTALL),
	re.compile(r'Position Title\:\<\/b\>\s*(?P<job__job_name__job_name>[^\<\>]*?)\s*\<\/p\>', flags=re.DOTALL),
	re.compile(r'hiringOrganization\"\:\{\"\@type\"\:\"Organization\"\,\"name\"\:\"(?P<job__job_company_name__company_name>[^\"]*?)\"\,', flags=re.DOTALL),
	re.compile(r'Date Posted\: \<span\>(?P<job__job_post_month__month>[A-z]+) \d+\<\/span\>', flags=re.DOTALL),
	re.compile(r'Date Posted\: \<span\>[A-z]+ (?P<job__job_post_day__day>\d+)\<\/span\>', flags=re.DOTALL),
	re.compile(r'\<dt\>Job Location\<\/dt\>\s*\<dd\>(?P<job__job_location__location>[^\<\>]*?)\<\/dd\>', flags=re.DOTALL),
	###
]

def page_parsing(
	page_html,
	page_url,
	):
	output = []
	output.append({'job__job_page_url__url':page_url})
	for r in re_page_url_attributes:
		for m in re.finditer(r,page_url):
			output.append(m.groupdict())
	###
	for m in re.finditer(
		re_nationality_blcok,
		page_html):
		g = m.group()
		for r in re_nationality_attributes:
			for m1 in re.finditer(r, g):
				output.append(m1.groupdict())
	###
	for m in re.finditer(
		re_residence_blcok,
		page_html):
		g = m.group()
		for r in re_residence_attributes:
			for m1 in re.finditer(r, g):
				output.append(m1.groupdict())
	###
	for m in re.finditer(
		re_skill_blcok,
		page_html):
		g = m.group()
		for r in re_skill_attributes:
			for m1 in re.finditer(r, g):
				output.append(m1.groupdict())
	###
	for r in re_job_attributes:
		for m in re.finditer(
			r,
			page_html):
			output.append(m.groupdict())
	###
	for e in output:
		for a in attribute_name_needs_clearning:
			if a in e:
				e[a] = re.sub(r'\<[^\<\>]*?\>', r' ', e[a]).strip()
		if 'job__job_indeed_id__job_id' in e:
				output.append({'job':'id:%s'%(e['job__job_indeed_id__job_id'])})
	###
	return output

'''
import pandas

page = pandas.read_json(
	'/dcd_data/bayt/page_html/source=date20210904/db671c031a98f07677549e393c273998.json',
	orient = 'records',
	lines = True,
	)

page_html = page['page_html'][0]
page_url = page['page_url'][0]

for e in page_parsing(
	page_html,
	page_url,
	):
	print(e)


print(page_url)
'''


month_lookup = {
	'January':1,
	'February':2,
	'March':3,
	'April':4,
	'May':5,
	'June':6,
	'July':7,
	'August':8,
	'September':9,
	'October':10,
	'November':11,
	'December':12,
	'Jan':1,
	'Feb':2,
	'Mar':3,
	'Apr':4,
	'May':5,
	'Jun':6,
	'Jul':7,
	'Aug':8,
	'Sep':9,
	'Oct':10,
	'Nov':11,
	'Dec':12,
}


def generate_post_date(
	month_str,
	day_str,
	crawling_date,
	):
	try:
		month = month_lookup[month_str]
		day = int(day_str)
		year = int(crawling_date.split('-')[0])
		return '%04d-%02d-%02d'%(
			year,
			month,
			day,
			)
	except:
		return None

'''
generate_post_date(
	month_str = "Aug",
	day_str = "30",
	crawling_date = "2021-09-04",
	)
'''	

###########bayt_parsing.py###########