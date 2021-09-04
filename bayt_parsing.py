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
	re.compile(r'\<div class\=\"[^\"]*?\"\>\<a href\=\"(?P<job__job_company_url__url>https\:\/\/ae\.indeed\.com\/cmp\/[^\"]*?)\" [^\<\>]*?>Randstad\<\/a\><\/div\>', flags=re.DOTALL),
	re.compile(r'\<div class\=\"icl\-u\-lg\-mr\-\-sm icl\-u\-xs\-mr\-\-xs\"\>(?P<job__job_company_name__company_name>[^\<\>]*?)\<\/div\>', flags=re.DOTALL),
	re.compile(r'\<div class\=\"icl\-u\-lg\-mr\-\-sm icl\-u\-xs\-mr\-\-xs\"\>[^\<\>]*?\<\/div\>\<\/div\>\<div\>(?P<job__job_location__location>[^\<\>]*?)\<\/div\>', flags=re.DOTALL),
	re.compile(r'\<\/div\>\<div id\=\"jobDescriptionText\" class\=\"jobsearch\-jobDescriptionText\"\>(?P<job__job_description__text>.*?)\<\/div\>\<div class\=\"jobsearch\-JobMetadataFooter\"\>', flags=re.DOTALL),
	re.compile(r'Footer\"\>\<div\>(?P<job__job_post_duration__duration>[^\<\>]*?)\<\/div\>\<div class\=\"mosaic', flags=re.DOTALL),
	re.compile(r'\<div\>(?P<job__job_post_duration__duration>[^\<\>]*?)\<\/div\>\<div id\=\"originalJobLinkContainer', flags=re.DOTALL),
	re.compile(r'Work Remotely\:\<\/p\>\<ul\>\<li\>(?P<job__job_work_remotely__work_remotely>[^\<\>]*?)\<\/li\>', flags=re.DOTALL),
	re.compile(r'\<p\>COVID-19 considerations\:\<br\/\>(?P<job__job_covid_consideration__covid_consideration>[^\<\>]*?)\<\/p\>', flags=re.DOTALL),
	re.compile(r'Ability to commute\/relocate\:\<\/p\>\<ul\>\<li\>(?P<job__job_ability_to_commute_relocate__ability_to_commute_relocate>[^\<\>]*?)\<\/li\>', flags=re.DOTALL),
	re.compile(r'Duty Schedule\:\s*(?P<job__job_duty_schedule__duty_schedule>[^\<\>]*?)\s*\<\/p\>', flags=re.DOTALL),
	re.compile(r'Rest Days\:\s*(?P<job__job_rest_days__rest_days>[^\<\>]*?)\s*\<\/p\>', flags=re.DOTALL),
	re.compile(r'\<p\>License\/Certification\:\<\/p\>\<ul\>(?P<job__job_required_license_certification__license_certification>.*?)\<\/ul\>', flags=re.DOTALL),
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

###########bayt_parsing.py###########