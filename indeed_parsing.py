###########indeed_parsing.py#############
import re
import datetime

###########

re_page_url = re.compile(
	r'jk\:\'(?P<page_url>[^\']*?)\'',
	flags=re.DOTALL)
page_url_prefix = 'https://ae.indeed.com/viewjob?jk='

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

re_job_attributes = [
re.compile(r'\<h1 class\=\"[^\"]*?"\>(?P<job__job_name__job_name>[^\<\>]*?)\<\/h1\>', flags=re.DOTALL),
re.compile(r'\<div class\=\"[^\"]*?\"\>\<a href\=\"https\:\/\/ae\.indeed\.com\/cmp\/[^\"]*?\" [^\<\>]*?>(?P<job__job_company_name__company_name>[^\<\>]*?)\<\/a\><\/div\>', flags=re.DOTALL),
re.compile(r'\<div class\=\"[^\"]*?\"\>\<a href\=\"(?P<job__job_company_url__url>https\:\/\/ae\.indeed\.com\/cmp\/[^\"]*?)\" [^\<\>]*?>Randstad\<\/a\><\/div\>', flags=re.DOTALL),
re.compile(r'div\>(?P<job__job_location__location>[^\<\>]*?)\<\/div\>\<\/div\>\<\/div\>\<\/div\>\<\/div\>\<div class\=\"jobsearch\-CompanyReview', flags=re.DOTALL),
re.compile(r'\<div class\=\"icl\-u\-lg\-mr\-\-sm icl\-u\-xs\-mr\-\-xs\"\>(?P<job__job_company_name__company_name>[^\<\>]*?)\<\/div\>', flags=re.DOTALL),
re.compile(r'\<div class\=\"icl\-u\-lg\-mr\-\-sm icl\-u\-xs\-mr\-\-xs\"\>[^\<\>]*?\<\/div\>\<\/div\>\<div\>(?P<job__job_location__location>[^\<\>]*?)\<\/div\>', flags=re.DOTALL),
re.compile(r'\<\/div\>\<div id\=\"jobDescriptionText\" class\=\"jobsearch\-jobDescriptionText\"\>(?P<job__job_description__text>.*?)\<\/div\>\<div class\=\"jobsearch\-JobMetadataFooter\"\>', flags=re.DOTALL),
re.compile(r'Footer\"\>\<div\>(?P<job__job_post_duration__duration>[^\<\>]*?)\<\/div\>\<div class\=\"mosaic', flags=re.DOTALL),
re.compile(r'\<div\>(?P<job__job_post_duration__duration>[^\<\>]*?)\<\/div\>\<div id\=\"originalJobLinkContainer', flags=re.DOTALL),
re.compile(r'\<p\>Salary\:\s*(?P<job__job_salary__salary>[^\<\>]*?)\s*\<\/p\>', flags=re.DOTALL),
re.compile(r'\<p\>Job Types\:\s*(?P<job__job_type_group__job_type_group>[^\<\>]*?)\s*\<\/p\>', flags=re.DOTALL),
re.compile(r'Work Remotely\:\<\/p\>\<ul\>\<li\>(?P<job__job_work_remotely__work_remotely>[^\<\>]*?)\<\/li\>', flags=re.DOTALL),
re.compile(r'\<p\>COVID-19 considerations\:\<br\/\>(?P<job__job_covid_consideration__covid_consideration>[^\<\>]*?)\<\/p\>', flags=re.DOTALL),
re.compile(r'\<p\>Contract length\:\s*(?P<job__job_contract_length__contract_length>[^\<\>]*?)\<\/p\>', flags=re.DOTALL),
re.compile(r'JobMetadataHeader-item \"\>\<span class\=\"icl\-u\-xs\-mr\-\-xs\"\>(?P<job__job_salary__salary>[^\<\>]*?)\<\/span\>', flags=re.DOTALL),
re.compile(r'Ability to commute\/relocate\:\<\/p\>\<ul\>\<li\>(?P<job__job_ability_to_commute_relocate__ability_to_commute_relocate>[^\<\>]*?)\<\/li\>', flags=re.DOTALL),
re.compile(r'Duty Schedule\:\s*(?P<job__job_duty_schedule__duty_schedule>[^\<\>]*?)\s*\<\/p\>', flags=re.DOTALL),
re.compile(r'Rest Days\:\s*(?P<job__job_rest_days__rest_days>[^\<\>]*?)\s*\<\/p\>', flags=re.DOTALL),
re.compile(r'\<p\>License\/Certification\:\<\/p\>\<ul\>(?P<job__job_required_license_certification__license_certification>.*?)\<\/ul\>', flags=re.DOTALL),
re.compile(r'\>(?P<job__job_category__job_category>[^\<\>]*?)\s*jobs in Abu Dhabi\<\/a\>', flags=re.DOTALL),
]


re_job_language_block = re.compile(r'\<p\>Language\:\<\/p\>\<ul\>.*?\<\/ul\>', flags=re.DOTALL)
re_job_language = [
re.compile(r'li\>(?P<job__job_required_language__language>[^\<\>]*?)\<\/li', flags=re.DOTALL),
]


re_job_experience_block = re.compile(r'\<p\>Experience\:\<\/p\>\<ul\>.*?\<\/ul\>', flags=re.DOTALL)
re_job_experience = [
re.compile(r'li\>(?P<job__job_required_experience__experience>[^\<\>]*?)\<\/li', flags=re.DOTALL),
]


re_job_education_block = re.compile(r'\<p\>Education\:\<\/p\>\<ul\>.*?\<\/ul\>', flags=re.DOTALL)
re_job_education = [
re.compile(r'li\>(?P<job__job_required_education__education>[^\<\>]*?)\<\/li', flags=re.DOTALL),
]

re_page_url_attributes = [
re.compile(r'\?jk\=(?P<job__job_indeed_id__job_id>.*?)$', flags=re.DOTALL),
]

attribute_name_needs_clearning = [
'job__job_description__text',
'job__job_required_license_certification__license_certification',
]

def job_page_parsing(
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
		re_job_education_block,
		page_html):
		g = m.group()
		for r in re_job_education:
			for m1 in re.finditer(r, g):
				output.append(m1.groupdict())
	###
	for m in re.finditer(
		re_job_language_block,
		page_html):
		g = m.group()
		for r in re_job_language:
			for m1 in re.finditer(r, g):
				output.append(m1.groupdict())
	####
	for m in re.finditer(
		re_job_experience_block,
		page_html):
		g = m.group()
		for r in re_job_experience:
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
		if 'job__job_type_group__job_type_group' in e:
			types = e['job__job_type_group__job_type_group'].split(',')
			for t in types:
				output.append({'job__job_type__job_type':t.strip()})
		if 'job__job_indeed_id__job_id' in e:
				output.append({'job':'id:%s'%(e['job__job_indeed_id__job_id'])})
	###
	return output

###########

'''
import pandas

page = pandas.read_json(
	'/dcd_data/indeed/20210812/job_page/8c2ba15384de2d26cfb61578c0ba3b53.json',
	orient = 'records',
	lines = True,
	)

page_html = page['page_html'][0]
page_url = page['page_url'][0]

for e in job_page_parsing(
	page_html,
	page_url,
	):
	print(e)

print(page_url)
'''


def generate_post_date_from_crawling_date_and_post_duration(
	crawling_date,
	post_duration,
	):
	crawling_date = datetime.datetime.strptime(crawling_date, '%Y-%m-%d')
	###
	try:
		post_crawlling_delta = int(re.search(r'\d+', post_duration).group())
	except:
		pass
	###
	try:
		post_duration = re.search(r'Just posted|Today', post_duration).group()
		post_crawlling_delta = 0
	except:
		pass
	###
	post_crawlling_delta = datetime.timedelta(days = post_crawlling_delta)
	post_date = crawling_date - post_crawlling_delta
	###
	post_date = post_date.strftime('%Y-%m-%d')
	return post_date


'''
generate_post_date_from_crawling_date_and_post_duration(
	crawling_date = '2021-08-10',
	post_duration = '2 days ago',
	)
'''


def extract_salary_amount_from_salary_description(
	salary_description,
	):
	output = []
	for m in re.finditer(r'[\d\,\.]+', salary_description):
		try:
			salary_amount = m.group()
			salary_amount = re.sub(r'[^\d\.]+', '', salary_amount)
			salary_amount = float(salary_amount)
			output.append(salary_amount)
		except:
			pass
	return output

'''
salary_description = 'AED5,000 a month'
salary_description = 'AED1,500 - AED1,800 a month'

extract_salary_from_salary_description(
	salary_description,
	)
'''



def extract_salary_frequency_from_salary_description(
	salary_description,
	):
	output = []
	for m in re.finditer(r'[^A-z](a|per)\s*(?P<salary_frequency>[A-z]+)$', salary_description):
		try:
			salary_frequency = m.group('salary_frequency')
			output.append(salary_frequency)
		except:
			pass
	return output

'''
salary_description = 'AED5,000 a month'
salary_description = 'AED1,500 - AED1,800 a month'

extract_salary_frequency_from_salary_description(
	salary_description,
	)
'''


def extract_contract_duration_number(
	duration_srting,
	):
	output = []
	for m in re.finditer(r'\d+', duration_srting):
		output.append(int(m.group()))
	return output

def extract_contract_duration_unit(
	duration_srting,
	):
	output = []
	for m in re.finditer(r'[A-z]+$', duration_srting):
		output.append(m.group())
	return output

'''
duration_srting = '24 months'
'''

###########indeed_parsing.py#############