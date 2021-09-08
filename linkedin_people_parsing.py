# -*- coding: utf-8 -*-

############linkedin_people_page_parsing.py############
import re
import hashlib

try:
	import html
except:
	import HTMLParser
	html = HTMLParser.HTMLParser()

re_profile = [
	re.compile(r'\,\{\"lastName\"\:\"(?P<people__last_name__last_name>[^\"]+)\"\,\"memorialized\"\:false\,\"profileProfileActions\"\:', flags=re.DOTALL),
	re.compile(r'\"multiLocaleLastName\"\:\{\"en_US\"\:\"(?P<people__last_name__last_name>[^\"]+)\"\,[^\{\}]*?\}\,\"\*profilePositionGroups\"', flags=re.DOTALL),
	re.compile(r'profile\.Profile\"\,\"firstName\"\:\"(?P<people__first_name__first_name>[^\"]+)\"\,\"\*profileTopPosition', flags=re.DOTALL),
	re.compile(r'multiLocaleFirstName\"\:\{\"[^\"]+\"\:\"(?P<people__first_name__first_name>[^\"]+)\"\}', flags=re.DOTALL),
	re.compile(r'multiLocaleLastName\"\:\{\"[^\"]+\"\:\"(?P<people__last_name__last_name>[^\"]+)\"\}', flags=re.DOTALL),
	re.compile(r'headline\"\:\"(?P<people__employment_headline__text>[^\"]+)\"\,\"fullNamePronunciationAudio', flags=re.DOTALL),
	re.compile(r'summary\":\"(?P<people__employment_summary__text>[^\"]+)\"\,\"', flags=re.DOTALL),
	re.compile(r'defaultLocalizedNameWithoutCountryName\"\:\"(?P<people__work_location__location>[^\"]+)\"', flags=re.DOTALL),
	re.compile(r'defaultLocalizedName\"\:\"(?P<people__work_location__location>[^\"]+)\"', flags=re.DOTALL),
	re.compile(r'fsd_profileLanguage.{0,100}\"name\"\:\"(?P<people__speaking_language__language>[^\"]+)\"', flags=re.DOTALL),
	re.compile(r'fsd_profilePosition.{0,200}multiLocaleCompanyName\"\:\{\"en_US\"\:\"(?P<people__people_current_orgnization_name__orgnization_name>[^\"]*?)\"\},', flags=re.DOTALL),
	re.compile(r'profile\.Certification.{0,200}multiLocaleAuthority\"\:\{\"en_US\"\:\"(?P<people__people_last_graduate_school_name__school_name>[^\"]*?)\"\}', flags=re.DOTALL),
	re.compile(r'li\:fsd_skill\:\([^\(\)]*?\)\"\,\"name\"\:\"(?P<people__people_skill__skill>[^\"]*?)\"\,', flags=re.DOTALL),
	re.compile(r'fsd_profilePosition.{0,100}multiLocaleCompanyName.{0,100}companyName\"\:\"(?P<people__people_current_orgnization_name__orgnization_name>[^\"]*?)\"\,', flags=re.DOTALL),
	]

re_certificate_block = [ 
	re.compile(r'dateRange.{0,100}start.{0,500}multiLocaleLicenseNumber.{0,200}Certification.{0,200}authority.{0,100}name.{0,200}', flags=re.DOTALL),
	re.compile(r'dateRange.{0,100}multiLocaleLicenseNumber.{0,100}fsd_company.{0,500}Certification.{0,200}authority.{0,100}\"name\".{0,200}licenseNumber.{0,200}', flags=re.DOTALL),	
	re.compile(r'dateRange.{0,100}start.{0,500}fsd_company.{0,200}licenseNumber.{0,500}url.{0,200}authority.{0,200}\"name\".{0,500}', flags=re.DOTALL),	
	re.compile(r'dateRange.{0,100}start.{0,500}fsd_company.{0,500}licenseNumber.{0,500}authority.{0,100}name\".{0,200}', flags=re.DOTALL),
	re.compile(r'dateRange.{0,100}start.{0,800}Certification.{0,500}authority.{0,200}\"name\".{0,100}licenseNumber.{0,200}', flags=re.DOTALL),
	]

re_re_certificate_attributes = [
	re.compile(r'^dateRange.{0,200}\"month\"\:(?P<certificate__certificate_issue_month__month>\d+)\,', flags=re.DOTALL),
	re.compile(r'^dateRange.{0,200}\"year\"\:(?P<certificate__certificate_issue_year__year>\d+)\,', flags=re.DOTALL),
	re.compile(r'\"authority\"\:\"(?P<certificate__certificate_issue_authority__authority>[^\"]+)\"\,', flags=re.DOTALL),
	re.compile(r'\"name\"\:\"(?P<certificate__certificate_name__certificate_name>[^\"]+)\"\,', flags=re.DOTALL),
	re.compile(r'\"licenseNumber\"\:\"(?P<certificate__certificate_number__certificate_number>[^\"]+)\"\,', flags=re.DOTALL),
	re.compile(r'\"url\"\:\"(?P<certificate__certificate_url__url>[^\"]+)\"', flags=re.DOTALL),
	]

re_volunteer_blocks = [
	re.compile(r'\"role.{0,100}dateRange.{0,100}start.{0,200}end.{0,500}multiLocaleCompanyName.{0,100}companyName.{0,100}cause.{0,100}description\"\:\"[^\"]*?\"\,', flags=re.DOTALL),
	re.compile(r'\"role.{0,200}dateRange.{0,500}multiLocaleCompanyName.{0,200}companyName.{0,200}description.{0,800}\"cause\"\:null\,', flags=re.DOTALL),
	re.compile(r'\"role\".{0,100}dateRange.{0,500}multiLocaleCompanyName.{0,100}companyName.{0,100}cause.{0,100}description\"\:\"[^\"]*?\"', flags=re.DOTALL),
	]

re_volunteer_atttibutes = [
	re.compile(r'\"role\"\:\"(?P<volunteer_experience__volunteer_experience_role__role>[^\"]*?)\"', flags=re.DOTALL),
	re.compile(r'start\"\:\{\"month\"\:(?P<volunteer_experience__volunteer_experience_start_month__month>\d+)\,\"year\"\:(?P<volunteer_experience__volunteer_experience_start_year__year>\d+)\,', flags=re.DOTALL),
	re.compile(r'\"end\"\:\{\"month\"\:(?P<volunteer_experience__volunteer_experience_end_month__month>\d+)\,\"year\"\:(?P<volunteer_experience__volunteer_experience_end_year__year>\d+)\,', flags=re.DOTALL),
	re.compile(r'\"companyName\"\:\"(?P<volunteer_experience__volunteer_experience_organization_name__organization_name>[^\"]*?)\"\,', flags=re.DOTALL),
	re.compile(r'\"cause\"\:\"(?P<volunteer_experience__volunteer_experience_cause__cause>[^\"]*?)\"', flags=re.DOTALL),
	re.compile(r'\"description\"\:\"(?P<volunteer_experience__volunteer_experience_description__text>[^\"]*?)\",', flags=re.DOTALL),
]

re_education_1 = [
	re.compile(r'dateRange\"\:\{\"start\"\:\{\"year\"\:\d+\,.*?schoolName.*?degreeUrn', flags=re.DOTALL),
	re.compile(r'dateRange\"\:null.*?schoolName.*?degreeUrn', flags=re.DOTALL),
	re.compile(r'dateRange.{0,100}start.{0,200}end.{0,500}fsd_company.{0,100}Education.{0,200}fsd_school.{0,200}schoolName.{0,100}fieldOfStudy.{0,100}degreeName\"\:\"[^\"]*?\"\,', flags=re.DOTALL),
	re.compile(r'dateRange.{0,100}start.{0,200}end.{0,500}description.{0,200}Education.{0,200}fsd_school.{0,500}schoolName.{0,100}fieldOfStudy.{0,100}degreeName\"\:\"[^\"]*?\"\,', flags=re.DOTALL),
	]


re_education_attributes = [
	re.compile(r'dateRange.{0,10}start\"\:\{\"year\"\:(?P<education__start_year__year>\d+)\,', flags=re.DOTALL),
	re.compile(r'end\"\:\{\"year\":(?P<education__end_year__year>\d+)\,', flags=re.DOTALL),
	re.compile(r'\"start\"\:\{\"month\"\:(?P<education__start_month__month>\d+)\,\"year\"\:(?P<education__start_year__year>\d+)\,', flags=re.DOTALL),
	re.compile(r'\"end\"\:\{\"month\"\:(?P<education__end_month__month>\d+)\,\"year\"\:(?P<education__end_year__year>\d+)\,', flags=re.DOTALL),
	re.compile(r'description\"\:\"(?P<education__education_description__text>.{0,3000})\"\,\"\*profileTreasuryMediaEducation', flags=re.DOTALL),
	re.compile(r'schoolName\"\:\"(?P<education__school_name__school_name>[^\"]+)\"', flags=re.DOTALL),
	re.compile(r'fieldOfStudy\"\:\"(?P<education__education_field__education_field>[^\"]+)\"', flags=re.DOTALL),
	re.compile(r'degreeName\"\:\"(?P<education__education_degree__education_degree>[^\"]+)\"', flags=re.DOTALL),
	re.compile(r'schoolUrn\"\:\"urn\:li\:fsd_school\:(?P<education__education_fsd_school__fsd_school__fsd_school>\d+)\"', flags=re.DOTALL),
	re.compile(r'fsd_school\:(?P<education__education_fsd_school__fsd_school__fsd_school>\d+)\"', flags=re.DOTALL),
	]

re_employment_1 = [
	re.compile(r'dateRange.{0,100}start.{0,100}year\"\:\d+\,.{0,300}companyName.*?(multiLocaleLocationName|multiLocaleTitle|dateRange)', flags=re.DOTALL),
	re.compile(r'dateRange.{0,100}start.{0,100}year.{0,200}end.{0,100}year.{0,500}companyName.{0,100}fsd_company.{0,100}title.{0,500}fsd_employmentType.{0,500}locationName.{0,500}fsd_geo\:\d+\"', flags=re.DOTALL),
	re.compile(r'dateRange.{0,100}start.{0,100}year.{0,200}end.{0,100}year.{0,500}companyName.{0,200}fsd_company.{0,100}title.{0,500}employmentType.{0,200}fsd_employmentType\:\d+\"', flags=re.DOTALL),
	re.compile(r'dateRange.{0,100}start.{0,200}year.{0,200}end.{0,100}year.{0,500}companyName.{0,200}fsd_company.{0,100}title.{0,200}fsd_region.{0,500}locationName.{0,500}fsd_geo.{0,500}multiLocaleDescription.{0,200}', flags=re.DOTALL),
	re.compile(r'dateRange.{0,100}start.{0,100}year.{0,200}end.{0,100}year.{0,500}companyName.{0,100}fsd_company.{0,100}title.{0,100}fsd_company.{0,100}fsd_region.{0,500}locationName.{0,200}multiLocaleTitle.{0,100}fsd_geo.{0,100}', flags=re.DOTALL),
	re.compile(r'dateRange.{0,100}start.{0,100}year.{0,200}end.{0,100}year.{0,500}companyName.{0,100}fsd_company.{0,100}title.{0,100}fsd_company\:\d+\"\,', flags=re.DOTALL),
	re.compile(r'dateRange.{0,100}start.{0,100}year.{0,200}end.{0,100}year.{0,500}companyName.{0,100}title.{0,200}', flags=re.DOTALL),
	re.compile(r'dateRange.{0,100}start.{0,200}end.{0,500}companyName.{0,100}description.{0,1000}fsd_company.{0,100}title.{0,500}locationName.{0,200}', flags=re.DOTALL),
	re.compile(r'dateRange.{0,100}start.{0,200}end.{0,500}companyName.{0,1000}fsd_company.{0,100}title.{0,200}multiLocaleGeoLocationName.{0,100}locationName.{0,500}geoLocationName.{0,100}multiLocaleDescription.{0,1000}multiLocaleLocationName', flags=re.DOTALL),
	]

'''
re_employment_2 = re.compile(r'dateRange.{0,2000}', flags=re.DOTALL)
re_employment_inner = re.compile(r'dateRange.*?dateRange\"', flags=re.DOTALL)
'''

re_employment_atrributes = [
	re.compile(r'dateRange.{0,10}start.{0,100}year\"\:(?P<employment__start_year__year>\d+)\,\"', flags=re.DOTALL),
	re.compile(r'dateRange.{0,10}start.{0,10}month\"\:(?P<employment__start_month__month>\d+)\,', flags=re.DOTALL),
	re.compile(r'end.{0,10}month\"\:(?P<employment__end_month__month>\d+).{0,10}year\"\:(?P<employment__end_year__year>\d+)\,', flags=re.DOTALL),
	re.compile(r'\"end\"\:\{\"year\"\:(?P<employment__end_year__year>\d+)\,', flags=re.DOTALL),
	re.compile(r'companyName\"\:\"(?P<employment__employment_company_name__company_name>[^\"]+)\"', flags=re.DOTALL),
	re.compile(r'description\"\:\"(?P<employment__description__text>[^\"]+)\"', flags=re.DOTALL),
	re.compile(r'title\"\:\"(?P<employment__employment_title__employment_title>[^\"]+)\"\,', flags=re.DOTALL),
	re.compile(r'locationName\"\:\"(?P<employment__employment_location__location>[^\"]+)\"', flags=re.DOTALL),
	re.compile(r'fsd_company\:(?P<employment__employment_fsd_company__fsd_company>\d+)\"\,', flags=re.DOTALL),
	re.compile(r'fsd_geo\:(?P<employment__employment_fsd_geo_country__fsd_geo_country>[a-z]+)\"\,', flags=re.DOTALL),
	re.compile(r'fsd_region\:\((?P<employment__employment_fsd_region__fsd_region>\d+)\,', flags=re.DOTALL),
	re.compile(r'fsd_employmentType\:(?P<employment__employment_fsd_employment_type__fsd_employment_type>\d+)\"', flags=re.DOTALL),
	]


re_company_inf = re.compile(r'"industry.{0,500}fsd_industry.{0,500}fsd_industry.{0,500}industryUrns.{0,500}LinkablePositionCompany.{0,500}url.*?https\:\/\/www\.linkedin\.com\/company\/.*?Company.*?employeeCountRange.*?fsd_company.*?name.*?logo.*?expiresAt', flags=re.DOTALL)
re_company_atrributes = [
	re.compile(r'fsd_company\:(?P<company__company_fsd_company__fsd_company>\d+)\"\,', flags=re.DOTALL),
	re.compile(r'fsd_industry\:(?P<company__company_fsd_industry__fsd_industry>\d+)\"', flags=re.DOTALL),
	re.compile(r'url\"\:\"https\:\/\/[a-z]+\.linkedin\.com\/company\/(?P<company__company_id__company_id>[^\"]+)\/\"\,', flags=re.DOTALL),
	re.compile(r'employeeCountRange\"\:\{\"start\"\:(?P<company__start_size__number>\d+)\,', flags=re.DOTALL),
	re.compile(r'employeeCountRange\"\:\{\"start\"\:\d+\,\"end\"\:(?P<company__end_size__number>\d+)\,', flags=re.DOTALL),
	re.compile(r'name\"\:\"(?P<company__company_name__company_name>[^\"]+)\"', flags=re.DOTALL),
	re.compile(r'logo"\:.{0,300}rootUrl\"\:\"(?P<company__logo_root_url__url>[^\"]+)\"\,.{0,300}fileIdentifyingUrlPathSegment\"\:\"(?P<company__logo_segment_url__url>[^\"]+)\"', flags=re.DOTALL),
	]

re_school_inf = [
	re.compile(r'School.{0,10}url.{0,100}linkedin\.com\/school\/.{0,300}School.{0,100}fsd_school.{0,100}name.{0,100}logo.{0,500}rootUrl.{0,500}fileIdentifyingUrlPathSegment.*?VectorArtifact', flags=re.DOTALL),
	re.compile(r'School.{0,100}fsd_school.{0,100}name.{0,100}logo.{0,300}rootUrl.{0,500}fileIdentifyingUrlPathSegment.{0,600}VectorArtifact', flags=re.DOTALL),
]
re_school_attributes = [
	re.compile(r'fsd_school\:(?P<school__fsd_school__fsd_school>\d+)\"\,', flags=re.DOTALL),
	re.compile(r'name\"\:\"(?P<school__school_name__school_name>[^\"]+)\"', flags=re.DOTALL),
	re.compile(r'logo"\:.{0,300}rootUrl\"\:\"(?P<school__logo_root_url__url>[^\"]+)\"\,.{0,300}fileIdentifyingUrlPathSegment\"\:\"(?P<school__logo_segment_url__url>[^\"]+)\"', flags=re.DOTALL),
	re.compile(r'url.{0,100}linkedin\.com\/school\/(?P<school__school_id__school_id>[^\"]*?)[\///]*\"', flags=re.DOTALL),
]

re_industry = [
	re.compile(r'fsd_industry\:(?P<industry__fsd_industry__fsd_industry>\d+)\"\,\"name\"\:\"(?P<industry__industry_name__industry_name>[^\"]+)\"\,', flags=re.DOTALL),
	re.compile(r'fsd_employmentType\:(?P<employment_type__employment_fsd_employment_type__fsd_employment_type>\d+)\".{0,40}\"name\"\:\"(?P<employment_type__employment_type_name__employment_type_name>[^\"]*?)\"\,', flags=re.DOTALL),
]

re_profile_photo = 	re.compile(r'rootUrl\"\:\"(?P<profile_photo_url__profile_photo_root_url__url>[^\"]*?profile\-displayphoto[^\"]*?)\".*?fileIdentifyingUrlPathSegment\"\:\"(?P<profile_photo_url__profile_photo_segment_url__url>[^\"]*?)\"\,', flags=re.DOTALL)

re_page_url = [
	re.compile(r'\/in\/(?P<people__people_id__people_id>[^\\\/]*?)(\/|$)', flags=re.DOTALL),
]


def linkedin_poeple_page_parsing(
	page_html,
	page_url,
	):
	page_html = html.unescape(page_html)
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
	####
	for r in re_volunteer_blocks:
		for m in re.finditer(r, page_html):
			b = m.group()
			e = {}
			for r1 in re_volunteer_atttibutes:
				for m1 in re.finditer(r1, b):
					e.update(m1.groupdict())
			output.append(e)
	####
	edu_strs = []
	for r in re_education_1:
		edu_strs += [m.group() \
			for m in re.finditer(r, \
			page_html)]
	for edu in edu_strs:
		e = {}
		for r in re_education_attributes:
			for m in re.finditer(r,	edu):
				e.update(m.groupdict())
		#print(e)
		output.append(e)
	#########
	employment_strs = []
	employment_hashs = []
	for r in re_employment_1:
		for m in re.finditer(
			r, 
			page_html):
			s = m.group()
			s_hash = hashlib.md5(s[0:600].encode('utf-8')).hexdigest()
			if s_hash not in employment_hashs:
				employment_hashs.append(s_hash)
				employment_strs.append(s)
	for s in employment_strs:
		e = {}
		for r in re_employment_atrributes:
			for m in re.finditer(r,	s):
				e.update(m.groupdict())
		output.append(e)
	#########
	company_strs = [m.group() \
		for m in re.finditer(re_company_inf, \
		page_html)]
	for s in company_strs:
		#print('\n\n'+education_id)
		e = {}
		for r in re_company_atrributes:
			for m in re.finditer(r,	s):
				e.update(m.groupdict())
		#print(e)
		output.append(e)
	#########
	school_strs = []
	school_hashs = []
	for r in re_school_inf:
		for m in re.finditer(r, page_html):
			s = m.group()
			s_hash = hashlib.md5(s[0:600].encode('utf-8')).hexdigest()
			if s_hash not in school_hashs:
				school_strs.append(s)
				school_hashs.append(s_hash)
	for s in school_strs:
		#print('\n\n'+education_id)
		e = {}
		for r in re_school_attributes:
			for m in re.finditer(r,	s):
				e.update(m.groupdict())
		#print(e)
		output.append(e)
	#########
	for r in re_industry:
		for m in re.finditer(r,	page_html):
			output.append(m.groupdict())
	#print(e)
	for m in re.finditer(re_profile_photo, page_html):
		output.append(m.groupdict())
	for r1 in re_certificate_block:
		for m in re.finditer(r1, page_html):
			b = m.group()
			e = {}
			for r in re_re_certificate_attributes:
				for m1 in re.finditer(r, b):
					e.update(m1.groupdict())
			output.append(e)
	#########
	people__first_name__first_name = None
	people__last_name__last_name = None
	for e in output:
		if 'people__people_id__people_id' in e:
			output.append({"people":"ln:%s"%(e["people__people_id__people_id"])})
		if bool(re.search(r'^certificate__', list(e.keys())[0])):
			certificate = ' '.join([e[k] for k in e if k not in [
				'certificate__certificate_issue_month__month',
				'certificate__certificate_number__certificate_number',
				'certificate__certificate_url__url',
				]])
			e['certificate'] = certificate
			output.append({'people__people_certificate__certificate':certificate})	
		if bool(re.search(r'^volunteer_experience__', list(e.keys())[0])):
			volunteer_experience = ' '.join([e[k] for k in e])
			if 'volunteer_experience__volunteer_experience_organization_name__organization_name' in e \
				and 'volunteer_experience__volunteer_experience_role__role' in e \
				and 'volunteer_experience__volunteer_experience_start_year__year' in e:
				volunteer_experience = '%s %s %s'%(
					e['volunteer_experience__volunteer_experience_organization_name__organization_name'],
					e['volunteer_experience__volunteer_experience_role__role'],
					e['volunteer_experience__volunteer_experience_start_year__year'],
					)
			e['volunteer_experience'] = volunteer_experience
			output.append({'people__people_volunteer_experience__volunteer_experience':volunteer_experience})	
		if bool(re.search(r'^education__', list(e.keys())[0])):
			education = ' '.join([e[k] for k in e])
			e['education'] = education
			output.append({'people__people_education__education':education})
		if bool(re.search(r'^employment__', list(e.keys())[0])):
			employment = ' '.join([e[k] for k in e])
			if 'employment__start_year__year' in e \
				and 'employment__start_month__month' in e \
				and 'employment__employment_company_name__company_name' in e \
				and 'employment__employment_fsd_company__fsd_company' in e:
				employment = '%s %s %s %s'%(
					e['employment__start_year__year'],
					e['employment__start_month__month'],
					e['employment__employment_company_name__company_name'],
					e['employment__employment_fsd_company__fsd_company'],
					)
			e['employment'] = employment
			output.append({'people__people_employment__employment':employment})
		if bool(re.search(r'^company__', list(e.keys())[0])):
			company = ' '.join([e[k] for k in e])
			if 'company__company_id__company_id' in e:
				company = 'ln:%s'%(e['company__company_id__company_id'])
			if 'company__company_name__company_name' in e \
				and 'company__company_fsd_company__fsd_company' in e:
				company = '%s %s'%(
					e['company__company_name__company_name'],
					e['company__company_fsd_company__fsd_company'],
					)
			e['company'] = company
			output.append({'people__people_company__company':company})
		if bool(re.search(r'^school__', list(e.keys())[0])):
			school = ' '.join([e[k] for k in e])	
			if 'school__school_id__school_id' in e:
				school = 'ln:%s'%(e['school__school_id__school_id'])				
			if 'school__fsd_school__fsd_school' in e and 'school__school_name__school_name' in e:
				school = '%s %s'%(
					e['school__school_name__school_name'],
					e['school__fsd_school__fsd_school'],
					)
			e['school'] = school
			output.append({'people__people_school__school':school})
		if bool(re.search(r'^(industry)__', list(e.keys())[0])):
			industry = ' '.join([e[k] for k in e])			
			e['industry'] = industry
		if bool(re.search(r'^(employment_type)__', list(e.keys())[0])):
			industry = ' '.join([e[k] for k in e])
			e['employment_type'] = industry
		if 'company__logo_root_url__url' in e \
			and 'company__logo_segment_url__url' in e:
			e['company__company_logo_url__photo_url'] = '%s%s'%(
				e['company__logo_root_url__url'],
				e['company__logo_segment_url__url'])
		if 'school__logo_root_url__url' in e \
			and 'school__logo_segment_url__url' in e:
			e['school__school_logo_url__photo_url'] = '%s%s'%(e['school__logo_root_url__url'],
				e['school__logo_segment_url__url'])
		if 'profile_photo_url__profile_photo_root_url__url' in e \
			and 'profile_photo_url__profile_photo_segment_url__url' in e:
			profile_photo_url = '%s%s'%(
				e['profile_photo_url__profile_photo_root_url__url'],
				e['profile_photo_url__profile_photo_segment_url__url'])
			e['profile_photo_url'] = profile_photo_url
			output.append({'people__profile_photo_url__profile_photo_url':profile_photo_url})
		if 'people__first_name__first_name' in e:
			people__first_name__first_name = e['people__first_name__first_name']
		if 'people__last_name__last_name' in e:
			people__last_name__last_name = e['people__last_name__last_name']
	if people__first_name__first_name is not None \
		and people__last_name__last_name is not None:
		people__people_name__people_name = '%s %s'%(
			people__first_name__first_name,
			people__last_name__last_name)
		output.append({'people__people_name__people_name':people__people_name__people_name})			
	return output

#########




'''

import pandas

page = pandas.read_json(
	'/kg_data/linkedin_people/people_about_page/source=dcd/a11feec65d9fd224fc6f073a15469348.json',
	orient = 'records',
	lines = True,
	)


page_html = page['page_html'][0]
page_url = page['page_url'][0]

print(page_url)

output = linkedin_poeple_page_parsing(
	page_html,
	page_url,
	)

triplets = yan_page_parsed_to_triplet.parsed_info_2_kg_triplets(output)
jessica_neo4j.ingest_knowledge_triplets_to_neo4j(
	triplets, 
	neo4j_session)

for m in re.finditer(
	r'.{0,200}PhD in Public Health.{0,200}',
	page_html,
	):
	s = m.group()
	print('\n--------------\n')
	print(s)

'''

#########


re_skill = re.compile(r'\{\"name\"\:\"(?P<people__skill>[^\"]+)\"\,\"entityUrn')

def linkedin_people_skill_parsing(page_html):
	output = []
	for m in re.finditer(re_skill, page_html):
		output.append(m.groupdict())
	return output

#########

re_page_url_link = [
	re.compile(r'\<cite\>(?P<page_url>[^\<\>]*?linkedin[^\<\>]*?\/in\/[^\<\>]*?)\<\/cite\>', flags=re.DOTALL),
	re.compile(r'\<div class\=\"yuRUbf\"\>\<a href\=\"(?P<page_url>[^\"]*?linkedin[^\"]*?\/in\/[^\"\/\?]*?)(\?|\"|\/)', flags=re.DOTALL),
	re.compile(r'\<div class\=\"yuRUbf\"\>\<a href\=\"(?P<page_url>[^\"]*?dhow[^\"]*?\/biographies\/\d+\/[^\"\/\?]*?)(\?|\"|\/)', flags=re.DOTALL),
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

############linkedin_people_page_parsing.py############