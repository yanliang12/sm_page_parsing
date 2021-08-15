##############dubizzle_parsing.py##############
import re
import datetime

###########

re_page_url = re.compile(
	r'\"(?P<page_url>https\:\/\/abudhabi\.dubizzle\.com\/property\-for\-rent\/[^\\\/]*?\/[^\\\/]*?\/\d+\/\d+\/\d+\/[^\\\/]*?\/)\"',
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
	'/dcd_data/dubizzle/property_list_page/download_date=20210814/1c889523357a0e7e03f5297d0389074e.json',
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



##########

re_property_attributes = [
	re.compile(r'\<span data\-ui\-id\=\"location\"\>(?P<property__property_location__location>[^\<\>]*?)\<\/span\>', flags=re.DOTALL),
	re.compile(r'\\\"long\\\"\:(?P<geo_point__geo_point_longitude__longitude>[\d\.]*?)\,\\\"lat\\\"\:(?P<geo_point__geo_point_latitude__latitude>[\d\.]*?)\}', flags=re.DOTALL),
	re.compile(r'\"Price\_\_PaymentFrequency[^\"]*?\"\>(?P<property__property_rental_payment_frequency__payment_frequency>[^\<\>]*?)\<\/span\>', flags=re.DOTALL),
	re.compile(r'price\\\"\:\{\\\"amount\\\"\:(?P<property__property_rental_price_amount__number>[\d\.]*?)\,', flags=re.DOTALL),
	re.compile(r'price\\\"\:\{[^\{\}]*?currency\\\"\:\\\"(?P<property__property_rental_price_currency__currency>[A-Z]*?)\\\",', flags=re.DOTALL),
	re.compile(r'Posted On\-value\" [^\<\>]*?\>\<span [^\<\>]*?\>(?P<property__property_rental_post_date__date>[^\<\>]*?)\<\/span\>', flags=re.DOTALL),
	re.compile(r'\<svg .*?SizeFilled\_svg\_\_a.*?\>(?P<property__property_size__size>[^\<\>]*?)\<\/span\>', flags=re.DOTALL),
	re.compile(r'\>(?P<property__property_bedroom_number__bedroom_number>[^\<\>]*?)\<\/span\>\<span data\-testid\=\"listing\-key\-fact\-bathrooms\"', flags=re.DOTALL),
	re.compile(r'\>(?P<property__property_bath_number__bath_number>[^\<\>]*?)\<\/span\>\<span data\-testid\=\"listing\-key\-fact\-size', flags=re.DOTALL),
	re.compile(r'phoneNumber\\\"\:\\\"(?P<property__property_agent_phone__phone>[^\"\\]*?)\\\"', flags=re.DOTALL),
	re.compile(r'\\\"dedLicenceNo\\\"\:\\\"(?P<property__property_ded_licence_number__licence_number>[^\"\\]*?)\\\"\,', flags=re.DOTALL),
	re.compile(r'agent_name\\\"\:\\\"(?P<property__property_agent_name__agent_name>[^\"]*?)\\\"', flags=re.DOTALL),
	re.compile(r'agent_logo\\\"\:\\\"(?P<property__property_agent_logo_url__photo_url>[^\"]*?)\\\"', flags=re.DOTALL),
	re.compile(r'class\=\"Description\_\_DescriptionText[^\"\<\>"]*?\"\>\<span\>(?P<property__property_description__text>.*?)\<\/span\>', flags=re.DOTALL),
	re.compile(r'\<span data\-testid\=\"listing\-key\-fact\-furnished\" [^\<\>]*?\>\<svg .*?\<\/svg\>\s*\<[^\<\>]*?\>(?P<property__property_furnished__furnished>[^\<\>]*?)\<\/span\>', flags=re.DOTALL),
	re.compile(r'(?P<property__property_photo_url__photo_url>https\:\/\/images\.dubizzle\.com\/v\d+\/files\/[^\\\/]*?\/image\;s\=\d+x\d+)', flags=re.DOTALL),
]

re_amenity_block = re.compile(r'Amenities\<\/h5\>.*?\<hr class', flags=re.DOTALL)
re_amenity_attributes = [
	re.compile(r'\<p color\=\"\#2b2d2e\" data\-ui\=\"[^\"]*?\" [^\<\>]*?\>(?P<property__property_amenity__property_amenity>[^\<\>]*?)\<\/p\>', flags=re.DOTALL),
]

re_page_url_attributes = [
	re.compile(r'\/(?P<property__property_location_emirate__emirate>[^\/\.]*?)\.dubizzle', flags=re.DOTALL),
	re.compile(r'dubizzle\.com\/[^\\\/]*?\/(?P<property__property_category__property_category>[^\/\\]*?)\/', flags=re.DOTALL),
	re.compile(r'\/(?P<property__property_type__property_type>[^\/\\]*?)\/\d+\/\d+\/', flags=re.DOTALL),
	re.compile(r'\/(?P<post_date__date_year__year>\d+)\/(?P<post_date__date_month__month>\d+)\/(?P<post_date__date_day__day>\d+)\/', flags=re.DOTALL),
	re.compile(r'\/\d+\/\d+\/\d+\/(?P<property__property_id__property_id>[^\/\.]*?)\/', flags=re.DOTALL),
]

def page_parsing(
	page_html,
	page_url,
	):
	###
	output = []
	###
	output.append({
		'property__property_page_url__url':page_url
		})
	###
	for r in re_page_url_attributes:
		for m in re.finditer(r, page_url):
			output.append(m.groupdict())
	###
	for m in re.finditer(re_amenity_block, 
		page_html):
		b = m.group()
		for r in re_amenity_attributes:
			for m1 in re.finditer(r, b):
				output.append(m1.groupdict())
	###
	for r in re_property_attributes:
		for m in re.finditer(r, page_html):
			output.append(m.groupdict())
	###
	for e in output:
		if 'geo_point__geo_point_longitude__longitude' in e and 'geo_point__geo_point_latitude__latitude':
			e['geo_point'] = '%s,%s'%(
				e['geo_point__geo_point_latitude__latitude'],
				e['geo_point__geo_point_longitude__longitude'],			
				)
			output.append({'property__property_geo_location__geo_point':
				e['geo_point']
				})
		if 'property__property_description__text' in e:
			e['property__property_description__text'] = re.sub(
				r'\<[^\<\>]*?\>', 
				r'',
				e['property__property_description__text'],
				)
		if 'post_date__date_year__year' in e \
			and 'post_date__date_month__month' \
			and 'post_date__date_day__day':
			e['post_date'] = '%04d-%02d-%02d'%(
				int(e['post_date__date_year__year']),
				int(e['post_date__date_month__month']),
				int(e['post_date__date_day__day']),
				)
			output.append({'property__property_post_date__post_date':
				e['post_date']})
		if 'property__property_id__property_id' in e:
			output.append({
				'property':'dz:%s'%(e['property__property_id__property_id'])
				})
	###
	return output

'''
import pandas

page = pandas.read_json(
	'/dcd_data/dubizzle/property_page/download_date=20210815/e6af055243c28753d22c487adc0a67f6.json',
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

##############dubizzle_parsing.py##############