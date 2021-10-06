###########propertyfinder_parsing.py#############
import re
import datetime

page_url_prefix = 'https://www.propertyfinder.ae'

###########

re_sale_page_url = re.compile(
	r'\<a href\=\"(?P<page_url>\/en\/buy\/[^\"]*?\.html)\" class\=\"',
	flags=re.DOTALL)

def parsing_from_sale_list_to_url(
	page_html,
	page_url,
	):
	output = []
	for m in re.finditer(
		re_sale_page_url,
		page_html):
		page_url1 = m.group('page_url')
		page_url1 = '%s%s'%(page_url_prefix, page_url1)
		output.append({'page_url':page_url1})
	return output

###########

re_page_url = re.compile(
	r'\<a href\=\"(?P<page_url>\/en\/rent\/[^\"]*?\.html)\" class\=\"',
	flags=re.DOTALL)

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

re_page_url_attributes = [
	re.compile(r'\/(?P<property__property_id__property_id>[^\/\.]*?)\.html', flags=re.DOTALL),
	re.compile(r'propertyfinder\.ae\/en\/(?P<property__property_purpose__property_purpose>[^\\\/]*?)\/', flags=re.DOTALL),
]


re_property_attributes = [
	re.compile(r'active" href\="\/en\/buy\/.*?price\"\:(?P<property__property_sale_price_amount__number>[\d\.]+)\,', flags=re.DOTALL),
	re.compile(r'active" href\="\/en\/buy\/.*?price_period_label"\:"(?P<property__property_sale_price_currency__currency>[A-Z]{3})"', flags=re.DOTALL),
	###
	re.compile(r'active" href\="\/en\/rent\/.*?property_price \= \'(?P<property__property_rental_price_amount__number>[\d\.]*?)\'\;', flags=re.DOTALL),
	re.compile(r'active" href\="\/en\/rent\/.*?property_rental_period \= \'(?P<property__property_rental_payment_frequency__payment_frequency>[^\']*?)\'\;', flags=re.DOTALL),
	re.compile(r'active" href\="\/en\/rent\/.*?priceCurrency\"\:\"(?P<property__property_rental_price_currency__currency>[A-Z]*?)\"\,', flags=re.DOTALL),
	###
	re.compile(r'\"Subco\.\"\:\[\"(?P<property__property_community__community>[^\"]*?)\"\]', flags=re.DOTALL),
	re.compile(r'\"Community\"\:\[\"(?P<property__property_district__district>[^\"]*?)\"\]', flags=re.DOTALL),
	re.compile(r'\"City\"\:\[\"(?P<property__property_city__city>[^\"]*?)\"\]', flags=re.DOTALL),
	re.compile(r'\>(?P<property__property_building__building>[^\<\>]*?)\<\/span\>\<\/a\>\<meta property\=\"position\" content\=\"4\"', flags=re.DOTALL),
	re.compile(r'\>(?P<property__property_community__community>[^\<\>]*?)\<\/span\>\<\/a\>\<meta property\=\"position\" content\=\"3\"', flags=re.DOTALL),
	re.compile(r'\>(?P<property__property_district__district>[^\<\>]*?)\<\/span\>\<\/a\>\<meta property\=\"position\" content\=\"2\"', flags=re.DOTALL),
	re.compile(r'\>(?P<property__property_city__city>[^\<\>]*?)\<\/span\>\<\/a\>\<meta property\=\"position\" content\=\"1\"', flags=re.DOTALL),
	re.compile(r'\>(?P<property__property_country__country>[^\<\>]*?)\<\/span\>\<\/a\>\<meta property\=\"position\" content\=\"0\"', flags=re.DOTALL),
	re.compile(r'property_name \= \"(?P<property__property_name__property_name>[^\"]*?)\"\;', flags=re.DOTALL),
	re.compile(r'property_sub_category = \"(?P<property__property_category__property_category>[^\"]*?)\"\;', flags=re.DOTALL),
	re.compile(r'property_type = \"(?P<property__property_type__property_type>[^\"]*?)\"\;', flags=re.DOTALL),
	re.compile(r'property_bedrooms \= \'(?P<property__property_bedroom_number__bedroom_number>\d*?)\'\;', flags=re.DOTALL),
	re.compile(r'property_bathrooms \= \'(?P<property__property_bath_number__bath_number>\d*?)\'\;', flags=re.DOTALL),
	re.compile(r'property_listed_days \= \'(?P<post_date__date_day__day>\d+)\/(?P<post_date__date_month__month>\d+)\/(?P<post_date__date_year__year>\d+)\'\;', flags=re.DOTALL),
	re.compile(r'\"latitude\"\:(?P<geo_point__geo_point_latitude__latitude>[\d\.]*?)\,\"longitude\"\:(?P<geo_point__geo_point_longitude__longitude>[\d\.]*?)\}', flags=re.DOTALL),
	re.compile(r'\<div class\=\"text text\-\-size3\"\>(?P<property__property_location__location>[^\<\>]*?)\<\/div\>', flags=re.DOTALL),
	re.compile(r'LocationFeatureSpecification\"\,\"value\"\:[^\:\,]*?\,\"name\"\:\"(?P<property__property_amenity__property_amenity>[^\"]*?)\"', flags=re.DOTALL),
	re.compile(r'phone\"\,\"value\"\:\"(?P<property__property_agent_phone__phone>[^\"]*?)\"\,', flags=re.DOTALL),
	re.compile(r'\\\"dedLicenceNo\\\"\:\\\"(?P<property__property_ded_licence_number__licence_number>[^\"\\]*?)\\\"\,', flags=re.DOTALL),
	re.compile(r'license_number\"\:\"(?P<property__property_ded_licence_number__licence_number>[^\"]*?)\"\,', flags=re.DOTALL),
	re.compile(r'agent_name \= \"(?P<property__property_agent_name__agent_name>[^\"]*?)\"\;', flags=re.DOTALL),
	re.compile(r'agent_title \= \"(?P<property__property_agent_title__agent_title>[^\"]*?)\"\;', flags=re.DOTALL),
	re.compile(r'broker_id \= \'(?P<property__property_broker_id__broker_id>\d*?)\'\;', flags=re.DOTALL),
	re.compile(r'broker_name \= \"(?P<property__property_broker_name__broker_name>[^\"]*?)\"\;', flags=re.DOTALL),
	re.compile(r'broker_location \= \'(?P<property__property_broker_location__broker_location>[^\']*?)\'\;', flags=re.DOTALL),
	re.compile(r'Description\<\/h3\>\<div [^\<\>]*?\>(?P<property__property_description__text>.*?)\<\/div\>', flags=re.DOTALL),
	re.compile(r'language\"\,\"id\"\:\"\d+\"\,\"attributes\"\:\{\"name\"\:\"(?P<property__property_agent_language__language>[^\"]*?)\"\}', flags=re.DOTALL),
	re.compile(r'\"image_property\"\:\"(?P<property__property_photo_url__photo_url>[^\"]*?)\"\,', flags=re.DOTALL),
	re.compile(r'broker\-image\" src\=\"(?P<property__property_broker_logo_url__photo_url>[^\"]*?)\"', flags=re.DOTALL),
	re.compile(r'property_size_sqft \= \'(?P<property__property_size__size>[^\']*?)\'\;', flags=re.DOTALL),
	re.compile(r'Completion\:\s*\<\/div\>\<div class\="text text\-\-bold property\-facts__content"\>\s*(?P<property__property_completion_status__property_completion_status>[^\<\>]*?)\s*\<', flags=re.DOTALL),
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
				'property':'pf:%s'%(e['property__property_id__property_id'])
				})
	###
	output = [dict(t) for t in {tuple(d.items()) for d in output}]
	return output


'''
import pandas

page = pandas.read_json(
	'/dcd_data/propertyfinder/page_html/source=date202110/39e41b85e79c84362985c733970becff.json',
	orient = 'records',
	lines = True,
	)

page_html = page['page_html'][0]
page_url = page['page_url'][0]


for e in page_parsing(
	page_html,
	page_url,
	):
	if 'property__property_purpose__property_purpose' in e:
		print(e)


print(page_url)
'''
###########propertyfinder_parsing.py#############