###########bayut_parsing.py#############

import re

re_page = [
	re.compile(r'name\"\:\"(?P<property__post_category__post_category>[^\"]*?) in (?P<property__property_city__city>[^\"]*?)\"\,\"position\"\:2\,', flags=re.DOTALL),
	re.compile(r'for Sale.*?Price\"\>(?P<property__property_sale_price_amount__number>[\d\,\.]+)\<\/span\>', flags=re.DOTALL),
	re.compile(r'for Sale.*?Currency\"\>(?P<property__sale_price_currency__currency>[A-Z]{3})\<\/span\>', flags=re.DOTALL),
	re.compile(r'for Rent.*?Price\"\>(?P<property__property_rental_price_amount__number>[\d\,\.]+)\<\/span\>', flags=re.DOTALL),
	re.compile(r'for Rent.*?Currency\"\>(?P<property__property_rental_price_currency__currency>[A-Z]{3})\<\/span\>', flags=re.DOTALL),
	re.compile(r'rent_frequency"\:"(?P<property__property_rental_payment_frequency__payment_frequency>[^\"]*?)"', flags=re.DOTALL),
	re.compile(r'Beds\"\>\<span .*?\>(?P<property__property_bedroom_number__bedroom_number>[^\<\>]*?)\<', flags=re.DOTALL),
	re.compile(r'Baths\"\>\<span .*?\>(?P<property__property_bath_number__bath_number>[^\<\>]*?)\<', flags=re.DOTALL),
	re.compile(r'Area\"\>\<span .*?\>\<span\>(?P<property__property_size__size>[^\<\>]*?)\<', flags=re.DOTALL),
	re.compile(r'aria\-label\="Type"\>(?P<property__property_type__property_type>[^\<\>]*?)\<', flags=re.DOTALL),
	re.compile(r'Completion status"\>(?P<property__property_completion_status__property_completion_status>[^\<\>]*?)\<', flags=re.DOTALL),
	re.compile(r'Purpose"\>(?P<property__property_purpose__property_purpose>[^\<\>]*?)\<', flags=re.DOTALL),
	re.compile(r'Furnishing"\>(?P<property__property_furnished__furnished>[^\<\>]*?)\<', flags=re.DOTALL),
	re.compile(r'Absolute creation date"\>(?P<property__property_post_date__post_date>[^\<\>]*?)\<', flags=re.DOTALL),
	re.compile(r'GeoCoordinates","latitude"\:(?P<geo_point__geo_point_latitude__latitude>[\d\.]*?),"longitude"\:(?P<geo_point__geo_point_longitude__longitude>[\d\.]*?)\}', flags=re.DOTALL),
	re.compile(r'name"\:"(?P<property__property_district__district>[^\"]*?)","position"\:3,', flags=re.DOTALL),
	re.compile(r'name"\:"(?P<property__property_community__community>[^\"]*?)","position"\:4,', flags=re.DOTALL),
	re.compile(r'name"\:"(?P<property__property_sub_community__sub_community>[^\"]*?)","position"\:5,', flags=re.DOTALL),
	re.compile(r'name"\:"(?P<property__property_building__building>[^\"]*?)","position"\:6,', flags=re.DOTALL),
	re.compile(r'Agent name" .*?\>(?P<property__property_agent_name__agent_name>[^\<\>]*?)\<', flags=re.DOTALL),
	re.compile(r'primaryPhoneNumber"\:"(?P<property__property_agent_phone__phone>[^\"\\]*?)"', flags=re.DOTALL),
	re.compile(r'mobilePhoneNumber\"\:\"(?P<property__property_agent_mobile_phone__mobile_phone>[^\"\\]*?)\"', flags=re.DOTALL),
	re.compile(r'agency"\:\{"external_id"\:"\d+","name"\:"(?P<property__property_broker_name__broker_name>[^\"]*?)",', flags=re.DOTALL),
	re.compile(r'\<\/use\>\<\/svg\>\<\/div\>\<div .*?\>\<span .*?\>(?P<property__property_amenity__property_amenity>[^\<\>]*?)\<', flags=re.DOTALL),
	]


def page_parsing(
	page_html,
	page_url,
	):
	output = []
	for r in re_page:
		for m in re.finditer(r, page_html):
			output.append(m.groupdict())
	for e in output:
		if 'geo_point__geo_point_longitude__longitude' in e and 'geo_point__geo_point_latitude__latitude':
			e['geo_point'] = '%s,%s'%(
				e['geo_point__geo_point_latitude__latitude'],
				e['geo_point__geo_point_longitude__longitude'],			
				)
			output.append({'property__property_geo_location__geo_point':
				e['geo_point']
				})
	return output

'''
import pandas

page = pandas.read_json(
	'/dcd_data/bayut/page_html/source=data202110/1afad4eb49ef3dfcd0e42907edc6dbfb.json',
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

###########bayut_parsing.py#############