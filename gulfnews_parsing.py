###########gulfnews_parsing.py###########
import re
import pandas

##########

re_news_attributes = [
	re.compile(r'class\="image\-caption" itemprop\="description"\>\s*(?P<news__news_image_capition__text>[^\<\>]*?)\s*\<', flags=re.DOTALL),
	re.compile(r'\<div class\=\"story\-block\"\>(?P<news__news_body__text>.*?)\<\/div\>\s*\<\/div\>', flags=re.DOTALL),
	re.compile(r'\<meta name\="description" content\="(?P<news__news_description__description>[^\"]*?)"\s*\/\>', flags=re.DOTALL),
	re.compile(r'\'publishedDate\'\:\s*\'(?P<news__news_publish_time__time>[^\']*?)\'\,', flags=re.DOTALL),
	re.compile(r'\'page_section\'\:\s*\'(?P<news__news_category__news_category>[^\']*?)\'\,', flags=re.DOTALL),
	re.compile(r'\'page_subsection\'\:\s*\'(?P<news__news_sub_category__news_sub_category>[^\']*?)\'\,', flags=re.DOTALL),
	re.compile(r'\'title\'\:\s*\'(?P<news__news_title__title>[^\']*?)\',', flags=re.DOTALL),
]

re_author_block = re.compile(r'authors\'\:\s*\[[^\[\]]+\]\,', flags=re.DOTALL)

re_author_attribute = [
	re.compile(r'\"(?P<news__news_author_name__author_name>[^\"]*?)\"', flags=re.DOTALL),
	]

def page_parsing(
	page_html, 
	page_url,
	):
	output = []
	for r in re_news_attributes:
		for m in re.finditer(r, page_html):
			output.append(m.groupdict())
	for a in re.finditer(re_author_block, page_html):
		b = a.group()
		for r1 in re_author_attribute:
			for m1 in re.finditer(r1, b):
				output.append(m1.groupdict())
	for e in output:
		for k in e:
			if k in ['news__news_body__text']:
				e[k] = re.sub(r'\<[^\<\>]*?\>', r'', e[k])
	return output

##########

'''
page = pandas.read_json(
	'/dcd_data/gulfnews/page_html/source=date2021/0bb82176789ac4ecc32b5bea83fec8e9.json',
	lines = True,
	orient = 'records')

page_url = page['page_url'][0]
page_html = page['page_html'][0]

print(page_url)

output = page_parsing(
	page_html, 
	page_url,
	)

for e in output:
	print(e)

'''

###########gulfnews_parsing.py###########