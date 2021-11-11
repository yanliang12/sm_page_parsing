###########thenationalnews_parsing.py###########
import re
import pandas

##########

re_news_attributes = [
	re.compile(r'body\-paragraph\"\>(?P<news__news_body__text>.+)\<div class\=\"updated\-date ', flags=re.DOTALL),
	re.compile(r'active\-section"\>\<a href\="\/[^\\\/]*?\/"\>(?P<news__news_category__news_category>[^\<\>]*?)\<\/a\>', flags=re.DOTALL),
	re.compile(r'Updated\:\s*(?P<news__news_update_time__time>[^\<\>]*?)\<\/div\>', flags=re.DOTALL),
	re.compile(r'font_color_grey_2"\>(?P<news__news_publish_time__time>[^\<\>]{4,20})\<\/div', flags=re.DOTALL),
	re.compile(r'\/Author\/(?P<news__news_author_id__author_id>[^\\\/]*?)\/"\>(?P<news__news_author_name__author_name>[^\<\>]*?)\<\/a\>', flags=re.DOTALL),
	re.compile(r'image\-metadata"\>\<span\>\s*(?P<news__news_image_capition__text>[^\<\>]*?)\s*\<\/span\>', flags=re.DOTALL),
	re.compile(r'\<meta name\="title" content\="(?P<news__news_title__title>[^\']*?)"\/\>', flags=re.DOTALL),
	re.compile(r'sub\-headline"\>(?P<news__news_description__description>[^\<\>]*?)\<\/h2\>', flags=re.DOTALL),
	re.compile(r'active\-section"\>\<a href\="\/[^\\\/]*?\/[^\\\/]*?\/"\>(?P<news__news_sub_category__news_sub_category>[^\<\>]*?)\<\/a\>', flags=re.DOTALL),
]

def page_parsing(
	page_html, 
	page_url,
	):
	output = []
	for r in re_news_attributes:
		for m in re.finditer(r, page_html):
			output.append(m.groupdict())
	for e in output:
		for k in e:
			if k in ['news__news_body__text']:
				e[k] = re.sub(r'\<[^\<\>]*?\>', r'', e[k])
	return output

##########

'''

page = pandas.read_json(
	'/dcd_data/thenationalnews/page_html/source=date2021/d2f4fe3ade98622366dce46631226e60.json',
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
###########thenationalnews_parsing.py###########