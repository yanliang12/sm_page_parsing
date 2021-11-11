###########khaleejtimes_parsing.py###########
import re
import pandas

###########

re_news_attributes = [
	re.compile(r'\<li\>\<a href\="\/"\>Home\<\/a\>\<\/li\>\s*\<li\>\/\<\/li\>\s*\<li class\="active"\>\<a href\="\/[^\"]*?"\>(?P<news__news_category__news_category>[^\<\>]*?)\<\/a\>\<\/li\>\s*\<li\>\/\<\/li\>\s*\<li class\="active"\>\<a href\="\/[^\"]*?"\>(?P<news__news_sub_category__news_sub_category>[^\<\>]*?)\<\/a\>\<\/li\>', flags=re.DOTALL),
	re.compile(r'tags\-btm\-nf"\>\s*\<li\>\<a href\="[^\"]*"\>(?P<news__news_sub_category__news_sub_category>[^\<\>]*?)\<\/a\>\<\/li\>', flags=re.DOTALL),
	re.compile(r'\<meta property\="og\:title" content\="(?P<news__news_title__title>[^\"]*?)"\/\>', flags=re.DOTALL),
	re.compile(r'"description"\: "(?P<news__news_description__description>[^\"]*?)",', flags=re.DOTALL),
	re.compile(r'by \<a href\="\/(?P<news__news_author_id__author_id>[^\"]*?)"\>\<h3\>(?P<news__news_author_name__author_name>[^\<\>]*?)\<\/h3\>\<\/a\>', flags=re.DOTALL),
	re.compile(r'Published\:\<\/span\>\s*(?P<news__news_publish_time__time>[^\<\>]*?)\s*\<\/p\>', flags=re.DOTALL),
	re.compile(r'class\="article\-lead\-img\-caption"\>\s*(?P<news__news_image_capition__text>[^\<\>]*?)\s*\<em', flags=re.DOTALL),
	re.compile(r'\<\!\-\- element\: body \-\-\>(?P<news__news_body__text>.+)\<ul class\="tags\-btm\-nf"\>', flags=re.DOTALL),
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

###########

'''
page = pandas.read_json(
	'/dcd_data/khaleejtimes/page_html/source=date2021/9a7872071dcb7f60b5d74998c3bb9dde.json',
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

###########khaleejtimes_parsing.py###########