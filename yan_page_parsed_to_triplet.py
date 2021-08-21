############yan_page_parsed_to_triplet.py############
import hashlib 

def str_md5(input):
	return hashlib.md5(input.encode()).hexdigest()

def parsed_info_2_kg_triplets(input):
	try:
		output = []
		############
		'''
		find the main entity of the page
		'''
		main_subject_type = None
		main_subject_name = None
		for e in input:
			keys = list(e.keys())
			if len(keys) == 1:
				key_parsed = keys[0].split('__')
				if len(key_parsed) == 1:
					main_subject_type = key_parsed[0]
					main_subject_name = e[key_parsed[0]]
		##############
		'''
		find the attributes of the main entity
		'''
		for e in input:
			keys = list(e.keys())
			if len(keys) == 1:
				key_parsed = keys[0].split('__')
				if len(key_parsed) == 3:
					relation = key_parsed[1]
					object_type = key_parsed[2]
					object_name = e[keys[0]]
					triplet = {
						'subject_name':main_subject_name,
						'subject_type':main_subject_type,
						'relation':relation,
						'object_name':object_name,
						'object_type':object_type,
						}
					output.append(triplet)
		##############
		'''
		process the multi key entities
		'''
		for e in input:
			keys = list(e.keys())
			if len(keys) > 1:
				sub_subject_name = None
				sub_subject_type = None
				'''
				find the main entity of the multi-key entity
				'''
				for k in keys:
					key_parsed = k.split('__')
					if len(key_parsed) == 1:
						sub_subject_name = e[k]
						sub_subject_type = k
				if sub_subject_name is None:
					print(e)
				'''
				find the attributes of the
				'''
				for k in keys:
					key_parsed = k.split('__')
					if len(key_parsed) == 3:
						relation = key_parsed[1]
						object_type = key_parsed[2]
						object_name = e[k]
						triplet = {
							'subject_name':sub_subject_name,
							'subject_type':sub_subject_type,
							'relation':relation,
							'object_name':object_name,
							'object_type':object_type,
							}
						output.append(triplet)
		###########
		for t in output:
			try:
				subject = str_md5(t['subject_name']+t['subject_type'])
				object = str_md5(t['object_name']+t['object_type'])
				t['subject'] = subject
				t['object'] = object
			except:
				print(e)
		############
		return output
	except Exception as e:
		return []


'''
input = [
{'company':'group42ai'},
{'company__company_name__company_name':'Group 42'},
{'geo_point':'21.1,54.6','geo_point__longitude__longitude':'21.1','geo_point__latitude__latitude':'54.6'},
{'company__company_geo_location__geo_point':'21.1,54.6'},
]


output = parsed_info_2_kg_triplets(input)

for t in output:
	print(t)

'''
############yan_page_parsed_to_triplet.py############