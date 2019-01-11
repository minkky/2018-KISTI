#how to use : python convertJson.py ../data/filename.json ../upload/filename.json
import codecs, json, sys

data = []

read_file = sys.argv[1]
output_file = sys.argv[2]
output_file = output_file.split('.json')[0] + "_"

with codecs.open(read_file, 'rU', 'utf-8') as f:
	index = 1
	count = 1
	file_name = output_file + str(index) + ".json"
	print(file_name)

	for line in f:
		strr = json.loads(line)
		strr = str(strr)
		data.append(strr)
		sp = strr.split(',')
		
		etc = ""
		for i in range(len(sp)):
			sp[i] = str(sp[i])
			if "LAT" in sp[i]:
				LAT = sp[i].split(':')[1]
			elif "LNG" in sp[i]:
				LNG = sp[i].split(':')[1]
			else:
				etc = etc + sp[i]
				if i != len(sp) -1:
					etc = etc + ", "
		if LAT != "" and LNG != "":
			location = "{ location : [" + LNG + ", " + LAT+"]," + etc
#			location = "{ location : { type: 'Point', coordinates:[" + LNG + ", " + LAT+"]}," + etc
			location = location.replace("'", "\"")
		
		with open(file_name, 'a+') as file:
			file.write(location+'\n')
		if count % 10000 == 0:
			print(">" + 10000*index + " imported !")
			
		if count % 50000 == 0:
			index = index +1
			file_name = output_file + str(index) + ".json"
			print(file_name)
		count = count + 1
