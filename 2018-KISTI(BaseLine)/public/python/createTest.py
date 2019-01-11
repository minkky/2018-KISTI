import codecs, json, sys

read_file = "../data/busan.json"
output_file1 = "../load/busan_1.json"
output_file2 = "../load/busan_2.json"
data = []

with codecs.open(read_file, 'rU', 'utf-8') as f:
	index = 1
	count = 1

	for line in f:
		strr = json.loads(line)
		strr = str(strr)
		data.append(strr)
		sp = strr.split(',')
		etc = "" #hum or temp
		etc_name = ""
		time = ""

		for i in range(len(sp)):
			sp[i] = str(sp[i])
			if "LAT" in sp[i]:
				LAT = sp[i].split(':')[1]
			elif "LNG" in sp[i]:
				LNG = sp[i].split(':')[1]
			elif "HUM" in sp[i] and count%2 ==0:
				etc = sp[i].split(':')[1]
				etc_name = "HUM"
			elif "TEMP" in sp[i] and count%2 != 0:
				etc = sp[i].split(':')[1]
				etc_name = "TEMP"
			elif "time" in sp[i]:
				time = sp[i].split(':')[1] + ":" + sp[i].split(':')[2] + ":" + sp[i].split(':')[3]

		location = "{ location : [" + LNG + "," + LAT + " ], time: "+ time +", values:{" + etc_name + ":" + etc + "}}"
		location = location.replace("'", "\"")
		count = count +1
		#print(location)

		if count % 2 == 0:
			file_name = output_file1
		else:
			file_name = output_file2

		with open(file_name, 'a+') as file:
			file.write(location+'\n')