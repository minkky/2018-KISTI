import random
import datetime
import codecs, json, sys

minlng = 126.407395
maxlng = 129.249726
minlat = 34.537523
maxlat = 37.754601
output_file1 = "../load/data_1.json"
output_file2 = "../load/data_2.json"

#{ location : [ 129.09633, 35.173691 ], time:  "2017-10-24T00:01:00.425Z", values:{TEMP: 17}}
time = datetime.datetime.now()
for i in range(1, 100000):
	add = random.randrange(0, 30)
	addtime = datetime.timedelta(minutes = add)
	mill = str(random.randrange(0, 1000)).zfill(3)
	lng = random.uniform(minlng, maxlng)
	lat = random.uniform(minlat, maxlat)
	tt = (time + addtime).isoformat()
	data = "{location : [" + str(lng) + ", " + str(lat) + "], time : \"" + tt + "." + mill + "Z\", values : {"

	if(i%2 != 0):
		data = data + "TEMP : " + str(random.randrange(0,30))
		file_name = output_file1
	else:
		data = data + "HUM : " + str(random.randrange(0,55))
		file_name = output_file2
	
	data = data + "}}"
	with open(file_name, 'a+') as file:
		file.write(data+'\n')
	