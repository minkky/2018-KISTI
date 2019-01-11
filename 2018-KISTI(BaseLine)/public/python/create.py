import random
import datetime
import codecs, json, sys

init_lng = 126.923953
init_lat = 35.455391
print(init_lat,",",init_lng)
cur_lng = init_lng
cur_lat = init_lat
cur_time = datetime.datetime.now()

#output_file1 = "data_1.json"
output_file1 = "data_2.json"

for i in range(80000):
	prev_lng = cur_lng
	prev_lat = cur_lat
	add_lng = random.uniform(0, 0.000098)
	add_lat = random.uniform(0, 0.000098)
	cond = random.randrange(0, 10)
	tcond = random.randrange(0, 4)

	if(tcond == 0):
		addtime = random.uniform(0, 10)
	elif(tcond == 1):
		addtime = random.uniform(0, 30)
	elif(tcond == 2):
		addtime = random.uniform(0, 20)
	else:
		addtime = random.uniform(0, 45)
	
	cur_time = cur_time + datetime.timedelta(seconds = addtime)
	


	if(cond == 0 or cond == 3 or cond == 7 or cond == 8):
		cur_lng = prev_lng + add_lng
		cur_lat = prev_lat
	elif(cond == 1 or cond == 2 or cond == 4 or cond == 5 or cond == 9):
		cur_lng = prev_lng
		cur_lat = prev_lat + add_lat
	else:
		cur_lng = prev_lng + add_lng
		cur_lat = prev_lat + add_lat
	
	#data = "{location : [" + str(cur_lng) + "," + str(cur_lat) +"], time: \""+ str(cur_time.isoformat()).split('.')[0] + "." + str(cur_time.isoformat()).split('.')[1][:3]+"Z\", values: {TEMP:" + str(random.randrange(0,30)) + "}}" 
	data = "{location : [" + str(cur_lng) + "," + str(cur_lat) +"], time: \""+ str(cur_time.isoformat()).split('.')[0] + "." + str(cur_time.isoformat()).split('.')[1][:3]+"Z\", values: {HUM:" + str(random.randrange(0,55)) + "}}" 

	with open(output_file1, 'a+') as file:
		file.write(data+'\n')
		