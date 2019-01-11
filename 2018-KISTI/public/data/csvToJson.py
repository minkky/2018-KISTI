import csv, json, sys, os, pymongo
from collections import defaultdict
from datetime import datetime

def getConfig():
	file = open('../../config.js', "r")
	readFile = file.read()
	for line in readFile.splitlines():
		if 'mongodb_path' in line:
			mongodb_path = line.split('\'')[3]
		if 'DB_USER' in line:
			DB_USER = line.split('\'')[3]
		if 'mongodb_url' in line:
			mongodb_url = line.split('\'')[3]
		if 'partition' in line:
			partition = line.split('\'')[3]
	file.close()
	return mongodb_path, DB_USER, mongodb_url, partition

def get_csv_filename(arg):
	for i in arg:
		if ".csv" in i:
			return i

def getFileDate(csv_filename):
	filenameSplit = csv_filename.split("_")
	made = filenameSplit[len(filenameSplit)-1]
	return made.replace(".csv", "")

def csvToJson(csv_filename):
	#print(csv_filename + "를 읽어오는 중입니다.")
	made = getFileDate(csv_filename)

	filelist = {}
	format = "%Y-%m-%d %H:%M:%S"
	cnt = 0
	for row in csv.DictReader(open(csv_filename, encoding="utf-8-sig")):
		cnt += 1
		exist_NA = False
		node_id = row['node_id']
		lng = row['lng']
		lat = row['lat']
		time = row['timestamp']
		if time != '0000-00-00 00:00:00':
			time = datetime.strptime(time, format)
			time = time.isoformat()
		else:
			continue

		delCols = []
		for (k, v) in row.items():
			if k == 'lng' or k == 'lat' or k == 'timestamp' or k == 'node_id':
				delCols.append(k)
				if v == 'NA':
					exist_NA = True
					break
			elif v == 'NA':
				delCols.append(k)
			elif v.find('\\') != -1:
				delCols.append(k)
				
		if exist_NA == True:
			continue

		for item in delCols:
			del row[item]

		try:
			filelist[node_id]
		except:
			filelist[node_id] = open('../load/' + node_id + "_" + made + '.json', 'a+', encoding='utf-8-sig')
		
		loc = [float(lng), float(lat)]
		data = {}
		data['thingId'] = node_id
		data['location'] = loc
		data['time'] = time
		values = {}
		for (k, v) in row.items():
			values[k] = v
		data['values'] = values
		filelist[node_id].write(json.dumps(data) + "\n")
		'''if cnt % 100000 == 0:
			print(cnt)
		'''

	for f in filelist:
		filelist[f].close()
	print("Complete : csv to json")


def getLatestDate():
	for x in pymongo.MongoClient(mongodb_url)[DB_USER][partition].find({"_id":"metadata"}, { "_id": 0, "latestUpdate":1}):
		latestUpdate = x["latestUpdate"]
		return latestUpdate
	return "not exist"

def updateLastestDateInDB(fileDate):
	if getLatestDate() != "not exist":
		latestUpdate = getLatestDate()
		latestUpdate = datetime.strptime(latestUpdate, "%Y-%m-%d")
		compareDate = datetime.strptime(fileDate, "%Y%m%d")
		if latestUpdate < compareDate:
			latestUpdate = str(compareDate.date())
			print("latest update Date 가 변경되었습니다.")
		else:
			return
	else:
		latestUpdate = str(datetime.strptime(fileDate, "%Y%m%d").date())
	pymongo.MongoClient(mongodb_url)[DB_USER][partition].update({"_id":"metadata"}, {"$set":{"latestUpdate":latestUpdate}}, upsert=True)

mongodb_path, DB_USER, mongodb_url, partition = getConfig()
csv_filename = get_csv_filename(sys.argv)
fileDate = getFileDate(csv_filename)

csvToJson(csv_filename)
updateLastestDateInDB(fileDate)
