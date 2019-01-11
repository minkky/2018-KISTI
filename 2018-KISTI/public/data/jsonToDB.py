import os, pymongo, time

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

mongodb_path, DB_USER, mongodb_url, partition = getConfig()
mongo_path = "cd " + mongodb_path
mongoimport = "mongoimport --db " + DB_USER + " --collection "
import_condition = " --drop --file "
json_path = os.path.abspath(os.path.join(os.getcwd(), "../load"))
json_list = os.listdir(json_path)
json_list.sort()

def getFileDate(csv_filename):
	filenameSplit = csv_filename.split("_")
	made = filenameSplit[len(filenameSplit)-1]
	return made.replace(".csv", "")

def getLatestDate():
	for x in pymongo.MongoClient(mongodb_url)[DB_USER][partition].find({"_id":"metadata"}, { "_id": 0, "latestUpdate":1}):
		latestUpdate = x["latestUpdate"]
		return latestUpdate

def jsonToDB():

	latestUpdate = getLatestDate()
	latestUpdate = time.strptime(latestUpdate, "%Y-%m-%d")
	for f in json_list:
		if f.find('.json') is not -1:
			collection_name = f.replace('.json', '')
			collection_date = time.strptime(collection_name.split("_")[1], "%Y%m%d")
			if latestUpdate <= collection_date:			
				import_cmd = mongoimport + collection_name + import_condition + json_path + "\\" +f
				os.system(mongo_path + " && " + import_cmd)
	#print("Complete : json to mongoimport")

jsonToDB()