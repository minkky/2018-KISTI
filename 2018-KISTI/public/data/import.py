import sys, os, datetime


def get_csv_filename(arg):
	for i in arg:
		if ".csv" in i:
			return i

arg = get_csv_filename(sys.argv)
csv_to_json = "csvToJson.py " + arg
json_to_db = "jsonToDB.py " + arg
cmd = [csv_to_json, json_to_db]

def importer():
	if arg is None:
		print("Error: 입력 python import.py <csv_filename>.csv")
	else:
		for c in cmd:
			try:
				open(os.path.join(os.getcwd(), arg))
			except FileNotFoundError:
				print("FileNotFoundError")
				break
			else:
				print("[start] ", datetime.datetime.now())
				os.system('python ' + c)
				print("[end] ",datetime.datetime.now())

importer()