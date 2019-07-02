import sqlite3,hashlib,sys


def makeDBDict(pathToDB):
	tableCon = sqlite3.connect(pathToDB)
	tableCur = tableCon.cursor()

	tableCur.execute("SELECT name FROM sqlite_master WHERE type='table';")
	tableNames = tableCur.fetchall()
	tableNames = [x[0] for x in tableNames]

	#now build dict {tableName:[hashOfRow,hashOfRow,hashOfRow...]}
	tbleDict = dict()
	for dbTable in tableNames:
		tbleDict[dbTable] = set()
		tableCur.execute("SELECT * FROM {}".format(dbTable))
		for rec in tableCur.fetchall():
			tbleDict[dbTable].add(hashlib.md5(str(rec)).hexdigest())

	return tbleDict


if len(sys.argv) < 3:
	print("Missing arguements")
	sys.exit(0)

print("Old db being diffed: {}".format(sys.argv[1]))
print("New db being diffed: {}".format(sys.argv[2]))
oldDB = makeDBDict(sys.argv[1])
newDB = makeDBDict(sys.argv[2])

for tbl in oldDB:
	if tbl not in newDB:
		newRecords = 0
		survivors = 0
	else:
		survivors = oldDB[tbl] and newDB[tbl]
		newRecords = newDB[tbl] - oldDB[tbl]
		recordRetentionRate = (abs(float(len(list(oldDB[tbl]))) - float(len(list(survivors)))) / float(len(list(oldDB[tbl])))) * 100.0
		numberOfExistingRecords = len(list(oldDB[tbl]))
		numberOfNewRecords = len(list(newDB[tbl]))

		print("Table Name:{}".format(tbl))
		print("survivors:{}".format(len(survivors)))
		print("record loss rate:{}".format(recordRetentionRate))
		print("number of old records:{}".format(numberOfExistingRecords))
		print("number of new records:{}".format(numberOfNewRecords))
