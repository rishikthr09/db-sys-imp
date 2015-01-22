f = open("query.txt");
for i in f.readlines():
	print 'dbs.queryType("' + i[:-2] + '");'
