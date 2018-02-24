f = open("sampleQuery");
for i in f.readlines():
	print 'dbs.queryType("' + i[:-1] + '");'
