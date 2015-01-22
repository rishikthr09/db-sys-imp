f = open("bicycle_report.csv");
for i in f.readlines():
	a = i[:-1].split(";");
	for j in range(len(a)):
		if(j!=len(a)-1):
			print '"' + a[j] + '",',
		else:
		 	print '"' + a[j] + '"'
