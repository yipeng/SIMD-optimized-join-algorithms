maxkeyval = 10000

for j in xrange(1, 101): # first 100 entries are duplicates
	print "1" + '|' + str(j)

for i in xrange(2, maxkeyval+1-99):
	print str(i) + '|' + str(i)
