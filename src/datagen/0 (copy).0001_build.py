maxkeyval = 10000

for j in xrange(1, 2): # first 100 entries are duplicates
	print "1" + '|' + str(j)

for j in xrange(2, 2501): # first 100 entries are duplicates
	print str(j) + '|' + str(j)

for j in xrange(1, 2): # first 100 entries are duplicates
	print "2501" + '|' + str(j)

for j in xrange(2, 2501): # first 100 entries are duplicates
	print str(j+2500) + '|' + str(j+2500)

for j in xrange(1, 2): # first 100 entries are duplicates
	print "5001" + '|' + str(j)

for j in xrange(2, 2501): # first 100 entries are duplicates
	print str(j+5000) + '|' + str(j+5000)

for j in xrange(1, 2): # first 100 entries are duplicates
	print "7501" + '|' + str(j)

for j in xrange(2, 2501): # first 100 entries are duplicates
	print str(j+7500) + '|' + str(j+7500)
