import random

# Generate 10x bigger probe side.
#

maxkeyval =  10000;

for iteration in xrange(100):
	values = range(maxkeyval, maxkeyval+maxkeyval+1)

	if iteration < 5000:
		print str("1" + '|' + str(values[i]))
	for i in xrange(len(values)-5000):
		print str((5000+iteration*maxkeyval) + (i+1)) + '|' + str(values[i])

