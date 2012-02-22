import random

# Generate 10x bigger probe side.
#
maxkeyval = (16*1024*8*3);

for iteration in xrange(10):
	values = range(1, maxkeyval+1)
	random.shuffle(values)

	for i in xrange(len(values)):
		print str((iteration*maxkeyval) + (i+1)) + '|' + str(values[i])
