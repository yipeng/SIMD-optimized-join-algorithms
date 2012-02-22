import random

# Generate 10x bigger probe side.
#
maxkeyval = 16*1024;

for iteration in xrange(10):
	values = range(1, maxkeyval*8+1)
	random.shuffle(values)
        values = values[0:maxkeyval]

	for i in xrange(len(values)):
		print str((iteration*maxkeyval) + (i+1)) + '|' + str(values[i])
