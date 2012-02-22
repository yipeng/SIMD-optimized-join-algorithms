maxkeyval = 16*1024*8*3;

# gen 6mb 
#maxkeyval = 16*1024*8*3 + 60000;
# gen 5mb 
#maxkeyval = 16*1024*8*3;

for i in xrange(1, maxkeyval+1):
	print str(i) + '|' + str(i)
