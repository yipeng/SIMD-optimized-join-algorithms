# vi:ts=2

path:	"datagen/";
bucksize:	1048576 ;

partitioner:
{
	build:
	{
		algorithm:	"no";
		pagesize:		PAGESIZE;
		attribute:	1;
	};

	probe:
	{
		algorithm:	"ALGORITHM";
		pagesize:		PAGESIZE;
		attribute:	2;
		passes:			NUMPASSES;
	};

	hash:
	{
		fn:				"modulo";
		range:		[1,16777216];
		buckets:	BUCKETS;
		skipbits:	SKIPBITS;
	};
};

build:
{
	file: 	"016M_build.tbl";
	schema: ("long", "long");
	jattr:	1;
	select:	(2);
};

probe:
{
	file:		"256M_probe.tbl";
	schema:	("long", "long");
	jattr:	2;
	select:	(1);
};

output:	"test.tbl";

hash:
{
	fn:				"modulo";
	range:		[1,16777216];
	buckets:	8388608;
};

algorithm:
{
	copydata:				"yes";
	partitionbuild:	"no";
	buildpagesize:  32;
	partitionprobe:	"yes";
	steal:					"yes";
};

threads:		THREADS;
