
.PHONY: all clean doc distclean 

include system.inc

CXX=g++
# CPPFLAGS are flags for the C Pre-Processor
# CXXFLAGS are flags for the C++ compiler
# Default rules in make pass CPPFLAGS to just about everything
CPPFLAGS=-Idist/include/
#CPPFLAGS+=-DPREFETCH
#CPPFLAGS+=-DOUTPUT_AGGREGATE
#CPPFLAGS+=-DOUTPUT_WRITE_NT
#CPPFLAGS+=-DOUTPUT_WRITE_NORMAL
#CPPFLAGS+=-DOUTPUT_ASSEMBLE
#CPPFLAGS+=-DDEBUG #-DDEBUG2
CPPFLAGS+=-DSUSPEND_OUTPUT
#CPPFLAGS+=-msse -mmmx -msse2 -msse3 -msse4 -mfpmath=sse
CPPFLAGS+=-msse3 -mfpmath=sse


CXXFLAGS=$(SYSFLAGS)
CXXFLAGS+=-g -fif-conversion -fif-conversion2 -funroll-loops -O3#-Wall
#CXXFLAGS+= -pg # profile code
#CXXFLAGS+=-O3
LDFLAGS=-Ldist/lib/
LDLIBS=-lconfig++ -lpthread -lbz2

ifeq ($(HOSTTYPE),sparc)
LDLIBS+=-lcpc
CXXFLAGS+=-mcpu=ultrasparc
endif

all: dist multijoin multijoin-serial

FILES = schema.o hash.o parser.o table.o joinerfactory.o page.o \
	algo/nl.o algo/base.cpp ProcessorMap.o Barrier.o loader.o \
	partitionerfactory.o partitioner.o algo/hashbase.o \
	algo/storage.o affinitizer.o algo/hashtable.o \
	comparator.o algo/flatmem.o


clean:
	rm -f *.o
	rm -f algo/*.o
	rm -f multijoin multijoin-serial

distclean: clean
	rm -rf dist

doc: Doxyfile
	doxygen


multijoin-serial: $(FILES) main-serial.o

multijoin: $(FILES) main.o

dist:
	./pre-init.sh
