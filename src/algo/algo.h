/*
    Copyright 2011, Spyros Blanas.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef __MYALGO__
#define __MYALGO__

#include <vector>
#include <libconfig.h++>
#include "../table.h"
#include "../hash.h"
#include "../lock.h"
#include "../Barrier.h"
#include "../partitioner.h"
#include "hashtable.h"
//#include <xmmintrin.h>
//#include <emmintrin.h>
//#include <pmmintrin.h>
//#include <tmmintrin.h>
//#include <smmintrin.h>

/* 1 is inner! */

class BaseAlgo {
	public:
		BaseAlgo(const libconfig::Setting& cfg);
		virtual ~BaseAlgo() { }

		/**
		 * General initialization function, guaranteed to be called by only *one* thread.
		 * Copies arguments to private space, generates output and s1 schema.
		 * (build schema is just {int, s1 schema}).
		 */
		virtual void init(
			Schema* schema1, vector<unsigned int> select1, unsigned int jattr1,
			Schema* schema2, vector<unsigned int> select2, unsigned int jattr2);

		virtual void destroy();

		virtual void build(SplitResult t, int threadid) = 0; 
		virtual PageCursor* probe(SplitResult t, int threadid) = 0;

	protected:
		/* s1, s2 are the two inputs, sout the output, sbuild the way the
		 * hash table has been built.
		 */
		Schema* s1, * s2, * sout, * sbuild;
		vector<unsigned int> sel1, sel2;
		unsigned int ja1, ja2, size, s1cols;
};

/**
 * Non thread-safe class
 */
class NestedLoops : public BaseAlgo {
	public:
		NestedLoops(const libconfig::Setting& cfg);
		virtual ~NestedLoops() { }
		virtual void destroy();
		virtual void build(SplitResult t, int threadid);
		virtual PageCursor* buildIsPart(SplitResult t, int threadid);
		virtual HashTable buildIsPartHashV(SplitResult t, int threadid);
		virtual WriteTable* probeCursor(PageCursor* t, int threadid, bool atomic, WriteTable* ret = NULL);
		virtual PageCursor* probe(SplitResult t, int threadid);
		virtual PageCursor* probeN2(SplitResult t, int threadid);
		virtual PageCursor* probeStar(SplitResult t, int threadid, PageCursor* t1);
		virtual PageCursor* probeStarHashV(SplitResult tin, int threadid, HashTable hashtable);
		virtual PageCursor* probeNestedHash(SplitResult tin, int threadid, HashTable hashtable);
		virtual WriteTable* probe(PageCursor* t, int threadid, WriteTable* ret);
	protected:
		int nthreads;
		HashFunction* _hashfn;
#ifdef OUTPUT_AGGREGATE
		int* aggregator;
		static const int AGGLEN=512;
#endif
	private:

		PageCursor* realbuildCursor(PageCursor* t, int threadid);
		HashTable realbuildHashCursor(PageCursor* t, int threadid);
		/**
		 * Joins matching tuples in two buckets, calling \a joinPageTup for
		 * each tuple in \a b2.
		 * \a b1 conforms to \a sbuild and \a b2 to \a s2.
		 * @param output Output table to append joined tuples.
		 * @param p1 Page as first input to join.
		 * @param p2 Page as other input to join.
                 *
                 * Tuple-based nested loop X
		 */
		void joinPagePage1(WriteTable* output, Page* p1, Page* p2);
		void joinPagePage2(WriteTable* output, Page* p1, Page* p2);
		void joinDuplicateOuter(WriteTable* output, Page* p1, Page* p2);
		/**
		 * Joins matching tuples in bucket with single tuple. 
		 * \a bucket conforms to \a sbuild and \a tuple to \a s2.
		 * @param output Output table to append joined tuples.
		 * @param page Page as first input to join.
		 * @param tuple Tuple as other input to join.
                 *
                 * Page nested loop
		 */
		void joinPageTup(WriteTable* output, Page* page, void* tuple);
		void joinPageTupExp(WriteTable* output, Page* page, void* tuple);
		void joinPageTupExp1(WriteTable* output, Page* page, void* tuple);
		void joinPageTupExp1x(WriteTable* output, Page* page, void* tuple);
		void joinPageTupExp2(WriteTable* output, Page* page, void* tuple);
		void joinPageTupExp3(WriteTable* output, Page* page, void* tuple);
        void joinPageTupExperimental(WriteTable* output, Page* page, void* tuple);
        void joinPageTupExperimental2(WriteTable* output, Page* page, void* tuple);
        void joinPageTupExperimental3(WriteTable* output, Page* page, void* tuple);
        void joinPageTupExperimental301(WriteTable* output, Page* page, void* tuple);
        void joinPageTupExperimental302(WriteTable* output, Page* page, void* tuple);
        void DuplicateInner(WriteTable* output, Page* page, void* tuple);
        void DuplicateOuter(WriteTable* output, Page* page, int tuple[4]);
        void joinPageTupExperimental5(WriteTable* output, Page* page, void* tuple);
//        void helloworld();
//        void print128_num(__m128i vector);
		/**
		 * Joins matching tuples in bucket with single tuple. 
		 * \a bucket conforms to \a sbuild and \a tuple to \a s2.
		 * @param output Output table to append joined tuples.
		 * @param page Page as first input to join.
		 * @param tuple Tuple as other input to join.
		 * @deprecated by \ref joinPageTup
                 *
                 * Block nested loop
		 */
		void joinBucketTup(WriteTable* output, Bucket* bucket, void* tuple) {
			joinPageTup(output, bucket, tuple);
		}

		void prepareBuild(WriteTable* build, PageCursor* orig);
		PageCursor* t1; 	// storage for first table

                /*
                 * Yipeng's changes
                 * Block Nested loop join
                 *
                 * Block size of smaller (outer) table + 1 page size of larger (inner)
                 *  + 1 page size of output
		 */
                 // void joinBlockPage(WriteTable* output, Bucket* p1, Page* p2);
};

class HashBase : public BaseAlgo {
	public:
		HashBase(const libconfig::Setting& cfg);
		virtual ~HashBase();
		virtual void init(
			Schema* schema1, vector<unsigned int> select1, unsigned int jattr1,
			Schema* schema2, vector<unsigned int> select2, unsigned int jattr2);
		virtual void destroy();
		virtual void build(SplitResult t, int threadid) = 0;
		virtual PageCursor* probe(SplitResult t, int threadid) = 0;
	protected:
		HashFunction* _hashfn;
		HashTable hashtable;
		int nthreads;
		int outputsize;
#ifdef OUTPUT_AGGREGATE
		int* aggregator;
		static const int AGGLEN=512;
#endif
};

class StoreCopy : public HashBase {
	public:
		StoreCopy(const libconfig::Setting& cfg) : HashBase(cfg) {}
		virtual ~StoreCopy() {}

		virtual void init(
			Schema* schema1, vector<unsigned int> select1, unsigned int jattr1,
			Schema* schema2, vector<unsigned int> select2, unsigned int jattr2);
		virtual void destroy();
		
		virtual void build(SplitResult t, int threadid) = 0;
		virtual PageCursor* probe(SplitResult t, int threadid) = 0;

	protected:
		void buildCursor(PageCursor* t, int threadid, bool atomic);

		WriteTable* probeCursor(PageCursor* t, int threadid, bool atomic, WriteTable* ret = NULL);

	private:
		template <bool atomic>
		void realbuildCursor(PageCursor* t, int threadid);

		template <bool atomic>
		WriteTable* realprobeCursor(PageCursor* t, int threadid, WriteTable* ret = NULL);
		template <bool atomic>
		WriteTable* realprobeCursorBranchPoorly(PageCursor* t, int threadid, WriteTable* ret = NULL);
		template <bool atomic>
		WriteTable* simdprobeCursor(PageCursor* t, int threadid, WriteTable* ret = NULL);
};

class StorePointer : public HashBase {
	public:
		StorePointer(const libconfig::Setting& cfg) : HashBase(cfg) {}
		virtual ~StorePointer() {}

		virtual void init(
			Schema* schema1, vector<unsigned int> select1, unsigned int jattr1,
			Schema* schema2, vector<unsigned int> select2, unsigned int jattr2);
		virtual void destroy();

		virtual void build(SplitResult t, int threadid) = 0;
		virtual PageCursor* probe(SplitResult t, int threadid) = 0;

	protected:
		void buildCursor(PageCursor* t, int threadid, bool atomic);

		WriteTable* probeCursor(PageCursor* t, int threadid, bool atomic, WriteTable* ret = NULL);

	private:
		template <bool atomic>
		void realbuildCursor(PageCursor* t, int threadid);

		template <bool atomic>
		WriteTable* realprobeCursor(PageCursor* t, int threadid, WriteTable* ret = NULL);
};


template <typename Super>
class BuildIsPart : public Super {
	public:
		BuildIsPart(const libconfig::Setting& cfg) : Super(cfg) {}
		virtual ~BuildIsPart() {}
		virtual void build(SplitResult t, int threadid);
};

template <typename Super>
class BuildNestedLoopPart : public Super {
	public:
		BuildNestedLoopPart(const libconfig::Setting& cfg) : Super(cfg) {}
		virtual ~BuildNestedLoopPart() {}
		virtual void build(SplitResult t, int threadid);
};

template <typename Super>
class BuildIsNotPart : public Super {
	public:
		BuildIsNotPart(const libconfig::Setting& cfg) : Super(cfg) {}
		virtual ~BuildIsNotPart() {}
		virtual void build(SplitResult t, int threadid);
};


template <typename Super>
class ProbeIsPart : public Super {
	public:
		ProbeIsPart(const libconfig::Setting& cfg) : Super(cfg) {}
		virtual ~ProbeIsPart() {}
		virtual PageCursor* probe(SplitResult t, int threadid);
};


template <typename Super>
class ProbeIsNestedLoopPart : public Super {
	public:
	ProbeIsNestedLoopPart(const libconfig::Setting& cfg) : Super(cfg) {}
		virtual ~ProbeIsNestedLoopPart() {}
		virtual PageCursor* probe(SplitResult t, int threadid);
};

template <typename Super>
class ProbeIsNotPart : public Super {
	public:
		ProbeIsNotPart(const libconfig::Setting& cfg) : Super(cfg) {}
		virtual ~ProbeIsNotPart() {}
		virtual PageCursor* probe(SplitResult t, int threadid);
};

/** 
 * Work-stealing prober. 
 * Works only with non-partitioned build, otherwise will lose data. 
 */
template <typename Super>
class ProbeSteal : public Super {
	public:
		ProbeSteal(const libconfig::Setting& cfg) : Super(cfg) {}
		virtual ~ProbeSteal() {}
		virtual PageCursor* probe(SplitResult t, int threadid);
};

class FlatMemoryJoiner : public HashBase {
	public:
		FlatMemoryJoiner(const libconfig::Config& cfg);
		virtual ~FlatMemoryJoiner();

		virtual void init(
			Schema* schema1, vector<unsigned int> select1, unsigned int jattr1,
			Schema* schema2, vector<unsigned int> select2, unsigned int jattr2);
		virtual void destroy();
		virtual void build(SplitResult ignored, int threadid);
		virtual PageCursor* probe(SplitResult ignored, int threadid);

		void custominit(Table* tbuild, Table* tprobe);

	protected:
		RadixPartitioner partitioner;
		Page* probetable;
		vector<unsigned int>* phistogram;
		unsigned long totaltuples;
		vector<WriteTable*> result;
};

#include "build.inl"
#include "probe.inl"
#endif
