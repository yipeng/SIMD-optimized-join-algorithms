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

#include "algo.h"
#include <iostream>
#include <fstream>
#include <cstring>
#include <sstream>
#include <string>
#include <stdint.h>
#include <xmmintrin.h>
#include <emmintrin.h>
#include <pmmintrin.h>
//#include <tmmintrin.h>
//#include <smmintrin.h>
#include <math.h>


NestedLoops::NestedLoops(const libconfig::Setting& cfg) 
: BaseAlgo(cfg), t1(0) {
	size = cfg["bucksize"];
	nthreads = cfg["threads"];
	_hashfn = HashFactory::createHashFunction(cfg["hash"]);

#ifdef OUTPUT_AGGREGATE
	aggregator = new int[AGGLEN*nthreads];
	for (int i=0; i<AGGLEN*nthreads; ++i) {
		aggregator[i] = 0;
	}
#endif
}

//void StoreCopy::realbuildCursor(PageCursor* t, int threadid)
//{
//	int i = 0;
//	void* tup;
//	Page* b;
//	Schema* s = t->schema();
//	unsigned int curbuc;
//	while(b = (atomic ? t->atomicReadNext() : t->readNext())) {
//		i = 0;
//		while(tup = b->getTupleOffset(i++)) {
//			// find hash table to append
//			curbuc = _hashfn->hash(s->asLong(tup, ja1));
//			void* target = atomic ?
//				hashtable.atomicAllocate(curbuc) :
//				hashtable.allocate(curbuc);
//
//			sbuild->writeData(target, 0, s->calcOffset(tup, ja1));
//			for (unsigned int j=0; j<sel1.size(); ++j)
//				sbuild->writeData(target,		// dest
//						j+1,	// col in output
//						s->calcOffset(tup, sel1[j]));	// src for this col
//
//		}
//	}

WriteTable* NestedLoops::probeCursor(PageCursor* t, int threadid, bool atomic, WriteTable* ret)
{
	if (atomic)
		return probe(t, threadid, ret);
	return probe(t, threadid, ret);
}


HashTable NestedLoops::realbuildHashCursor(PageCursor* t, int threadid) {

	int i = 0;
	void* tup;
	Page* b;
	Schema* s = t->schema();
	HashTable hashtable;
	hashtable.init(_hashfn->buckets(), size, sbuild->getTupleSize());
	unsigned int curbuc;
	while(b = t->atomicReadNext()) { // iteration starts from 2???
		i = 0;
		while(tup = b->getTupleOffset(i++)) {
			// find hash table to append
			curbuc = _hashfn->hash(s->asLong(tup, ja1));
			void* target = hashtable.atomicAllocate(curbuc);

			sbuild->writeData(target, 0, s->calcOffset(tup, ja1));

			for (unsigned int j=0; j<sel1.size(); ++j)
				sbuild->writeData(target,		// dest
						j+1,	// col in output
						s->calcOffset(tup, sel1[j]));	// src for this col
		}
	}
	t->reset();
#ifndef SUSPEND_OUTPUT
	//	 print build tables, this should be in DEBUG
	//	 needs to be relative path too
		std::stringstream sstm;
		sstm << "./datagen/output/build" << threadid;
		std::string result = sstm.str();
		ofstream foutput(result.c_str());
		foutput << "Revealing build table... \n" << flush;
		i = -1;
		t->reset();
		while(b = t->atomicReadNext()){
			while(tup = b->getTupleOffset(i++)) {
				foutput << sbuild->asLong(tup, 0) << endl;
			}
		}
		t->reset();
#endif
		return hashtable;
}

PageCursor* NestedLoops::realbuildCursor(PageCursor* t, int threadid) {
	WriteTable* wrtmp = new WriteTable();
	wrtmp->init(sbuild, size);
	prepareBuild(wrtmp, t);
	t1 = wrtmp; // this shared object is the problem ?

	PageCursor* z =  wrtmp;
#ifndef SUSPEND_OUTPUT
		std::stringstream sstm;
		sstm << "./datagen/output/pBuild" << threadid;
		std::string result = sstm.str();
		ofstream foutput(result.c_str());
		foutput << "Revealing build table... \n" << flush;
		void* tup;
		Page* b;
		int i = 0;
		while(b = t1->readNext()){
			while(tup = b->getTupleOffset(i++)) {
				foutput << sbuild->asLong(tup, 0) << endl;
			}
		}
	foutput.close();
#endif
	t1->reset();
	return wrtmp;
}

/** 
 * Build phase reads tuples from \a t and stores them, join attribute first, in
 * a new table pointed to by private variable \a t1.
 */
void NestedLoops::build(SplitResult tin, int threadid) {
	WriteTable* wrtmp = new WriteTable();
	wrtmp->init(sbuild, size);
	PageCursor* t = (*tin)[0];
	prepareBuild(wrtmp, t);
	t1 = wrtmp;

#ifndef SUSPEND_OUTPUT
		std::stringstream sstm;
		sstm << "./datagen/output/build" << threadid;
		std::string result = sstm.str();
		ofstream foutput(result.c_str());
		foutput << "Revealing build table... \n" << flush;
		void* tup;
		Page* b;
		int i = 0;
		while(b= t1->readNext()){
			while(tup = b->getTupleOffset(i++)) {
				foutput << sbuild->asLong(tup, 0) << endl;
			}
		}
#endif
	t1->reset();
}

/**
 * Build phase reads tuples from \a t and stores them, join attribute first, in
 * a new table pointed to by private variable \a t1.
 */
PageCursor* NestedLoops::buildIsPart(SplitResult tin, int threadid) {
	PageCursor* ret;
	for (int i=threadid; i<tin->size(); i+=nthreads) {
		PageCursor* t = (*tin)[i];
		ret = realbuildCursor(t, threadid); // n^2 version
	}
	return ret; // does this WORK? It should work!


}

HashTable NestedLoops::buildIsPartHashV(SplitResult tin, int threadid) {
	HashTable ret;
	int size = tin->size();
	for (int i=threadid; i<size; i+=nthreads) {
		PageCursor* t = (*tin)[i];
		ret = realbuildHashCursor(t, threadid); // hash version
	}
	return ret;
}


void NestedLoops::prepareBuild(WriteTable* wrtmp, PageCursor* t) {
	char tmp[sout->getTupleSize()];
	int i = 0;
	void* tup;
	Page* b;
	while(b = t->readNext()) {
		i = 0;
		while(tup = b->getTupleOffset(i++)) {
			sbuild->writeData(tmp, 0, t->schema()->calcOffset(tup, ja1));
			for (unsigned int j=0; j<sel1.size(); ++j)
				sbuild->writeData(tmp,		// dest
						j+1,	// col in output
						t->schema()->calcOffset(tup, sel1[j]));	// src for this col
			wrtmp->append(tmp);
		}
	}
}

inline PageCursor* NestedLoops::probeNestedHash(SplitResult tin, int threadid, HashTable ht) {

	WriteTable* ret = new WriteTable();
	ret->init(sout, size);
	HashTable::Iterator it = ht.createIterator();
	PageCursor* t = (*tin)[threadid];
	while (Page* b2 = t->readNext()) {
		unsigned int i = 0;
		while (void* tup2 = b2->getTupleOffset(i++)) { // probe // outer
			unsigned int curbuc = _hashfn->hash(s2->asLong(tup2, ja2));
			ht.placeIterator(it, curbuc);
			// Duplicate Outer
			unsigned int __attribute__ ((aligned(16))) data2 = s2->asLong(tup2, ja2); // probe
			__m128i i2 = _mm_set1_epi32(data2);
			unsigned int __attribute__ ((aligned(16))) vector1[4];

			int sout_size = sout->getTupleSize();
			char tmp[sout_size*4];

			void *t124[4];
			while (t124[0] = it.readnext()) { // iterate inner
			t124[1] = it.readnext();
			t124[2] = it.readnext();
			t124[3] = it.readnext();

			vector1[0] = sbuild->asLong(t124[0],0); // 0?
			vector1[1] = sbuild->asLong(t124[1],0);
			vector1[2] = sbuild->asLong(t124[2],0);
			vector1[3] = sbuild->asLong(t124[3],0);

			__m128i eq128 = _mm_cmpeq_epi32(_mm_load_si128(( __m128i *)vector1), i2);

			int move = _mm_movemask_epi8(eq128);
			if (move!=0){
				int k = 0, m = 0;
				for (int j = 0; j <= 12; j+=4) {
					if (s1->getTupleSize())
						s1->copyTuple(&tmp[sout_size*k], sbuild->calcOffset(t124[m], 1));
					int sel2_size =  sel2.size();
					for (int n = 0; n < sel2_size; ++n)
						sout->writeData(&tmp[sout_size*k], // dest
								s1->columns() + n, // col in output
								s2->calcOffset(tup2, sel2[n])); // src for this col
					k+= ((move >> j) & 1);
					m++;
				}
			}
#ifndef SUSPEND_OUTPUT
			int q =0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move >> j) & 1;
				if (mask_bit==1){
					ret->append(&tmp[sout_size*q]);
					q+= mask_bit;
				}
			}
#endif
			}

		}
	}
	ret->reset();
#ifndef SUSPEND_OUTPUT
	std::stringstream sstm1;
	sstm1 << "./datagen/output/results" << threadid;
	std::string result = sstm1.str();
	ofstream foutput(result.c_str());
	foutput << "Revealing outputZ... \n" << flush;
	vector<PageCursor*> jr; // remember result
	jr.push_back(ret);

	for (vector<PageCursor*>::iterator i1 = jr.begin();
			i1 != jr.end(); ++i1) {
		Page* b;
		if (!*i1)
			continue;
		void* tuple;
		while (b = (*i1)->readNext()) {
			int i = 0;
			while (tuple = b->getTupleOffset(i++)) {
				foutput << (*i1)->schema()->prettyprint(tuple, '|') << '\n';
			}
		}
	}
	foutput << flush;
	foutput.close();
	ret->reset();
#endif
	return ret;
}

PageCursor* NestedLoops::probeStarHashV(SplitResult tin, int threadid, HashTable ht) {

	WriteTable* ret = new WriteTable();
	ret->init(sout, size);
	char tmp[sout->getTupleSize()];
	void* tup1;
	void* tup2;
	Page* b2;
	unsigned int curbuc, i;

	HashTable::Iterator it = ht.createIterator();
	PageCursor* t = (*tin)[threadid];
	while (b2 = t->readNext()) {
		i = 0;
		while (tup2 = b2->getTupleOffset(i++)) {
			curbuc = _hashfn->hash(s2->asLong(tup2, ja2));
			ht.placeIterator(it, curbuc);

			while (tup1 = it.readnext()) {
				if (sbuild->asLong(tup1,0) != s2->asLong(tup2,ja2) ) {
					continue;
				}

				// copy payload of first tuple to destination
				if (s1->getTupleSize())
					s1->copyTuple(tmp, sbuild->calcOffset(tup1,1));

				// copy each column to destination
				for (unsigned int j=0; j<sel2.size(); ++j)
							sout->writeData(tmp,		// dest
							s1->columns()+j,	// col in output
							s2->calcOffset(tup2, sel2[j]));	// src for this col
				ret->append(tmp);

			}

		}
	}
	ret->reset();
#ifndef SUSPEND_OUTPUT
	std::stringstream sstm1;
	sstm1 << "./datagen/output/results" << threadid;
	std::string result = sstm1.str();
	ofstream foutput(result.c_str());
	foutput << "Revealing outputZ... \n" << flush;
	vector<PageCursor*> jr; // remember result
	jr.push_back(ret);

	for (vector<PageCursor*>::iterator i1 = jr.begin();
			i1 != jr.end(); ++i1) {
		Page* b;
		if (!*i1)
			continue;
		void* tuple;
		while (b = (*i1)->readNext()) {
			int i = 0;
			while (tuple = b->getTupleOffset(i++)) {
				foutput << (*i1)->schema()->prettyprint(tuple, '|') << '\n';
			}
		}
	}
	foutput << flush;
	foutput.close();
	ret->reset();
#endif
	return ret;
}

PageCursor* NestedLoops::probeStar(SplitResult tin, int threadid, PageCursor* t1) {
	t1->reset();

#ifndef SUSPEND_OUTPUT
	std::stringstream sstm;
	sstm << "./datagen/output/build4probe" << threadid;
	std::string result2 = sstm.str();
	ofstream foutput2(result2.c_str());
	foutput2 << "Revealing build table. b4 probe.. \n" << flush;
	void* tup;
	Page* b;
	int i = 0;
	while(b= t1->readNext()){
		while(tup = b->getTupleOffset(i++)) {
			foutput2 << sbuild->asLong(tup, 0) << endl;
		}
	}
	t1->reset();
#endif

	WriteTable* ret = new WriteTable();
	ret->init(sout, size);
	Page* p1, * p2;
	PageCursor* t = (*tin)[threadid];
	while(p1 = t1->readNext()) {// t1 is built / outer table R / smaller table
		while(p2 = t->readNext()) {  // t is probe /inner  table S // could benefit from double buffering
			joinPagePage1(ret, p1, p2);
		}
		t->reset();
	}
	//ret->reset();
#ifndef SUSPEND_OUTPUT
	std::stringstream sstm1;
	sstm1 << "./datagen/output/results" << threadid;
	std::string result = sstm1.str();
	ofstream foutput(result.c_str());
	foutput << "Revealing output1... \n" << flush;
	vector<PageCursor*> jr; // remember result

	jr.push_back(ret);

	for (vector<PageCursor*>::iterator i1 = jr.begin();
			i1 != jr.end(); ++i1) {
		Page* b;
		if (!*i1)
			continue;
		void* tuple;
		while (b = (*i1)->readNext()) {
			int i = 0;
			while (tuple = b->getTupleOffset(i++)) {
				foutput << (*i1)->schema()->prettyprint(tuple, '|') << '\n';
			}
		}
	}
	foutput << flush;
	foutput.close();
#endif
	ret->reset();
	return ret;
}

/** Probe phase does a nested loops join between \a t and local \a t1. */
PageCursor* NestedLoops::probe(SplitResult tin, int threadid) {
#ifndef SUSPEND_OUTPUT
		std::stringstream sstm;
		sstm << "./datagen/output/build4probe" << threadid;
		std::string result2 = sstm.str();
		ofstream foutput2(result2.c_str());
		foutput2 << "Revealing build table. b44 probe.. \n" << flush;
		void* tup;
		Page* b;
		int i = 0;
		while(b= t1->readNext()){
			while(tup = b->getTupleOffset(i++)) {
				foutput2 << sbuild->asLong(tup, 0) << endl;
			}
		}
		t1->reset();
#endif
	WriteTable* ret = new WriteTable();
	ret->init(sout, size);
	Page* p1, * p2;
	PageCursor* t = (*tin)[0];
	while(p1 = t1->readNext()) {// t1 is built / outer table R / smaller table
		while(p2 = t->readNext()) {  // t is probe /inner  table S // could benefit from double buffering
			joinPagePage1(ret, p1, p2);
		}
		t->reset();
	}
	ret->reset();
#ifndef SUSPEND_OUTPUT
		std::stringstream sstm1;
		sstm1 << "./datagen/output/results" << threadid;
		std::string result = sstm1.str();
		ofstream foutput(result.c_str());
		foutput << "Revealing output... \n" << flush;
		vector<PageCursor*> jr; // remember result
		jr.push_back(ret);

		for (vector<PageCursor*>::iterator i1 = jr.begin();
				i1 != jr.end(); ++i1) {
			Page* b;
			if (!*i1)
				continue;
			void* tuple;
			while (b = (*i1)->readNext()) {
				int i = 0;
				while (tuple = b->getTupleOffset(i++)) {
					foutput << (*i1)->schema()->prettyprint(tuple, '|') << '\n';
				}
			}
		}
		foutput << flush;
		foutput.close();
#endif
	return ret;
}


/** Probe phase does a nested loops join between \a t and local \a t1. */
PageCursor* NestedLoops::probeN2(SplitResult tin, int threadid) {
#ifndef SUSPEND_OUTPUT
		std::stringstream sstm;
		sstm << "./datagen/output/build4probe" << threadid;
		std::string result2 = sstm.str();
		ofstream foutput2(result2.c_str());
		foutput2 << "Revealing build table. b44 probe.. \n" << flush;
		void* tup;
		Page* b;
		int i = 0;
		while(b= t1->readNext()){
			while(tup = b->getTupleOffset(i++)) {
				foutput2 << sbuild->asLong(tup, 0) << endl;
			}
		}
		t1->reset();
#endif
	WriteTable* ret = new WriteTable();
	ret->init(sout, size);
	Page* p1, * p2;
	PageCursor* t = (*tin)[0];
	while(p1 = t1->readNext()) {// t1 is built / outer table R / smaller table
		while(p2 = t->readNext()) {  // t is probe /inner  table S // could benefit from double buffering
			joinPagePage2(ret, p1, p2);
		}
		t->reset();
	}
	ret->reset();
#ifndef SUSPEND_OUTPUT
		std::stringstream sstm1;
		sstm1 << "./datagen/output/results" << threadid;
		std::string result = sstm1.str();
		ofstream foutput(result.c_str());
		foutput << "Revealing output... \n" << flush;
		vector<PageCursor*> jr; // remember result
		jr.push_back(ret);

		for (vector<PageCursor*>::iterator i1 = jr.begin();
				i1 != jr.end(); ++i1) {
			Page* b;
			if (!*i1)
				continue;
			void* tuple;
			while (b = (*i1)->readNext()) {
				int i = 0;
				while (tuple = b->getTupleOffset(i++)) {
					foutput << (*i1)->schema()->prettyprint(tuple, '|') << '\n';
				}
			}
		}
		foutput << flush;
		foutput.close();
#endif
	return ret;
}

/** Probe phase does a nested loops join between \a t and local \a t1. */
WriteTable* NestedLoops::probe(PageCursor* t, int threadid, WriteTable* ret) {
	Page* p1, * p2;
	while(p1 = t1->readNext()) {// t1 is built / outer table R / smaller table
		while(p2 = t->readNext()) {  // t is probe /inner  table S // could benefit from double buffering
			joinPagePage1(ret, p1, p2);
		}
		t->reset();
	}
	return ret;
}

void NestedLoops::destroy() {
	BaseAlgo::destroy();
	delete t1;
}

void NestedLoops::joinPagePage1(WriteTable* output, Page* p1, Page* p2) {
	int i = 0;
	void* tup;
	while (tup = p2->getTupleOffset(i++)) { // probe
		joinPageTupExperimental302(output, p1, tup);
	}
}

void NestedLoops::joinDuplicateOuter(WriteTable* output, Page* p1, Page* p2) {
	int i = 0;
	void* tup;
	int __attribute__ ((aligned(16))) vector1[4];


	int size3 = p2->capacity() / p2->tuplesize;
	for (int i = 3; i < size3; i += 4) {
		vector1[0] = p2->getTupleOffset1(i-3);
		vector1[1] = p2->getTupleOffset1(i-2);
		vector1[2] = p2->getTupleOffset1(i-1);
		vector1[3] = p2->getTupleOffset1(i);
		DuplicateOuter(output, p1, vector1);
	}
}

void NestedLoops::joinPagePage2(WriteTable* output, Page* p1, Page* p2) {
	int i = 0;
	void* tup;
	while (tup = p2->getTupleOffset(i++)) { // probe
		joinPageTup(output, p1, tup);
	}
}

/*
 * This version declares variables outside the loop but is about X slower
 * http://stackoverflow.com/questions/407255/difference-between-declaring-variables-before-or-in-loop
 */

void NestedLoops::joinPageTupExperimental2(WriteTable* output, Page* page, void* tuple) {

	unsigned int __attribute__ ((aligned(16))) res[4], vector1[4];
	unsigned int __attribute__ ((aligned(16))) data2[4] = { s2->asLong(tuple, ja2), s2->asLong(tuple, ja2), s2->asLong(tuple, ja2), s2->asLong(tuple, ja2)};
	__m128i i1, eq128;
	__m128i i2 = _mm_load_si128(( __m128i *)data2); //i2 = _mm_set1_epi32(data2);

	unsigned int tupSize = sout->getTupleSize();
	char tmp[tupSize*4];
	unsigned int j, k, m, n;
	int move;
	int mask_bit;

	void *tup1, *tup2, *tup3, *tup4;
	char* ds = (char*)page->getDataSpace();
	int pageTupSize = page->tuplesize;
	int size3 = page->capacity() / pageTupSize;

	for (int i = 3; i < size3; i += 4) {
		tup1 = ds + static_cast<unsigned long>(i - 3) * pageTupSize;
		tup2 = ds + static_cast<unsigned long>(i - 2) * pageTupSize;
		tup3 = ds + static_cast<unsigned long>(i - 1) * pageTupSize;
		tup4 = ds +  static_cast<unsigned long>(i) * pageTupSize;
		vector1[0] = sbuild->asLong(tup1, 0); vector1[1] = sbuild->asLong(tup2, 0); vector1[2] = sbuild->asLong(tup3, 0); vector1[3] = sbuild->asLong(tup4, 0);

		i1 = _mm_load_si128(( __m128i *)vector1);
		eq128 = _mm_cmpeq_epi32(i1, i2);

		/*
		 * if mask is all 1s print all without compare (nah!)
		 * if mask is all zeros, compare once and continue
		 * if mask is not all zeros, perform comparisons
		 */
		move = _mm_movemask_epi8(eq128);
		if (move!=0){ // binary 0000000000000000
			k = 0; m = 0;
			for (j = 0; j <= 12; j+=4) {
				mask_bit = (move >> j) & 1;
				if (s1->getTupleSize())
					s1->copyTuple(&tmp[tupSize*k], sbuild->calcOffset(ds + static_cast<unsigned long>(i - 3 + m) * pageTupSize, 1));

				for (n = 0; n < sel2.size(); ++n)
					sout->writeData(&tmp[tupSize*k], // dest
							s1->columns() + n, // col in output
							s2->calcOffset(tuple, sel2[n])); // src for this col
				k+= mask_bit;
				m++;
			}

#ifndef SUSPEND_OUTPUT
			k=0;
			for (j = 0; j <= 12; j+=4) {
				mask_bit = (move >> j) & 1;
				if (mask_bit==1){
					output->append(&tmp[tupSize*k]);
					k+= mask_bit;
				}
			}
#endif
		}
	}
}

/*
 * This version declares variables inside loops
 */
inline void NestedLoops::joinPageTupExperimental3(WriteTable* output, Page* page, void* tuple) {

	unsigned int __attribute__ ((aligned(16))) vector1[4];
	unsigned int __attribute__ ((aligned(16))) data2[4] = { s2->asLong(tuple, ja2), s2->asLong(tuple, ja2), s2->asLong(tuple, ja2), s2->asLong(tuple, ja2)};

	__m128i i2 = _mm_load_si128(( __m128i *)data2); //i2 = _mm_set1_epi32(data2);

	char tmp[sout->getTupleSize()*4];
	void *tup1, *tup2, *tup3, *tup4;
	char* ds = (char*)page->getDataSpace();
	int pageTupSize = page->tuplesize;
	int size3 = page->capacity() / pageTupSize;

	for (int i = 3; i < size3; i += 4) {
		tup1 = ds + static_cast<unsigned long>(i - 3) * pageTupSize;
		tup2 = ds + static_cast<unsigned long>(i - 2) * pageTupSize;
		tup3 = ds + static_cast<unsigned long>(i - 1) * pageTupSize;
		tup4 = ds +  static_cast<unsigned long>(i) * pageTupSize;
		vector1[0] = sbuild->asLong(tup1, 0);
		vector1[1] = sbuild->asLong(tup2, 0);
		vector1[2] = sbuild->asLong(tup3, 0);
		vector1[3] = sbuild->asLong(tup4, 0);

		__m128i i1 = _mm_load_si128(( __m128i *)vector1);
		__m128i eq128 = _mm_cmpeq_epi32(i1, i2);

		/*
		 * if mask is all 1s print all without compare (nah!)
		 * if mask is all zeros, compare once and continue
		 * if mask is not all zeros, perform comparisons
		 */
		int move = _mm_movemask_epi8(eq128);
		if (move!=0){ // binary 0000000000000000
			int k = 0, m = 0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move >> j) & 1;
				if (s1->getTupleSize())
					s1->copyTuple(&tmp[sout->getTupleSize()*k], sbuild->calcOffset(ds + static_cast<unsigned long>(i - 3 + m) * pageTupSize, 1));

				for (int n = 0; n < sel2.size(); ++n)
					sout->writeData(&tmp[sout->getTupleSize()*k], // dest
							s1->columns() + n, // col in output
							s2->calcOffset(tuple, sel2[n])); // src for this col
				k+= mask_bit;
				m++;
			}

			//#ifndef SUSPEND_OUTPUT
			//			int q =0;
			//			for (int j = 0; j <= 12; j+=4) {
			//				int mask_bit = (move >> j) & 1;
			//				if (mask_bit==1){
			//					output->append(&tmp[sout->getTupleSize()*q]);
			//					q+= mask_bit;
			//#ifdef OUTPUT_AGGREGATE
			//					aggregator = new int[AGGLEN
			//					                     aggregator[ (threadid * AGGLEN) +
			//					                                 + (sbuild->asLong(ds + static_cast<unsigned long>(i - 3 + m) * pageTupSize,0) & (AGGLEN-1)) ]++;
			//#endif
			//				}
			//			}
			//#endif
		}
	}
}

inline void NestedLoops::joinPageTupExperimental301(WriteTable* output, Page* page, void* tuple) {

	unsigned int __attribute__ ((aligned(16))) vector1[4];
	unsigned int __attribute__ ((aligned(16))) data2 = s2->asLong(tuple, ja2);
	__m128i i2 = _mm_set1_epi32(data2);
	char* ds = (char*) page->getDataSpace();
	int pageTupSize = page->tuplesize; // this can be moved up since pgsize is standard
	int size3 = page->capacity() / pageTupSize;

	for (int i = 3; i < size3; i += 4) {
		vector1[0] = *reinterpret_cast<long*>(ds + static_cast<unsigned long>(i - 3) * pageTupSize);
		vector1[1] = *reinterpret_cast<long*>(ds + static_cast<unsigned long>(i - 2) * pageTupSize);
		vector1[2] = *reinterpret_cast<long*>(ds + static_cast<unsigned long>(i - 1) * pageTupSize);
		vector1[3] = *reinterpret_cast<long*>(ds + static_cast<unsigned long>(i) * pageTupSize);

		__m128i eq128 = _mm_cmpeq_epi32(_mm_load_si128(( __m128i *)vector1), i2);

		int move = _mm_movemask_epi8(eq128);
		if (move!=0){
			int k = 0, m = 0;
			int sout_size = sout->getTupleSize();
			char tmp[sout_size*4];
			for (int j = 0; j <= 12; j+=4) {
				s1->copyTuple(&tmp[sout_size*k], sbuild->calcOffset(ds + static_cast<unsigned long>(i - 3 + m) * pageTupSize, 1));
				int sel2_size =  sel2.size();
				for (int n = 0; n < sel2_size; ++n)
					sout->writeData(&tmp[sout_size*k], // dest
							s1->columns() + n, // col in output
							s2->calcOffset(tuple, sel2[n])); // src for this col
				k+= ((move >> j) & 1);
				m++;
			}
#ifndef SUSPEND_OUTPUT
			int q =0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move >> j) & 1;
				if (mask_bit==1){
					output->append(&tmp[sout_size*q]);
					q+= mask_bit;
#ifdef OUTPUT_AGGREGATE
					aggregator = new int[AGGLEN
					                     aggregator[ (threadid * AGGLEN) +
					                                 + (sbuild->asLong(ds + static_cast<unsigned long>(i - 3 + m) * pageTupSize,0) & (AGGLEN-1)) ]++;
#endif
				}
			}
#endif
		}
	}
}

inline void NestedLoops::DuplicateInner(WriteTable* output, Page* page, void* tuple) {

	unsigned int __attribute__ ((aligned(16))) vector1[4];
	unsigned int __attribute__ ((aligned(16))) data2 = s2->asLong(tuple, ja2); //; probe
	__m128i i2 = _mm_set1_epi32(data2);
	int size3 = page->capacity() / page->tuplesize;

	for (int i = 3; i < size3; i += 4) {
		vector1[0] = page->getTupleOffset1(i-3);
		vector1[1] = page->getTupleOffset1(i-2);
		vector1[2] = page->getTupleOffset1(i-1);
		vector1[3] = page->getTupleOffset1(i);

		__m128i eq128 = _mm_cmpeq_epi32(_mm_load_si128(( __m128i *)vector1), i2);

		int move = _mm_movemask_epi8(eq128);
		if (move!=0){
			int k = 0, m = 0;
			int sout_size = sout->getTupleSize();
			char tmp[sout_size*4];
			for (int j = 0; j <= 12; j+=4) {
				s1->copyTuple(&tmp[sout_size*k], sbuild->calcOffset(page->getTupleOffset(i-3+m), 1));
				int sel2_size =  sel2.size();
				for (int n = 0; n < sel2_size; ++n)
					sout->writeData(&tmp[sout_size*k], // dest
							s1->columns() + n, // col in output
							s2->calcOffset(tuple, sel2[n])); // src for this col
				k+= ((move >> j) & 1);
				m++;
			}
#ifndef SUSPEND_OUTPUT
			int q =0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move >> j) & 1;
				if (mask_bit==1){
					output->append(&tmp[sout_size*q]);
					q+= mask_bit;
#ifdef OUTPUT_AGGREGATE
					aggregator = new int[AGGLEN
					                     aggregator[ (threadid * AGGLEN) +
					                                 + (sbuild->asLong(page->getTupleOffset(i-3+m),0) & (AGGLEN-1)) ]++;
#endif
				}
			}
#endif
		}
	}
}

inline void NestedLoops::DuplicateOuter(WriteTable* output, Page* page, int tuple[4]) {

	int i = 0;
	__m128i i1 = _mm_load_si128(( __m128i *)tuple);

	while( page->getTupleOffset1(i++)){
		unsigned int __attribute__ ((aligned(16))) data2 = page->getTupleOffset1(i); //; probe
		__m128i i2 = _mm_set1_epi32(data2);
		__m128i eq128 = _mm_cmpeq_epi32(i2, i1);

		// this is unfinished... seg faults
		int move = _mm_movemask_epi8(eq128);
		if (move!=0){
			int k = 0, m = 0;
			int sout_size = sout->getTupleSize();
			char tmp[sout_size*4];
			for (int j = 0; j <= 12; j+=4) {
				s1->copyTuple(&tmp[sout_size*k], sbuild->calcOffset(page->getTupleOffset(i-3+m), 1));
				int sel2_size =  sel2.size();
				for (int n = 0; n < sel2_size; ++n)
					sout->writeData(&tmp[sout_size*k], // dest
							s1->columns() + n, // col in output
							s2->calcOffset(tuple, sel2[n])); // src for this col
				k+= ((move >> j) & 1);
				m++;
			}
#ifndef SUSPEND_OUTPUT
			int q =0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move >> j) & 1;
				if (mask_bit==1){
					output->append(&tmp[sout_size*q]);
					q+= mask_bit;
#ifdef OUTPUT_AGGREGATE
					aggregator = new int[AGGLEN
					                     aggregator[ (threadid * AGGLEN) +
					                                 + (sbuild->asLong(page->getTupleOffset(i-3+m),0) & (AGGLEN-1)) ]++;
#endif
				}
			}
#endif
		}
	}
}

inline void NestedLoops::joinPageTupExperimental302(WriteTable* output, Page* page, void* tuple) {

	unsigned int __attribute__ ((aligned(16))) vector1[4];
	unsigned int __attribute__ ((aligned(16))) data2 = s2->asLong(tuple, ja2); //; probe
	__m128i i2 = _mm_set1_epi32(data2);
	int size3 = page->capacity() / page->tuplesize;

	for (int i = 3; i < size3; i += 4) {
		vector1[0] = page->getTupleOffset1(i-3);
		vector1[1] = page->getTupleOffset1(i-2);
		vector1[2] = page->getTupleOffset1(i-1);
		vector1[3] = page->getTupleOffset1(i);

		__m128i eq128 = _mm_cmpeq_epi32(_mm_load_si128(( __m128i *)vector1), i2);

		int move = _mm_movemask_epi8(eq128);
		if (move!=0){
			int k = 0, m = 0;
			int sout_size = sout->getTupleSize();
			char tmp[sout_size*4];
			for (int j = 0; j <= 12; j+=4) {
				s1->copyTuple(&tmp[sout_size*k], sbuild->calcOffset(page->getTupleOffset(i-3+m), 1));
				int sel2_size =  sel2.size();
				for (int n = 0; n < sel2_size; ++n)
					sout->writeData(&tmp[sout_size*k], // dest
							s1->columns() + n, // col in output
							s2->calcOffset(tuple, sel2[n])); // src for this col
				k+= ((move >> j) & 1);
				m++;
			}
#ifndef SUSPEND_OUTPUT
			int q =0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move >> j) & 1;
				if (mask_bit==1){
					output->append(&tmp[sout_size*q]);
					q+= mask_bit;
#ifdef OUTPUT_AGGREGATE
					aggregator = new int[AGGLEN
					                     aggregator[ (threadid * AGGLEN) +
					                                 + (sbuild->asLong(page->getTupleOffset(i-3+m),0) & (AGGLEN-1)) ]++;
#endif
				}
			}
#endif
		}
	}
}

/*
 * This version declares variables inside loops
 */
void NestedLoops::joinPageTupExperimental5(WriteTable* output, Page* page, void* tuple) {

	unsigned int __attribute__ ((aligned(16))) res[4];
	unsigned int __attribute__ ((aligned(16))) data2[4] = { s2->asLong(tuple, ja2), s2->asLong(tuple, ja2), s2->asLong(tuple, ja2), s2->asLong(tuple, ja2)};

	__m128i i2 = _mm_load_si128(( __m128i *)data2); //i2 = _mm_set1_epi32(data2);

	char tmp[sout->getTupleSize()*4];
	void *tup1, *tup2, *tup3, *tup4;
	char* ds = (char*)page->getDataSpace();
	int pageTupSize = page->tuplesize;
	int size3 = page->capacity() / pageTupSize;

	for (int i = 31; i < size3; i += 32) {
		unsigned int __attribute__ ((aligned(16))) vector2[4]
		                                                   = { sbuild->asLong(ds + static_cast<unsigned long>(i - 15) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 14) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 13) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 12) * pageTupSize, 0)
		};
		unsigned int __attribute__ ((aligned(16))) vector3[4]
		                                                   = { sbuild->asLong(ds + static_cast<unsigned long>(i - 11) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 10) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 9) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 8) * pageTupSize, 0)
		};
		unsigned int __attribute__ ((aligned(16))) vector4[4]
		                                                   = { sbuild->asLong(ds + static_cast<unsigned long>(i - 7) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 6) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 5) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 4) * pageTupSize, 0)
		};
		unsigned int __attribute__ ((aligned(16))) vector1[4]
		                                                   = { sbuild->asLong(ds + static_cast<unsigned long>(i - 3) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 2) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 1) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 0) * pageTupSize, 0)
		};
		unsigned int __attribute__ ((aligned(16))) vector6[4]
		                                                   = { sbuild->asLong(ds + static_cast<unsigned long>(i - 19) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 18) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 17) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 16) * pageTupSize, 0)
		};
		unsigned int __attribute__ ((aligned(16))) vector7[4]
		                                                   = { sbuild->asLong(ds + static_cast<unsigned long>(i - 23) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 22) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 21) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 20) * pageTupSize, 0)
		};
		unsigned int __attribute__ ((aligned(16))) vector8[4]
		                                                   = { sbuild->asLong(ds + static_cast<unsigned long>(i - 27) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 26) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 25) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 24) * pageTupSize, 0)
		};
		unsigned int __attribute__ ((aligned(16))) vector9[4]
		                                                   = { sbuild->asLong(ds + static_cast<unsigned long>(i - 31) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 30) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 29) * pageTupSize, 0),
		                                                		   sbuild->asLong(ds + static_cast<unsigned long>(i - 28) * pageTupSize, 0)
		};

		__m128i i1 = _mm_load_si128(( __m128i *)vector1);
		__m128i i3 = _mm_load_si128(( __m128i *)vector2);
		__m128i i4 = _mm_load_si128(( __m128i *)vector3);
		__m128i i5 = _mm_load_si128(( __m128i *)vector4);
		__m128i i6 = _mm_load_si128(( __m128i *)vector6);
		__m128i i7 = _mm_load_si128(( __m128i *)vector7);
		__m128i i8 = _mm_load_si128(( __m128i *)vector8);
		__m128i i9 = _mm_load_si128(( __m128i *)vector9);
		__m128i eq128 = _mm_cmpeq_epi32(i1, i2);
		__m128i eq2 = _mm_cmpeq_epi32(i3, i2);
		__m128i eq3 = _mm_cmpeq_epi32(i4, i2);
		__m128i eq4 = _mm_cmpeq_epi32(i5, i2);
		__m128i eq6 = _mm_cmpeq_epi32(i6, i2);
		__m128i eq7 = _mm_cmpeq_epi32(i7, i2);
		__m128i eq8 = _mm_cmpeq_epi32(i8, i2);
		__m128i eq9 = _mm_cmpeq_epi32(i9, i2);
		/*
		 * if mask is all 1s print all without compare (nah!)
		 * if mask is all zeros, compare once and continue
		 * if mask is not all zeros, perform comparisons
		 */
		int move = _mm_movemask_epi8(eq128);
		if (move!=0){ // binary 0000000000000000
			int k = 0, m = 0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move >> j) & 1;
				if (s1->getTupleSize())
					s1->copyTuple(&tmp[sout->getTupleSize()*k], sbuild->calcOffset(ds + static_cast<unsigned long>(i - 3 + m) * pageTupSize, 1));

				for (int n = 0; n < sel2.size(); ++n)
					sout->writeData(&tmp[sout->getTupleSize()*k], // dest
							s1->columns() + n, // col in output
							s2->calcOffset(tuple, sel2[n])); // src for this col
				k+= mask_bit;
				m++;
			}

#ifndef SUSPEND_OUTPUT
			k=0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move >> j) & 1;
				if (mask_bit==1){
					output->append(&tmp[sout->getTupleSize()*k]);
					k+= mask_bit;
				}
			}
#endif
		}

		int move2 = _mm_movemask_epi8(eq2);
		if (move2!=0){ // binary 0000000000000000
			int k = 0, m = 0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move2 >> j) & 1;
				if (s1->getTupleSize())
					s1->copyTuple(&tmp[sout->getTupleSize()*k], sbuild->calcOffset(ds + static_cast<unsigned long>(i - 15 + m) * pageTupSize, 1));

				for (int n = 0; n < sel2.size(); ++n)
					sout->writeData(&tmp[sout->getTupleSize()*k], // dest
							s1->columns() + n, // col in output
							s2->calcOffset(tuple, sel2[n])); // src for this col
				k+= mask_bit;
				m++;
			}

#ifndef SUSPEND_OUTPUT
			k=0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move2 >> j) & 1;
				if (mask_bit==1){
					output->append(&tmp[sout->getTupleSize()*k]);
					k+= mask_bit;
				}
			}
#endif
		}
		int move3 = _mm_movemask_epi8(eq3);
		if (move3!=0){ // binary 0000000000000000
			int k = 0, m = 0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move3 >> j) & 1;
				if (s1->getTupleSize())
					s1->copyTuple(&tmp[sout->getTupleSize()*k], sbuild->calcOffset(ds + static_cast<unsigned long>(i - 11 + m) * pageTupSize, 1));

				for (int n = 0; n < sel2.size(); ++n)
					sout->writeData(&tmp[sout->getTupleSize()*k], // dest
							s1->columns() + n, // col in output
							s2->calcOffset(tuple, sel2[n])); // src for this col
				k+= mask_bit;
				m++;
			}

#ifndef SUSPEND_OUTPUT
			k=0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move3 >> j) & 1;
				if (mask_bit==1){
					output->append(&tmp[sout->getTupleSize()*k]);
					k+= mask_bit;
				}
			}
#endif
		}
		int move4 = _mm_movemask_epi8(eq4);
		if (move4!=0){ // binary 0000000000000000
			int k = 0, m = 0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move4 >> j) & 1;
				if (s1->getTupleSize())
					s1->copyTuple(&tmp[sout->getTupleSize()*k], sbuild->calcOffset(ds + static_cast<unsigned long>(i - 7 + m) * pageTupSize, 1));

				for (int n = 0; n < sel2.size(); ++n)
					sout->writeData(&tmp[sout->getTupleSize()*k], // dest
							s1->columns() + n, // col in output
							s2->calcOffset(tuple, sel2[n])); // src for this col
				k+= mask_bit;
				m++;
			}

#ifndef SUSPEND_OUTPUT
			k=0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move4 >> j) & 1;
				if (mask_bit==1){
					output->append(&tmp[sout->getTupleSize()*k]);
					k+= mask_bit;
				}
			}
#endif
		}
		int move5 = _mm_movemask_epi8(eq6);
		if (move5!=0){ // binary 0000000000000000
			int k = 0, m = 0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move5 >> j) & 1;
				if (s1->getTupleSize())
					s1->copyTuple(&tmp[sout->getTupleSize()*k], sbuild->calcOffset(ds + static_cast<unsigned long>(i - 19 + m) * pageTupSize, 1));

				for (int n = 0; n < sel2.size(); ++n)
					sout->writeData(&tmp[sout->getTupleSize()*k], // dest
							s1->columns() + n, // col in output
							s2->calcOffset(tuple, sel2[n])); // src for this col
				k+= mask_bit;
				m++;
			}

#ifndef SUSPEND_OUTPUT
			k=0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move5>> j) & 1;
				if (mask_bit==1){
					output->append(&tmp[sout->getTupleSize()*k]);
					k+= mask_bit;
				}
			}
#endif
		}

		int move6 = _mm_movemask_epi8(eq7);
		if (move6!=0){ // binary 0000000000000000
			int k = 0, m = 0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move6 >> j) & 1;
				if (s1->getTupleSize())
					s1->copyTuple(&tmp[sout->getTupleSize()*k], sbuild->calcOffset(ds + static_cast<unsigned long>(i - 23 + m) * pageTupSize, 1));

				for (int n = 0; n < sel2.size(); ++n)
					sout->writeData(&tmp[sout->getTupleSize()*k], // dest
							s1->columns() + n, // col in output
							s2->calcOffset(tuple, sel2[n])); // src for this col
				k+= mask_bit;
				m++;
			}

#ifndef SUSPEND_OUTPUT
			k=0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move6 >> j) & 1;
				if (mask_bit==1){
					output->append(&tmp[sout->getTupleSize()*k]);
					k+= mask_bit;
				}
			}
#endif
		}

		int move7 = _mm_movemask_epi8(eq8);
		if (move7){ // binary 0000000000000000
			int k = 0, m = 0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move7 >> j) & 1;
				if (s1->getTupleSize())
					s1->copyTuple(&tmp[sout->getTupleSize()*k], sbuild->calcOffset(ds + static_cast<unsigned long>(i - 27 + m) * pageTupSize, 1));

				for (int n = 0; n < sel2.size(); ++n)
					sout->writeData(&tmp[sout->getTupleSize()*k], // dest
							s1->columns() + n, // col in output
							s2->calcOffset(tuple, sel2[n])); // src for this col
				k+= mask_bit;
				m++;
			}

#ifndef SUSPEND_OUTPUT
			k=0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move7 >> j) & 1;
				if (mask_bit==1){
					output->append(&tmp[sout->getTupleSize()*k]);
					k+= mask_bit;
				}
			}
#endif
		}

		int move8 = _mm_movemask_epi8(eq9);
		if (move8!=0){ // binary 0000000000000000
			int k = 0, m = 0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move8 >> j) & 1;
				if (s1->getTupleSize())
					s1->copyTuple(&tmp[sout->getTupleSize()*k], sbuild->calcOffset(ds + static_cast<unsigned long>(i - 31 + m) * pageTupSize, 1));

				for (int n = 0; n < sel2.size(); ++n)
					sout->writeData(&tmp[sout->getTupleSize()*k], // dest
							s1->columns() + n, // col in output
							s2->calcOffset(tuple, sel2[n])); // src for this col
				k+= mask_bit;
				m++;
			}

#ifndef SUSPEND_OUTPUT
			k=0;
			for (int j = 0; j <= 12; j+=4) {
				int mask_bit = (move8 >> j) & 1;
				if (mask_bit==1){
					output->append(&tmp[sout->getTupleSize()*k]);
					k+= mask_bit;
				}
			}
#endif
		}
	}
}

//__m128i mask = _mm_set1_epi16(1);
//uint64_t *values;
//unsigned int vmask;
//vector1 = { sbuild->asLong(tup1, 0), sbuild->asLong(tup2, 0), sbuild->asLong(tup3, 0), sbuild->asLong(tup4, 0) };
//data2 = {s2->asLong(tuple, ja2), s2->asLong(tuple, ja2), data2[2] = s2->asLong(tuple, ja2), data2[3] = s2->asLong(tuple, ja2)};

//vmask = _mm_movemask_epi8(eq128);
//		finally = _mm_and_si128(eq128, mask);
//		values = (uint64_t*) &finally;

/*
 * Slow becomes of extra compares.
 */

void NestedLoops::joinPageTupExperimental(WriteTable* output, Page* page, void* tuple) {

	char tmp[sout->getTupleSize()];
	void *tup1, *tup2;
	int pageTupSize = page->tuplesize;
	int size3 = page->capacity() / pageTupSize;

	__m128i i1, i2, eq128, finally;
	__m128i mask = _mm_set1_epi16(1);
	//unsigned long res1[2];
	uint64_t *values;
	unsigned long long data1[2], data2[2];


	for (int i = 1; i < size3; i += 2) {
		tup1 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i - 1) * pageTupSize;
		tup2 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i) * pageTupSize;

		data1[0] = sbuild->asLong(tup1, 0);
		data1[1] = sbuild->asLong(tup2, 0);
		data2[0] = s2->asLong(tuple, ja2);
		data2[1] = s2->asLong(tuple, ja2);

		i1 = _mm_load_si128(( __m128i *)data1);     // this is equivalent to unsigned long long
		i2 = _mm_load_si128(( __m128i *)data2);
		eq128 = _mm_cmpeq_epi8(i1, i2);
		//_mm_store_si128 ((__m128i *)&res1, eq128);
		finally = _mm_and_si128(eq128, mask);
		values = (uint64_t*) &finally;

		if (values[0]==281479271743489){
			if (s1->getTupleSize())
				s1->copyTuple(tmp, sbuild->calcOffset(tup1, 1));

			for (unsigned int j = 0; j < sel2.size(); ++j)
				sout->writeData(tmp, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp);
#endif
		}

		if (values[1]==281479271743489){
			if (s1->getTupleSize())
				s1->copyTuple(tmp, sbuild->calcOffset(tup1, 1));

			for (unsigned int j = 0; j < sel2.size(); ++j)
				sout->writeData(tmp, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp);
#endif
		}
	}
}





//void NestedLoops::joinPageTupExperimental(WriteTable* output, Page* page, void* tuple) {
//
//    char tmp[sout->getTupleSize()];
//    void *tup1, *tup2;
//    int pageTupSize = page->tuplesize;
//    int size3 = page->capacity() / pageTupSize;
//
//    for (int i = 1; i < size3; i += 2) {
//    	unsigned long res1[2] = {0, 0}; // no need to set?
//
//        tup1 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i - 1) * pageTupSize;
//        tup2 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i) * pageTupSize;
//
//	        unsigned long long _data[2] = {sbuild->asLong(tup1, 0), sbuild->asLong(tup2, 0)};
//	        unsigned long long _data2[2] = {s2->asLong(tuple, ja2), s2->asLong(tuple, ja2)};
//	        unsigned long long _data3[2];
//	        unsigned long long _data4[2];
//	        __m128i *p;
//	        p = (__m128i *) _data3;
//
//	        __m128i i1 = _mm_load_si128((__m128i *) _data);
//	        // this is equ to unsigned long long
//	        __m128i i2 = _mm_load_si128((__m128i *) _data2);
//	        __m128i eq128 = _mm_cmpeq_epi8(i1, i2);
//
//        _mm_store_si128 ((__m128i *)&res1, eq128);
//
////        std::stringstream sstm1;
////        sstm1 << "./datagen/output/check" << endl;
////        std::string result = sstm1.str();
////        ofstream foutput(result.c_str());
////        foutput << "Revealing build table... \n" << flush;
////        for (int i = 0; i < 40; i++) {
////            foutput << res1[i] << endl;
////
////        }
//
//
//
//        if (sbuild->asLong(tup1, 0) == s2->asLong(tuple, ja2)) {
//            if (s1->getTupleSize())
//                s1->copyTuple(tmp, sbuild->calcOffset(tup1, 1));
//
//            for (unsigned int j = 0; j < sel2.size(); ++j)
//                sout->writeData(tmp, // dest
//                    s1->columns() + j, // col in output
//                    s2->calcOffset(tuple, sel2[j])); // src for this col
//#ifndef SUSPEND_OUTPUT
//            output->append(tmp);
//#endif
//        }
//
//        if (sbuild->asLong(tup2, 0) == s2->asLong(tuple, ja2)) { // this does not generalize
//            if (s1->getTupleSize())
//                s1->copyTuple(tmp, sbuild->calcOffset(tup2, 1));
//
//            for (unsigned int j = 0; j < sel2.size(); ++j)
//                sout->writeData(tmp, // dest
//                    s1->columns() + j, // col in output
//                    s2->calcOffset(tuple, sel2[j])); // src for this col
//#ifndef SUSPEND_OUTPUT
//            output->append(tmp);
//#endif
//        }
//}
//}


//bool AreSame(double a, double b)
//{
//    return fabs(a - b) < EPSILON;
//}

void NestedLoops::joinPageTupExp(WriteTable* output, Page* page, void* tuple) {
	int i = 0;
	char tmp[sout->getTupleSize()];
	void* tup, *tup1, *tup2;
	int pageTupSize = page->tuplesize;
	int size3 = page->capacity() / pageTupSize;

	for (i = 1; i < size3; i += 2) {
		tup1 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i - 1) * pageTupSize;
		tup2 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i) * pageTupSize;

		if (sbuild->asLong(tup1, 0) == s2->asLong(tuple, ja2)) {
			// copy payload of first tuple to destination
			if (s1->getTupleSize())
				s1->copyTuple(tmp, sbuild->calcOffset(tup1, 1));

			// copy each column to destination
			for (unsigned int j = 3; j < sel2.size(); j+=4)
				sout->writeData(tmp, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp);
#endif
		}

		if (sbuild->asLong(tup2, 0) == s2->asLong(tuple, ja2)) { // this does not generalize
			// copy payload of first tuple to destination
			if (s1->getTupleSize())
				s1->copyTuple(tmp, sbuild->calcOffset(tup2, 1));

			// copy each column to destination
			for (unsigned int j = 3; j < sel2.size(); j+=4)
				sout->writeData(tmp, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp);
#endif
		}
	}
	//size = 1;
}

/*
 * This version is 1 + 2
 */
void NestedLoops::joinPageTupExp3(WriteTable* output, Page* page, void* tuple) {
	int i = 0;
	char tmp[sout->getTupleSize()];
	void* tup, *tup1, *tup2;
	int pageTupSize = page->tuplesize;
	int size3 = page->capacity() / pageTupSize;

	for (i = 1; i < size3; i += 2) {
		tup1 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i - 1) * pageTupSize;
		tup2 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i) * pageTupSize;

		if (sbuild->asLong(tup1, 0) == s2->asLong(tuple, ja2)) {
			// copy payload of first tuple to destination
			if (s1->getTupleSize())
				s1->copyTuple(tmp, sbuild->calcOffset(tup1, 1));

			// copy each column to destination
			for (unsigned int j = 3; j < sel2.size(); j+=4)
				sout->writeData(tmp, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp);
#endif
		}

		if (sbuild->asLong(tup2, 0) == s2->asLong(tuple, ja2)) { // this does not generalize
			// copy payload of first tuple to destination
			if (s1->getTupleSize())
				s1->copyTuple(tmp, sbuild->calcOffset(tup2, 1));

			// copy each column to destination
			for (unsigned int j = 3; j < sel2.size(); j+=4)
				sout->writeData(tmp, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp);
#endif
		}
	}
	//size = 1;
}

/*
 * This version is 2x outer loop
 */
//void NestedLoops::joinPageTupExp1(WriteTable* output, Page* page, void* tuple) {
//	int i = 0;
//	char tmp[sout->getTupleSize()];
//	void* tup, *tup1, *tup2;
//	int pageTupSize = page->tuplesize;
//	int size3 = page->capacity() / pageTupSize;
//
//	for (i = 1; i < size3; i += 2) {
//		tup1 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i - 1) * pageTupSize;
//		tup2 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i) * pageTupSize;
//
//		if (sbuild->asLong(tup1, 0) == s2->asLong(tuple, ja2)) {
//			// copy payload of first tuple to destination
//			if (s1->getTupleSize())
//				s1->copyTuple(tmp, sbuild->calcOffset(tup1, 1));
//
//			// copy each column to destination
//			for (unsigned int j = 0; j < sel2.size(); ++j)
//				sout->writeData(tmp, // dest
//						s1->columns() + j, // col in output
//						s2->calcOffset(tuple, sel2[j])); // src for this col
//#ifndef SUSPEND_OUTPUT
//			output->append(tmp);
//#endif
//		}
//
//		if (sbuild->asLong(tup2, 0) == s2->asLong(tuple, ja2)) { // this does not generalize
//			// copy payload of first tuple to destination
//			if (s1->getTupleSize())
//				s1->copyTuple(tmp, sbuild->calcOffset(tup2, 1));
//
//			// copy each column to destination
//			for (unsigned int j = 0; j < sel2.size(); ++j)
//				sout->writeData(tmp, // dest
//						s1->columns() + j, // col in output
//						s2->calcOffset(tuple, sel2[j])); // src for this col
//#ifndef SUSPEND_OUTPUT
//			output->append(tmp);
//#endif
//		}
//	}
//	//size = 1;
//}


/*
 * This version is 10x outer loop
 */
void NestedLoops::joinPageTupExp1(WriteTable* output, Page* page, void* tuple) {
	int i = 0;
	char tmp1[sout->getTupleSize()];
	char tmp2[sout->getTupleSize()];
	char tmp3[sout->getTupleSize()];
	char tmp4[sout->getTupleSize()];
	char tmp5[sout->getTupleSize()];
	char tmp6[sout->getTupleSize()];
	char tmp7[sout->getTupleSize()];
	char tmp8[sout->getTupleSize()];
	char tmp9[sout->getTupleSize()];
	char tmp10[sout->getTupleSize()];
	void* tup;
	int pageTupSize = page->tuplesize;
	int size3 = page->capacity() / pageTupSize;

	for (i = 9; i < size3; i += 10) {
		void* tup10 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i - 9) * pageTupSize;
		void* tup9 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i - 8) * pageTupSize;
		void* tup8 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i - 7) * pageTupSize;
		void* tup7 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i - 6) * pageTupSize;
		void*tup6 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i - 5) * pageTupSize;
		void*tup5 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i-4) * pageTupSize;
		void*tup4 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i - 3) * pageTupSize;
		void*tup3 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i - 2) * pageTupSize;
		void*tup1 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i - 1) * pageTupSize;
		void*tup2 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i) * pageTupSize;

		if (sbuild->asLong(tup10, 0) == s2->asLong(tuple, ja2)) {
			// copy payload of first tuple to destination
			if (s1->getTupleSize())
				s1->copyTuple(tmp10, sbuild->calcOffset(tup10, 1));

			// copy each column to destination
			for (unsigned int j = 0; j < sel2.size(); ++j)
				sout->writeData(tmp10, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp10);
#endif
		}

		if (sbuild->asLong(tup9, 0) == s2->asLong(tuple, ja2)) {
			// copy payload of first tuple to destination
			if (s1->getTupleSize())
				s1->copyTuple(tmp9, sbuild->calcOffset(tup9, 1));

			// copy each column to destination
			for (unsigned int j = 0; j < sel2.size(); ++j)
				sout->writeData(tmp9, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp9);
#endif
		}

		if (sbuild->asLong(tup8, 0) == s2->asLong(tuple, ja2)) {
			// copy payload of first tuple to destination
			if (s1->getTupleSize())
				s1->copyTuple(tmp8, sbuild->calcOffset(tup8, 1));

			// copy each column to destination
			for (unsigned int j = 0; j < sel2.size(); ++j)
				sout->writeData(tmp8, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp8);
#endif
		}

		if (sbuild->asLong(tup7, 0) == s2->asLong(tuple, ja2)) {
			// copy payload of first tuple to destination
			if (s1->getTupleSize())
				s1->copyTuple(tmp7, sbuild->calcOffset(tup7, 1));

			// copy each column to destination
			for (unsigned int j = 0; j < sel2.size(); ++j)
				sout->writeData(tmp7, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp7);
#endif
		}

		if (sbuild->asLong(tup6, 0) == s2->asLong(tuple, ja2)) {
			// copy payload of first tuple to destination
			if (s1->getTupleSize())
				s1->copyTuple(tmp6, sbuild->calcOffset(tup6, 1));

			// copy each column to destination
			for (unsigned int j = 0; j < sel2.size(); ++j)
				sout->writeData(tmp6, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp6);
#endif
		}

		if (sbuild->asLong(tup5, 0) == s2->asLong(tuple, ja2)) {
			// copy payload of first tuple to destination
			if (s1->getTupleSize())
				s1->copyTuple(tmp5, sbuild->calcOffset(tup5, 1));

			// copy each column to destination
			for (unsigned int j = 0; j < sel2.size(); ++j)
				sout->writeData(tmp5, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp5);
#endif
		}

		if (sbuild->asLong(tup4, 0) == s2->asLong(tuple, ja2)) {
			// copy payload of first tuple to destination
			if (s1->getTupleSize())
				s1->copyTuple(tmp4, sbuild->calcOffset(tup4, 1));

			// copy each column to destination
			for (unsigned int j = 0; j < sel2.size(); ++j)
				sout->writeData(tmp4, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp4);
#endif
		}

		if (sbuild->asLong(tup3, 0) == s2->asLong(tuple, ja2)) {
			// copy payload of first tuple to destination
			if (s1->getTupleSize())
				s1->copyTuple(tmp3, sbuild->calcOffset(tup3, 1));

			// copy each column to destination
			for (unsigned int j = 0; j < sel2.size(); ++j)
				sout->writeData(tmp3, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp3);
#endif
		}

		if (sbuild->asLong(tup1, 0) == s2->asLong(tuple, ja2)) {
			// copy payload of first tuple to destination
			if (s1->getTupleSize())
				s1->copyTuple(tmp2, sbuild->calcOffset(tup1, 1));

			// copy each column to destination
			for (unsigned int j = 0; j < sel2.size(); ++j)
				sout->writeData(tmp2, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp2);
#endif
		}

		if (sbuild->asLong(tup2, 0) == s2->asLong(tuple, ja2)) { // this does not generalize
			// copy payload of first tuple to destination
			if (s1->getTupleSize())
				s1->copyTuple(tmp1, sbuild->calcOffset(tup2, 1));

			// copy each column to destination
			for (unsigned int j = 0; j < sel2.size(); ++j)
				sout->writeData(tmp1, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp1);
#endif
		}
	}
	//size = 1;
}

/*
 * This version is 4x inner loop  and 2x outer loop
 */

void NestedLoops::joinPageTupExp2(WriteTable* output, Page* page, void* tuple) {
	int i = 0;
	char tmp[sout->getTupleSize()];
	void* tup, *tup1, *tup2;
	int pageTupSize = page->tuplesize;
	int size3 = page->capacity() / pageTupSize;

	for (i = 1; i < size3; i += 2) {
		tup1 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i - 1) * pageTupSize;
		tup2 = (char*) page->getDataSpace() + static_cast<unsigned long long> (i) * pageTupSize;

		if (sbuild->asLong(tup1, 0) == s2->asLong(tuple, ja2)) {
			// copy payload of first tuple to destination
			if (s1->getTupleSize())
				s1->copyTuple(tmp, sbuild->calcOffset(tup1, 1));

			// copy each column to destination
			for (unsigned int j = 3; j < sel2.size(); j+=4)
				sout->writeData(tmp, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp);
#endif
		}

		if (sbuild->asLong(tup2, 0) == s2->asLong(tuple, ja2)) { // this does not generalize
			// copy payload of first tuple to destination
			if (s1->getTupleSize())
				s1->copyTuple(tmp, sbuild->calcOffset(tup2, 1));

			// copy each column to destination
			for (unsigned int j = 3; j < sel2.size(); j+=4)
				sout->writeData(tmp, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp);
#endif
		}
	}
	//size = 1;
}




void NestedLoops::joinPageTup(WriteTable* output, Page* page, void* tuple) {
	int i = 0;
	char tmp[sout->getTupleSize()];
	void* tup1;

	while (tup1 = page->getTupleOffset(i++)) { // page is outer
		if (sbuild->asLong(tup1, 0) == s2->asLong(tuple, ja2)) {
			// copy payload of first tuple to destination
			if (s1->getTupleSize())
				s1->copyTuple(tmp, sbuild->calcOffset(tup1, 1));

			// copy each column to destination
			for (unsigned int j = 0; j < sel2.size(); ++j)
				sout->writeData(tmp, // dest
						s1->columns() + j, // col in output
						s2->calcOffset(tuple, sel2[j])); // src for this col
#ifndef SUSPEND_OUTPUT
			output->append(tmp);
#endif
		}
	}
}

