#pragma once
#ifndef CUCKOOFILTER_H_
#define CUCKOOFILTER_H_

#include<string.h>
#include<stdlib.h>
#include"hashfunction.h"
#include<random>
//#include"uint.h"

#define MaxNumKicks 500

using namespace std;

typedef struct {
	size_t index;//8bits,unsigned int
	uint32_t fingerprint;//32bits
} Victim;

typedef struct {
	char* bit_array;
} Bucket;




class CuckooFilter {
private:

	int capacity;
	size_t single_table_length;
	size_t fingerprint_size;//
	size_t bits_per_bucket;//
	size_t bytes_per_bucket;//
	uint32_t random_seed;

	Bucket* bucket;//

	uint32_t mask;//

public:
	size_t hash_counter;
	bool is_full;
	bool is_empty;//
	int counter;
	int kick_count;

	//construction and distruction function
	CuckooFilter(const size_t single_table_length, const size_t fingerprint_size, const int capacity);//
	~CuckooFilter();//

	//insert & query & delete function
	int  insertItem(uint64_t item);

	bool insertItem(uint64_t index, const uint32_t fingerprint, bool kickout, Victim& victim);
	bool queryItem(uint64_t item);
	bool deleteItem(uint64_t item);
	//插入实现
	bool insertImpl(const uint32_t index, const uint32_t fingerprint, const bool kickout, Victim& victim);
	bool queryImpl(const uint32_t index, const uint32_t fingerprint);
	bool deleteImpl(const uint32_t index, const uint32_t fingerprint);

	//generate two candidate bucket addresses

	void generateIF(uint64_t item, uint32_t& index, uint32_t& fingerprint);

	void generateA(uint32_t index, uint32_t fingerprint, uint32_t& alt_index);

	//read from bucket & write into bucket
	uint32_t read(const uint32_t index, const size_t pos);
	void write(const uint32_t index, const size_t pos, const uint32_t fingerprint);

	//move corresponding fingerprints to sparser CF
	bool transfer(CuckooFilter* tarCF);

};



#endif //CUCKOOFILTER_H_
