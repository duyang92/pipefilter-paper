#pragma once
#ifndef ARKFILTER_H_
#define ARKFILTER_H_
#include "Ark_Bucket.h"
#include<string.h>
#include<stdlib.h>
#include"hashfunction.h"
#include<random>
#define MaxNumKicks_AF 1000
using namespace std;


class ArkFilter {
private:
	int capacity;
	Ark_Bucket* buckets;
public:
	
	int counter;
	size_t single_table_length;
	ArkFilter(size_t capacity);
	~ArkFilter();
	uint32_t random_seed;
	uint64_t generateFp(uint64_t item);



	uint32_t getRIndex(uint64_t fingerprint);

	uint32_t getQIndex(uint64_t fingerprint);


	int insertItem(uint64_t item);

	int queryItem(uint64_t item);

	int deleteItem(uint64_t item);
};

#endif