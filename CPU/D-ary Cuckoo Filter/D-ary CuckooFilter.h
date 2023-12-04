#pragma once
#include<string.h>
#include<stdlib.h>
#include "hashfunction.h"
#include<cmath>
#include<random>
#define MaxNumKicks_D 500
#define MAX 20
typedef struct {
	char* bit_array;
} Bucket_D;

class DCuckooFilter_V2 {
private:
	size_t capacity;
	size_t fingerprint_size;
	size_t bucket_bits;
	size_t bucket_bytes;
	Bucket_D* buckets;
	uint32_t mask;//
	size_t num_candidate_buckets;
	uint32_t random_seed;
public:
	bool is_full;
	bool is_empty;
	size_t size;

	int kickout;

	DCuckooFilter_V2(size_t para_capacity, size_t para_fpsize, size_t candidate_buckets);

	bool insertItem(uint64_t item);

	bool queryItem(uint64_t item);

	bool queryImpl(const uint32_t index, const uint32_t fingerprint);

	bool insertImpl(uint32_t fp, uint32_t index);

	bool deleteItem(uint64_t item);

	bool deleteImpl(const uint32_t index, const uint32_t fingerprint);

	void get_IF(uint64_t item, uint32_t& fp, uint32_t& index);

	size_t get_altindex(uint32_t index, uint32_t fp);

	uint32_t swap_fp(uint32_t index, uint32_t fp);

	uint32_t read(uint32_t index, size_t pos);

	void write(uint32_t index, size_t pos, uint32_t fingerprint);

	size_t xor_(uint32_t fp, uint32_t index, size_t base);

	void base10toX(size_t a[], uint32_t t, size_t x);

	size_t baseXto10(size_t a[], size_t x);

	size_t IndexHash(uint32_t hv);

};