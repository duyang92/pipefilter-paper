#pragma once
#ifndef PIPEFILTER_H_SIMD_block
#define PIPEFILTER_H_SIMD_block
#include <immintrin.h>
#include <emmintrin.h>
#include <smmintrin.h>

#include<string.h>
#include<stdlib.h>
#include"hashfunction.h"
#include<vector>
#include<random>
#define COUNTER_PER_BUCKET_block 8
#define MaxNumCirculate_SIMD_block 500
#define ROUNDDOWN_block(a, b) ((a) - ((a) % (b)))
#define ROUNDUP_block(a, b) ROUNDDOWN_block((a) + (b - 1), b)
using namespace std;

typedef struct {
	uint32_t bit_array[COUNTER_PER_BUCKET_block];
} Bucket_SIMD_block;

class PipeFilter_SIMD_block {
private:
	int capacity;
	size_t single_table_length;
	
	size_t fingerprint_size;
	size_t bits_per_bucket;
	size_t bytes_per_bucket;
	size_t k;
	uint32_t mask;
	size_t a, b, c;
	size_t a1, b1, c1;
	size_t a1_b1_c1;
	size_t a_b;
	size_t a_b_c;
public:
	size_t single_num;
	alignas(32) Bucket_SIMD_block* bucket;
	uint32_t random_seed;
	size_t kick_count;
	size_t page_size;
	size_t page_num;
	unsigned int  buckets_page;
	unsigned int buckets_page_log;
	size_t base_num;
	uint32_t hv;
	size_t stage;
	bool is_full;
	bool is_empty;
	int counter;

	PipeFilter_SIMD_block(const size_t single_table_length,
		const size_t fingerprint_size, const int capacity, const size_t k);

	~PipeFilter_SIMD_block();

	void generateIF(uint64_t item, uint32_t& index, uint32_t& fingerprint);

	bool insertItem(uint64_t item);

	inline int SIMD_match_4(size_t stage_k, size_t pos, uint32_t fp);

	inline int SIMD_match_2(size_t stage_k, size_t pos, uint32_t fp);

	inline int SIMD_match_8(size_t stage_k, size_t pos, uint32_t fp);

	void generateNextIndex(const uint32_t index, uint32_t& next_index, const uint32_t fingerprint, const size_t stage_k, const uint32_t hv);

	bool queryItem(uint64_t item);

	bool deleteItem(uint64_t item);

	size_t generateCurIndex(const uint32_t index, const uint32_t fingerprint, const size_t cur_stage);
};

#endif