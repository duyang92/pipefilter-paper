#pragma once
#ifndef VACUUMFILTER_H_
#define VACUUMFILTER_H_

#include<string.h>
#include<stdlib.h>
#include "hashfunction.h"
#include<random>
#include<iostream>
#define MaxNumKicks_Vacuum 500

#define ROUNDDOWN_VACUUM(a, b) ((a) - ((a) % (b)))
#define ROUNDUP_VACUUM(a, b) ROUNDDOWN_VACUUM((a) + (b - 1), b)
const int AR = 4;


typedef struct {
	char* bit_array;
} Bucket_Vacuum;

class VacuumFilter {
	size_t single_table_length;
	size_t fingerprint_size;

	size_t bits_per_bucket;
	size_t bytes_per_bucket;

	uint32_t random_seed;

	Bucket_Vacuum* bucket;

	uint32_t mask;

public:
	int big_seg;
	int len[AR];
	bool is_full;
	bool is_empty;
	int counter;
	VacuumFilter(const size_t single_table_length, const size_t fingerprint_size);
	~VacuumFilter();

	bool insertItem(uint64_t item);
	bool queryItem(uint64_t item);
	bool deleteItem(uint64_t item);

	bool insertImpl(const uint32_t index, const uint32_t fingerprint, uint32_t* tags);
	bool queryImpl(const uint32_t index, const uint32_t fingerprint);
	bool deleteImpl(const uint32_t index, const uint32_t fingerprint);

	void generateIF(uint64_t item, uint32_t& index, uint32_t& fingerprint, int fingerprint_size);

	void generateA(uint32_t index, uint32_t fingerprint, uint32_t& alt_index);

	uint32_t read(const uint32_t index, const size_t pos);
	void write(const uint32_t index, const size_t pos, const uint32_t fingerprint);


	double F_d_V(double x, double c);

	double F_V(double x, double c);

	double solve_equation_V(double c);

	double balls_in_bins_max_load_V(double balls, double bins);

	int proper_alt_range_V(int M, int i, int* len);
};
#endif