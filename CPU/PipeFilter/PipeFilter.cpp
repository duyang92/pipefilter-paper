#include "PipeFilter.h"

#include<iostream>


bool is_prime_SIMD_block(int n) {
	if (n <= 1)
		return false;
	for (auto i = 2; i<int(sqrt(n)) + 1; i++) {
		if (n % i == 0)
			return false;
	}
	return true;
}

int ext_gcd_SIMD_block(int a, int b, int& x, int& y) {
	if (b == 0) {
		x = 1;
		y = 0;
		return a;
	}
	else {
		int gcd = ext_gcd_SIMD_block(b, a % b, y, x);
		y -= (a / b) * x;
		return gcd;
	}
}


int mod_inv_SIMD_block(int a, int m) {
	int x, y;
	int gcd = ext_gcd_SIMD_block(a, m, x, y);
	if (gcd != 1) {
		return -1;
	}
	else {
		return (x % m + m) % m;
	}
}

int gcd_SIMD_block(int a, int b) {
	if (b == 0) {
		return a;
	}
	else {
		return gcd_SIMD_block(b, a % b);
	}
}

vector<int> get_a_b_c_SIMD_block(int capacity) {
	vector<int> lst;
	for (int num = 2; num < capacity; ++num) {
		if (is_prime_SIMD_block(num) && gcd_SIMD_block(num, capacity) == 1) {
			lst.push_back(num);
		}
		if (lst.size() == 3) {
			break;
		}
	}
	return lst;
}

unsigned int findLargestPowerOfTwo_block(unsigned int num) {
	num |= (num >> 1);
	num |= (num >> 2);
	num |= (num >> 4);
	num |= (num >> 8);
	num |= (num >> 16);
	return (num + 1) >> 1;
}
mt19937 g_SIMD_block(12821);

PipeFilter_SIMD_block::PipeFilter_SIMD_block(const size_t table_length,
	const size_t fingerprint_bits, const int single_capacity, const size_t k) {
	this->fingerprint_size = fingerprint_bits;
	this->bytes_per_bucket = (fingerprint_size * COUNTER_PER_BUCKET_block + 7) >> 3;
	this->single_table_length = table_length;
	this->counter = 0;
	this->capacity = single_capacity;
	single_num = table_length / k;
	this->is_full = false;
	this->is_empty = true;
	this->hv = 0;
	mask = (1ULL << fingerprint_size) - 1;
	this->k = k;
	this->stage = 0;
	this->page_size = pow(2, 18);
	this->buckets_page = findLargestPowerOfTwo_block(this->page_size / this->bytes_per_bucket);
	this->buckets_page_log = log2(buckets_page);
	this->single_table_length = ROUNDUP_block(single_table_length, buckets_page);
	this->page_num = single_table_length / buckets_page;
	this->base_num = 0;
	this->single_num = buckets_page / k;
	this->random_seed = g_SIMD_block();
	vector<int> res = get_a_b_c_SIMD_block(this->single_num);
	this->a = res[0];
	this->b = res[1];
	this->c = res[2];
	this->a_b = a * b;
	this->a_b_c = a * b * c;
	this->kick_count = 0;
	this->a1 = mod_inv_SIMD_block(a, single_num);
	this->b1 = mod_inv_SIMD_block(b, single_num);
	this->c1 = mod_inv_SIMD_block(c, single_num);
	this->a1_b1_c1 = a1 * b1 * c1;

	bucket = new Bucket_SIMD_block[single_table_length];
	for (auto j = 0; j < this->single_table_length; j++) {
		for (auto t = 0; t < COUNTER_PER_BUCKET_block; t++) {
			bucket[j].bit_array[t] = 0;
		}
	}
}

PipeFilter_SIMD_block::~PipeFilter_SIMD_block() {
	delete[] bucket;
}

void PipeFilter_SIMD_block::generateIF(uint64_t item, uint32_t& index, uint32_t& fingerprint) {
	uint64_t hv = HashFunc::MurmurHash2_x64((char*)&item, sizeof(item), random_seed);
	index = ((uint32_t)(hv >> 32)) & (this->single_table_length - 1);
	fingerprint = (uint32_t)(hv & 0xFFFFFFFF);
	fingerprint &= ((0x1ULL << fingerprint_size) - 1);
	fingerprint += (fingerprint == 0);
	this->hv = fingerprint ^ random_seed;
}

inline int PipeFilter_SIMD_block::SIMD_match_4(size_t stage_k, size_t pos, uint32_t fp) {
	const __m128i item = _mm_set1_epi32((int)fp);
	int matched = 0;
	__m128i* keys_p = (__m128i*)(this->bucket[pos].bit_array);
	__m128i a_comp = _mm_cmpeq_epi32(item, keys_p[0]);
	int mask = _mm_movemask_epi8(a_comp);
	if (mask != 0) {
		int matched_index = _tzcnt_u32((uint32_t)mask) >> 2;
		return matched_index;
	}
	else return -1;
}

inline int PipeFilter_SIMD_block::SIMD_match_2(size_t stage_k, size_t pos, uint32_t fp) {
	const __m128i item = _mm_set1_epi32((int)fp);
	int matched = 0;
	__m128i keys_p = _mm_set_epi32(-1, -1, this->bucket[pos].bit_array[1], this->bucket[pos].bit_array[0]);
	__m128i a_comp = _mm_cmpeq_epi32(item, keys_p);
	int mask = _mm_movemask_epi8(a_comp);
	if (mask != 0) {
		int matched_index = _tzcnt_u32((uint32_t)mask) >> 2;
		return matched_index;
	}
	else return -1;
}

inline int PipeFilter_SIMD_block::SIMD_match_8(size_t stage_k, size_t pos, uint32_t fp) {
	const __m256i item = _mm256_set1_epi32((int)fp);
	int matched = 0;
	__m256i* keys_p = (__m256i*)(this->bucket[pos].bit_array);
	__m256i a_comp = _mm256_cmpeq_epi32(item, keys_p[0]);
	int mask = _mm256_movemask_epi8(a_comp);
	if (mask != 0) {
		int matched_index = _tzcnt_u32((uint32_t)mask) >> 2;
		return matched_index;
	}
	else return -1;
}

void PipeFilter_SIMD_block::generateNextIndex(const uint32_t index, uint32_t& next_index, const uint32_t fingerprint, const size_t stage_k, const uint32_t hv) {

	switch (stage_k) {
	case(0):
		next_index = (index * a - hv) % single_num;
		break;
	case(1):
		next_index = ((index + hv) * b - hv) % single_num;
		break;
	case(2):
		next_index = ((index + hv) * c - hv) % single_num;
		break;
	case(3):
		next_index = ((index + hv) * a1_b1_c1) % single_num;
		break;

	}
}


size_t PipeFilter_SIMD_block::generateCurIndex(const uint32_t index, const uint32_t fingerprint, const size_t cur_stage) {
	switch (cur_stage) {
	case(0):
		return index;
	case(1):
		return (index * a - hv) % single_num;
	case(2):
		return (index * a_b - hv) % single_num;
	case(3):
		return (index * a_b_c - hv) % single_num;
	}
}


bool PipeFilter_SIMD_block::insertItem(uint64_t item) {
	uint32_t index, next_index;
	uint32_t fingerprint;
	uint32_t fingerprint_old;
	generateIF(item, index, fingerprint);
	int count_k = 0;
	int temp;
	int matched = -1;
	int kicknum = (stage + k - 1) % k;
	this->base_num = (index >> this->buckets_page_log) << this->buckets_page_log;
	index = index & (this->single_num - 1);
	index = generateCurIndex(index, fingerprint, stage);
	size_t bucket_pos;
	bucket_pos = index + single_num * stage+base_num;
	_mm_prefetch(reinterpret_cast<const char*>(bucket[bucket_pos].bit_array), _MM_HINT_T0);
	while (true) {
		for (auto i = 0; i < k; i++) {
			temp = (i + stage) % k;
			matched = SIMD_match_8(temp, bucket_pos, (uint32_t)0);
			if (matched != -1) {
				this->bucket[bucket_pos].bit_array[matched] = fingerprint;
				stage = (stage + 1) % k;
				return true;
			}
			else {
				if (i == kicknum) {
					int j = rand() % COUNTER_PER_BUCKET_block;
					fingerprint_old = this->bucket[bucket_pos].bit_array[j];
					this->bucket[bucket_pos].bit_array[j] = fingerprint;
					fingerprint = fingerprint_old;
					count_k++;
					kick_count++;
					kicknum = (kicknum + k - 1) % k;
					this->hv = fingerprint ^ random_seed;
				}
			}
			int next_bucket = (temp + 1) % k;
			generateNextIndex(index, next_index, fingerprint, temp, hv);
			index = next_index;
			bucket_pos = index + single_num * next_bucket+base_num;
			_mm_prefetch(reinterpret_cast<const char*>(bucket[bucket_pos].bit_array), _MM_HINT_T0);

		}
		if (count_k > MaxNumCirculate_SIMD_block)
			break;
	}
	return false;
}


bool PipeFilter_SIMD_block::queryItem(uint64_t item) {
	uint32_t index, next_index;
	uint32_t fingerprint;
	generateIF(item, index, fingerprint);
	this->base_num = (index >> this->buckets_page_log) << this->buckets_page_log;
	index = index & (this->single_num - 1);
	int matched = -1;
	size_t bucket_pos;
	bucket_pos = index+base_num;
	for (auto i = 0; i < this->k; i++) {
		matched = SIMD_match_8(i, bucket_pos, fingerprint);
		if (matched != -1) {
			return true;
		}
		int next_bucket = (i + 1) % k;
		generateNextIndex(index, next_index, fingerprint, i, hv);
		index = next_index;
		bucket_pos = index + single_num * next_bucket + base_num;
	}
	return false;
}

bool PipeFilter_SIMD_block::deleteItem(uint64_t item) {
	uint32_t index, next_index, bucket_num;
	uint32_t fingerprint;
	generateIF(item, index, fingerprint);
	this->base_num = (index >> this->buckets_page_log) << this->buckets_page_log;
	index = index % this->single_num;
	int matched = -1;
	for (auto i = 0; i < this->k; i++) {
		bucket_num = index + single_num * i+base_num;
		matched = SIMD_match_2(i, bucket_num, fingerprint);
		if (matched != -1) {
			this->bucket[bucket_num].bit_array[matched] = 0;
			return true;
		}
		generateNextIndex(index, next_index, fingerprint, i, hv);
		index = next_index;
	}
	return false;
}

