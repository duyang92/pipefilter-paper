#include "D-ary CuckooFilter.h"
#include<iostream>
mt19937 g_Dary(12821);
DCuckooFilter_V2::DCuckooFilter_V2(size_t para_capacity, size_t para_fpsize, size_t candidate_buckets) {
	this->capacity = para_capacity;
	this->fingerprint_size = para_fpsize;
	this->bucket_bits = this->fingerprint_size * 4;
	this->bucket_bytes = (this->bucket_bits + 7) >> 3;
	this->is_full = false;
	this->is_empty = true;
	this->size = 0;
	this->mask = (1ULL << this->fingerprint_size) - 1;
	this->num_candidate_buckets = candidate_buckets;
	this->kickout = 0;
	random_seed = g_Dary();
	buckets = new Bucket_D[capacity];
	for (auto i = 0; i < this->capacity; i++) {
		buckets[i].bit_array = new char[bucket_bytes];
		memset(buckets[i].bit_array, 0, bucket_bytes);
	}
}

/**
 * @brief 
 * @param item 
 * @return 
*/
bool DCuckooFilter_V2::insertItem(uint64_t item) {
	uint32_t fp;
	uint32_t index;
	get_IF(item, fp, index);
	uint32_t indexs[4];
	indexs[0] = index;
	for (auto j = 1; j < this->num_candidate_buckets; j++) {
		indexs[j] = get_altindex(indexs[j - 1], fp);
	}
	for (auto i = 0; i < MaxNumKicks_D; i++) {
		for (auto j = 0; j < this->num_candidate_buckets; j++) {
			if (insertImpl(fp, indexs[j]))
				return true;
		}
		
		size_t eviction_index = indexs[rand() % this->num_candidate_buckets];
		indexs[0] = eviction_index;
		fp = swap_fp(indexs[0], fp);
		for (auto j = 1; j < this->num_candidate_buckets; j++) {
			indexs[j] = get_altindex(indexs[j - 1], fp);
		}
	}
	return false;
}

bool DCuckooFilter_V2::queryItem(uint64_t item) {
	uint32_t fp;
	uint32_t index;
	get_IF(item, fp, index);
	uint32_t indexs[4];
	indexs[0] = index;
	for (auto j = 1; j < this->num_candidate_buckets; j++) {
		indexs[j] = get_altindex(indexs[j - 1], fp);
	}
	for (auto j = 0; j < this->num_candidate_buckets; j++) {
		if (queryImpl(indexs[j],fp))
			return true;
	}
	return false;
}

bool DCuckooFilter_V2::queryImpl(const uint32_t index, const uint32_t fingerprint) {
	for (size_t pos = 0; pos < 4; pos++) {
		if (read(index, pos) == fingerprint) {
			return true;
		}
	}
	return false;
}

bool DCuckooFilter_V2::deleteItem(uint64_t item) {
	uint32_t fp;
	uint32_t index;
	get_IF(item, fp, index);
	size_t indexs[4];
	indexs[0] = index;
	for (auto j = 1; j < this->num_candidate_buckets; j++) {
		indexs[j] = get_altindex(indexs[j - 1], fp);
	}
	for (auto j = 0; j < this->num_candidate_buckets; j++) {
		if (deleteImpl(indexs[j], fp))
			return true;
	}
	return false;
}

bool DCuckooFilter_V2::deleteImpl(const uint32_t index, const uint32_t fingerprint) {
	for (size_t pos = 0; pos < 4; pos++) {
		if (read(index, pos) == fingerprint) {
			write(index, pos, 0);
			this->size--;
			if (this->size < this->capacity) {
				this->is_full = false;
			}
			if (this->size == 0) {
				this->is_empty = true;
			}
			return true;
		}
	}
	return false;
}

/**
 * @brief 
 * @param fp 
 * @param index 
 * @return 
*/
bool DCuckooFilter_V2::insertImpl(uint32_t fp, uint32_t index) {
	for (size_t pos = 0; pos < 4; pos++) {//
		if (read(index, pos) == 0) {//
			write(index, pos, fp);
			this->size++;
			if (this->size == capacity) {
				this->is_full = true;
			}

			if (this->size > 0) {
				this->is_empty = false;
			}
			return true;
		}
	}
	return false;
}

/**
 * @brief 
 * @param fp 
 * @param index 
 * @param base 
 * @return 
*/
size_t DCuckooFilter_V2::xor_(uint32_t fp, uint32_t index, size_t base) {
	size_t a[MAX], b[MAX], result[MAX];
	memset(a, 0, sizeof(a));
	memset(b, 0, sizeof(b));
	memset(result, 0, sizeof(result));
	base10toX(a, fp, base);
	base10toX(b, index, base);
	for (size_t i = 0; i < MAX; i++) {
		result[i] = (a[i] + b[i]) % base;
	}
	return baseXto10(result, base);
}

/**
 * @brief 
 * @param a 
 * @param x 
 * @return 
*/
size_t DCuckooFilter_V2::baseXto10(size_t a[], size_t x) {
	size_t base10 = 0;
	for (size_t i = 0; i < MAX; i++) {
		base10 = a[i] * pow(x, i) + base10;
	}
	return base10;
}

/**
 * @brief 
 * @param a 
 * @param t 
 * @param x 
*/
void DCuckooFilter_V2::base10toX(size_t a[], uint32_t t, size_t x) {
	uint32_t m = t;
	size_t i = 0;
	while (t)
	{
		t /= x;
		a[i++] = m - x * t;
		m = t;
	}
}

/**
 * @brief 
 * @param item 
 * @param fp 
 * @param index 
*/
void DCuckooFilter_V2::get_IF(uint64_t item, uint32_t& fp, uint32_t& index) {

	uint64_t hv = HashFunc::MurmurHash2_x64((char*)&item, sizeof(item), random_seed);
	index = ((uint32_t)(hv >> 32)) % this->capacity;
	fp = (uint32_t)(hv & 0xFFFFFFFF);//
	fp &= ((0x1ULL << fingerprint_size) - 1);//
	fp += (fp == 0);//
}

/**
 * @brief 
 * @param index 
 * @param fp 
 * @return 
*/
size_t DCuckooFilter_V2::get_altindex(uint32_t index, uint32_t fp) {
	return xor_(IndexHash((fp * 0x5bd1e995)), index, 4)%this->capacity;
}

/**
 * @brief 
 * @param index 
 * @param pos 
 * @return 
*/
uint32_t DCuckooFilter_V2::read(uint32_t index, size_t pos) {
	const char* p = buckets[index].bit_array;
	uint32_t fingerprint;
	if (this->fingerprint_size == 4) {
		p += (pos >> 1);
		uint8_t bits_8 = *(uint8_t*)p;
		if ((pos & 1) == 0) {//
			fingerprint = bits_8 & 0xf;
		}
		else {//
			fingerprint = (bits_8 >> 4) & 0xf;
		}
	}
	else if (fingerprint_size == 6) {
		uint8_t bits_8;
		uint16_t bits_16;
		switch (pos) {
		case(0):
			bits_8 = *(uint8_t*)p;
			fingerprint = bits_8 & 0x3f;
			break;
		case(1):
			bits_16 = *(uint16_t*)p;
			fingerprint = (bits_16 >> 6) & 0x3f;
			break;
		case(2):
			p += 1;
			bits_16 = *(uint16_t*)p;
			fingerprint = (bits_16 >> 4) & 0x3f;
			break;
		case(3):
			p += 2;
			bits_8 = *(uint8_t*)p;
			fingerprint = (bits_8 >> 2) & 0x3f;
			break;
		}

	}
	else if (fingerprint_size == 8) {
		p += pos;//
		uint8_t bits_8 = *(uint8_t*)p;
		fingerprint = bits_8 & 0xff;//
	}
	else if (fingerprint_size == 10) {
		uint16_t bits_16;
		switch (pos) {
		case(0):
			bits_16 = *(uint16_t*)p;
			fingerprint = (bits_16) & 0x03ff;
			break;
		case(1):
			p += 1;
			bits_16 = *(uint16_t*)p;
			fingerprint = (bits_16 >> 2) & 0x03ff;
			break;
		case(2):
			p += 2;
			bits_16 = *(uint16_t*)p;
			fingerprint = (bits_16 >> 4) & 0x03ff;
			break;
		case(3):
			p += 3;
			bits_16 = *(uint16_t*)p;
			fingerprint = (bits_16 >> 6) & 0x03ff;
			break;
		}
	}
	else if (fingerprint_size == 12) {
		p += pos + (pos >> 1);
		uint16_t bits_16 = *(uint16_t*)p;
		if ((pos & 1) == 0) {
			fingerprint = bits_16 & 0xfff;
		}
		else {
			fingerprint = (bits_16 >> 4) & 0xfff;
		}
	}
	else if (fingerprint_size == 14) {
		uint16_t bits_16;
		uint32_t bits_32;
		switch (pos) {
		case(0):
			bits_16 = *(uint16_t*)p;
			fingerprint = (bits_16) & 0x3fff;
			break;
		case(1):
			p += 1;
			bits_32 = *(uint32_t*)p;
			fingerprint = (bits_32 >> 6) & 0x00003fff;
			break;
		case(2):
			p += 3;
			bits_32 = *(uint32_t*)p;
			fingerprint = (bits_32 >> 4) & 0x00003fff;
			break;
		case(3):
			p += 5;
			bits_16 = *(uint16_t*)p;
			fingerprint = (bits_16 >> 2) & 0x3fff;
			break;
		}
	}
	else if (fingerprint_size == 16) {
		p += (pos << 1);
		uint16_t bits_16 = *(uint16_t*)p;
		fingerprint = bits_16 & 0xffff;
	}
	else if (fingerprint_size == 18) {
		uint32_t bits_32;
		switch (pos) {
		case(0):
			bits_32 = *(uint32_t*)p;
			fingerprint = (bits_32 & 0x0003ffff);
			break;
		case(1):
			p += 2;
			bits_32 = *(uint32_t*)p;
			fingerprint = (bits_32 & 0x000ffffc) >> 2;
			break;
		case(2):
			p += 4;
			bits_32 = *(uint32_t*)p;
			fingerprint = (bits_32 >> 4) & 0x0003ffff;
			break;
		case(3):
			p += 6;
			bits_32 = *(uint32_t*)p;
			fingerprint = (bits_32 >> 6) & 0x0003ffff;
			break;
		}
	}
	else if (fingerprint_size == 20) {
		uint32_t bits_32;
		switch (pos) {
		case(0):
			bits_32 = *(uint32_t*)p;
			fingerprint = bits_32 & 0x000fffff;
			break;
		case(1):
			p += 2;
			bits_32 = *(uint32_t*)p;
			fingerprint = (bits_32 >> 4) & 0x000fffff;
			break;
		case(2):
			p += 4;
			bits_32 = *(uint32_t*)p;
			fingerprint = (bits_32 >> 8) & 0x000fffff;
			break;
		case(3):
			p += 7;
			bits_32 = *(uint32_t*)p;
			fingerprint = (bits_32 >> 4) & 0x000fffff;
			break;
		}
	}
	else if (fingerprint_size == 24) {
		p += pos + (pos << 1);
		uint32_t bits_32 = *(uint32_t*)p;
		fingerprint = (bits_32 >> 4);
	}
	else if (fingerprint_size == 32) {
		p += (pos << 2);
		uint32_t bits_32 = *(uint32_t*)p;
		fingerprint = bits_32 & 0xffffffff;
	}
	else {
		fingerprint = 0;
	}
	return fingerprint & this->mask;
}

void DCuckooFilter_V2::write(uint32_t index, size_t pos, uint32_t fingerprint) {
	char* p = buckets[index].bit_array;
	if (fingerprint_size == 4) {
		p += (pos >> 1);
		if ((pos & 1) == 0) {//
			*((uint8_t*)p) &= 0xf0;
			*((uint8_t*)p) |= fingerprint;
		}
		else {
			*((uint8_t*)p) &= 0x0f;
			*((uint8_t*)p) |= (fingerprint << 4);
		}
	}
	else if (fingerprint_size == 6) {
		switch (pos) {
		case(0):
			*((uint8_t*)p) &= 0xc0;
			*((uint8_t*)p) |= fingerprint;
			break;
		case(1):
			*((uint16_t*)p) &= 0xf03f;
			*((uint16_t*)p) |= fingerprint << 6;
			break;
		case(2):
			p += 1;
			*((uint16_t*)p) &= 0xfc0f;
			*((uint16_t*)p) |= fingerprint << 4;
			break;
		case(3):
			p += 2;
			*((uint8_t*)p) &= 0x03;
			*((uint8_t*)p) |= fingerprint << 2;
			break;
		}


	}
	else if (fingerprint_size == 8) {
		((uint8_t*)p)[pos] = fingerprint;
	}
	else if (fingerprint_size == 10) {
		switch (pos) {
		case(0):
			*((uint16_t*)p) &= 0xfc00;
			*((uint16_t*)p) |= fingerprint;
			break;
		case(1):
			p += 1;
			*((uint16_t*)p) &= 0xf003;
			*((uint16_t*)p) |= fingerprint << 2;
			break;
		case(2):
			p += 2;
			*((uint16_t*)p) &= 0xc00f;
			*((uint16_t*)p) |= fingerprint << 4;
			break;
		case(3):
			p += 3;
			*((uint16_t*)p) &= 0x003f;
			*((uint16_t*)p) |= fingerprint << 6;
			break;
		}
	}
	else if (fingerprint_size == 12) {
		p += (pos + (pos >> 1));
		if ((pos & 1) == 0) {
			*((uint16_t*)p) &= 0xf000; //Little-Endian
			*((uint16_t*)p) |= fingerprint;
		}
		else {
			*((uint16_t*)p) &= 0x000f;
			*((uint16_t*)p) |= fingerprint << 4;
		}
	}
	else if (fingerprint_size == 14) {
		switch (pos) {
		case(0):
			*((uint16_t*)p) &= 0xc000;
			*((uint16_t*)p) |= fingerprint;
			break;
		case(1):
			p += 1;
			*((uint32_t*)p) &= 0xfff0003f;
			*((uint32_t*)p) |= fingerprint << 6;
			break;
		case(2):
			p += 3;
			*((uint32_t*)p) &= 0xfffc000f;
			*((uint32_t*)p) |= fingerprint << 4;
			break;
		case(3):
			p += 5;
			*((uint16_t*)p) &= 0x0003;
			*((uint16_t*)p) |= fingerprint << 2;
			break;
		}
	}
	else if (fingerprint_size == 16) {
		((uint16_t*)p)[pos] = fingerprint;
	}
	else if (fingerprint_size == 18) {
		switch (pos) {
		case(0):
			*((uint32_t*)p) &= 0xfffc0000;
			*((uint32_t*)p) |= fingerprint;
			break;
		case(1):
			p += 2;
			*((uint32_t*)p) &= 0xfff00003;
			*((uint32_t*)p) |= fingerprint << 2;
			break;
		case(2):
			p += 4;
			*((uint32_t*)p) &= 0xffc0000f;
			*((uint32_t*)p) |= fingerprint << 4;
			break;
		case(3):
			p += 6;
			*((uint32_t*)p) &= 0xff00003f;
			*((uint32_t*)p) |= fingerprint << 6;
			break;
		}
	}
	else if (fingerprint_size == 20) {
		switch (pos) {
		case(0):
			*((uint32_t*)p) &= 0xfff00000;
			*((uint32_t*)p) |= fingerprint;
			break;
		case(1):
			p += 2;
			*((uint32_t*)p) &= 0xff00000f;
			*((uint32_t*)p) |= fingerprint << 4;
			break;
		case(2):
			p += 5;
			*((uint32_t*)p) &= 0xfff00000;
			*((uint32_t*)p) |= fingerprint;
			break;
		case(3):
			p += 7;
			*((uint32_t*)p) &= 0xff00000f;
			*((uint32_t*)p) |= fingerprint << 4;
			break;
		}
	}
	else if (fingerprint_size == 24) {
		p += (pos + (pos << 1));
		*((uint32_t*)p) &= 0xff000000; //Little-Endian
		*((uint32_t*)p) |= fingerprint;
	}
	else if (fingerprint_size == 32) {
		((uint32_t*)p)[pos] = fingerprint;
	}
}


uint32_t DCuckooFilter_V2::swap_fp(uint32_t index, uint32_t fp) {
	this->kickout++;
	int pos = rand() % 4;
	uint32_t evict_fp = read(index, pos);
	write(index, pos, fp);
	return evict_fp;
}

size_t DCuckooFilter_V2::IndexHash(uint32_t hv){
	return hv % this->capacity;
}