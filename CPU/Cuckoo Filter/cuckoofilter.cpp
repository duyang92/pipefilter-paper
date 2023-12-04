
#include "cuckoofilter.h"
#include<iostream>

mt19937 g_Cuk(12821);
CuckooFilter::CuckooFilter(const size_t table_length, const size_t fingerprint_bits, const int single_capacity) {
	fingerprint_size = fingerprint_bits;
	bits_per_bucket = fingerprint_size * 4;
	bytes_per_bucket = (fingerprint_size * 4 + 7) >> 3;
	single_table_length = table_length;
	counter = 0;
	capacity = single_capacity;
	is_full = false;
	is_empty = true;
	kick_count = 0;
	mask = (1ULL << fingerprint_size) - 1;
	this->hash_counter = 0;
	random_seed = g_Cuk();
	bucket = new Bucket[single_table_length];
	for (size_t i = 0; i < single_table_length; i++) {
		bucket[i].bit_array = new char[bytes_per_bucket];
		memset(bucket[i].bit_array, 0, bytes_per_bucket);
	}

}

CuckooFilter::~CuckooFilter() {
	delete[] bucket;
}


/**
 * @brief
 *
 * @param item
 * @param victim 
 * @return int
 */
int CuckooFilter::insertItem(uint64_t item) {
	uint32_t index, alt_index;
	uint32_t fingerprint;

	bool kickout = false;
	generateIF(item, index, fingerprint);
	Victim victim;
	if (insertImpl(index, fingerprint, kickout, victim)) {
		return true;
	}
	generateA(index, fingerprint, alt_index);
	if (insertImpl(alt_index, fingerprint, kickout, victim)) {
		return true;
	}
	for (size_t count = 0; count < MaxNumKicks; count++) {
		bool kickout = (count != 0);
		if (insertImpl(index, fingerprint, kickout, victim)) {
			return true;
		}

		if (kickout) {
			this->kick_count++;
			index = victim.index;
			fingerprint = victim.fingerprint;
			generateA(index, fingerprint, alt_index);
			index = alt_index;
		}
		else {
			generateA(index, fingerprint, alt_index);
			index = alt_index;
		}

	}


	return false;
}

/**
 * @brief 
 *
 * @param index
 * @param fingerprint
 * @param kickout
 * @param victim
 * @return true
 * @return false
 */

/**
 * .
 * 
 * \param item
 * \return 
 */
bool CuckooFilter::queryItem(uint64_t item) {
	uint32_t index, alt_index;
	uint32_t fingerprint;
	generateIF(item, index, fingerprint);
	
	if (queryImpl(index, fingerprint)) {
		return true;
	}
	generateA(index, fingerprint, alt_index);
	if (queryImpl(alt_index, fingerprint)) {
		return true;
	}
	return false;
}

/**
 * @brief 
 * @param item 
 * @return 
*/
bool CuckooFilter::deleteItem(uint64_t item) {
	uint32_t index, alt_index;
	uint32_t fingerprint;
	generateIF(item, index, fingerprint);
	
	if (deleteImpl(index, fingerprint)) {
		return true;
	}
	generateA(index, fingerprint, alt_index);
	if (deleteImpl(alt_index, fingerprint)) {
		return true;
	}

	return false;
}



bool CuckooFilter::insertImpl(const uint32_t index, const uint32_t fingerprint, const bool kickout, Victim& victim) {
	for (size_t pos = 0; pos < 4; pos++) {
		if (read(index, pos) == 0) {
			write(index, pos, fingerprint);
			counter++;
			if (this->counter == capacity) {
				this->is_full = true;
			}

			if (this->counter > 0) {
				this->is_empty = false;
			}
			return true;
		}
	}
	if (kickout) {
		int j = rand() % 4;
		victim.index = index;
		victim.fingerprint = read(index, j);
		write(index, j, fingerprint);
	}
	return false;
}

bool CuckooFilter::queryImpl(const uint32_t index, const uint32_t fingerprint) {

	for (size_t pos = 0; pos < 4; pos++) {
		if (read(index, pos) == fingerprint) {
			return true;
		}
	}
	return false;
}

bool CuckooFilter::deleteImpl(const uint32_t index, const uint32_t fingerprint) {
	for (size_t pos = 0; pos < 4; pos++) {
		if (read(index, pos) == fingerprint) {
			write(index, pos, 0);
			counter--;
			if (counter < this->capacity) {
				this->is_full = false;
			}
			if (counter == 0) {
				this->is_empty = true;
			}
			return true;
		}
	}
	return false;
}


/**
 * @brief 
 * @param item 
 * @param index 
 * @param fingerprint 
 * @param fingerprint_size 
 * @param single_table_length 
*/
void CuckooFilter::generateIF(uint64_t item, uint32_t& index, uint32_t& fingerprint) {
	uint64_t hv = HashFunc::MurmurHash2_x64((char*) & item, sizeof(item), random_seed);
	index = ((uint32_t)(hv >> 32)) % single_table_length;//hash(x)
	fingerprint = (uint32_t)(hv & 0xFFFFFFFF);
	fingerprint &= ((0x1ULL << fingerprint_size) - 1);
	fingerprint += (fingerprint == 0);

}


void CuckooFilter::generateA(uint32_t index, uint32_t fingerprint, uint32_t& alt_index) {
	uint32_t hv = HashFunc::BobHash(&fingerprint, sizeof(fingerprint), 1234);
	alt_index = (index ^ hv) % single_table_length;
}



uint32_t CuckooFilter::read(uint32_t index, size_t pos) {
	const char* p = bucket[index].bit_array;
	uint32_t fingerprint;
	if (this->fingerprint_size == 3) {
		uint8_t bits_8;
		uint16_t bits_16;
		switch (pos) {
		case(0):
			bits_8 = *(uint8_t*)p;
			fingerprint = bits_8 & 0x3;
			break;
		case(1):
			bits_8 = *(uint8_t*)p;
			fingerprint = (bits_8 >> 3) & 0x3;
			break;
		case(2):
			bits_16 = *(uint16_t*)p;
			fingerprint = (bits_16 >> 6) & 0x3;
			break;
		case(3):
			p += 1;
			bits_8 = *(uint8_t*)p;
			fingerprint = (bits_8 >> 1) & 0x3;
			break;
		}
	}
	else if (this->fingerprint_size == 4) {
		p += (pos >> 1);
		uint8_t bits_8 = *(uint8_t*)p;
		if ((pos & 1) == 0) {
			fingerprint = bits_8 & 0xf;
		}
		else {
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
		p += pos;
		uint8_t bits_8 = *(uint8_t*)p;
		fingerprint = bits_8 & 0xff;
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


void CuckooFilter::write(uint32_t index, size_t pos, uint32_t fingerprint) {
	char* p = bucket[index].bit_array;
	if (fingerprint_size == 3) {
		switch (pos) {
		case(0):
			*((uint8_t*)p) &= 0xf8;
			*((uint8_t*)p) |= fingerprint;
			break;
		case(1):
			*((uint8_t*)p) &= 0xc7;
			*((uint8_t*)p) |= fingerprint << 3;
			break;
		case(2):
			*((uint16_t*)p) &= 0xfe3f;
			*((uint16_t*)p) |= fingerprint << 6;
			break;
		case(3):
			p += 1;
			*((uint8_t*)p) &= 0xf1;
			*((uint8_t*)p) |= fingerprint << 1;
			break;
		}


	}
	else if (fingerprint_size == 4) {
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



bool CuckooFilter::transfer(CuckooFilter* tarCF) {
	uint32_t fingerprint = 0;

	for (size_t i = 0; i < single_table_length; i++) {
		for (int j = 0; j < 4; j++) {
			fingerprint = read(i, j);
			if (fingerprint != 0) {
				if (tarCF->is_full == true) {
					return false;
				}
				if (this->is_empty == true) {
					return false;
				}
				Victim victim = { 0, 0 };
				if (tarCF->insertImpl(i, fingerprint, false, victim)) {
					this->write(i, j, 0);
					this->counter--;

					if (this->counter < capacity) {
						this->is_full = false;
					}
					if (this->counter == 0) {
						this->is_empty = true;
					}

					if (tarCF->counter == capacity) {
						tarCF->is_full = true;
					}

					if (tarCF->counter > 0) {
						tarCF->is_empty = false;
					}
				}
			}
		}
	}
	return true;
}

