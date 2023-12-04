#include "VacuumFilter.h"

mt19937 Vacuum_rand(12821);
VacuumFilter::VacuumFilter(const size_t single_table_length,
	const size_t fingerprint_size) {

	this->fingerprint_size = fingerprint_size;

	this->bits_per_bucket = fingerprint_size * 4;

	this->bytes_per_bucket = (fingerprint_size * 4 + 7) >> 3;

	this->single_table_length = single_table_length;

	this->mask = (1ULL << fingerprint_size) - 1;

	this->random_seed = Vacuum_rand();

	is_full = false;

	is_empty = true;
	
	counter = 0;

	bucket = new Bucket_Vacuum[single_table_length];
	for (size_t i = 0; i < single_table_length; i++) {
		bucket[i].bit_array = new char[bytes_per_bucket];
		memset(bucket[i].bit_array, 0, bytes_per_bucket);
	}

	this->big_seg = 1024;
	this->big_seg = max(big_seg, proper_alt_range_V(single_table_length, 0, len));
	this->big_seg--;

	len[0] = big_seg;
	for (int i = 1; i < AR; i++)
		len[i] = proper_alt_range_V(single_table_length, i, len) - 1;
	len[AR - 1] = (len[AR - 1] + 1) * 2 - 1;
	if (AR == 1) this->single_table_length = ROUNDUP_VACUUM(single_table_length, len[0] + 1);

}


VacuumFilter::~VacuumFilter() {
	delete[] bucket;
}


void VacuumFilter::generateIF(uint64_t item, uint32_t& index, uint32_t& fingerprint, int fingerprint_size) {
	uint64_t hv = HashFunc::MurmurHash2_x64((char*) & item, sizeof(item), random_seed);

	index=(((uint64_t)(hv >> 32) * (uint64_t)single_table_length) >> 32);

	fingerprint = (uint32_t)(hv & 0xFFFFFFFF);
	fingerprint &= ((0x1ULL << fingerprint_size) - 1);
	fingerprint += (fingerprint == 0);
}


void VacuumFilter::generateA(uint32_t index, uint32_t fingerprint, uint32_t& alt_index) {
	uint32_t hv = HashFunc::BobHash(&fingerprint, sizeof(fingerprint), 1234);
	int seg = len[hv & (AR - 1)];
	hv = hv & seg;
	hv += (hv == 0);
	alt_index=(index ^ hv);
}

bool VacuumFilter::insertItem(uint64_t item) {
	uint32_t index,alt_index;
	uint32_t fingerprint,old_fingerprint;
	uint32_t tags[4];
	uint32_t temp_tags[4];
	generateIF(item, index, fingerprint, fingerprint_size);
	if (insertImpl(index, fingerprint, tags)) {
		return true;
	}
	generateA(index, fingerprint, alt_index);
	if (insertImpl(alt_index, fingerprint,tags)) {
		return true;
	}

	for (auto i = 0; i < MaxNumKicks_Vacuum;i++) {
		if (insertImpl(index, fingerprint, tags)) {
			return true;
		}
		for (auto j = 0; j < 4; j++) {
			uint32_t alt;
			generateA(index, tags[j], alt);
			if (insertImpl(alt, tags[j], temp_tags)) {
				write(index, j, fingerprint);
				return true;
			}
		}

		int r = rand() % 4;
		old_fingerprint = tags[r];
		write(index,r, fingerprint);
		fingerprint = old_fingerprint;
		generateA(index, fingerprint, alt_index);
		index = alt_index;
	}

	return false;

}


bool VacuumFilter::insertImpl(const uint32_t index, const uint32_t fingerprint, uint32_t* tags) {
	for (auto pos = 0; pos < 4; pos++) {
		tags[pos] = read(index, pos);
		if (tags[pos] == 0) {
			write(index, pos, fingerprint);
			return true;
		}
	}
	return false;
}

uint32_t VacuumFilter::read(uint32_t index, size_t pos) {
	const char* p = bucket[index].bit_array;
	uint32_t fingerprint;
	if (this->fingerprint_size == 4) {
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


void VacuumFilter::write(uint32_t index, size_t pos, uint32_t fingerprint) {
	char* p = bucket[index].bit_array;
	if (fingerprint_size == 4) {
		p += (pos >> 1);
		if ((pos & 1) == 0) {
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



double VacuumFilter::F_d_V(double x, double c) { return log(c) - log(x); }
double VacuumFilter::F_V(double x, double c) { return 1 + x * (log(c) - log(x) + 1) - c; }
double VacuumFilter::solve_equation_V(double c) {
	double x = c + 0.1;
	while (abs(F_V(x, c)) > 0.001) x -= F_V(x, c) / F_d_V(x, c);
	return x;
}
double VacuumFilter::balls_in_bins_max_load_V(double balls, double bins) {
	double m = balls;
	double n = bins;
	if (n == 1) return m;
	double ret = (m / n) + 1.5 * sqrt(2 * m / n * log(n));
	return ret;
}

int VacuumFilter::proper_alt_range_V(int M, int i, int* len) {
	double b = 4;      // slots per bucket
	double lf = 0.95;  // target load factor
	int alt_range = 8;
	for (; alt_range < M;) {
		double f = (4 - i) * 0.25;
		if (balls_in_bins_max_load_V(f * b * lf * M, M * 1.0 / alt_range) <
			0.97 * b * alt_range) {
			return alt_range;
		}
		alt_range <<= 1;
	}
	return alt_range;
}

bool VacuumFilter::queryItem(uint64_t item) {
	uint32_t index, alt_index;
	uint32_t fingerprint;
	generateIF(item, index, fingerprint, fingerprint_size);
	
	if (queryImpl(index, fingerprint)) {
		return true;
	}
	generateA(index, fingerprint, alt_index);
	if (queryImpl(alt_index, fingerprint)) {
		return true;
	}
	return false;
}


bool VacuumFilter::queryImpl(const uint32_t index, const uint32_t fingerprint) {
	for (size_t pos = 0; pos < 4; pos++) {
		if (read(index, pos) == fingerprint) {
			return true;
		}
	}
	return false;
}

bool VacuumFilter::deleteItem(uint64_t item) {
	uint32_t index, alt_index;
	uint32_t fingerprint;
	generateIF(item, index, fingerprint, fingerprint_size);
	
	if (deleteImpl(index, fingerprint)) {
		return true;
	}
	generateA(index, fingerprint, alt_index);
	if (deleteImpl(alt_index, fingerprint)) {
		return true;
	}

	return false;
}


bool VacuumFilter::deleteImpl(const uint32_t index, const uint32_t fingerprint) {
	for (size_t pos = 0; pos < 4; pos++) {
		if (read(index, pos) == fingerprint) {
			write(index, pos, 0);
			return true;
		}
	}
	return false;
}



