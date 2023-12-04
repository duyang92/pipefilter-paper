#include <stdint.h>
#include<cmath>
#include<random>
#define Max_Kick_FPGA 500
const uint8_t slots = 4;
const uint32_t table_size = (1 << 20) / slots;
const uint8_t fp_size = 16;

const uint8_t bytes_per_bucket = (fp_size * slots + 7) >> 3;

const uint8_t stages = 2;
const uint32_t mask = (1 << fp_size) - 1;
const uint32_t single_table_length = table_size / stages;
uint32_t random_seed = 1234567;
uint32_t hash_fp;

uint8_t paras[1];

uint32_t bucket[stages][single_table_length][slots];



bool is_prime_FPGA(int n) {
	if (n <= 1)
		return false;
	for (auto i = 2; i<int(sqrt(n)) + 1; i++) {
		if (n % i == 0)
			return false;
	}
	return true;
}

int ext_gcd_FPGA(int a, int b, int& x, int& y) {
	if (b == 0) {
		x = 1;
		y = 0;
		return a;
	}
	else {
		int gcd = ext_gcd_FPGA(b, a % b, y, x);
		y -= (a / b) * x;
		return gcd;
	}
}


int gcd_FPGA(int a, int b) {
	if (b == 0) {
		return a;
	}
	else {
		return gcd_FPGA(b, a % b);
	}
}

void get_a_b_c_FPGA(uint32_t capacity) {
	int flag = 0;
	for (int num = 2; num < capacity; ++num) {
		if (is_prime_FPGA(num) && gcd_FPGA(num, capacity) == 1) {
			paras[flag] = num;
			flag++;
		}
		if (flag == 3) {
			break;
		}
	}
}

void init() {
	for (unsigned int i = 0; i < stages; i++) {
		for (unsigned int j = 0; j < single_table_length; j++) {
			for (unsigned int k = 0; k < slots; k++) {
				bucket[i][j][k] = 0;
			}
		}
	}
}

void getIF(uint64_t item, uint32_t& index, uint32_t& fingerprint) {
	uint64_t hv = item * 0x5bd1e995;

	index = ((uint32_t)(hv >> 32)) & (single_table_length - 1);//hash(x)

	fingerprint = (uint32_t)(hv & 0xFFFFFFFF);//
	fingerprint &= mask;//
	fingerprint += (fingerprint == 0);//
	hash_fp = fingerprint ^ random_seed;
}

void generateNextIndex(uint32_t index, uint32_t& next_index, uint8_t stage) {
	switch (stage) {
	case(0):
		next_index = (index * paras[0] - hash_fp) % single_table_length;
		break;
	case(1):
		next_index = 0;
		break;

	}
}

bool insertImpl(uint32_t index, uint32_t fingerprint, uint8_t stage) {
	for (auto pos = 0; pos < slots; pos++) {
		if (bucket[stage][index][pos] == 0) {
			bucket[stage][index][pos] = fingerprint;
			return true;
		}
	}
	return false;
}


bool insertItem(uint64_t item) {
	uint32_t index, next_index;//
	uint32_t fingerprint, fingerprint_old;
	getIF(item, index, fingerprint);

	for (auto i = 0; i < stages; i++) {

		if (insertImpl(index, fingerprint, i)) {
			return true;
		}
		generateNextIndex(index, next_index, i);
		index = next_index;
	}
	return false;

}


bool queryImpl(uint32_t index, uint32_t fingerprint,int stage) {
	for (int pos = 0; pos < 4; pos++) {
		if (bucket[stage][index][pos] == fingerprint) {
			return true;
		}
	}
	return false;
}


bool queryItem(uint64_t item) {
	uint32_t index, next_index;
	uint32_t fingerprint;
	getIF(item, index, fingerprint);
	for (int i = 0; i < stages; i++) {
		if (queryImpl(index, fingerprint, i)) {

			return true;
		}
		generateNextIndex(index, next_index, i);
		index = next_index;
	}
	return false;
}

bool deleteImpl(uint32_t index, uint32_t fingerprint, int stage) {
	for (int pos = 0; pos < 4; pos++) {
		if (bucket[stage][index][pos] == fingerprint) {
			bucket[stage][index][pos] = 0;
			return true;
		}
	}
	return false;
}


bool deleteItem(uint64_t item) {
	uint32_t index, next_index;
	uint32_t fingerprint;
	getIF(item, index, fingerprint);
	for (int i = 0; i < stages; i++) {
		if (deleteImpl(index, fingerprint, i)) {

			return true;
		}
		generateNextIndex(index, next_index, i);
		index = next_index;
	}
	return false;
}








