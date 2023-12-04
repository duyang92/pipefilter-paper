#include "ArkFilter.h"
mt19937 g_Ark(12821);
ArkFilter::ArkFilter(size_t capacity) {
	this->capacity = capacity;
	this->counter = 0;
	this->single_table_length = capacity / 4;
	buckets = new Ark_Bucket[single_table_length];
	random_seed = g_Ark();
}

ArkFilter::~ArkFilter() {
	delete[] buckets;
}

uint64_t ArkFilter::generateFp(uint64_t item) {

	uint64_t hash = HashFunc::MurmurHash2_x64((char*)&item, sizeof(item), random_seed);

	uint64_t fingerprint = hash;

	fingerprint = fingerprint % (single_table_length * (single_table_length - 1));

	return fingerprint;
}


uint32_t ArkFilter::getRIndex(uint64_t fingerprint) {
	uint32_t R_index = fingerprint % single_table_length;
	return R_index;
}

uint32_t ArkFilter::getQIndex(uint64_t fingerprint) {
	uint32_t Q_index = fingerprint / single_table_length;
	return Q_index;
}

int ArkFilter::insertItem(uint64_t item) {
	
	uint64_t fingerprint = generateFp(item);

	uint32_t R_index = getRIndex(fingerprint);
	uint32_t Q_index = getQIndex(fingerprint);
	//cout << R_index << ' ' << Q_index << endl;
	if (buckets[R_index].R_insert(Q_index)) {
		return true;
	}
	if (buckets[Q_index].Q_insert(R_index)) {
		return true;
	}
	uint32_t indexs[2] = {R_index ,Q_index};
	int j = rand() % 2;
	uint32_t evit_index = indexs[j];
	j = rand() % 4;
	uint32_t old_item = buckets[evit_index].bucket_ark[j].item;
	bool old_flag = buckets[evit_index].bucket_ark[j].flag;

	if (evit_index == R_index) {
		buckets[evit_index].bucket_ark[j].item = Q_index;
		buckets[evit_index].bucket_ark[j].flag = 0;
	}
	if (evit_index == Q_index) {
		buckets[evit_index].bucket_ark[j].item = R_index;
		buckets[evit_index].bucket_ark[j].flag = 1;
	}

	for (auto i = 0; i < MaxNumKicks_AF; i++) {
		
		if (old_flag == 1) {
			if (buckets[old_item].R_insert(evit_index))
				return true;
		}
		else {
			if (buckets[old_item].Q_insert(evit_index))
				return true;
		}


		Ark_Slot evit_slot = buckets[old_item].swap(evit_index, old_flag);
		evit_index = old_item;
		old_item = evit_slot.item;
		old_flag = evit_slot.flag;

	}
	return false;

}




int ArkFilter::queryItem(uint64_t item) {
	uint64_t fingerprint = generateFp(item);

	uint32_t R_index = getRIndex(fingerprint);
	uint32_t Q_index = getQIndex(fingerprint);
	//cout << R_index << ' ' << Q_index << endl;
	if (buckets[R_index].R_query(Q_index, 0)) {
		return true;
	}
	if (buckets[Q_index].Q_query(R_index, 1)) {
		return true;
	}
	return false;
}


int ArkFilter::deleteItem(uint64_t item) {
	uint64_t fingerprint = generateFp(item);

	uint32_t R_index = getRIndex(fingerprint);
	uint32_t Q_index = getQIndex(fingerprint);

	if (buckets[R_index].R_delete(Q_index, 0)) {
		return true;
	}
	if (buckets[Q_index].Q_delete(R_index, 1)) {
		return true;
	}
	return false;
}