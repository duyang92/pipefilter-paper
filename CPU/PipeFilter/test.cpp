
#include<bitset>
#include<iostream>
#include<vector>
#include<fstream>
#include<cmath>
#include<algorithm>
#include <random>
#include<set>

#include "PipeFilter.h"

#include <iomanip>
using namespace std;
mt19937 rd(12821);

#define capacity (1<<22)
#define fingerprint_size 16
#define insert_cap (1<<22)
const size_t delta = pow(2, 12);
uint64_t res[insert_cap];

uint32_t bm1 = static_cast<std::uint32_t>(0xFF00FF00);
uint32_t bm2 = static_cast<std::uint32_t>(0x00FF00FF);
void random_gen(int n, uint64_t* store, mt19937& rd) {
	uint64_t rand_range = pow(2, 64) / n;
	for (int i = 0; i < n; i++) {
		uint64_t rand = rand_range * i + rd() % rand_range;
		store[i] = rand;
	}
}

double calculateMean(const std::vector<double>& vec) {
	if (vec.empty()) {
		return 0.0;
	}

	double sum = 0.0;
	for (const double& value : vec) {
		sum += value;
	}

	return sum / static_cast<double>(vec.size());
}



template <typename T>
double test_insert(T& obj, double alpha) {
	int insert_num = int(insert_cap * alpha - delta / 2);
	for (auto k = 0; k < insert_num; k++) {
		if (obj.insertItem(res[k])) {

		}
	}
	double start = clock();
	for (auto k = 0; k < delta; k++) {
		if (obj.insertItem(res[insert_num + k])) {

		}
	}
	double end = clock();


	return (delta / ((end - start) / CLOCKS_PER_SEC) / 1000000.0);
}

template <typename T>
double test_lookup(T& obj, double alpha) {
	int insert_num = int(insert_cap * alpha - delta / 2);
	for (auto k = 0; k < insert_num; k++) {
		if (obj.insertItem(res[k])) {

		}
	}
	double start = clock();
	for (auto k = 0; k < delta; k++) {
		if (obj.queryItem(res[insert_num])) {

		}
	}
	double end = clock();


	return (delta / ((end - start) / CLOCKS_PER_SEC) / 1000000.0);
}

template <typename T>
double test_delete(T& obj, double alpha) {
	int insert_num = int(insert_cap * alpha - delta / 2);
	for (auto k = 0; k < insert_num; k++) {
		if (obj.insertItem(res[k])) {

		}
	}
	double start = clock();
	for (auto k = 0; k < delta; k++) {
		if (obj.deleteItem(res[insert_num])) {

		}
	}
	double end = clock();


	return (delta / ((end - start) / CLOCKS_PER_SEC) / 1000000.0);
}

int main() {


	mt19937 rd(12821);
	random_gen(insert_cap, res, rd);
	vector<double> alpha_list = { 0.1 };
	const int alpha_len = 18;
	vector<double>final_res;
	cout << "start: " << endl;
	for (auto i = 0; i < alpha_list.size(); i++) {
		//cout << alpha_list[i] << endl;
		const int circulate_num = 1;
		vector<double> cal_mean;
		for (auto j = 0; j < circulate_num; j++) {

			PipeFilter_SIMD_block filter = PipeFilter_SIMD_block(capacity / 4, fingerprint_size, capacity, 4);

			cal_mean.push_back(test_insert(filter, alpha_list[i]));
		}
		final_res.push_back(calculateMean(cal_mean));
	}

	for (auto i = 0; i < alpha_list.size(); i++) {
		cout << "The throughput with load factor= "
			<< fixed << left << setw(8) << setprecision(3)
			<< setw(8) << alpha_list[i]
			<< "is "
			<< setw(8) << final_res[i]
			<< "."
			<< endl;
	}
}