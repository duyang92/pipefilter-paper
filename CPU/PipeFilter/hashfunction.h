#pragma once
#ifndef HASHFUNCTION_H
#define HASHFUNCTION_H

#include<string>
#include<iostream>
using namespace std;

class HashFunc {
public:
	HashFunc();
	~HashFunc();
	// static std::string sha1(const char* key);
	// static std::uint32_t sha1_seed(const char* key, uint32_t seed);
	// static std::string md5(const char* key);
	static std::uint64_t MurmurHash2_x64(const char* key, int len, uint32_t seed);
	static uint32_t BobHash(const void* buf, size_t length, uint32_t seed = 0);
	static uint32_t BobHash(const std::string& s, uint32_t seed = 0);

	static uint32_t MurmurHash(const void* buf, size_t length, uint32_t seed = 0);
	static uint32_t MurmurHash(const std::string& s, uint32_t seed = 0);
};


#endif //HASHFUNCTION_H
