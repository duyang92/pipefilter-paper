#pragma once
#ifndef ARKBUCKET_H_
#define ARKBUCKET_H_
#include <cmath>
#include<string.h>
#include<stdlib.h>
#include"hashfunction.h"
#include<random>
#define BUCKET_SIZE 4
typedef struct {
	size_t item;
	bool flag;
}Ark_Slot;

class Ark_Bucket {
public:
	Ark_Bucket();
	Ark_Slot bucket_ark[BUCKET_SIZE];
	bool Q_insert(uint32_t item);

	bool R_insert(uint32_t item);

	bool Q_delete(uint32_t item, bool flag);

	bool R_delete(uint32_t item, bool flag);

	bool Q_query(uint32_t item, bool flag);

	bool R_query(uint32_t item, bool flag);

	bool insert(uint32_t item, bool flag);

	Ark_Slot swap(uint32_t item, bool flag);
};

#endif