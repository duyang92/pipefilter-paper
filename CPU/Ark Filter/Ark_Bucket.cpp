#include "Ark_Bucket.h"

Ark_Bucket::Ark_Bucket() {
    for (auto i = 0; i < BUCKET_SIZE; i++) {
        bucket_ark[i].item = -1;
        bucket_ark[i].flag = 0;
    }
}

bool Ark_Bucket::Q_insert(uint32_t item) {
    for (auto i = 0; i < BUCKET_SIZE; i++) {
        if (bucket_ark[i].item == -1) {
            bucket_ark[i].item = item;
            bucket_ark[i].flag = 1;
            return true;
        }
    }
    return false;
}

bool Ark_Bucket::R_insert(uint32_t item) {
    for (auto i = 0; i < BUCKET_SIZE; i++) {
        if (bucket_ark[i].item == -1) {
            bucket_ark[i].item = item;
            bucket_ark[i].flag = 0;
            return true;
        }
    }
    return false;
}

bool Ark_Bucket::Q_delete(uint32_t item, bool flag) {
    for (auto i = 0; i < BUCKET_SIZE; i++) {
        if (bucket_ark[i].item == item && bucket_ark[i].flag == flag) {
            bucket_ark[i].item = -1;
            return true;
        }
    }
    return false;
}

bool Ark_Bucket::R_delete(uint32_t item, bool flag) {
    for (auto i = 0; i < BUCKET_SIZE; i++) {
        if (bucket_ark[i].item == item && bucket_ark[i].flag == flag) {
            bucket_ark[i].item = -1;
            return true;
        }
    }
    return false;
}

bool Ark_Bucket::Q_query(uint32_t item, bool flag) {
    for (auto i = 0; i < BUCKET_SIZE; i++) {
        if (bucket_ark[i].item == item && bucket_ark[i].flag == flag) {
            return true;
        }
    }
    return false;
}

bool Ark_Bucket::R_query(uint32_t item, bool flag) {
    for (auto i = 0; i < BUCKET_SIZE; i++) {
        if (bucket_ark[i].item == item && bucket_ark[i].flag == flag) {
            return true;
        }
    }
    return false;
}

bool Ark_Bucket::insert(uint32_t item, bool flag) {
    for (auto i = 0; i < BUCKET_SIZE; i++) {
        if (bucket_ark[i].item == -1) {
            bucket_ark[i].item = item;
            bucket_ark[i].flag = std::abs(flag - 1);
            return true;
        }
    }
    return false;
}

Ark_Slot Ark_Bucket::swap(uint32_t item, bool flag) {
    int j = rand() % 4;
    Ark_Slot evit_slot = bucket_ark[j];
    bucket_ark[j].item = item;
    bucket_ark[j].flag = flag ? 0 : 1;
    return evit_slot;
}
