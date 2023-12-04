#include "hashfunction.h"
#define rot(x,k) (((x)<<(k)) | ((x)>>(32-(k))))
#define mix(a,b,c)                              \
    {                                           \
        a -= c;  a ^= rot(c, 4);  c += b;       \
        b -= a;  b ^= rot(a, 6);  a += c;       \
        c -= b;  c ^= rot(b, 8);  b += a;       \
        a -= c;  a ^= rot(c,16);  c += b;       \
        b -= a;  b ^= rot(a,19);  a += c;       \
        c -= b;  c ^= rot(b, 4);  b += a;       \
    }
#define final(a,b,c)                            \
    {                                           \
        c ^= b; c -= rot(b,14);                 \
        a ^= c; a -= rot(c,11);                 \
        b ^= a; b -= rot(a,25);                 \
        c ^= b; c -= rot(b,16);                 \
        a ^= c; a -= rot(c,4);                  \
        b ^= a; b -= rot(a,14);                 \
        c ^= b; c -= rot(b,24);                 \
    }
// Assuming little endian
#define HASH_LITTLE_ENDIAN 1
// string  HashFunc::sha1(const char* key) {
// 	EVP_MD_CTX *mdctx;
// 	mdctx = EVP_MD_CTX_new();
// 	unsigned char md_value[EVP_MAX_MD_SIZE];
// 	unsigned int md_len;

// 	EVP_DigestInit(mdctx, EVP_sha1());
// 	EVP_DigestUpdate(mdctx, (const void*)key, sizeof(key));
// 	EVP_DigestFinal_ex(mdctx, md_value, &md_len);
// 	EVP_MD_CTX_reset(mdctx);
// 	EVP_MD_CTX_free(mdctx);

// 	return std::string((char*)md_value, (size_t)md_len);
// }

// uint32_t HashFunc::sha1_seed(const char* key, uint32_t seed) {
//     string s = sha1(key);
//     uint32_t res = *((uint32_t*)(s.c_str()));
//     return res ^ seed;
// }

// string HashFunc::md5(const char* key) {
// 	EVP_MD_CTX* mdctx;
// 	mdctx = EVP_MD_CTX_new();
// 	unsigned char md_value[EVP_MAX_MD_SIZE];
// 	unsigned int md_len;

// 	EVP_DigestInit(mdctx, EVP_md5());
// 	EVP_DigestUpdate(mdctx, (const void*)key, sizeof(key));
// 	EVP_DigestFinal_ex(mdctx, md_value, &md_len);
// 	EVP_MD_CTX_reset(mdctx);
// 	EVP_MD_CTX_free(mdctx);

// 	return std::string((char*)md_value, (size_t)md_len);
// }

uint64_t HashFunc::MurmurHash2_x64(const char* key, int len, uint32_t seed) {
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;

    uint64_t h = seed ^ (len * m);

    const uint64_t* data = (const uint64_t*)key;
    const uint64_t* end = data + (len / 8);

    while (data != end)
    {
        uint64_t k = *data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const uint8_t* data2 = (const uint8_t*)data;

    switch (len & 7)
    {
    case 7: h ^= ((uint64_t)data2[6]) << 48;
    case 6: h ^= ((uint64_t)data2[5]) << 40;
    case 5: h ^= ((uint64_t)data2[4]) << 32;
    case 4: h ^= ((uint64_t)data2[3]) << 24;
    case 3: h ^= ((uint64_t)data2[2]) << 16;
    case 2: h ^= ((uint64_t)data2[1]) << 8;
    case 1: h ^= ((uint64_t)data2[0]);
        h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

uint32_t HashFunc::BobHash(const std::string& s, uint32_t seed) {
    return BobHash(s.data(), s.length(), seed);
}

uint32_t HashFunc::BobHash(const void* buf, size_t length, uint32_t seed)
{
    uint32_t a, b, c;                                          /* internal state */
    union { const void* ptr; size_t i; } u;     /* needed for Mac Powerbook G4 */

    /* Set up the internal state */
    // Is it safe to use key as the initial state setter?
    a = b = c = 0xdeadbeef + ((uint32_t)length) + seed;

    u.ptr = buf;
    if (HASH_LITTLE_ENDIAN && ((u.i & 0x3) == 0)) {
        const uint32_t* k = (const uint32_t*)buf;         /* read 32-bit chunks */


        /*------ all but last block: aligned reads and affect 32 bits of (a,b,c) */
        while (length > 12)
        {
            a += k[0];
            b += k[1];
            c += k[2];
            mix(a, b, c);
            length -= 12;
            k += 3;
        }

        /*----------------------------- handle the last (probably partial) block */
        /*
         * "k[2]&0xffffff" actually reads beyond the end of the string, but
         * then masks off the part it's not allowed to read.  Because the
         * string is aligned, the masked-off tail is in the same word as the
         * rest of the string.  Every machine with memory protection I've seen
         * does it on word boundaries, so is OK with this.  But VALGRIND will
         * still catch it and complain.  The masking trick does make the hash
         * noticably faster for short strings (like English words).
         */
#ifndef VALGRIND

        switch (length)
        {
        case 12: c += k[2]; b += k[1]; a += k[0]; break;
        case 11: c += k[2] & 0xffffff; b += k[1]; a += k[0]; break;
        case 10: c += k[2] & 0xffff; b += k[1]; a += k[0]; break;
        case 9: c += k[2] & 0xff; b += k[1]; a += k[0]; break;
        case 8: b += k[1]; a += k[0]; break;
        case 7: b += k[1] & 0xffffff; a += k[0]; break;
        case 6: b += k[1] & 0xffff; a += k[0]; break;
        case 5: b += k[1] & 0xff; a += k[0]; break;
        case 4: a += k[0]; break;
        case 3: a += k[0] & 0xffffff; break;
        case 2: a += k[0] & 0xffff; break;
        case 1: a += k[0] & 0xff; break;
        case 0: return c;              /* zero length strings require no mixing */
        }

#else /* make valgrind happy */

        const u_int8_t* k8;
        k8 = (const u_int8_t*)k;
        switch (length)
        {
        case 12: c += k[2]; b += k[1]; a += k[0]; break;
        case 11: c += ((uint32_t)k8[10]) << 16;  /* fall through */
        case 10: c += ((uint32_t)k8[9]) << 8;    /* fall through */
        case 9: c += k8[8];                   /* fall through */
        case 8: b += k[1]; a += k[0]; break;
        case 7: b += ((uint32_t)k8[6]) << 16;   /* fall through */
        case 6: b += ((uint32_t)k8[5]) << 8;    /* fall through */
        case 5: b += k8[4];                   /* fall through */
        case 4: a += k[0]; break;
        case 3: a += ((uint32_t)k8[2]) << 16;   /* fall through */
        case 2: a += ((uint32_t)k8[1]) << 8;    /* fall through */
        case 1: a += k8[0]; break;
        case 0: return c;
        }

#endif /* !valgrind */

    }
    else if (HASH_LITTLE_ENDIAN && ((u.i & 0x1) == 0)) {
        const uint16_t* k = (const uint16_t*)buf;         /* read 16-bit chunks */
        const uint8_t* k8;

        /*--------------- all but last block: aligned reads and different mixing */
        while (length > 12)
        {
            a += k[0] + (((uint32_t)k[1]) << 16);
            b += k[2] + (((uint32_t)k[3]) << 16);
            c += k[4] + (((uint32_t)k[5]) << 16);
            mix(a, b, c);
            length -= 12;
            k += 6;
        }

        /*----------------------------- handle the last (probably partial) block */
        k8 = (const uint8_t*)k;
        switch (length)
        {
        case 12: c += k[4] + (((uint32_t)k[5]) << 16);
            b += k[2] + (((uint32_t)k[3]) << 16);
            a += k[0] + (((uint32_t)k[1]) << 16);
            break;
        case 11: c += ((uint32_t)k8[10]) << 16;     /* fall through */
        case 10: c += k[4];
            b += k[2] + (((uint32_t)k[3]) << 16);
            a += k[0] + (((uint32_t)k[1]) << 16);
            break;
        case 9: c += k8[8];                      /* fall through */
        case 8: b += k[2] + (((uint32_t)k[3]) << 16);
            a += k[0] + (((uint32_t)k[1]) << 16);
            break;
        case 7: b += ((uint32_t)k8[6]) << 16;      /* fall through */
        case 6: b += k[2];
            a += k[0] + (((uint32_t)k[1]) << 16);
            break;
        case 5: b += k8[4];                      /* fall through */
        case 4: a += k[0] + (((uint32_t)k[1]) << 16);
            break;
        case 3: a += ((uint32_t)k8[2]) << 16;      /* fall through */
        case 2: a += k[0];
            break;
        case 1: a += k8[0];
            break;
        case 0: return c;                     /* zero length requires no mixing */
        }

    }
    else {                        /* need to read the key one byte at a time */
        const uint8_t* k = (const uint8_t*)buf;

        /*--------------- all but the last block: affect some 32 bits of (a,b,c) */
        while (length > 12)
        {
            a += k[0];
            a += ((uint32_t)k[1]) << 8;
            a += ((uint32_t)k[2]) << 16;
            a += ((uint32_t)k[3]) << 24;
            b += k[4];
            b += ((uint32_t)k[5]) << 8;
            b += ((uint32_t)k[6]) << 16;
            b += ((uint32_t)k[7]) << 24;
            c += k[8];
            c += ((uint32_t)k[9]) << 8;
            c += ((uint32_t)k[10]) << 16;
            c += ((uint32_t)k[11]) << 24;
            mix(a, b, c);
            length -= 12;
            k += 12;
        }

        /*-------------------------------- last block: affect all 32 bits of (c) */
        switch (length)                   /* all the case statements fall through */
        {
        case 12: c += ((uint32_t)k[11]) << 24;
        case 11: c += ((uint32_t)k[10]) << 16;
        case 10: c += ((uint32_t)k[9]) << 8;
        case 9: c += k[8];
        case 8: b += ((uint32_t)k[7]) << 24;
        case 7: b += ((uint32_t)k[6]) << 16;
        case 6: b += ((uint32_t)k[5]) << 8;
        case 5: b += k[4];
        case 4: a += ((uint32_t)k[3]) << 24;
        case 3: a += ((uint32_t)k[2]) << 16;
        case 2: a += ((uint32_t)k[1]) << 8;
        case 1: a += k[0];
            break;
        case 0: return c;
        }
    }

    final(a, b, c);
    return c;
}


uint32_t HashFunc::MurmurHash(const void* buf, size_t len, uint32_t seed)
{
    // 'm' and 'r' are mixing constants generated offline.
    // They're not really 'magic', they just happen to work well.

    const unsigned int m = 0x5bd1e995;
    const int r = 24;

    // Initialize the hash to a 'random' value
    uint32_t h = seed ^ len;

    // Mix 4 bytes at a time into the hash
    const unsigned char* data = (const unsigned char*)buf;

    while (len >= 4) {
        unsigned int k = *(unsigned int*)data;

        k *= m;
        k ^= k >> r;
        k *= m;

        h *= m;
        h ^= k;

        data += 4;
        len -= 4;
    }

    // Handle the last few bytes of the input array
    switch (len) {
    case 3: h ^= data[2] << 16;
    case 2: h ^= data[1] << 8;
    case 1: h ^= data[0];
        h *= m;
    };

    // Do a few final mixes of the hash to ensure the last few
    // bytes are well-incorporated.
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    return h;
}

uint32_t HashFunc::MurmurHash(const std::string& s, uint32_t seed)
{
    return MurmurHash(s.data(), s.length(), seed);
}
