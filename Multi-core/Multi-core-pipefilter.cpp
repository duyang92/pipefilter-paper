#include <stdint.h>
#include<cmath>
#include<random>
#include "hashfunction.h"

#include <immintrin.h>
#include <emmintrin.h>
#include <smmintrin.h>
#include <chrono>
#include <pthread.h>
#include <string.h>
#include <stdio.h>
#include<unistd.h>
using namespace std;

#define SUB_FILTER_NUM 4 //子过滤器的数量
#define PER_FILTER_THREAD_NUM 4 //每个子过滤器对应的线程数量
//#define THREAD_NUM SUB_FILTER_NUM*PER_FILTER_THREAD_NUM //总线程数量
#define BATCH_SIZE (1<<10)//batch大小
#define BUFFER_SIZE (1<<10)//缓冲区大小

const int THREAD_NUM = SUB_FILTER_NUM*PER_FILTER_THREAD_NUM;//总线程数量

const size_t table_size = (1 << 21) / SUB_FILTER_NUM;
const size_t insert_num = (1 << 23);
const size_t fp_size = 16;

const size_t bytes_per_bucket = (fp_size * 4 + 7) >> 3;
const size_t slots = 4;
const size_t stages = 4;
const uint32_t mask = (1 << fp_size) - 1;
const size_t single_table_length = table_size / stages;
uint8_t paras[3];
uint32_t paras_reverse[3];
uint32_t random_seed = 1234;

uint64_t keys[insert_num];
typedef struct {
    uint32_t bit_array[slots];
} Bucket_SIMD_PF;


typedef struct {
    Bucket_SIMD_PF** bucket;
}PipeFilter;

struct failTrans {
    uint32_t fingerprint;
    uint32_t index;
    uint16_t TTL;
};


mt19937 g_PF(12821);

void random_gen(int n, uint64_t* store, std::mt19937& rd) {
    uint64_t rand_range = pow(2, 64) / n;
    for (int i = 0; i < n; i++) {
        uint64_t rand = rand_range * i + rd() % rand_range;
        store[i] = rand;
    }
}
bool is_prime_PF(int n) {
    if (n <= 1)
        return false;
    for (auto i = 2; i<int(sqrt(n)) + 1; i++) {
        if (n % i == 0)
            return false;
    }
    return true;
}

int ext_gcd_PF(int a, int b, int& x, int& y) {
    if (b == 0) {
        x = 1;
        y = 0;
        return a;
    }
    else {
        int gcd = ext_gcd_PF(b, a % b, y, x);
        y -= (a / b) * x;
        return gcd;
    }
}

// 计算 a 在模 m 意义下的乘法逆元

int gcd_PF(int a, int b) {
    if (b == 0) {
        return a;
    }
    else {
        return gcd_PF(b, a % b);
    }
}

void get_a_b_c_d_PF(int capacity) {
    int flag = 0;
    for (int num = 2; num < capacity; ++num) {
        if (is_prime_PF(num) && gcd_PF(num, capacity) == 1) {
            paras[flag] = num;
            flag++;
        }
        if (flag == 3) {
            break;
        }
    }

}

int mod_inv(int a, int m) {
    int x, y;
    int gcd = ext_gcd_PF(a, m, x, y);
    if (gcd != 1) {
        // a 在模 m 意义下没有乘法逆元
        return -1;
    }
    else {
        return (x % m + m) % m;
    }
}


void get_reverse() {
    paras_reverse[0] = mod_inv(paras[0], single_table_length);
    paras_reverse[1] = mod_inv(paras[1], single_table_length);
    paras_reverse[2] = mod_inv(paras[2], single_table_length);
}


void init(PipeFilter& pf, size_t l) {

    pf.bucket = new Bucket_SIMD_PF * [stages];
    for (unsigned int i = 0; i < stages; i++) {
        pf.bucket[i] = new Bucket_SIMD_PF[l];
        for (unsigned int j = 0; j < l; j++) {
            for (unsigned int k = 0; k < slots; k++) {
                pf.bucket[i][j].bit_array[k]=0;
            }
        }
    }
}


uint32_t getIF(uint64_t item, uint32_t& index, uint32_t& fingerprint) {
    uint64_t hv = HashFunc::MurmurHash2_x64((char*)&item, sizeof(uint64_t), random_seed);

    index = ((uint32_t)(hv >> 32)) % (single_table_length);//hash(x)

    fingerprint = (uint32_t)(hv & 0xFFFFFFFF);//保留低32位
    fingerprint &= mask;//只保留低fingerprint_size位
    fingerprint += (fingerprint == 0);//指纹为0则加1
    return fingerprint ^ random_seed;
}

void getNextIndex(const uint32_t index, uint32_t& next_index, const size_t stage_k, uint32_t hash_fp) {

    switch (stage_k) {
    case(0):
        next_index = (index * paras[0] - hash_fp) % single_table_length;
        break;
    case(1):
        next_index = ((index + hash_fp) * paras[1] - hash_fp) % single_table_length;
        break;
    case(2):
        next_index = ((index + hash_fp) * paras[2] - hash_fp) % single_table_length;
        break;
    case(3):
        next_index = ((index + hash_fp) * paras_reverse[2] * paras_reverse[1]
            * paras_reverse[0]) % single_table_length;
        break;

    }
}


failTrans insertItem(uint64_t item, PipeFilter& pf) {
    uint32_t index, next_index;
    uint32_t fingerprint;
    uint32_t fingerprint_old;
    uint32_t hash_fp = getIF(item, index, fingerprint);
    bool res;
    uint32_t expected_val = 0;
    res=false;
    for (auto i = 0; i < stages; i++) {
        for (auto j = 0; j < 4; j++) {
            if(pf.bucket[i][index].bit_array[j] == 0){
                // pf.bucket[i][index].bit_array[j] = fingerprint;
                // res = true;
                res = __sync_bool_compare_and_swap(&(pf.bucket[i][index].bit_array[j]), expected_val, fingerprint);
            }
            
            if (res) {
                return { 0,0,0 };
            }
            //expected_val = 0;
        }
        if (i < 3) {
            getNextIndex(index, next_index, i, hash_fp);
            index = next_index;
        }
    }
    int kick = fingerprint & 0x3;
    // fingerprint_old = pf.bucket[stages - 1][index].bit_array[kick];
    // pf.bucket[stages - 1][index].bit_array[kick]=fingerprint;
    fingerprint_old=__sync_lock_test_and_set(&(pf.bucket[stages - 1][index].bit_array[kick]),fingerprint); 
    fingerprint = fingerprint_old;
    hash_fp = fingerprint ^ random_seed;
    getNextIndex(index, next_index, stages - 1, hash_fp);
    index = next_index;
    return { fingerprint,index,1 };
}

// failTrans insertItem(uint64_t item, PipeFilter& pf) {
//     uint32_t index, next_index;
//     uint32_t fingerprint;
//     uint32_t fingerprint_old;
//     uint32_t hash_fp = getIF(item, index, fingerprint);
//     bool res;
//     uint32_t expected_val = 0;
//     for (auto i = 0; i < stages; i++) {
//         for (auto j = 0; j < 4; j++) {
//             res = __sync_bool_compare_and_swap(&(pf.bucket[i][index].bit_array[j]), expected_val, fingerprint);
//             if (res) {
//                 return { 0,0,0 };
//             }
//             //expected_val = 0;
//         }
//         if (i < 3) {
//             getNextIndex(index, next_index, i, hash_fp);
//             index = next_index;
//         }
//     }
//     int kick = fingerprint & 0x3;
//     fingerprint_old=__sync_lock_test_and_set(&(pf.bucket[stages - 1][index].bit_array[kick]),fingerprint); 
//     //fingerprint_old = pf.bucket[stages - 1][index].bit_array[kick];
//     //pf.bucket[stages - 1][index].bit_array[kick]=fingerprint;
//     fingerprint = fingerprint_old;
//     hash_fp = fingerprint ^ random_seed;
//     getNextIndex(index, next_index, stages - 1, hash_fp);
//     index = next_index;
//     return { fingerprint,index,1 };
// }


failTrans insertKickItem(failTrans item, PipeFilter& pf) {
    uint32_t index = item.index;
    uint32_t next_index;
    uint32_t fingerprint = item.fingerprint;
    uint32_t fingerprint_old;
    uint32_t hash_fp = fingerprint ^ random_seed;
    bool res;
    uint32_t expected_val = 0;
    for (auto i = 0; i < stages; i++) {

        for (auto j = 0; j < 4; j++) {
            if(pf.bucket[i][index].bit_array[j] == 0){
                // pf.bucket[i][index].bit_array[j] = fingerprint;
                // res = true;
                res = __sync_bool_compare_and_swap(&(pf.bucket[i][index].bit_array[j]), expected_val, fingerprint);
            }
            if (res) {
                return { 0,0,0 };
            }
            //expected_val = 0;
        }
        if (i < 3) {
            getNextIndex(index, next_index, i, hash_fp);
            index = next_index;
        }
    }

    int kick = fingerprint & 0x3;
    // fingerprint_old = pf.bucket[stages - 1][index].bit_array[kick];
    // pf.bucket[stages - 1][index].bit_array[kick]=fingerprint;
    fingerprint_old=__sync_lock_test_and_set(&(pf.bucket[stages - 1][index].bit_array[kick]),fingerprint); 
    fingerprint = fingerprint_old;
    hash_fp = fingerprint ^ random_seed;
    getNextIndex(index, next_index, stages - 1, hash_fp);
    uint16_t TTL = item.TTL + 1;
    return { fingerprint,index,TTL };
}

failTrans queryItem(uint64_t item, PipeFilter& pf) {
    uint32_t index, next_index;
    uint32_t fingerprint;
    uint32_t hash_fp = getIF(item, index, fingerprint);
    bool res;
    for (auto i = 0; i < stages; i++) {
        for (auto j = 0; j < 4; j++) {
            if(pf.bucket[i][index].bit_array[j] == fingerprint) {
                return { 0,0,0 };
            }
        }
        getNextIndex(index, next_index, i, hash_fp);
        index = next_index;
    }
    return { fingerprint,index,1 };
}

failTrans queryKickItem(failTrans item, PipeFilter& pf) {
    uint32_t index = item.index;
    uint32_t next_index;
    uint32_t fingerprint = item.fingerprint;
    uint32_t hash_fp = fingerprint ^ random_seed;
    bool res;
    for (auto i = 0; i < stages; i++) {
        for (auto j = 0; j < 4; j++) {
            if(pf.bucket[i][index].bit_array[j] == fingerprint) {
                return { 0,0,0 };
            }
        }
        getNextIndex(index, next_index, i, hash_fp);
        index = next_index;
    }
    uint16_t TTL = item.TTL + 1;
    return { fingerprint,index,TTL };
}


failTrans deleteItem(uint64_t item, PipeFilter& pf) {
    uint32_t index, next_index;
    uint32_t fingerprint;
    uint32_t hash_fp = getIF(item, index, fingerprint);
    int count_k = 0;
    bool res;
    uint32_t fingerprint_temp = fingerprint;
    uint32_t expected_val = 0;
    for (auto i = 0; i < stages; i++) {
        for (auto j = 0; j < 4; j++) {
            if(pf.bucket[i][index].bit_array[j] == fingerprint){
                // pf.bucket[i][index].bit_array[j] = fingerprint;
                // res = true;
                res = __sync_bool_compare_and_swap(&(pf.bucket[i][index].bit_array[j]), fingerprint, expected_val);
            }
            if (res) {
                return { 0,0,0 };
            }
            //fingerprint = fingerprint_temp;
        }
        if (i < 3) {
            getNextIndex(index, next_index, i, hash_fp);
            index = next_index;
        }
    }
    return { fingerprint,index,1 };
}

failTrans deleteKickItem(failTrans item, PipeFilter& pf) {
    uint32_t index = item.index;
    uint32_t next_index;
    uint32_t fingerprint = item.fingerprint;
    uint32_t hash_fp = fingerprint ^ random_seed;
    uint32_t expected_val = 0;
    uint32_t fingerprint_temp = fingerprint;
    bool res;

    for (auto i = 0; i < stages; i++) {
        for (auto j = 0; j < 4; j++) {
            if(pf.bucket[i][index].bit_array[j] == fingerprint){
                // pf.bucket[i][index].bit_array[j] = fingerprint;
                // res = true;
                res = __sync_bool_compare_and_swap(&(pf.bucket[i][index].bit_array[j]), fingerprint, expected_val);
            }
            if (res) {
                return { 0,0,0 };
            }
            //fingerprint = fingerprint_temp;
        }
        if (i < 3) {
            getNextIndex(index, next_index, i, hash_fp);
            index = next_index;
        }
    }
    uint16_t TTL = item.TTL + 1;
    return { fingerprint,index,TTL };
}



// 线程之间传递的消息块
typedef struct
{
    failTrans block_message[BATCH_SIZE];
}Message;

PipeFilter pf[SUB_FILTER_NUM];

size_t buffer_result[4][THREAD_NUM];
double times[4];

// 线程之间的存储传递消息的缓冲区
Message buffer[THREAD_NUM][BUFFER_SIZE];
// 缓冲区头尾指针
volatile int thread_front[THREAD_NUM];
volatile int thread_rear[THREAD_NUM];

void* Insert_thread(void* args)
{
    Message message;// 存储插入失败后返回的元素，当累积到一定数量传递给下一个线程
    int message_length = 0;// 插入失败元素数量

    failTrans ans;
    int count = 0;
    int id = *((int*)args);
    uint64_t cache[BATCH_SIZE];
    failTrans cache2[BATCH_SIZE];// 缓冲区，从线程传输的失败元素队列中取数据
    int cache_index = 0;
    // 当前是第几批
    int batch_index = id;//每次加THREAD_NUM

    while (1)
    {
        //printf("%d::%d--%d, %d\n",id,thread_front[id],thread_rear[id],batch_index * BATCH_SIZE);
         if(id!=(id%THREAD_NUM)){ 
             printf("%d mod %d = %d,,,,,%d\n",id,THREAD_NUM,id%THREAD_NUM,6%8);
             //cout<<id<<' % '<<<<id%THREAD_NUM<<endl;
         }
        if (thread_front[id] != thread_rear[id])
        {
            size_t curr_batch = BATCH_SIZE;
            memcpy(cache2, buffer[id][thread_front[id]].block_message, curr_batch * sizeof(failTrans));
            thread_front[id] = (thread_front[id] + 1) % BUFFER_SIZE;

            for (cache_index = 0; cache_index < curr_batch; cache_index++)
            {
                ans = insertKickItem(cache2[cache_index], pf[id/PER_FILTER_THREAD_NUM]);
                if (ans.TTL == 0) {
                    count++;
                }
                else
                {
                    if(ans.TTL != 4*SUB_FILTER_NUM)
                    {
                        message.block_message[message_length] = ans;
                        message_length++;
                    }

                    if (message_length == BATCH_SIZE)
                    {
                        memcpy(buffer[(id + PER_FILTER_THREAD_NUM) % THREAD_NUM][thread_rear[(id + 1) % THREAD_NUM]].block_message, message.block_message, sizeof(Message));
                        thread_rear[(id + 1) % THREAD_NUM] = (thread_rear[(id + 1) % THREAD_NUM] + 1) % BUFFER_SIZE;
                        message_length = 0;
                    }
                }
            }
        }
        else if (batch_index * BATCH_SIZE < insert_num)
        {
            
            size_t curr_batch = min(insert_num - batch_index * BATCH_SIZE, (size_t)BATCH_SIZE);
            memcpy((void*)cache, (void*)(keys + batch_index * BATCH_SIZE), curr_batch * sizeof(uint64_t));
            for (size_t j = 0; j < curr_batch; j++)
            {
                //printf("%d::%ld\n",id,cache[j]);
                ans = insertItem(cache[j], pf[id/PER_FILTER_THREAD_NUM]);

                if (ans.TTL == 0)
                {
                    count++;
                }
                else
                {
                    if(ans.TTL != 4*SUB_FILTER_NUM)
                    {
                        message.block_message[message_length] = ans;
                        message_length++;
                    }

                    if (message_length == BATCH_SIZE)
                    {
                        memcpy(buffer[(id + PER_FILTER_THREAD_NUM) % THREAD_NUM][thread_rear[(id + 1) % THREAD_NUM]].block_message,
                            message.block_message, sizeof(Message));
                        thread_rear[(id + 1) % THREAD_NUM] = (thread_rear[(id + 1) % THREAD_NUM] + 1) % BUFFER_SIZE;
                        message_length = 0;
                    }
                }
            }
            batch_index += THREAD_NUM;
        }
        else
        {
            break;
        }
    }
    buffer_result[0][id] = count;
    return 0;
}

void* Query_thread(void* args)
{
    Message message;// 存储插入失败后返回的元素，当累积到一定数量传递给下一个线程
    int message_length = 0;// 插入失败元素数量

    failTrans ans;
    int count = 0;
    int id = *((int*)args);
    uint64_t cache[BATCH_SIZE];
    failTrans cache2[BATCH_SIZE];// 缓冲区，从线程传输的失败元素队列中取数据
    int cache_index = 0;
    // 当前是第几批
    int batch_index = id;//每次加THREAD_NUM
    
    // if(id>=THREAD_NUM){
    //     cout<<2222<<endl;
    // }
    while (1)
    {
        //cout<<1111<<id<<endl;
//         if(id!=(id%THREAD_NUM)){
            
//             cout<<id<<'\t'<<(id%THREAD_NUM)<<THREAD_NUM<<endl;
//         }
        //printf("%d::%d--%d, %d\n",id,thread_front[id],thread_rear[id],batch_index * BATCH_SIZE);
        if (thread_front[id] != thread_rear[id])
        {
            size_t curr_batch = BATCH_SIZE;
            memcpy(cache2, buffer[id][thread_front[id]].block_message, curr_batch * sizeof(failTrans));
            thread_front[id] = (thread_front[id] + 1) % BUFFER_SIZE;

            for (cache_index = 0; cache_index < curr_batch; cache_index++)
            {
                ans = queryKickItem(cache2[cache_index], pf[id/PER_FILTER_THREAD_NUM]);
                if (ans.TTL == 0) {
                    count++;
                }
                else
                {
                    if(ans.TTL != SUB_FILTER_NUM)
                    {
                        message.block_message[message_length] = ans;
                        message_length++;
                    }

                    if (message_length == BATCH_SIZE)
                    {
                        memcpy(buffer[(id + PER_FILTER_THREAD_NUM) % THREAD_NUM][thread_rear[(id + 1) % THREAD_NUM]].block_message, message.block_message, sizeof(Message));
                        thread_rear[(id + 1) % THREAD_NUM] = (thread_rear[(id + 1) % THREAD_NUM] + 1) % BUFFER_SIZE;
                        message_length = 0;
                    }
                }
            }
        }
        else if (batch_index * BATCH_SIZE < insert_num)
        {
            
            size_t curr_batch = min(insert_num - batch_index * BATCH_SIZE, (size_t)BATCH_SIZE);
            memcpy((void*)cache, (void*)(keys + batch_index * BATCH_SIZE), curr_batch * sizeof(uint64_t));
            for (size_t j = 0; j < curr_batch; j++)
            {
                //printf("%d::%ld\n",id,cache[j]);
                ans = queryItem(cache[j], pf[id/PER_FILTER_THREAD_NUM]);

                if (ans.TTL == 0)
                {
                    count++;
                }
                else
                {
                    if(ans.TTL != SUB_FILTER_NUM)
                    {
                        message.block_message[message_length] = ans;
                        message_length++;
                    }

                    if (message_length == BATCH_SIZE)
                    {
                        memcpy(buffer[(id + PER_FILTER_THREAD_NUM) % THREAD_NUM][thread_rear[(id + 1) % THREAD_NUM]].block_message,
                            message.block_message, sizeof(Message));
                        thread_rear[(id + 1) % THREAD_NUM] = (thread_rear[(id + 1) % THREAD_NUM] + 1) % BUFFER_SIZE;
                        message_length = 0;
                    }
                }
            }
            batch_index += THREAD_NUM;
        }
        else
        {
            break;
        }
    }
    buffer_result[1][id] = count;
    return 0;
}


void* Delete_thread(void* args)
{
    Message message;// 存储插入失败后返回的元素，当累积到一定数量传递给下一个线程
    int message_length = 0;// 插入失败元素数量

    failTrans ans;
    int count = 0;
    int id = *((int*)args);
    uint64_t cache[BATCH_SIZE];
    failTrans cache2[BATCH_SIZE];// 缓冲区，从线程传输的失败元素队列中取数据
    int cache_index = 0;
    // 当前是第几批
    int batch_index = id;//每次加THREAD_NUM

    while (1)
    {
        if (thread_front[id] != thread_rear[id])
        {
            size_t curr_batch = BATCH_SIZE;
            memcpy(cache2, buffer[id][thread_front[id]].block_message, curr_batch * sizeof(failTrans));
            thread_front[id] = (thread_front[id] + 1) % BUFFER_SIZE;

            for (cache_index = 0; cache_index < curr_batch; cache_index++)
            {
                ans = deleteKickItem(cache2[cache_index], pf[id/PER_FILTER_THREAD_NUM]);
                if (ans.TTL == 0) {
                    count++;
                }
                else
                {
                    if(ans.TTL != SUB_FILTER_NUM)
                    {
                        message.block_message[message_length] = ans;
                        message_length++;
                    }

                    if (message_length == BATCH_SIZE)
                    {
                        memcpy(buffer[(id + PER_FILTER_THREAD_NUM) % THREAD_NUM][thread_rear[(id + 1) % THREAD_NUM]].block_message, message.block_message, sizeof(Message));
                        thread_rear[(id + 1) % THREAD_NUM] = (thread_rear[(id + 1) % THREAD_NUM] + 1) % BUFFER_SIZE;
                        message_length = 0;
                    }
                }
            }
        }
        else if (batch_index * BATCH_SIZE < insert_num)
        {
            size_t curr_batch = min(insert_num - batch_index * BATCH_SIZE, (size_t)BATCH_SIZE);
            memcpy((void*)cache, (void*)(keys + batch_index * BATCH_SIZE), curr_batch * sizeof(uint64_t));
            for (size_t j = 0; j < curr_batch; j++)
            {
                ans = deleteItem(cache[j], pf[id/PER_FILTER_THREAD_NUM]);

                if (ans.TTL == 0)
                {
                    count++;
                }
                else
                {
                    if(ans.TTL != SUB_FILTER_NUM)
                    {
                        message.block_message[message_length] = ans;
                        message_length++;
                    }

                    if (message_length == BATCH_SIZE)
                    {
                        memcpy(buffer[(id + PER_FILTER_THREAD_NUM) % THREAD_NUM][thread_rear[(id + 1) % THREAD_NUM]].block_message,
                            message.block_message, sizeof(Message));
                        thread_rear[(id + 1) % THREAD_NUM] = (thread_rear[(id + 1) % THREAD_NUM] + 1) % BUFFER_SIZE;
                        message_length = 0;
                    }
                }
            }
            batch_index += THREAD_NUM;
        }
        else
        {
            break;
        }
    }
    buffer_result[2][id] = count;
    return 0;
}

int main() {
    int buffer_no[THREAD_NUM];

    for(int i=0;i<SUB_FILTER_NUM;i++)
    {
        init(pf[i], single_table_length);
    }

    for (int i = 0; i < THREAD_NUM; i++)
    {
        buffer_result[0][i] = 0;
        buffer_result[1][i] = 0;
        buffer_result[2][i] = 0;
        buffer_no[i] = i;
        thread_front[i] = 0;
        thread_rear[i] = 0;
    }
    get_a_b_c_d_PF(single_table_length);
    get_reverse();
    random_gen(insert_num, keys, g_PF);

    // 创建线程
    pthread_t threads[THREAD_NUM];
    
    


    // 启动计时器
    auto startTime = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < THREAD_NUM; i++)
    {
        pthread_create(&threads[i], NULL, Insert_thread, &buffer_no[i]);
    }

    for (int i = 0; i < THREAD_NUM; i++)
    {
        pthread_join(threads[i], NULL);
    }
    // 停止计时器
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
    times[0] = duration.count();
    //cout << 0 << "-->"<< 1.0 * times[0] / 1000000 << endl;
    

    
    
    
    
    
    // 查询操作
    // 开始计时   
    for (int i = 0; i < THREAD_NUM; i++)
    {
        buffer_no[i] = i;
        thread_front[i] = 0;
        thread_rear[i] = 0;
    }
    startTime = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < THREAD_NUM; i++)
    {
        pthread_create(&threads[i], NULL, Query_thread, &buffer_no[i]);
    }

    for (int i = 0; i < THREAD_NUM; i++)
    {
        pthread_join(threads[i], NULL);
    }
    // 停止计时器
    endTime = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
    times[1] = duration.count();
    //cout << 1 << "-->"<< 1.0 * times[1] / 1000000 << endl;


    // 删除操作
    // 开始计时
    for (int i = 0; i < THREAD_NUM; i++)
    {
        buffer_no[i] = i;
        thread_front[i] = 0;
        thread_rear[i] = 0;
    }
    startTime = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < THREAD_NUM; i++)
    {
        pthread_create(&threads[i], NULL, Delete_thread, &buffer_no[i]);
    }

    for (int i = 0; i < THREAD_NUM; i++)
    {
        pthread_join(threads[i], NULL);
    }
    // 停止计时器
    endTime = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
    times[2] = duration.count();
    //cout << 2 << "-->"<< 1.0 * times[2] / 1000000 << endl;
    
    cout << "当前线程数量：" << THREAD_NUM << endl;
    string operates[3] = { "插入","查询","删除" };
    for (int i = 0; i < 3; i++)
    {
        int sum = 0;
        for (int j = 0; j < THREAD_NUM; j++)
        {
            sum += buffer_result[i][j];
            //if(i==0){
                //cout<<buffer_result[i][j]<<endl;
            //}
        }

        cout << operates[i] << "用时：" << 1.0 * times[i] / 1000000 << "秒" << endl;
        // 计算总吞吐量
        double throughput = 1.0 * insert_num / times[i];
        // 输出结果
        cout << operates[i] << "吞吐量：" << throughput << " M/s" << std::endl;

        if (i == 0){
            cout << operates[i] << "成功元素数量：" << sum << ", 空间利用率：" << sum / (table_size * 4 * SUB_FILTER_NUM * 1.0) << endl << endl;
            
        }
        else
            cout << operates[i] << "成功元素数量：" << sum << endl << endl;
    }
    return 0;
}