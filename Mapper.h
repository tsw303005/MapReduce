#ifndef MAPPER_H
#define MAPPER_H

#include <iostream>
#include <fstream>
#include <string>
#include <pthread.h>
#include <queue>

void* MapperFunction(void* input);
void InputSplit(int chunk);
void Map();
void Partition();

struct Mapper {
    pthread_mutex_t *lock;
    std::queue<int> *job;
    unsigned int *available_num;
};

#endif