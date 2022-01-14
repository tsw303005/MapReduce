#ifndef MAPPER_H
#define MAPPER_H

#include <iostream>
#include <fstream>
#include <string>
#include <pthread.h>
#include <queue>
#include <unordered_map>

typedef std::unordered_map<std::string, int> Count;
typedef std::vector<std::string> Word;

void* MapperFunction(void* input);
void InputSplit(int chunk, int chunk_size, std::string source_file, Count *word_count, Word *words);
void Map(std::string line, Count *word_count, Word *words);
int Partition(int num_reducer, int chunk);

struct Mapper {
    pthread_mutex_t *lock;
    std::queue<int> *job;
    std::string source_file;
    int *available_num;
    int chunk_size;
    int num_reducer;
};

#endif