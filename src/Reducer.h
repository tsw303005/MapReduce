#ifndef REDUCER_H
#define REDUCER_H

#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <vector>
#include <pthread.h>
#include <queue>
#include <utility>
#include <algorithm>
#include <cstdio>
#include <map>
#include "Mapper.h"

typedef std::pair<std::string, int> Item;
typedef std::vector<Item> Total;
typedef std::map<std::string, std::vector<int> > Collect;

bool cmp(Item a, Item b);

void* ReducerFunction(void* input);
void ReadFile(int num_reducer, int task, Total *total);
void Sort(Total *total);
void Group(Total *total, Collect *group);
void Reduce(Collect *group, Count *word_count);
void Output(Count *word_count, int task, std::string job_name, std::string output_dir);

struct Reducer {
    pthread_mutex_t *lock;
    std::string job_name;
    std::string output_dir;
    std::queue<int> *job;

    int task;
    int *available_num;
    int num_reducer;
    int scheduler_index;
    int rank;
};

#endif