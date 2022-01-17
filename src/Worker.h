#ifndef WORKER_H
#define WORKER_H

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <queue>
#include <utility>
#include <cstdio>
#include <algorithm>
#include <map>
#include <mpi.h>
#include <pthread.h>

class Worker {
public:
    Worker(char **argv, int cpu_num, int rank, int size);
    ~Worker();
    void ThreadPoolMapper();
    void ThreadPoolReducer();
    void InputSplit();
    void Group();
    void Reduce();
    void Output();
    void Sort();
    void Map();
    void* MapperFunction(void* input);
    void* ReducerFunction(void* input);
    int Partition(); 

private:
    int available_num; // check availabl thread
    int mapper_thread_number;
    int reducer_thread_number; // to tell from num_reducer
    int scheduler_index;
    int num_reducer;
    int chunk_size;
    int node_num;
    int cpu_num;
    int delay;
    int rank;

    pthread_t *threads;
    pthread_mutex_t *lock;
    pthread_mutex_t *send_lock;

    std::string input_filename;
    std::string job_name;
    std::string output_dir;
    std::queue<Chunk> *job_mapper;
    std::queue<int> *job_reducer;
};

#endif