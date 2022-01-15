#ifndef WORKER_H
#define WORKER_H

#include <iostream>
#include <queue>
#include <mpi.h>
#include <pthread.h>
#include "Reducer.h"
#include "Mapper.h"

class Worker {
public:
    Worker(int cpus, int mapper_num, int rank, int size,
            int chunk_size, int num_reducer, int delay, std::string source_file, std::string job_name);
    ~Worker();
    void ThreadPool(int task);

private:
    int rank;
    int mapper_thread_number;
    int reducer_thread_number; // to tell from num_reducer
    int node_num;
    int scheduler_index;
    int chunk_size;
    int num_reducer;
    int delay;
    int *available_num; // check availabl thread

    pthread_t *threads;
    pthread_mutex_t *lock;

    std::string source_file;
    std::string job_name;
    std::queue<int> *job;
};

#endif