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
    Worker(unsigned int cpus, unsigned int mapper_num, unsigned int rank, unsigned int size);
    ~Worker();
    void ThreadPool(unsigned int task);
    unsigned int *available_num;
    pthread_mutex_t *lock;
    std::queue<int> *job;

private:
    unsigned int rank;
    unsigned int mapper_num;
    unsigned int node_num;
    pthread_t *threads;
};

#endif