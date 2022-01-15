#ifndef SEHEDULER_H
#define SEHEDULER_H

#include <iostream>
#include <vector>
#include <fstream>
#include <unordered_map>
#include <string>
#include <sstream>
#include <queue>
#include <mpi.h>

class Scheduler {
public:
    Scheduler(int delay, int worker_num);
    ~Scheduler();
    void GetMapperTask(std::string locality_config_filename);
    void AssignMapperTask();
    void GetReducerTask(int num_reducer);
    void AssignReducerTask();
    void EndWorkerExcecute(int num);

private:
    std::vector<int> MapperTaskPool;
    std::queue<int> ReducerTaskPool;
    std::unordered_map<int, int> Locality;
    int execution_time;
    int delay;
    int worker_num;
    int num_reducer;
    int chunk_number;
};

#endif