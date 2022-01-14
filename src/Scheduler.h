#ifndef SEHEDULER_H
#define SEHEDULER_H

#include <iostream>
#include <vector>
#include <fstream>
#include <unordered_map>
#include <string>
#include <sstream>
#include <mpi.h>

class Scheduler {
public:
    Scheduler(int delay, int worker_num);
    void GetMapperTask(std::string locality_config_filename);
    void AssignMapperTask();
    void GetReducerTask();
    void AssignReducerTask();
    void EndWorkerExcecute();

private:
    std::vector<int> MapperTaskPool;
    std::vector<int> ReducerTaskPool;
    std::unordered_map<int, int> Locality;
    int execution_time;
    int delay;
    int worker_num;
};

#endif