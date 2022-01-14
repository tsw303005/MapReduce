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
    Scheduler(unsigned int delay, unsigned int worker_num);
    void GetMapperTask(std::string locality_config_filename);
    void AssignMapperTask();
    void GetReducerTask();
    void AssignReducerTask();
    void EndWorkerExcecute();

private:
    std::vector<unsigned int> MapperTaskPool;
    std::vector<unsigned int> ReducerTaskPool;
    std::unordered_map<unsigned int, unsigned int> Locality;
    unsigned int execution_time;
    unsigned int delay;
    unsigned int worker_num;
};

#endif