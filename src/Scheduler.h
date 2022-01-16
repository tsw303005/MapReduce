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
#include <sys/time.h>
#include <ctime>

class Scheduler {
public:
    Scheduler(int delay, int worker_num, std::string job_name, char **input, int rank, int cpu);
    ~Scheduler();
    void GetMapperTask(std::string locality_config_filename);
    void AssignMapperTask();
    void GetReducerTask();
    void AssignReducerTask();
    void EndWorkerExcecute(int num);
    void Shuffle();
    time_t GetTime();

private:
    std::vector<int> MapperTaskPool;
    std::queue<int> ReducerTaskPool;
    std::unordered_map<int, int> Locality;
    std::unordered_map<int, double> RecordTime;
    std::string job_name;
    std::ofstream *write_log;
    int execution_time;
    int delay;
    int worker_num;
    int num_reducer;
    int chunk_number;
    double start_time;
};

#endif