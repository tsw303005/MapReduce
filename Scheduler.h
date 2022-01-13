#include <iostream>
#include <vector>
#include <mpi.h>
#include <unordered_map>

class Scheduler {
public:
    Scheduler(unsigned int delay, unsigned int worker_num);
    void GetMapperTask();
    void AssignMapperTask();
    void GetReducerTask();
    void AssignReducerTask();

private:
    std::vector<int> MapperTaskPool;
    std::vector<int> ReducerTaskPool;
    std::unordered_map<int, int> Locality;
    unsigned int execution_time;
    unsigned int delay;
    unsigned int worker_num;
};