#include "Scheduler.h"

Scheduler::Scheduler(unsigned int delay, unsigned int worker_num) {
    this->delay = delay;
    this->worker_num = worker_num;
    this->execution_time = 0;
    this->MapperTaskPool.clear();
    this->ReducerTaskPool.clear();
}

void Scheduler::GetMapperTask(std::string locality_config_filename) {
    // read number of chunk and its locality
    
}