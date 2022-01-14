#include "Scheduler.h"
#include "Worker.h"
#include <mpi.h>

#define debug 1;
/*
use process 0 as the jobtracker(scheduler)
use other process as the tasktracker(worker)


*/

int main(int argc, char **argv) {
    // initialize MPI
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // process parameter
    std::string job_name = std::string(argv[1]);
    std::string input_filename = std::string(argv[4]);
    std::string locality_config_filename = std::string(argv[6]);
    std::string output_dir = std::string(argv[7]);
    unsigned int num_reducer = std::stoi(argv[2]);
    unsigned int delay = std::stoi(argv[3]);
    unsigned int chunk_size = std::stoi(argv[5]);

    if (rank == size-1) { // Scheduler
        Scheduler scheduler(delay, (unsigned)size - 1);
        scheduler.GetMapperTask(locality_config_filename);
        scheduler.AssignMapperTask();
    } else { // worker
        cpu_set_t cpuset;
        sched_getaffinity(0, sizeof(cpuset), &cpuset);
        unsigned int ncpus = CPU_COUNT(&cpuset);

        Worker worker(ncpus, ncpus - 1, (unsigned int)rank, (unsigned int)size);
        worker.ThreadPool(1); // mapper task

    }

    return 0;
}