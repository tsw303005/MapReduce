#include "Scheduler.h"
#include "Worker.h"
#include <mpi.h>

#define debug 1;
/*
use process rank - 1 as the jobtracker(scheduler)
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
    int num_reducer = std::stoi(argv[2]);
    int delay = std::stoi(argv[3]);
    int chunk_size = std::stoi(argv[5]);

    if (rank == size - 1) { // Scheduler
        Scheduler scheduler(delay, size - 1);
        scheduler.GetMapperTask(locality_config_filename);
        scheduler.AssignMapperTask();
        scheduler.GetReducerTask(num_reducer);
        scheduler.AssignReducerTask();
    } else { // worker
        cpu_set_t cpuset;
        sched_getaffinity(0, sizeof(cpuset), &cpuset);
        int ncpus = CPU_COUNT(&cpuset);

        Worker worker(ncpus, ncpus - 1, rank, size, chunk_size, num_reducer, input_filename, job_name);
        worker.ThreadPool(1); // mapper task
        worker.ThreadPool(2); // reducer task
    }

    return 0;
}