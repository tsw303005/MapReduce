#include "Mapper.h"
#include "Reducer.h"
#include "Scheduler.h"
#include <mpi.h>
#include <omp.h>

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
    int num_reducer = std::stoi(argv[2]);
    int delay = std::stoi(argv[3]);
    int chunk_size = std::stoi(argv[5]);

    if (rank == 0) { // Scheduler
        Scheduler scheduler(delay, size-1);
        
    } else { // worker

    }

    return 0;
}