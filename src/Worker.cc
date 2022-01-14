#include "Worker.h"
#include "Mapper.h"
#include "Reducer.h"

Worker::Worker(int cpus, int mapper_num, int rank, int size,
                int chunk_size, int num_reducer, std::string source_file) {
    this->source_file = source_file;
    this->threads = new pthread_t[cpus];
    this->mapper_num = mapper_num;
    this->rank = rank;
    this->node_num = size;
    this->scheduler_index = size - 1;
    this->chunk_size = chunk_size;
    this->num_reducer = num_reducer;

    this->lock = new pthread_mutex_t;
    this->job = new std::queue<int>;
    this->available_num = new int;
    pthread_mutex_init(this->lock, NULL);
}

Worker::~Worker() {
    delete this->threads;
    pthread_mutex_destroy(this->lock);
}

void Worker::ThreadPool(int task) {
    bool task_finished = false;
    MPI_Status status;
    int flag = 1;

    if (task == 1) { // mapper
        int chunk_index;
        Mapper mapper;

        *this->available_num = this->mapper_num;
        mapper.available_num = this->available_num;
        mapper.lock = this->lock;
        mapper.job = this->job;
        mapper.chunk_size = this->chunk_size;
        mapper.source_file = this->source_file;
        mapper.num_reducer = this->num_reducer;


        // allocate mapper thread
        for (int i = 1; i <= this->mapper_num; i++) {
            pthread_create(&this->threads[i], NULL, &MapperFunction, &mapper);
        }

        while (!task_finished) {
            // check available thread number
            while (!(*this->available_num));

            MPI_Send(&this->rank, 1, MPI_INT, this->scheduler_index, 1, MPI_COMM_WORLD);
            MPI_Recv(&chunk_index, 1, MPI_INT, this->scheduler_index, 1, MPI_COMM_WORLD, &status);

            pthread_mutex_lock(this->lock);
            this->job->push(chunk_index);
            pthread_mutex_unlock(this->lock);
            if (chunk_index == -1) { // mapper job done
                task_finished = true;
            }
        }

        // check all mapper thread return
        for (int i = 1; i <= this->mapper_num; i++) {
            pthread_join(this->threads[i], NULL);
        }

        // inform scheduler
        MPI_Send(&flag, 1, MPI_INT, this->scheduler_index, 1, MPI_COMM_WORLD);
    } else { // reducer

    }
}