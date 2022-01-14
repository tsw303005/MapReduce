#include "Worker.h"
#include "Mapper.h"
#include "Reducer.h"

Worker::Worker(unsigned int cpus, unsigned int mapper_num, unsigned int rank, unsigned int size) {
    this->threads = new pthread_t[cpus];
    this->mapper_num = mapper_num;
    this->rank = rank;
    this->node_num = size;

    this->lock = new pthread_mutex_t;
    this->job = new std::queue<int>;
    this->available_num = new unsigned int;
    pthread_mutex_init(this->lock, NULL);
}

Worker::~Worker() {
    delete this->threads;
    pthread_mutex_destroy(this->lock);
}

void Worker::ThreadPool(unsigned int task) {
    bool task_finished = false;
    MPI_Status status;

    if (task == 1) { // mapper
        int chunk_index;
        Mapper mapper;

        *this->available_num = this->mapper_num;
        mapper.available_num = this->available_num;
        mapper.lock = this->lock;
        mapper.job = this->job;

        // allocate mapper thread
        for (int i = 1; i <= this->mapper_num; i++) {
            pthread_create(&this->threads[i], NULL, &MapperFunction, &mapper);
        }

        while (!task_finished) {
            // check available thread number
            while (!(*this->available_num));

            MPI_Send(&this->rank, 1, MPI_UNSIGNED, this->node_num - 1, 1, MPI_COMM_WORLD);
            MPI_Recv(&chunk_index, 1, MPI_INT, this->node_num - 1, 1, MPI_COMM_WORLD, &status);

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
        MPI_Send(&task_finished, 1, MPI_C_BOOL, this->node_num - 1, 1, MPI_COMM_WORLD);
    } else { // reducer

    }
}