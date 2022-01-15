#include "Worker.h"
#include "Mapper.h"
#include "Reducer.h"

Worker::Worker(int cpus, int mapper_num, int rank, int size,
                int chunk_size, int num_reducer, int delay, std::string source_file, std::string job_name) {
    this->source_file = source_file;
    this->job_name = job_name;
    this->threads = new pthread_t[cpus];
    this->mapper_thread_number = mapper_num;
    this->reducer_thread_number = cpus - mapper_num;
    this->rank = rank;
    this->node_num = size;
    this->scheduler_index = size - 1;
    this->chunk_size = chunk_size;
    this->num_reducer = num_reducer;
    this->delay = delay;

    this->lock = new pthread_mutex_t;
    this->available_num = new int;
    pthread_mutex_init(this->lock, NULL);
}

Worker::~Worker() {
    delete this->threads;
    pthread_mutex_destroy(this->lock);
    std::cout << "[Info]: Worker "<< this->rank << " terminate\n";
}

void Worker::ThreadPool(int task) {
    bool task_finished = false;
    MPI_Status status;
    int flag = 1;

    this->job = new std::queue<int>;

    if (task == 1) { // mapper
        int chunk_index;
        Mapper mapper;

        *this->available_num = this->mapper_thread_number;
        mapper.available_num = this->available_num;
        mapper.lock = this->lock;
        mapper.job = this->job;
        mapper.chunk_size = this->chunk_size;
        mapper.source_file = this->source_file;
        mapper.num_reducer = this->num_reducer;
        mapper.delay = this->delay;
        mapper.rank = this->rank;
        mapper.worker_num = this->node_num - 1;


        // allocate mapper thread
        for (int i = 0; i < this->mapper_thread_number; i++) {
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
        for (int i = 0; i < this->mapper_thread_number; i++) {
            pthread_join(this->threads[i], NULL);
        }

        // delete queue
        delete this->job;

        // inform scheduler
        MPI_Send(&flag, 1, MPI_INT, this->scheduler_index, 1, MPI_COMM_WORLD);
    } else if (task == 2) { // reducer
        int reducer_index;
        int chunk_number;
        Reducer reducer;

        MPI_Recv(&chunk_number, 1, MPI_INT, this->scheduler_index, 1, MPI_COMM_WORLD, &status);

        *this->available_num = this->reducer_thread_number;
        reducer.available_num = this->available_num;
        reducer.num_reducer = this->num_reducer;
        reducer.lock = this->lock;
        reducer.job = this->job;
        reducer.job_name = this->job_name;
        reducer.chunk_number = chunk_number;

        for (int i = 0; i < this->reducer_thread_number; i++) {
            pthread_create(&this->threads[i], NULL, &ReducerFunction, &reducer);
        }

        while (!task_finished) {
            // check available thread
            while (!(*this->available_num));

            MPI_Send(&this->rank, 1, MPI_INT, this->scheduler_index, 1, MPI_COMM_WORLD);
            MPI_Recv(&reducer_index, 1, MPI_INT, this->scheduler_index, 1, MPI_COMM_WORLD, &status);

            pthread_mutex_lock(this->lock);
            this->job->push(reducer_index);
            pthread_mutex_unlock(this->lock);
            if (reducer_index == -1) { // reducer job done
                task_finished = true;
            }
        }

        // check all mapper thread return
        for (int i = 0; i < this->reducer_thread_number; i++) {
            pthread_join(this->threads[i], NULL);
        }

        // delete queue
        delete this->job;

        // inform scheduler
        MPI_Send(&flag, 1, MPI_INT, this->scheduler_index, 1, MPI_COMM_WORLD);
    }
}