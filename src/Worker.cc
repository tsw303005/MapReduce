#include "Worker.h"
#include "Mapper.h"
#include "Reducer.h"

Worker::Worker(int cpus, int mapper_num, int rank, int size, int chunk_size,
                int num_reducer, int delay, std::string source_file, std::string job_name, std::string output_dir) {
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
    this->output_dir = output_dir;

    this->lock = new pthread_mutex_t;
    this->available_num = new int;
    pthread_mutex_init(this->lock, NULL);
    pthread_mutex_init(this->send_lock, NULL);
}

Worker::~Worker() {
    pthread_mutex_destroy(this->lock);
    pthread_mutex_destroy(this->send_lock);
    std::cout << "[Info]: Worker "<< this->rank << " terminate\n";
}

void Worker::ThreadPool(int task) {
    bool task_finished = false;
    MPI_Status status;
    int signal = 1;
    int request[3];

    if (task == 1) { // mapper
        int chunk_index[2];
        Mapper mapper;

        this->job_mapper = new std::queue<Chunk>;
        *this->available_num = this->mapper_thread_number;
        mapper.available_num = this->available_num;
        mapper.lock = this->lock;
        mapper.send_lock = this->send_lock;
        mapper.job = this->job_mapper;
        mapper.chunk_size = this->chunk_size;
        mapper.source_file = this->source_file;
        mapper.num_reducer = this->num_reducer;
        mapper.delay = this->delay;
        mapper.rank = this->rank;
        mapper.worker_num = this->node_num - 1;
        mapper.scheduler_index = this->scheduler_index;


        // allocate mapper thread
        for (int i = 0; i < this->mapper_thread_number; i++) {
            pthread_create(&this->threads[i], NULL, &MapperFunction, &mapper);
        }

        while (!task_finished) {
            // check available thread number
            while ((*this->available_num) == 0);

            request[0] = 0;
            request[1] = this->rank;
            request[2] = 0;
            pthread_mutex_lock(this->send_lock);
            MPI_Send(request, 3, MPI_INT, this->scheduler_index, 0, MPI_COMM_WORLD);
            pthread_mutex_unlock(this->send_lock);
            MPI_Recv(chunk_index, 2, MPI_INT, this->scheduler_index, 0, MPI_COMM_WORLD, &status);

            pthread_mutex_lock(this->lock);
            this->job_mapper->push({chunk_index[0], chunk_index[1]});
            pthread_mutex_unlock(this->lock);
            if (chunk_index[0] == -1) { // mapper job done
                task_finished = true;
            }
        }

        // check all mapper thread return
        for (int i = 0; i < this->mapper_thread_number; i++) {
            pthread_join(this->threads[i], NULL);
        }

        // delete queue
        delete this->job_mapper;
    } else if (task == 2) { // reducer
        int reducer_index;
        Reducer reducer;

        this->job_reducer = new std::queue<int>;
        *this->available_num = this->reducer_thread_number;
        reducer.available_num = this->available_num;
        reducer.num_reducer = this->num_reducer;
        reducer.lock = this->lock;
        reducer.send_lock = this->send_lock;
        reducer.job = this->job_reducer;
        reducer.job_name = this->job_name;
        reducer.output_dir = this->output_dir;
        reducer.scheduler_index = this->scheduler_index;
        reducer.rank = this->rank;

        for (int i = 0; i < this->reducer_thread_number; i++) {
            pthread_create(&this->threads[i], NULL, &ReducerFunction, &reducer);
        }

        while (!task_finished) {
            // check available thread
            while (!(*this->available_num));

            request[0] = 0;
            request[1] = this->rank;
            request[2] = 0;
            pthread_mutex_lock(this->send_lock);
            MPI_Send(request, 3, MPI_INT, this->scheduler_index, 0, MPI_COMM_WORLD);
            pthread_mutex_unlock(this->send_lock);
            MPI_Recv(&reducer_index, 1, MPI_INT, this->scheduler_index, 0, MPI_COMM_WORLD, &status);

            pthread_mutex_lock(this->lock);
            this->job_reducer->push(reducer_index);
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
        delete this->job_reducer;
    }

    MPI_Barrier(MPI_COMM_WORLD);
}