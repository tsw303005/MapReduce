#include "Worker.h"
#include "Mapper.h"
#include "Reducer.h"

Worker::Worker(char **argv, int cpu_num, int rank, int size) {
    this->job_name = std::string(argv[1]);
    this->num_reducer = std::stoi(argv[2]);
    this->delay = std::stoi(argv[3]);
    this->input_filename = std::string(argv[4]);
    this->chunk_size = std::stoi(argv[5]);
    this->output_dir = std::string(argv[7]);
    this->rank = rank;
    this->size = size;
    this->cpu_num = cpu_num;
    this->node_num = rank;
    this->available_num = 0;
    this->scheduler_index = size - 1;
    this->mapper_thread_number = cpu_num - 1;
    this->reducer_thread_number = 1;
    this->threads = new pthread_t[cpu_num];
    this->lock = new pthread_mutex_t;
    this->send_lock = new pthread_mutex_t;
    pthread_mutex_init(this->lock, NULL);
    pthread_mutex_init(this->send_lock, NULL);
}

Worker::~Worker() {
    pthread_mutex_destroy(this->lock);
    pthread_mutex_destroy(this->send_lock);
    std::cout << "[Info]: Worker "<< this->rank << " terminate\n";
}

void Worker::ThreadPoolMapper() {
    bool task_finished = false;
    MPI_Status status;
    int signal = 1;
    int request[2];
    int chunk_index[2];
    Mapper mapper;

    this->job_mapper = new std::queue<Chunk>;

    // allocate mapper thread
    for (int i = 0; i < this->mapper_thread_number; i++) {
        pthread_create(&this->threads[i], NULL, &MapperFunction, (void*)this);
    }

    while (!task_finished) {
        // check available thread number
        while ((*this->available_num) == 0);

        request[0] = 0;
        request[1] = 0;
        pthread_mutex_lock(this->send_lock);
        MPI_Send(request, 2, MPI_INT, this->scheduler_index, 0, MPI_COMM_WORLD);
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
    MPI_Barrier(MPI_COMM_WORLD);
}

void Worker::ThreadPoolReducer() {
    bool task_finished = false;
    MPI_Status status;
    int signal = 1;
    int request[2];
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
        request[1] = 0;
        pthread_mutex_lock(this->send_lock);
        MPI_Send(request, 2, MPI_INT, this->scheduler_index, 0, MPI_COMM_WORLD);
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
    MPI_Barrier(MPI_COMM_WORLD);
}