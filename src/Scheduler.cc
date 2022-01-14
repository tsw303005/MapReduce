#include "Scheduler.h"

Scheduler::Scheduler(int delay, int worker_num) {
    this->delay = delay;
    this->worker_num = worker_num;
    this->execution_time = 0;
    this->chunk_number = 0;
    this->MapperTaskPool.clear();
    while (!this->ReducerTaskPool.empty())
        this->ReducerTaskPool.pop();
}


// scheduer need to know the number of chunk and its locality
void Scheduler::GetMapperTask(std::string locality_config_filename) {
    // read number of chunk and its locality
    int chunk_index;
    int loc_num;
    std::ifstream input_file(locality_config_filename);

    while (input_file >> chunk_index >> loc_num) {
        this->MapperTaskPool.push_back(chunk_index);
        this->Locality[chunk_index] = loc_num;
        this->chunk_number += 1;
    }

    input_file.close();
}

// scheduler get the number of reducer task
void Scheduler::GetReducerTask(int num_reducer) {
    for (int i = 1; i <= num_reducer; i++) {
        this->ReducerTaskPool.push(i);
    }
    this->num_reducer = num_reducer;
}

// Scheduler assign the mapper task
void Scheduler::AssignMapperTask() {
    MPI_Status status;
    int worker_index;
    int task_num;
    int task;

    while (!this->MapperTaskPool.empty()) {
        // receive available mapper thread from any node
        MPI_Recv(&worker_index, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        // assign mapper task to the node and consider its locality
        for (int i = 0; i < this->MapperTaskPool.size(); i++) {
            // 起床記得處理 locality
            if (worker_index == this->Locality[this->MapperTaskPool[i]] % this->worker_num) {
                task_num = i;
                break;
            } else if (i == this->MapperTaskPool.size() - 1) task_num = 0, this->execution_time += this->delay;
        }
        task = this->MapperTaskPool[task_num];
        this->MapperTaskPool.erase(this->MapperTaskPool.begin() + task_num);

        // Send task to the worker
        MPI_Send(&task, 1, MPI_INT, worker_index, 1, MPI_COMM_WORLD);
    }

    // end other worker
    this->EndWorkerExcecute(0);
}

void Scheduler::AssignReducerTask() {
    MPI_Status status;
    int worker_index;
    int task;

    for (int i = 0; i < this->worker_num; i++) {
        MPI_Send(&this->chunk_number, 1, MPI_INT, i, 1, MPI_COMM_WORLD);
    }

    while (!this->ReducerTaskPool.empty()) {
        // receive reducer thread from any node
        MPI_Recv(&worker_index, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        // assign reducer task
        task = this->ReducerTaskPool.front();
        this->ReducerTaskPool.pop();

        // Send task to the worker
        MPI_Send(&task, 1, MPI_INT, worker_index, 1, MPI_COMM_WORLD);
    }

    // end
    this->EndWorkerExcecute(1);
}

void Scheduler::EndWorkerExcecute(int num) {
    int worker_index;
    int signal;
    MPI_Status status;

    // tell other node there is no mapper function
    for (int i = 0; i < this->worker_num; i++) {
        int termination_signal = -1;
        MPI_Recv(&worker_index, 1, MPI_INT, i, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        MPI_Send(&termination_signal, 1, MPI_INT, i, 1, MPI_COMM_WORLD);
    }

    for (int i = 0; i < this->worker_num; i++) {
        MPI_Recv(&signal, 1, MPI_INT, i, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    }
    std::string job_name = (!num) ? "Mapper" : "Reducer";
    std::cout << "[Info]: " << job_name << " Task terminate seccessfully\n";
}