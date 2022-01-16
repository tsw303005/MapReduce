#include "Scheduler.h"
#include "Reducer.h"

Scheduler::Scheduler(int delay, int worker_num, std::string job_name, char **input, int rank, int cpu) {
    std::string log_file = job_name + ".log";
    this->job_name = job_name;
    this->write_log = new std::ofstream(log_file);
    this->delay = delay;
    this->worker_num = worker_num;
    this->execution_time = 0;
    this->chunk_number = 0;
    this->num_reducer = std::stoi(input[2]);
    this->MapperTaskPool.clear();
    this->start_time = MPI_Wtime();
    while (!this->ReducerTaskPool.empty())
        this->ReducerTaskPool.pop();
    
    // log event
    *this->write_log << GetTime() << ", Start_Job, " << input[1] << ", ";
    *this->write_log << rank << ", " << cpu << ", " << input[2] << ", ";
    *this->write_log << input[3] << ", " << input[4] << ", " << input[5] << ", ";
    *this->write_log << input[6] << ", " << input[7] << "\n";
}

Scheduler::~Scheduler() {
    // end
    std::cout << "[Info]: Seheduler terminate\n";
    std::cout << "[Info]: Total cost time: " << this->execution_time << "\n";
    *this->write_log << GetTime() << ", Finish_Job" << ", " << MPI_Wtime() - this->start_time << "\n";
    this->write_log->close();
}

void Scheduler::Shuffle() {
    // read file and start to shuffle
    std::string filename;
    std::string chunk_str;
    std::string reducer_num_str;
    std::string word;
    int count;
    int intermediate_kv_num = 0;
    double st_time = MPI_Wtime();
    double e_time;
    Total records;

    *this->write_log << GetTime() << ", Start_Shuffle, ";

    
    for (int i = 1; i <= this->num_reducer; i++) {
        reducer_num_str = std::to_string(i);
        records.clear();
        for (int j = 1; j <= this->chunk_number; j++) {
            chunk_str = std::to_string(j);
            filename = "./intermediate_file/" + chunk_str + "_" + reducer_num_str + ".txt";

            std::ifstream input_file(filename);
            while (input_file >> word >> count) {
                records.push_back({word, count});
                intermediate_kv_num += 1;
            }
            input_file.close();

            char *f;
            f = new char[filename.length() + 1];
            for (int i = 0; i < filename.length(); i++)
                f[i] = filename[i];
            
            f[filename.length()] = '\0';
            
            // delete intermediate file
            int result = std::remove(f);
            free(f);
        }
        filename = "./intermediate_file/" + reducer_num_str + ".txt";
        std::ofstream intermediate_file(filename);
        for (auto record : records) {
            intermediate_file << record.first << " " << record.second << "\n";
        }
        intermediate_file.close();
    }
    e_time = MPI_Wtime() - st_time;
    *this->write_log << intermediate_kv_num << "\n";
    *this->write_log << GetTime() << ", Finish_Shuffle, " << e_time << "\n";
}

// get time stamp
time_t Scheduler::GetTime() {
    struct timeval time_now{};
    gettimeofday(&time_now, nullptr);
    time_t msecs_time = (time_now.tv_sec * 1000) + (time_now.tv_usec / 1000);

    return msecs_time;
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
void Scheduler::GetReducerTask() {
    for (int i = 1; i <= this->num_reducer; i++) {
        this->ReducerTaskPool.push(i);
    }
}

// Scheduler assign the mapper task
void Scheduler::AssignMapperTask() {
    MPI_Status status;
    int request[3]; // 0: request, 1: node index, 2: finished job index
    int task_num;
    int task;
    int termination_signal = -1;
    int check = 1;
    int finish_job_num = this->MapperTaskPool.size();

    this->RecordTime.clear(); // clear time record

    while (!this->MapperTaskPool.empty() || finish_job_num > 0) {
        // receive request
        MPI_Recv(&request, 3, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        if (request[0] == 0) { // request job
            // if no mapper job, send terminal signal
            if (this->MapperTaskPool.empty()) {
                MPI_Send(&termination_signal, 1, MPI_INT, request[1], 1, MPI_COMM_WORLD);
                continue;
            }
            // assign mapper task to the node and consider its locality
            for (int i = 0; i < this->MapperTaskPool.size(); i++) {
                // 起床記得處理 locality
                if (request[1] == this->Locality[this->MapperTaskPool[i]] % this->worker_num) {
                    task_num = i;
                    break;
                } else if (i == this->MapperTaskPool.size() - 1) task_num = 0, this->execution_time += this->delay;
            }
            task = this->MapperTaskPool[task_num];
            this->MapperTaskPool.erase(this->MapperTaskPool.begin() + task_num);

            // Send task to the worker
            MPI_Send(&task, 1, MPI_INT, request[1], 1, MPI_COMM_WORLD);

            // record log
            *this->write_log << GetTime() << ", Dispatch_MapTask, " << task << ", " << request[1] << "\n";
            // record time
            this->RecordTime[task] = MPI_Wtime();
        } else if (request[0] == 1) { // tell job done
            this->RecordTime[request[2]] = MPI_Wtime() - this->RecordTime[request[2]];
            // record log
            *this->write_log << GetTime() << ", Complete_MapTask, " << request[2] << ", " << this->RecordTime[request[2]] << "\n";
            finish_job_num -= 1;
        }
    }

    // end other worker
    this->EndWorkerExcecute(0);
}

void Scheduler::AssignReducerTask() {
    MPI_Status status;
    int task;
    int termination_signal = -1;
    int request[3]; // 0: request, 1: node index, 2: finished job index
    int finish_job_num = this->ReducerTaskPool.size();

    this->RecordTime.clear(); // clear time record

    while (!this->ReducerTaskPool.empty() || finish_job_num > 0) {
        // receive reducer thread from any node
        MPI_Recv(&request, 3, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        if (request[0] == 0) {
            if (this->ReducerTaskPool.empty()) {
                MPI_Send(&termination_signal, 1, MPI_INT, request[1], 1, MPI_COMM_WORLD);
                continue;
            }
            // assign reducer task
            task = this->ReducerTaskPool.front();
            this->ReducerTaskPool.pop();

            // Send task to the worker
            MPI_Send(&task, 1, MPI_INT, request[1], 1, MPI_COMM_WORLD);
            // record log
            *this->write_log << GetTime() << ", Dispatch_ReduceTask, " << task << ", " << request[1] << "\n";
            // record time
            this->RecordTime[task] = MPI_Wtime();
        } else if (request[0] == 1) {
            this->RecordTime[request[2]] = MPI_Wtime() - this->RecordTime[request[2]];
            // record log
            *this->write_log << GetTime() << ", Complete_ReduceTask, " << request[2] << ", " << this->RecordTime[request[2]] << "\n";
            finish_job_num -= 1;
        }
    }

    // end
    this->EndWorkerExcecute(1);
}

void Scheduler::EndWorkerExcecute(int num) {
    int worker_index;
    int signal = 1;

    for (int i = 0; i < this->worker_num; i++) {
        MPI_Send(&signal, 1, MPI_INT, i, 1, MPI_COMM_WORLD);
    }
    std::string job_name = (!num) ? "Mapper" : "Reducer";
    std::cout << "[Info]: " << job_name << " Task terminate successfully\n";
}