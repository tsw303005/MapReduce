#include "Reducer.h"

void* ReducerFunction(void* input) {
    Reducer* reducer = (Reducer*)input;
    int task = -1;
    int request[2];
    bool flag = true;
    Count *word_count = new Count;
    Total *total = new Total;
    Collect *group = new Collect;

    while (flag) {
        pthread_mutex_lock(reducer->lock);
        if (!reducer->job->empty()) {
            task = reducer->job->front();
            if (task == -1) {
                flag = false;
            } else {
                (*reducer->available_num) -= 1;
                reducer->job->pop();
            }
        }
        pthread_mutex_unlock(reducer->lock);

        if (task != -1) {
            word_count->clear();
            total->clear();
            group->clear();

            // read file
            ReadFile(reducer->num_reducer, task, total);

            // sort words
            Sort(total);

            // Group
            Group(total, group);

            // reduce
            Reduce(group, word_count);

            // output
            Output(word_count, task, reducer->job_name, reducer->output_dir);

            request[0] = 1;
            request[1] = task;
            pthread_mutex_lock(reducer->send_lock);
            MPI_Send(request, 2, MPI_INT, reducer->scheduler_index, 0, MPI_COMM_WORLD);
            pthread_mutex_unlock(reducer->send_lock);

            // job terminate
            pthread_mutex_lock(reducer->lock);
            (*reducer->available_num) += 1;
            pthread_mutex_unlock(reducer->lock);

            task = -1;
        }
    }

    delete word_count;
    delete total;
    delete group;
    pthread_exit(NULL);
}

bool cmp(Item a, Item b) {
    return a.first < b.first;
}

void Sort(Total *total) {
    // sort according 
    sort(total->begin(), total->end(), cmp);
}

void Group(Total *total, Collect *group) {
    for (auto item : *total) {
        if (group->count(item.first) == 0) {
            std::vector<int> tmp;
            tmp.clear();
            (*group)[item.first] = tmp;
            (*group)[item.first].push_back(item.second);
        } else {
            (*group)[item.first].push_back(item.second);
        }
    }
}

void Reduce(Collect *group, Count *word_count) {
    for (auto item : *group) {
        (*word_count)[item.first] = 0;
        for (auto num : item.second) {
            (*word_count)[item.first] += num;
        }
    }
}

void ReadFile(int num_reducer, int task, Total *total) {
    std::string filename;
    std::string reducer_num_str = std::to_string(task);
    std::string word;
    int count;

    filename = "./intermediate_file/" + reducer_num_str + ".txt";

    std::ifstream input_file(filename);
    while (input_file >> word >> count) {
        total->push_back({word, count});
    }
    input_file.close();

    char *f;
    f = (char*)malloc(sizeof(char) * (filename.length() + 1));
    for (int i = 0; i < filename.length(); i++)
        f[i] = filename[i];
    
    f[filename.length()] = '\0';
    
    // delete intermediate file
    int result = std::remove(f);
    free(f);
}

void Output(Count *word_count, int task, std::string job_name, std::string output_dir) {
    std::string task_str = std::to_string(task);
    std::string filename = output_dir + job_name + "-" + task_str + ".out";
    std::ofstream myfile(filename);
    for (auto word : *word_count) {
        myfile << word.first << " " << word.second << "\n";
    }
    myfile.close();
}