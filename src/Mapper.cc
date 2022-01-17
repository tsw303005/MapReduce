#include "Mapper.h"

void* MapperFunction(void* input) {
    Worker *mapper = (Worker*)(input);
    Count *word_count = new Count;
    Word *words = new Word;
    Chunk chunk;
    int request[2];
    bool flag = true;

    chunk.first = -1;
    chunk.second = 0;
    while (flag) {
        pthread_mutex_lock(mapper->lock);
        if (!mapper->job_mapper->empty()) {
            chunk.first = mapper->job_mapper->front().first;
            if (chunk.first == -1) {
                flag = false;
            } else {
                (*mapper->available_num) -= 1;
                mapper->job_mapper->pop();
            }
        }
        pthread_mutex_unlock(mapper->lock);

        if (chunk.first != -1) {
            if (chunk.second != mapper->rank && WAIT) { // not locality file
                //std::cout << "[Info]: One thread sleep\n";
                sleep(mapper->delay);
            }
            word_count->clear();
            words->clear();
            
            // split chunk
            InputSplit(chunk.first, mapper->chunk_size, mapper->source_file, word_count, words);

            // get word partition
            std::vector<std::vector<std::string>> split_result(mapper->num_reducer+1);
            for (auto word : *words) {
                split_result[Partition(mapper->num_reducer, word)].push_back(word);
            }
            // generate intermediate file
            for (int i = 1; i <= mapper->num_reducer; i++) {
                std::string chunk_str = std::to_string(chunk.first);
                std::string reducer_num_str = std::to_string(i);
                std::string filename = "./intermediate_file/" + chunk_str + "_" + reducer_num_str + ".txt";
                std::ofstream myfile(filename);
                for (auto word : split_result[i]) {
                    myfile << word << " " << (*word_count)[word] << "\n";
                }
                myfile.close();
            }

            request[0] = 1;
            request[1] = chunk.first;
            pthread_mutex_lock(mapper->send_lock);
            MPI_Send(request, 2, MPI_INT, mapper->scheduler_index, 0, MPI_COMM_WORLD);
            pthread_mutex_unlock(mapper->send_lock);

            // job terminate
            pthread_mutex_lock(mapper->lock);
            (*mapper->available_num) += 1;
            pthread_mutex_unlock(mapper->lock);
            
            chunk.first = -1;
        }
    }

    delete word_count;
    delete words;
    pthread_exit(NULL);
}

void InputSplit(int chunk, int chunk_size, std::string source_file, Count *word_count, Word *words) {
    int start_pos = 1 + (chunk - 1) * chunk_size;
    // read chunk file
    std::ifstream input_file(source_file);
    std::string line;

    // find the chunk position
    for (int i = 1; i < start_pos; i++) {
        getline(input_file, line);
    }

    for (int i = 1; i <= chunk_size; i++) {
        getline(input_file, line);
        // call Map function
        Map(line, word_count, words);
    }
    input_file.close();
}

void Map(std::string line, Count *word_count, Word *words) {
    int pos = 0;
    std::string word;
    std::vector<std::string> tmp_words;
    
    while ((pos = line.find(" ")) != std::string::npos) {
        word = line.substr(0, pos);
        tmp_words.push_back(word);
        line.erase(0, pos + 1);
    }

    if (!line.empty())
        tmp_words.push_back(line);

    for (auto w : tmp_words) {
        if (word_count->count(w) == 0) {
            words->push_back(w);
            (*word_count)[w] = 1;
        } else {
            (*word_count)[w] += 1;
        }
    }
}

int Partition(int num_reducer, std::string word) {
    return ((word.length() % num_reducer) + 1);
}