#include "Mapper.h"

void* MapperFunction(void* input) {
    Mapper *mapper = (Mapper*)input;
    int chunk = -1;
    bool flag = true;
    Count *word_count = new Count;
    Word *words = new Word;

    while (flag) {
        pthread_mutex_lock(mapper->lock);
        if (!mapper->job->empty()) {
            chunk = mapper->job->front();
            if (chunk == -1) {
                flag = false;
            } else {
                (*mapper->available_num) -= 1;
                mapper->job->pop();
            }
        }
        pthread_mutex_unlock(mapper->lock);

        if (chunk != -1) {
            word_count->clear();
            words->clear();
            
            // split chunk
            InputSplit(chunk, mapper->chunk_size, mapper->source_file, word_count, words);
            // get word partition

            // job terminate
            pthread_mutex_lock(mapper->lock);
            (*mapper->available_num) += 1;
            pthread_mutex_unlock(mapper->lock);
            chunk = -1;
        }
    }

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
}

void Map(std::string line, Count *word_count, Word *words) {
    int pos = 0;
    std::string word;
    
    while ((pos = line.find(" ")) != std::string::npos) {
        word = line.substr(0, pos);

        if (word_count->count(word) == 0) {
            words->push_back(word);
            (*word_count)[word] = 1;
        } else {
            (*word_count)[word]++;
        }

        line.erase(0, pos + 1);
    }
}

int Partitaion(int num_reducer, int chunk) {

}