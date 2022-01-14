#include "Mapper.h"

void* MapperFunction(void* input) {
    Mapper *mapper = (Mapper*)input;
    int chunk = -1;
    bool flag = true;

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
            InputSplit(chunk);
            pthread_mutex_lock(mapper->lock);
            (*mapper->available_num) += 1;
            pthread_mutex_unlock(mapper->lock);
            chunk = -1;
        }
    }

    pthread_exit(NULL);
}

void InputSplit(int chunk) {
}