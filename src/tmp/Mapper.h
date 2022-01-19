#ifndef MAPPER_H
#define MAPPER_H

#include <iostream>
#include <fstream>
#include <string>
#include <pthread.h>
#include <queue>
#include <map>
#include <unistd.h>
#include <mpi.h>
#include "Worker.h"

#define WAIT 0 // simulate true delay

typedef std::map<std::string, int> Count;
typedef std::vector<std::string> Word;
typedef std::pair<int, int> Chunk;

void* MapperFunction(void* input);
void InputSplit(int chunk, int chunk_size, std::string source_file, Count *word_count, Word *words);
void Map(std::string line, Count *word_count, Word *words);
int Partition(int num_reducer, std::string word);

#endif