#!/bin/bash
rm ./result_file/*.out
rm MapReduce
mpicxx -std=c++17 main.cc Mapper.cc Reducer.cc Scheduler.cc Worker.cc Mapper.h Reducer.h Scheduler.h Worker.h -o MapReduce