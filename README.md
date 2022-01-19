# MapReduce
- I implement word count application using MapReduce framework with MPI and Pthread
- I also design several testcase to verify its correctness

## Build with
- C/C++
- MPI
- Pthread

## Getting Start
1. cd into src directory
2. excecute script.sh to compile those source files
3. choose the testcase you want to execute

- This is a simple example to execute this program with Makefile
- Makesure that you have already created those required directory (result_file, intermediate_file)

```
cd src
./script.sh
srun -N4 -c12 ./MapReduce TEST10 12 5 ./testcases/10.word 10 ./testcases/10.loc ./result_file/
```

## Implementation
1. Scheduler is responsible for assigning task to those worker and send termination signal if no task
2. Worker is responsible for handling request from Scheduler and generate intermediate Key-Value-Pair or generate final result
3. Scheduler Worker use MPI to send message
4. Worker can handle multiple task with Pthread
5. Use locality to file to simulate the real situation that worker will become slower if no data locality