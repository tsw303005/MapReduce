CC = mpicc
CXX = mpicxx
CXXFLAGS = -std=c++17 -O3
LDFLAGS = -pthread
PATH = /home/pp21/pp21s28/MapReduce/src/
OBJS = main.o Mapper.o Reducer.o Worker.o Scheduler.o
TARGET = MapReduce


.PHONY: all
all: $(TARGET)

.PHONY: clean
clean:
	rm -f $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) $(OBJS) -o $(TARGET)

Mapper.o: $(PATH)Mapper.cc $(PATH)Mapper.h
	$(CXX) -c $(PATH)Mapper.cc $(PATH)Mapper.h

Reducer.o: $(PATH)Reducer.cc $(PATH)Reducer.h
	$(CXX) -c $(PATH)Reducer.cc $(PATH)Reducer.h

Worker.o: $(PATH)Worker.cc $(PATH)Worker.h
	$(CXX) -c $(PATH)Worker.cc $(PATH)Worker.h

Scheduler.o: $(PATH)Scheduler.cc $(PATH)Scheduler.h
	$(CXX) -c $(PATH)Scheduler.cc $(PATH)Scheduler.h

main.o: $(PATH)main.cc
	$(CXX) -c $<