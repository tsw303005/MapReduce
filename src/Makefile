CXX = mpicxx
CXXFLAGS = -std=c++11 -O3 # -fsanitize=address -g
LDFLAGS = -pthread
TARGETS = 

.PHONY: all
all: $(TARGETS)

.PHONY: clean
clean:
	rm -f $(TARGETS)

%: %.cc
	$(CXX) -o $@ $(CXXFLAGS) $(LDFLAGS) $^