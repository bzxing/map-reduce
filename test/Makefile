CXX = g++
CPPFLAGS += -I/usr/local/include -pthread

CXXFLAGS += \
-std=c++14 -g \
-include ../src/my_assert.h \
-Wall -Wextra -Werror -Wconversion \
-Wno-unused-parameter -Wno-unused-result -Wno-unused-variable \
-Wno-unused-but-set-variable  -Wno-unused-function

LDFLAGS += -L/usr/local/lib `pkg-config --libs grpc++ grpc`       \
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed \
           -lprotobuf -lpthread -ldl -lboost_system -lboost_filesystem

MAPREDUCE_LIB = -L../external/lib -lmapreduce -lboost_system
MR_WORKER_LIB = -L../external/lib -lmr_worker -lboost_system

all: mrdemo mr_worker

mrdemo: main.o
	$(CXX) $^ -o $@ $(LDFLAGS) $(MAPREDUCE_LIB) $(LDFLAGS)

mr_worker: user_tasks.o
	$(CXX) $^ -o $@ $(LDFLAGS) $(MR_WORKER_LIB) $(LDFLAGS)

%.o: %.cc
	$(CXX) -c $< -I../external/include $(CXXFLAGS)

clean:
	rm -f *.o *.pb.cc *.pb.h mrdemo mr_worker
