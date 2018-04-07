#include "mr_tasks.h"
#include "worker.h"
#include <tbb/atomic.h>

tbb::atomic<bool> s_terminate_worker = false;
std::unique_ptr<WorkerConfig> WorkerConfig::s_inst;

