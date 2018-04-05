#include "mr_tasks.h"
#include "worker.h"


std::unique_ptr<WorkerConfig> WorkerConfig::s_inst;
std::unique_ptr<TaskEnv> TaskEnv::s_inst;

