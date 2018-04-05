#include "master.h"

tbb::atomic<unsigned> Task::s_task_uid = 0;
const MapReduceSpec * g_master_run_spec = nullptr;
