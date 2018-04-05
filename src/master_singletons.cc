#include "master.h"

tbb::atomic<bool> WorkerPoolManager::WorkerManager::s_terminate_worker_listening_thread = false; // Must be raised from false to true not the other way around
tbb::atomic<unsigned> Task::s_task_uid = 0;
const MapReduceSpec * g_master_run_spec = nullptr;

