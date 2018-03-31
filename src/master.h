#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"

enum class TaskType
{
	Map,
	Reduce
};

class Task
{


};

// Roles:
//  - Keeps connection to each worker
//  - Allow TaskAssigner to dispatch request to worker, and mark worker as busy
//  - Allow workers to come back to be marked as completed
//  - Clean up request sent to worker but not delivered
class WorkerManager
{
public:

private:

};

class TaskAllocator
{

};

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master
{

public:
	/* DON'T change the function signature of this constructor */
	/* CS6210_TASK: This is all the information your master will get from the framework.
		You can populate your other class data members here if you want */
	Master(const MapReduceSpec & spec, const std::vector<FileShard> & shards) :
		  m_spec(spec)
		, m_shards(shards)
	{

	}

	/* DON'T change this function's signature */
	/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */

	bool run()
	{
		for (const auto & worker : m_spec.get_worker_range())
		{

		}

		return true;
	}

private:
	/* NOW you can add below, data members and member functions as per the need of your implementation*/

	const MapReduceSpec & m_spec;
	const std::vector<FileShard> & m_shards;


};



