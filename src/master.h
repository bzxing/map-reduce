#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"


/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master
{

public:
	/* DON'T change the function signature of this constructor */
	/* CS6210_TASK: This is all the information your master will get from the framework.
		You can populate your other class data members here if you want */
	Master(const MapReduceSpec&, const std::vector<FileShard>&)
	{

	}

	/* DON'T change this function's signature */
	/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */

	bool run()
	{
		return true;
	}

private:
	/* NOW you can add below, data members and member functions as per the need of your implementation*/

};



