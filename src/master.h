#pragma once

#include <memory>
#include <functional>

#include <boost/numeric/conversion/cast.hpp>
#include <boost/assert.hpp>
#include <boost/range/iterator_range.hpp>

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>

#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"

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

template <class Reply>
struct OutgoingCallData
{
	Reply reply;
	grpc::ClientContext context;
	grpc::Status status;
	std::unique_ptr<grpc::ClientAsyncResponseReader<Reply>> response_reader;
};

template <class Reply, class Request, class Stub>
inline std::unique_ptr<OutgoingCallData<Reply>> make_async_call(
	  const Request & request
	, grpc::CompletionQueue & cq
	, Stub & stub
	, std::function< std::unique_ptr<grpc::ClientAsyncResponseReader<Reply>>(Stub&) > f_make_response_reader
	)
{
	auto call = std::make_unique<OutgoingCallData<Reply>>();

	call->response_reader = f_make_response_reader(stub);
	call->response_reader->StartCall();
	call->response_reader->Finish(&call->reply, &call->status, call.get());

	return call;
}

class WorkerList
{
public:
	WorkerList(const MapReduceSpec & spec) :
		m_spec(spec)
	{

	}

	auto get_workers() const
	{
		return m_spec.get_worker_range();
	}

	unsigned get_num_workers() const
	{
		return boost::numeric_cast<unsigned>(get_workers().size());
	}

	const std::string & get_worker(unsigned i) const
	{
		BOOST_ASSERT(i < get_num_workers());
		return get_workers()[i];
	}

private:
	const MapReduceSpec & m_spec;

};


// Role:
//  - Keeps channels and stubs for each worker
//
class OutgoingCallManager
{
public:
	OutgoingCallManager(  )
	{

	}

private:


};



class IncomingCallManager
{
public:

private:

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



