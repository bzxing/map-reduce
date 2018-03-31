#pragma once

#include <memory>
#include <functional>
#include <mutex>


#include <boost/numeric/conversion/cast.hpp>
#include <boost/assert.hpp>
#include <boost/range/iterator_range.hpp>

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>

#include <tbb/atomic.h>

#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"

#include "mapreduce_spec.h"
#include "file_shard.h"

using ScopedLock = std::unique_lock<std::mutex>;

constexpr int kTaskRequestAckTimeout = 4;

using TaskType = typename masterworker::TaskRequest::TaskType;

using FileList = std::vector<std::string>;

class Task
{
public:
	using RawRequest = masterworker::TaskRequest;

	template <class... Args>
	static std::unique_ptr<Task> create_new_task( Args&&... args)
	{
		return std::make_unique<Task>(std::forward<Args>(args)...);
	}

	const RawRequest & get_raw_request() const
	{
		return m_message;
	}

private:

	Task(
		 TaskType task_type
		,FileList && inputs
		,std::string && output
		)
	{
		m_message.set_task_uid( s_task_uid.fetch_and_increment() );

		m_message.set_task_type(task_type);

		for (auto && input : inputs)
		{
			m_message.add_input_files(std::move(input));
		}

		m_message.set_output_file(std::move(output));
	}

	Task(const Task &) = delete;
	Task & operator=(const Task &) = delete;

	RawRequest m_message;

	static tbb::atomic<unsigned> s_task_uid;
};

tbb::atomic<unsigned> Task::s_task_uid = 0;


// Roles:
//  - Keeps connection session for each worker
//  - Allow TaskAssigner to dispatch request to worker, and mark worker as busy
//  - Allow workers to come back to be marked as completed
//  - Clean up request sent to worker but not delivered
class WorkerManager
{
	// Begin class WorkerInfo //
	class WorkerInfo
	{
	public:
		WorkerInfo(const std::string & _address) :
			  m_address(_address)

		{

		}

		void connect()
		{
			m_channel = grpc::CreateChannel(m_address, grpc::InsecureChannelCredentials());
			m_stub = masterworker::WorkerService::NewStub(m_channel);
		}

		bool try_assign_task(Task * const new_task)
		{
			BOOST_ASSERT(m_channel);
			BOOST_ASSERT(m_stub);
			BOOST_ASSERT(new_task);

			bool success = true;

			// Atomically mark as busy, so other threads will get bounced back
			if (success)
			{
				Task * snapshot = m_task.compare_and_swap(new_task, nullptr);
			    success = (snapshot == nullptr);
			}

			// Try to create a new task, send to worker, and wait for acceptance
			if (success)
			{
				success = contact_worker_to_execute_task(*new_task);
			}

			// Before exiting, if not successful, revert m_task to null.
			if (!success)
			{
				Task * snapshot = m_task.fetch_and_store(nullptr);
				BOOST_ASSERT_MSG(snapshot == new_task,
					"Detected race condition! "
					"Worker's assigned task should not change during task assignment.");
			}

			return success;
		}

	private:

		bool contact_worker_to_execute_task( const Task & task )
		{
			using Reply = masterworker::TaskRequestAck;
			using Responder = grpc::ClientAsyncResponseReader<Reply>;
			using NextStatus = grpc::CompletionQueue::NextStatus;

			// Setup and issue the call

			grpc::ClientContext context;
			grpc::CompletionQueue cq;
			Reply reply;
			grpc::Status status;

			std::unique_ptr<Responder> responder = m_stub->PrepareAsyncDispatchTaskToWorker
				(&context, task.get_raw_request(), &cq);
			responder->StartCall();
			void * const send_tag = nullptr;
			responder->Finish(&reply, &status, send_tag);


			// Wait for a bit of time for a reply, until a set timeout.

			gpr_timespec deadline = gpr_now(GPR_CLOCK_MONOTONIC);
			deadline.tv_sec += kTaskRequestAckTimeout;

			void * receive_tag = nullptr;
            bool ok = false;
			NextStatus next_status = cq.AsyncNext(&receive_tag, &ok, deadline);


			// Will block until deadline...
			// Then we get the result (either success of fail)

			bool success = false;

			// Check whether the reply is successfully received (communication level check)
			if ( next_status == NextStatus::GOT_EVENT && ok )
			{
				BOOST_ASSERT(send_tag == receive_tag);

				// Check whether the reply indicates the task is accepted (logic level check)
				if ( status.ok() && reply.accepted() )
				{
					success = true;
				}
			}

			return success;


		}

		const std::string & m_address;
		tbb::atomic<Task *> m_task = nullptr;
			// Atomic pointer that can only go two ways:
			//   nullptr to task, or task to nullptr.

		std::shared_ptr<grpc::Channel> m_channel;
        std::unique_ptr<masterworker::WorkerService::Stub> m_stub;
	};
	// End class WorkerInfo //

	static std::unique_ptr<WorkerInfo> build_worker_info(const std::string & address)
	{
		auto worker_info = std::make_unique<WorkerInfo>(address);
		return worker_info;
	}

public:

	WorkerManager(const MapReduceSpec & spec)
	{
		const unsigned num_workers = spec.get_num_workers();
		m_workers.resize(num_workers);
		for (unsigned i = 0; i < num_workers; ++i)
		{
			m_workers[i] = build_worker_info( spec.get_worker(i) );
		}
	}

private:
	std::vector<std::unique_ptr<WorkerInfo>> m_workers;



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



