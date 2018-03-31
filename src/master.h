#pragma once

#include <memory>
#include <functional>
#include <mutex>
#include <limits>


#include <boost/numeric/conversion/cast.hpp>
#include <boost/assert.hpp>
#include <boost/range/iterator_range.hpp>
#include <boost/variant.hpp>

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>

#include <tbb/atomic.h>

#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"

#include "mapreduce_spec.h"
#include "file_shard.h"

using ScopedLock = std::unique_lock<std::mutex>;

constexpr int kTaskRequestAckTimeout = 2;

using TaskType = typename masterworker::TaskRequest::TaskType;
constexpr TaskType kMapTaskType = masterworker::TaskRequest::kMap;
constexpr TaskType kReduceTaskType = masterworker::TaskRequest::kReduce;

struct PrivateTaskGeneratorKey
{
private:
	friend class TaskGenerator;
	PrivateTaskGeneratorKey() {};
};

class Task
{
public:
	virtual ~Task() {}

	virtual TaskType get_task_type() const = 0;

	virtual masterworker::TaskRequest generate_request_message() const = 0;
};

//// Begin class MapTask ////
class MapTask : public Task
{
public:

	// Can only be called by holder of PrivateTaskGeneratorKey
	MapTask(
		  PrivateTaskGeneratorKey
		, unsigned uid
		, const FileShard & input_shard
		, std::string output_file
		)
	:
		  m_uid( uid )
		, m_input_shard( input_shard )
		, m_output_file( std::move(output_file) )
	{}

	// Non-copyable because UID
	MapTask(const MapTask &) = delete;
	MapTask & operator=(const MapTask &) = delete;

	virtual TaskType get_task_type() const override
	{
		return kMapTaskType;
	}

	virtual masterworker::TaskRequest generate_request_message() const override
	{
		masterworker::TaskRequest msg;
		msg.set_task_uid( m_uid );
		msg.set_task_type( get_task_type() );
		*msg.add_input_file() = generate_file_shard_message_field( m_input_shard );
		msg.set_output_file( m_output_file );
		return msg;
	}

private:
	static masterworker::FileShard
	generate_file_shard_message_field(const FileShard & file_shard)
	{
		masterworker::FileShard msg;
		msg.set_filename(file_shard.filename);
		msg.set_offset(file_shard.offset);
		msg.set_length(file_shard.shard_length);
		return msg;
	}

	unsigned m_uid = 0;
	const FileShard & m_input_shard;
	std::string m_output_file;
};
//// End class MapTask ////


//// Begin class ReduceTask ////
class ReduceTask : public Task
{
public:

	// Can only be called by holder of PrivateTaskGeneratorKey
	ReduceTask(
		  PrivateTaskGeneratorKey
		, unsigned uid
		, std::string input_file_1
		, std::string input_file_2
		, std::string output_file
		)
	:
		  m_uid( uid )
		, m_input_file_1( std::move(input_file_1) )
		, m_input_file_2( std::move(input_file_2) )
		, m_output_file( std::move(output_file) )
	{}

	// Non-copyable because UID
	ReduceTask(const ReduceTask &) = delete;
	ReduceTask & operator=(const ReduceTask &) = delete;

	virtual TaskType get_task_type() const override
	{
		return kReduceTaskType;
	}

	virtual masterworker::TaskRequest generate_request_message() const override
	{
		masterworker::TaskRequest msg;
		msg.set_task_uid( m_uid );
		msg.set_task_type( get_task_type() );
		*msg.add_input_file() = generate_file_shard_message_field( m_input_file_1 );
		*msg.add_input_file() = generate_file_shard_message_field( m_input_file_2 );
		msg.set_output_file( m_output_file );
		return msg;
	}

private:

	static masterworker::FileShard
	generate_file_shard_message_field(std::string filename)
	{
		masterworker::FileShard msg;
		msg.set_filename( std::move(filename) );
		msg.set_offset( 0 );
		msg.set_length( 0 );
		return msg;
	}

	unsigned m_uid = 0;
	std::string m_input_file_1;
	std::string m_input_file_2;
	std::string m_output_file;
};
//// End class ReduceTask ////

class TaskGenerator
{
public:

	// Delegate to the constructor of one variant under BaseTask (a.k.a MapTask or Reduce Task).
	// The parameter "args" must match the construction parameter signature of these
	// derived class, minus the first couple fields
	template <class... Args>
	static std::unique_ptr<Task> generate(
		  TaskType task_type
		, Args &&... args // Look at the constructor signatures of the derived classes of Task
		)
	{
		std::unique_ptr<Task> m_task;
		const unsigned uid = s_task_uid.fetch_and_increment();

		if (task_type = kMapTaskType)
		{
			m_task = std::make_unique<MapTask>
				(PrivateTaskGeneratorKey(), uid, std::forward<Args>(args)...);
		}
		else if (task_type = kReduceTaskType)
		{
			m_task = std::make_unique<ReduceTask>
				(PrivateTaskGeneratorKey(), uid, std::forward<Args>(args)...);
		}
		else
		{
			BOOST_ASSERT_MSG(false, "Unknown task type!");
		}

		BOOST_ASSERT(m_task);
		return m_task;
	}

private:

	static tbb::atomic<unsigned> s_task_uid;
};

tbb::atomic<unsigned> TaskGenerator::s_task_uid = 0;


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
			connect();
		}

		void connect()
		{
			m_outgoing_channel = grpc::CreateChannel(m_address, grpc::InsecureChannelCredentials());
			m_outgoing_stub = masterworker::WorkerService::NewStub(m_outgoing_channel);
		}

		bool try_assign_task(const Task * const new_task)
		{
			BOOST_ASSERT(m_outgoing_channel);
			BOOST_ASSERT(m_outgoing_stub);
			BOOST_ASSERT(new_task);

			bool success = true;

			// Atomically mark as busy, so other threads will get bounced back
			if (success)
			{
				const Task * snapshot = m_task.compare_and_swap(new_task, nullptr);
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
				const Task * snapshot = m_task.fetch_and_store(nullptr);
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

			std::unique_ptr<Responder> responder = m_outgoing_stub->PrepareAsyncDispatchTaskToWorker
				(&context, task.generate_request_message(), &cq);
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

		//// Data Fields /////

		// Server address, defined at construction time
		const std::string & m_address;

		// Atomic pointer to current task being attempted/executed.
		// Also used to atomically indicate whether worker is busy or not.
		// It can only change in two ways: nullptr to task, or task to nullptr.
		tbb::atomic<const Task *> m_task = nullptr;

		// Outgoing connection session data used to assign task to worker and get acknowledgment
		std::shared_ptr<grpc::Channel> m_outgoing_channel;
        std::unique_ptr<masterworker::WorkerService::Stub> m_outgoing_stub;

        // Incoming connection session used to receive signal of completion from worker
        // TODO:
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



