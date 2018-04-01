#pragma once

#include <memory>
#include <functional>
#include <mutex>
#include <limits>
#include <thread>


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

constexpr int kTaskExecutionTimeout = 10;
constexpr unsigned kTaskMaxNumAttempts = 5;

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
    enum class TaskState
    {
        kReady,
        kExecuting,
        kCompleted,
        kFailedUnrecoverable
    };

    Task() :
        m_uid( s_task_uid.fetch_and_increment() )
    {

    }

    virtual ~Task() {}

    unsigned get_id() const
    {
        return m_uid;
    }

    virtual TaskType get_task_type() const = 0;

    virtual masterworker::TaskRequest generate_request_message() const = 0;

    // Defines 3 valid state transactions
    bool try_set_executing()
    {
        return try_change_state(TaskState::kReady, TaskState::kExecuting);
    }
    bool try_unset_executing()
    {
        return try_change_state(TaskState::kExecuting, TaskState::kReady);
    }
    bool try_set_completed()
    {
        return try_change_state(TaskState::kExecuting, TaskState::kCompleted);
    }

    void report_execution_failure()
    {
        unsigned current_failure_count = m_failure_counter.fetch_and_increment() + 1;

        bool change_state_success = false;
        if (current_failure_count > kTaskMaxNumAttempts)
        {
            change_state_success = try_change_state(TaskState::kExecuting, TaskState::kFailedUnrecoverable);
        }
        else
        {
            change_state_success = try_change_state(TaskState::kExecuting, TaskState::kReady);
        }

        BOOST_ASSERT(change_state_success);
    }

    TaskState get_state() const
    {
        return m_state;
    }

private:
    bool try_change_state(TaskState prev_state, TaskState next_state)
    {
        TaskState snapshot = m_state.compare_and_swap(next_state, prev_state);
        return snapshot == prev_state;
    }

    unsigned m_uid = 0;
    tbb::atomic<TaskState> m_state = TaskState::kReady;
    tbb::atomic<unsigned> m_failure_counter = 0;

    static tbb::atomic<unsigned> s_task_uid;
};

tbb::atomic<unsigned> Task::s_task_uid = 0;

//// Begin class MapTask ////
class MapTask : public Task
{
public:

    // Can only be called by holder of PrivateTaskGeneratorKey
    MapTask(

          const FileShard & input_shard
        , std::string output_file
        )
    :
          m_input_shard( input_shard )
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
        msg.set_task_uid( get_id() );
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

          std::string input_file_1
        , std::string input_file_2
        , std::string output_file
        )
    :
          m_input_file_1( std::move(input_file_1) )
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
        msg.set_task_uid( get_id() );
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

    std::string m_input_file_1;
    std::string m_input_file_2;
    std::string m_output_file;
};
//// End class ReduceTask ////


// Roles:
//  - Keeps connection session for each worker
//  - Allow TaskAssigner to dispatch request to worker, and mark worker as busy
//  - Allow workers to come back to be marked as completed
//  - Clean up request sent to worker but not delivered
class WorkerPoolManager
{
    using Reply = masterworker::TaskAck;
    using Responder = grpc::ClientAsyncResponseReader<Reply>;
    using NextStatus = grpc::CompletionQueue::NextStatus;

    //// Begin class CallData ////
    class CallData
    {
    public:

        // Constructor will issue a call
        CallData( const Task * const task , masterworker::WorkerService::Stub & stub ) :
            m_task(task)
        {
            BOOST_ASSERT(task);
            m_timestamp = gpr_now(GPR_CLOCK_MONOTONIC);
            m_responder = stub.PrepareAsyncDispatchTaskToWorker
                (&m_context, task->generate_request_message(), &m_cq);
            m_responder->StartCall();
            m_responder->Finish(&m_reply, &m_status, this);
        }

        bool try_get_result()
        {
            gpr_timespec deadline = m_timestamp;
            deadline.tv_sec += kTaskExecutionTimeout;

            void * receive_tag = nullptr;
            bool ok = false;
            NextStatus next_status = m_cq.AsyncNext(&receive_tag, &ok, deadline);

            // Will block until deadline...
            // Then we get the result (either success of fail)

            bool success = false;

            // Check whether the m_reply is successfully received (communication level check)
            if ( next_status == NextStatus::GOT_EVENT && ok )
            {
                BOOST_ASSERT(this == receive_tag);

                // Check whether the m_reply indicates the task is accepted (logic level check)
                if ( m_status.ok() && m_reply.success() )
                {
                    BOOST_ASSERT(m_task->get_id() == m_reply.task_uid());
                    success = true;
                }
            }

            return success;
        }

        const Reply & get_reply() const
        {
            return m_reply;
        }

        const Task * get_task() const
        {
            return m_task;
        }

    private:
        const Task * m_task = nullptr;
        gpr_timespec m_timestamp;

        grpc::ClientContext m_context;
        grpc::CompletionQueue m_cq;
        std::unique_ptr<Responder> m_responder;

        Reply m_reply;
        grpc::Status m_status;
    };
    // End class CallData ////

    // Begin class WorkerManager //
    class WorkerManager
    {
    public:
        WorkerManager(const std::string & address) :
              m_address(address)
        {
            start();

            std::cout <<
                "Worker \"" + address + "\" Started! Channel State: "
                + std::to_string((long) m_outgoing_channel->GetState(false)) + "\n" << std::flush;
        }

        bool try_assign_task(Task * const new_task)
        {
            BOOST_ASSERT(new_task);

            bool success = true;

            if (success)
            {
                success = new_task->try_set_executing();
                BOOST_ASSERT(success); // Why shouldn't this step pass?
            }

            if (success)
            {
                success = try_contact_worker_for_assigning_task(new_task);
            }

            // Revert the task state, if task assignment failed.
            if (!success)
            {
                success = new_task->try_unset_executing();
                BOOST_ASSERT(success); // Why shouldn't this step pass?
            }
            else
            {
                std::cout << "Task " + std::to_string(new_task->get_id()) + " Assigned!\n" << std::flush;
            }
            // Else, leave the task state as kExecuting.

            return success;
        }

        void wait()
        {
            m_listen_for_completion_thread.join();
        }

        bool connection_is_available() const
        {
            grpc_connectivity_state state = m_outgoing_channel->GetState(false);

            switch (state)
            {
            case GRPC_CHANNEL_SHUTDOWN:
                return false;
            default:
                return true;
            }
            return true;
        }

    private:

        bool try_contact_worker_for_assigning_task(const Task * const new_task)
        {
            BOOST_ASSERT(m_outgoing_channel);
            BOOST_ASSERT(m_outgoing_stub);

            BOOST_ASSERT(new_task);

            // If channel is unavailable, reject it
            if (!connection_is_available())
            {
                return false;
            }

            //// Begin Critical Section ////
            {
                ScopedLock lock(m_call_data_mutex);

                // Check if worker is currently executing. Reject if it is.
                if (m_call_data)
                {
                    return false;
                }

                // Try farm out the task to worker. Constructor will make the call
                m_call_data = std::make_unique<CallData>(new_task, *m_outgoing_stub);
                m_call_data_cv.notify_one();
            }
            //// End Critical Section ////


            return true;
        }

        void start()
        {
            m_outgoing_channel = grpc::CreateChannel(m_address, grpc::InsecureChannelCredentials());
            BOOST_ASSERT(m_outgoing_channel);
            m_outgoing_stub = masterworker::WorkerService::NewStub(m_outgoing_channel);
            BOOST_ASSERT(m_outgoing_stub);

            start_listening_for_completion();
        }

        void start_listening_for_completion()
        {
            m_listen_for_completion_thread = std::thread(
                [this](){ thread_listening_for_completion(); } );
        }

        void thread_listening_for_completion()
        {
            std::cout << "Worker \"" + m_address + "\" started listening for completion!\n" << std::flush;
            for (;;)
            {
                ScopedLock lock(m_call_data_mutex);

                // Block until a new m_call_data is present.
                while (!m_call_data)
                {
                    m_call_data_cv.wait(lock);
                }

                bool success = m_call_data->try_get_result();
                // The use of const_cast here is legitimate because
                // WorkerManager created CallData with non-const pointer
                // in the first place, and CallData class is privately defined
                // in WorkerManager.
                Task * task = const_cast<Task *>(m_call_data->get_task());
                BOOST_ASSERT(task);

                if (success)
                {
                    bool change_state_success = task->try_set_completed();
                    BOOST_ASSERT(change_state_success);
                    std::cout << "Task " + std::to_string(task->get_id()) + " Completed!\n" << std::flush;
                }
                else
                {
                    task->report_execution_failure();
                    std::cout << "Task " + std::to_string(task->get_id()) + " Failed!\n" << std::flush;
                }

                // Can release m_call_data now.
                m_call_data = nullptr;
            }
        }

        //// Data Fields /////
        std::mutex m_call_data_mutex; // Guard everything non-const below.
                            // These fields are accessed by multiple threads.
        std::condition_variable m_call_data_cv; // Used to notify the completion thread
                                      // when there's a worker starting.

        // Server address, defined at construction time
        const std::string & m_address;

        // Outgoing connection session data used to assign task to worker and get acknowledgment
        std::shared_ptr<grpc::Channel> m_outgoing_channel;
        std::unique_ptr<masterworker::WorkerService::Stub> m_outgoing_stub;

        // Pointer to current call to the worker
        // Also used to atomically indicate whether worker is busy or not, to
        // the best of the master's knowledge
        // It can only change in two ways: nullptr to task, or task to nullptr.
        std::unique_ptr<CallData> m_call_data;

        std::thread m_listen_for_completion_thread;
    };
    // End class WorkerManager //

    static std::unique_ptr<WorkerManager> build_worker_manager(const std::string & address)
    {
        auto worker_info = std::make_unique<WorkerManager>(address);
        return worker_info;
    }

public:

    WorkerPoolManager(const MapReduceSpec & spec)
    {
        const unsigned num_workers = spec.get_num_workers();
        m_workers.resize(num_workers);
        for (unsigned i = 0; i < num_workers; ++i)
        {
            m_workers[i] = build_worker_manager( spec.get_worker(i) );
        }
    }

    WorkerManager * get_worker(unsigned i)
    {
        if ( i >= m_workers.size() )
        {
            return nullptr;
        }
        return m_workers[i].get();
    }

    void wait()
    {
        for (const auto & worker_mgr_ptr : m_workers)
        {
            worker_mgr_ptr->wait();
        }
    }

private:
    std::vector<std::unique_ptr<WorkerManager>> m_workers;
};

class TaskAllocator
{
public:

private:

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
        WorkerPoolManager worker_pool(m_spec);

        // Just a mock, try farm out one task
        MapTask task( m_shards[0], "mock_output.txt" );
        worker_pool.get_worker(0)->try_assign_task(&task);

        worker_pool.wait();
        return true;
    }

private:
    /* NOW you can add below, data members and member functions as per the need of your implementation*/

    const MapReduceSpec & m_spec;
    const std::vector<FileShard> & m_shards;


};



