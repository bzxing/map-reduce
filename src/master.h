#pragma once

#include <memory>
#include <functional>
#include <mutex>
#include <limits>
#include <thread>

#include <unistd.h>


#include <boost/numeric/conversion/cast.hpp>
#include <boost/assert.hpp>
#include <boost/range/iterator_range.hpp>
#include <boost/filesystem.hpp>

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>

#include <tbb/atomic.h>

#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"

#include "mapreduce_spec.h"
#include "file_shard.h"

#include "my_utils.h"



// I know it's a header file and unnamed namespace does nothing...But I'm too lazy :P


constexpr gpr_timespec kTaskExecutionTimeout = make_milliseconds(15'000, kClockType);
constexpr gpr_timespec kWorkerConfigTimeout = make_milliseconds(5'000, kClockType);

constexpr unsigned kTaskMaxNumAttempts = 50; // Hehe

// File-scope pointer to Spec due to laziness. Sorry..
// It should be set during the scope of Master::run()

extern const MapReduceSpec * g_master_run_spec;

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

    bool should_wait() const
    {
        switch (m_state)
        {
        case TaskState::kReady:
        case TaskState::kExecuting:
            return true;
        default:
            return false;
        }
    }

    bool should_assign() const
    {
        return m_state == TaskState::kReady;
    }

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

    bool report_execution_failure()
    {
        unsigned current_failure_count = m_failure_counter.fetch_and_increment() + 1;
        bool perm_fail = false;

        bool change_state_success = false;
        if (current_failure_count > kTaskMaxNumAttempts)
        {
            change_state_success = try_change_state(TaskState::kExecuting, TaskState::kFailedUnrecoverable);
            perm_fail = true;
        }
        else
        {
            change_state_success = try_change_state(TaskState::kExecuting, TaskState::kReady);
        }

        BOOST_ASSERT(change_state_success);
        return perm_fail;
    }

    TaskState get_state() const
    {
        return m_state;
    }

    unsigned get_failure_count() const
    {
        return m_failure_counter;
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



//// Begin class MapTask ////
class MapTask : public Task
{
public:

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
    MapTask(MapTask &&) = default;
    MapTask & operator=(MapTask &&) = default;

    virtual TaskType get_task_type() const override
    {
        return kMapTaskType;
    }

    virtual masterworker::TaskRequest generate_request_message() const override
    {
        masterworker::TaskRequest msg;
        msg.set_task_uid( get_id() );
        msg.set_user_id( g_master_run_spec->get_user_id() );
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
    ReduceTask(ReduceTask &&) = default;
    ReduceTask & operator=(ReduceTask &&) = default;

    virtual TaskType get_task_type() const override
    {
        return kReduceTaskType;
    }

    virtual masterworker::TaskRequest generate_request_message() const override
    {
        masterworker::TaskRequest msg;
        msg.set_task_uid( get_id() );
        msg.set_user_id( g_master_run_spec->get_user_id() );
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
    using TaskAck = masterworker::TaskAck;
    using TaskAckResponder = grpc::ClientAsyncResponseReader<TaskAck>;
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
            m_timestamp = gpr_now(kClockType);
            m_responder = stub.PrepareAsyncDispatchTaskToWorker
                (&m_context, task->generate_request_message(), &m_cq);
            m_responder->StartCall();
            m_responder->Finish(&m_reply, &m_status, this);
        }

        bool try_get_result()
        {
            gpr_timespec deadline = m_timestamp + kTaskExecutionTimeout;

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
                    unsigned expected_uid = m_task->get_id();
                    unsigned actual_uid = m_reply.task_uid();
                    if (expected_uid != actual_uid)
                    {
                        std::cout << ("UID MISMATCH!!!" + std::to_string(expected_uid) + " " + std::to_string(actual_uid) + "\n") << std::flush;
                    }

                    BOOST_ASSERT(expected_uid == actual_uid);
                    success = true;
                }
            }

            return success;
        }

        const TaskAck & get_reply() const
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
        std::unique_ptr<TaskAckResponder> m_responder;

        TaskAck m_reply;
        grpc::Status m_status;
    };
    // End class CallData ////

    // Begin class WorkerManager //
    class WorkerManager
    {
    public:
        WorkerManager(const MapReduceSpec & spec, unsigned worker_uid) :
              m_address( spec.get_worker(worker_uid) )
            , m_worker_uid( worker_uid )
        {
            start(spec);

            std::cout <<
                "Worker " + std::to_string(m_worker_uid) + " \"" + m_address + "\" Started! Channel State: "
                + std::to_string((long) m_channel->GetState(false)) + "\n" << std::flush;
        }

        bool try_assign_task(Task * const new_task)
        {
            BOOST_ASSERT(new_task);
            const unsigned task_failure_count = new_task->get_failure_count();

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
                //std::cout << "Task " + std::to_string(new_task->get_id()) + " Rejected!\n" << std::flush;
            }
            else
            {
                std::cout << "Task " + std::to_string(new_task->get_id())
                + " Assigned to worker " + std::to_string(m_worker_uid)
                + "! Failure count: " + std::to_string(task_failure_count) + "\n" << std::flush;
            }
            // Else, leave the task state as kExecuting.

            return success;
        }

        void wait()
        {
            m_listen_for_completion_thread.join();
        }

    private:

        bool try_contact_worker_for_assigning_task(const Task * const new_task)
        {
            BOOST_ASSERT(m_channel);
            BOOST_ASSERT(m_stub);

            BOOST_ASSERT(new_task);


            //// Begin Critical Section ////
            {
                ScopedLock lock(m_call_data_mutex, std::try_to_lock);

                if (lock)
                {
                    // Check if worker is currently executing. Reject if it is.
                    if (m_call_data)
                    {
                        return false;
                    }

                    // Try farm out the task to worker. Constructor will make the call
                    m_call_data = std::make_unique<CallData>(new_task, *m_stub);
                    m_call_data_cv.notify_one();

                    return true;
                }
                else
                {
                    // Lock acquisition failed, meaning worker might be busy.
                    // Try again.
                    return false;
                }
            }
            //// End Critical Section ////
        }

        void start(const MapReduceSpec & spec)
        {
            // Bring up the connection
            m_channel = grpc::CreateChannel(m_address, grpc::InsecureChannelCredentials());
            BOOST_ASSERT(m_channel);
            m_stub = masterworker::WorkerService::NewStub(m_channel);
            BOOST_ASSERT(m_stub);

            // Set config
            bool config_success = set_worker_config(spec);
            BOOST_ASSERT_MSG(config_success, ("Could not configure worker " + m_address).c_str());

            // Kick off thread that listens for completion
            start_listening_for_completion();
        }

        masterworker::WorkerConfig generate_worker_config(const MapReduceSpec & spec)
        {
            masterworker::WorkerConfig config;

            config.set_output_dir( spec.get_output_dir() );
            config.set_num_output_files( spec.get_num_output_files() );
            config.set_worker_uid( m_worker_uid );

            return config;
        }

        bool set_worker_config(const MapReduceSpec & spec)
        {
            using Ack = masterworker::Ack;

            grpc::ClientContext context;
            grpc::CompletionQueue cq;
            Ack reply;
            grpc::Status status;

            auto responder = m_stub->PrepareAsyncSetConfig
                (&context, generate_worker_config(spec), &cq);

            responder->StartCall();
            responder->Finish(&reply, &status, this);

            gpr_timespec deadline = gpr_now(kClockType) + kWorkerConfigTimeout;
            void * tag = nullptr;
            bool ok = false;
            NextStatus next_status = cq.AsyncNext(&tag, &ok, deadline);

            if ( next_status == NextStatus::GOT_EVENT && ok )
            {
                BOOST_ASSERT(this == tag);

                if ( status.ok() && reply.success() )
                {
                    return true;
                }
            }

            return false;
        }

        void start_listening_for_completion()
        {
            m_listen_for_completion_thread = std::thread(
                [this](){ thread_listening_for_completion(); } );
        }

        void thread_listening_for_completion()
        {
            std::cout << "Worker \"" + m_address + "\" started listening for completion!\n" << std::flush;

            ScopedLock lock(m_call_data_mutex);

            for (;;)
            {
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
        const unsigned m_worker_uid = std::numeric_limits<unsigned>::max();

        // Outgoing connection session data used to assign task to worker and get acknowledgment
        std::shared_ptr<grpc::Channel> m_channel;
        std::unique_ptr<masterworker::WorkerService::Stub> m_stub;

        // Pointer to current call to the worker
        // Also used to atomically indicate whether worker is busy or not, to
        // the best of the master's knowledge
        // It can only change in two ways: nullptr to task, or task to nullptr.
        std::unique_ptr<CallData> m_call_data;

        std::thread m_listen_for_completion_thread;
    };
    // End class WorkerManager //

public:

    WorkerPoolManager(const MapReduceSpec & spec)
    {
        const unsigned num_workers = spec.get_num_workers();
        m_workers.resize(num_workers);
        for (unsigned i = 0; i < num_workers; ++i)
        {
            m_workers[i] = std::make_unique<WorkerManager>(spec, i);
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

    unsigned get_num_workers() const
    {
        return boost::numeric_cast<unsigned>(m_workers.size());
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
        g_master_run_spec = &m_spec;

        initialize_output_directory();

        WorkerPoolManager worker_pool(m_spec);

        // Just a mock, try farm out one task

        constexpr unsigned kMaxTask = 262144;

        // std::vector<std::unique_ptr<MapTask>> task_vec;
        // for (unsigned i = 0; i < kMaxTask; ++i)
        // {
        //     task_vec.push_back( std::make_unique<MapTask>(m_shards[0], "mock_output.txt") );
        // }

        // for (;;)
        // {
        //     for (auto & task_ptr : task_vec)
        //     {
        //         if (task_ptr->get_state() == Task::TaskState::kReady)
        //         {
        //             worker_pool.get_worker(0)->try_assign_task( task_ptr.get() );
        //         }

        //         usleep(1000);
        //     }
        // }

        // Infinite loop assigning tasks to worker, until everything is processed

        std::vector<MapTask> task_pool;
        task_pool.reserve(m_shards.size());
        for (const auto & shard : m_shards)
        {
            task_pool.emplace_back( shard, std::string() );
        }


        // a very non-compliant iterator..
        class WorkerIter
        {
        public:
            WorkerIter(unsigned num_workers) :
                m_num_workers(num_workers)
            {}

            unsigned operator*() const
            {
                return m_id;
            }

            WorkerIter & operator++()
            {
                ++m_id;

                if (m_id >= m_num_workers)
                {
                    m_id = 0;
                }

                return *this;
            }

        private:
            unsigned m_id = 0;
            unsigned m_num_workers = 1;
        };

        WorkerIter worker_iter( worker_pool.get_num_workers() );
        for (;;)
        {
            // Scan through all tasks
            unsigned num_keep_waiting = 0;
            for ( auto & task : task_pool )
            {
                if (task.should_wait())
                {
                    ++num_keep_waiting;
                }

                if (task.should_assign())
                {
                    // Loop until can find an available worker to assign
                    while ( !worker_pool.get_worker(*++worker_iter)->try_assign_task(&task) );
                }
            }

            if (num_keep_waiting == 0)
            {
                break;
            }
        }

        std::cout << "All tasks completed!\n" << std::flush;


        worker_pool.wait();

        g_master_run_spec = nullptr;
        return true;
    }

private:
    void initialize_output_directory() const
    {
        const auto & output_dir = m_spec.get_output_dir();

        BOOST_ASSERT_MSG(
              !boost::filesystem::equivalent(
                  boost::filesystem::current_path()
                , output_dir
              )
            , "Target output directory can't be the same as current path!"
        );

        if (boost::filesystem::exists(output_dir))
        {
            std::cout << "Warning: Will erase existing location: \"" + output_dir + "\" to make room for output directory.\nAre you really sure?\n" << std::flush;
            std::cout << "To proceed, type the target location to erase:\n" << std::flush;
            std::string key_input;
            for (;;)
            {
                std::getline(std::cin, key_input);
                if (key_input == output_dir)
                {
                    std::cout << "Yes my lord. Proceeding...\n" << std::flush;
                    break;
                }
                else
                {
                    std::cout << "You only get one chance. Bye.\n" << std::flush;
                    exit(EXIT_FAILURE);
                }
            }

            boost::filesystem::remove_all(output_dir);
        }

        bool success = boost::filesystem::create_directory(output_dir);

        BOOST_ASSERT_MSG(success, "Failed creating output directory. Did you forget to delete it first??");
    }

    const MapReduceSpec & m_spec;
    const std::vector<FileShard> & m_shards;


};



