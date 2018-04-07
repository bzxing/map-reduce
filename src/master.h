#pragma once

#include <memory>
#include <functional>
#include <mutex>
#include <limits>
#include <thread>

#include <unistd.h>


#include <boost/numeric/conversion/cast.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/assert.hpp>
#include <boost/range/iterator_range.hpp>
#include <boost/filesystem.hpp>
#include <boost/container/flat_set.hpp>

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>

#include <tbb/atomic.h>

#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"

#include "mapreduce_spec.h"
#include "file_shard.h"

#include "my_utils.h"



// I know it's a header file and unnamed namespace does nothing...But I'm too lazy :P

constexpr gpr_timespec kWorkerConfigTimeout = make_milliseconds(5'000, kClockType);

constexpr unsigned kTaskMaxNumAttempts = 3; // Hehe

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

    bool perm_fail() const
    {
        return m_state == TaskState::kFailedUnrecoverable;
    }

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
    void force_unset_executing()
    {
        BOOST_ASSERT(try_change_state(TaskState::kExecuting, TaskState::kReady));
    }
    void force_set_completed()
    {
        BOOST_ASSERT(try_change_state(TaskState::kExecuting, TaskState::kCompleted));
    }

    std::pair<unsigned, bool> force_report_execution_failure(WorkerErrorEnum e)
    {
        unsigned current_failure_count = m_failure_counter.fetch_and_increment() + 1;
        bool perm_fail = false;

        if (current_failure_count > kTaskMaxNumAttempts || is_permanent_error(e) )
        {
            BOOST_ASSERT(try_change_state(TaskState::kExecuting, TaskState::kFailedUnrecoverable));
            perm_fail = true;
        }
        else
        {
            BOOST_ASSERT(try_change_state(TaskState::kExecuting, TaskState::kReady));
        }

        return std::make_pair(current_failure_count, perm_fail);
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

    static bool is_permanent_error(WorkerErrorEnum e)
    {
        switch (e)
        {
        case WorkerErrorEnum::kInputFileLengthError:
        case WorkerErrorEnum::kInputFileRangeError:
        case WorkerErrorEnum::kInputFileNewlineError:
        case WorkerErrorEnum::kInputFileGetlineError:
        case WorkerErrorEnum::kTaskTypeError:
        case WorkerErrorEnum::kTaskProcessorNotFoundError:
            return true;
        default:
            return false;
        }
    }

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
        )
    :
          m_input_shard( input_shard )
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
};
//// End class MapTask ////


//// Begin class ReduceTask ////
class ReduceTask : public Task
{
public:

    ReduceTask( std::vector<std::string> && file_list )
    : m_file_list( std::move(file_list) )
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

        for (const auto & filename : m_file_list)
        {
            *msg.add_input_file() = generate_file_shard_message_field(filename);
        }

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

    std::vector<std::string> m_file_list;


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

    class CloseCallData
    {
        using Ack = masterworker::Ack;
        using AckResponder = grpc::ClientAsyncResponseReader<Ack>;
    public:

        // Constructor will issue a call
        CloseCallData( masterworker::WorkerService::Stub & stub )
        {
            m_responder = stub.PrepareAsyncClose
                (&m_context, m_request, &m_cq);
            m_responder->StartCall();
            m_responder->Finish(&m_reply, &m_status, this);
            wait();
        }

        std::pair<bool, WorkerErrorEnum> wait()
        {
            gpr_timespec deadline = gpr_now(kClockType) + kIdleTimeout;

            void * receive_tag = nullptr;
            bool ok = false;
            // Wait for a long period of time...If fails, exit anyways haha.
            NextStatus next_status = m_cq.AsyncNext(&receive_tag, &ok, deadline);

            return std::make_pair(true, WorkerErrorEnum::kGood);
        }

    private:

        grpc::ClientContext m_context;
        grpc::CompletionQueue m_cq;
        std::unique_ptr<AckResponder> m_responder;

        Ack m_request;
        Ack m_reply;
        grpc::Status m_status;
    };

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

        std::pair<bool, WorkerErrorEnum> try_get_result()
        {
            gpr_timespec deadline = m_timestamp + kIdleTimeout;

            void * receive_tag = nullptr;
            bool ok = false;
            NextStatus next_status = m_cq.AsyncNext(&receive_tag, &ok, deadline);

            // Will block until deadline...
            // Then we get the result (either success of fail)

            // Check whether the m_reply is successfully received (communication level check)
            if ( next_status == NextStatus::GOT_EVENT && ok )
            {
                BOOST_ASSERT(this == receive_tag);

                // Check whether the m_reply indicates the task is accepted (logic level check)
                if ( m_status.ok() )
                {
                    bool success = m_reply.success();

                    // Good to have success. But before announcing it
                    // we have to make sure the task UID matches
                    if (success)
                    {
                        unsigned expected_uid = m_task->get_id();
                        unsigned actual_uid = m_reply.task_uid();

                        if (expected_uid != actual_uid)
                        {
                            std::cerr << ("UID MISMATCH!!!" + std::to_string(expected_uid) + " " + std::to_string(actual_uid) + "\n") << std::flush;
                            BOOST_ASSERT(false);
                        }
                    }

                    WorkerErrorEnum error_enum = m_reply.error_enum();
                    return std::make_pair(success, error_enum);
                }
            }

            return std::make_pair(false, WorkerErrorEnum::kReceptionError);
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
                new_task->force_unset_executing();
                //std::cout << "Task " + std::to_string(new_task->get_id()) + " Rejected!\n" << std::flush;
            }
            else
            {
                std::cout << "Task " + std::to_string(new_task->get_id())
                + " Assigned to worker " + std::to_string(m_worker_uid)
                + "!" + "\n" << std::flush;
            }
            // Else, leave the task state as kExecuting.

            return success;
        }

        static void signal_termination()
        {
            s_terminate_worker_listening_thread = true;
        }

        void wait()
        {
            m_listen_for_completion_thread.join();
        }

        void close()
        {
            CloseCallData close_call(*m_stub);
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

            while (!s_terminate_worker_listening_thread)
            {
                // Block until a new m_call_data is present.
                while (!m_call_data)
                {
                    m_call_data_cv.wait_for(lock, std::chrono::milliseconds(100));

                    if (s_terminate_worker_listening_thread)
                    {
                        return;
                    }
                }

                auto p = m_call_data->try_get_result();
                bool success = p.first;
                WorkerErrorEnum error_enum = p.second;
                // The use of const_cast here is legitimate because
                // WorkerManager created CallData with non-const pointer
                // in the first place, and CallData class is privately defined
                // in WorkerManager.
                Task * task = const_cast<Task *>(m_call_data->get_task());
                BOOST_ASSERT(task);

                unsigned failure_count = 0;
                bool perm_fail = false;
                if (success)
                {
                    task->force_set_completed();
                }
                else
                {
                    std::tie(failure_count, perm_fail) = task->force_report_execution_failure(error_enum);
                }


                std::cout <<
                    "Task " + std::to_string(task->get_id()) + " " + (success ? "suceeded" : "failed") + "! "
                    + "error_enum=" + worker_error_enum_to_string(error_enum) + " "
                    + "failure_count=" + (success ? std::string("dunno") : std::to_string(failure_count)) + " "
                    + "perm_fail=" + (perm_fail ? "true" : "false")
                    + "\n" << std::flush;

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

        static tbb::atomic<bool> s_terminate_worker_listening_thread;
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

    void close()
    {
        for (const auto & worker_mgr_ptr : m_workers)
        {
            worker_mgr_ptr->close();
        }
    }

    static void signal_termination()
    {
        WorkerManager::signal_termination();
    }

    unsigned get_num_workers() const
    {
        return boost::numeric_cast<unsigned>(m_workers.size());
    }

private:
    std::vector<std::unique_ptr<WorkerManager>> m_workers;
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

        m_worker_pool = std::make_unique<WorkerPoolManager>(m_spec);


        unsigned num_map_tasks = create_map_tasks();
        unsigned num_map_tasks_failed = finish_tasks();

        std::cout << "Mapping phase completed! Start reducing phase...\n" << std::flush;

        unsigned num_reduce_tasks = create_reduce_tasks();
        unsigned num_reduce_tasks_failed = finish_tasks();

        std::cout << "Reducing phase completed! Start summarizing phase...\n" << std::flush;

        summarize_reducer_output();
        std::cout << "Finished summarizing reducer output! Shutting down worker pool...\n" << std::flush;

        std::cout << std::to_string(num_map_tasks_failed) + " out of " + std::to_string(num_map_tasks) + " map tasks failed\n" << std::flush;
        std::cout << std::to_string(num_reduce_tasks_failed) + " out of " + std::to_string(num_reduce_tasks) + " reduce tasks failed\n" << std::flush;

        WorkerPoolManager::signal_termination();
        m_worker_pool->wait();
        m_worker_pool->close();

        g_master_run_spec = nullptr;
        return true;
    }

private:

    void summarize_reducer_output() const
    {
        const boost::filesystem::path output_dir = m_spec.get_output_dir();

        boost::filesystem::directory_iterator begin(output_dir);
        boost::filesystem::directory_iterator end;
        auto file_range = boost::make_iterator_range(begin, end);

        std::multiset< std::string > sorted_lines;

        for (const auto dir_entry : file_range)
        {
            const auto path = dir_entry.path();
            const std::string filename = path.filename().string();

            const std::regex expr("^reduce_k(\\d+)_w(\\d+)$");
            std::smatch what;
            const bool match_success = std::regex_search(filename, what, expr);

            if (match_success)
            {
                std::ifstream ifs(path.string());

                for (std::string line; std::getline(ifs, line);)
                {
                    sorted_lines.insert(std::move(line));
                }
            }
        }

        {
            auto final_output_path = output_dir;
            final_output_path /= "reduce_final";

            std::ofstream ofs(final_output_path.string());
            for (const auto & line : sorted_lines)
            {
                ofs << line << std::endl;
            }
        }
    }

    unsigned create_map_tasks()
    {
        m_task_pool.clear();
        m_task_pool.reserve(m_shards.size());

        for (const auto & shard : m_shards)
        {
            m_task_pool.emplace_back( std::make_unique<MapTask>(shard) );
        }
        return boost::numeric_cast<unsigned>(m_task_pool.size());
    }

    unsigned create_reduce_tasks()
    {
        const boost::filesystem::path output_dir = m_spec.get_output_dir();

        boost::filesystem::directory_iterator begin(output_dir);
        boost::filesystem::directory_iterator end;
        auto file_range = boost::make_iterator_range(begin, end);

        std::vector< std::vector<std::string> > hash_to_file_list;

        for (auto dir_entry : file_range)
        {
            std::string filename = dir_entry.path().filename().string();

            std::regex expr("^map_k(\\d+)_w(\\d+)$");
            std::smatch what;
            bool match_success = std::regex_search(filename, what, expr);

            if (match_success)
            {
                const unsigned hash = boost::lexical_cast<unsigned>(what[1].str());
                const unsigned worker_id = boost::lexical_cast<unsigned>(what[2].str());

                if (hash_to_file_list.size() <= hash)
                {
                    hash_to_file_list.resize(hash + 1);
                }

                std::vector<std::string> & file_list = hash_to_file_list[hash];

                if (file_list.size() <= worker_id)
                {
                    file_list.resize(worker_id + 1);
                }

                boost::filesystem::path filepath = output_dir;
                filepath /= filename;
                file_list[worker_id] = filepath.string();
            }
        }

        m_task_pool.clear();
        for (auto & file_list : hash_to_file_list)
        {
            m_task_pool.emplace_back(  std::make_unique<ReduceTask>( std::move(file_list) )  );
        }

        return boost::numeric_cast<unsigned>(m_task_pool.size());
    }

    unsigned finish_tasks()
    {
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

        WorkerIter worker_iter( m_worker_pool->get_num_workers() );
        for (;;)
        {
            // Scan through all tasks
            unsigned num_keep_waiting = 0;
            for ( const std::unique_ptr<Task> & task : m_task_pool )
            {
                if (task->should_wait())
                {
                    ++num_keep_waiting;
                }

                if (task->should_assign())
                {
                    // Loop until can find an available worker to assign
                    while ( !m_worker_pool->get_worker(*++worker_iter)->try_assign_task(task.get()) );
                }
            }

            if (num_keep_waiting == 0)
            {
                break;
            }
        }

        return boost::numeric_cast<unsigned>(
            std::count_if(m_task_pool.cbegin(), m_task_pool.cend(),
            [](const std::unique_ptr<Task> & task)
            {
                return task->perm_fail();
            }))  ;


    }

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

            constexpr bool skip_check = true;
            for (;;)
            {
                if (skip_check)
                {
                    break;
                }

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

        BOOST_ASSERT_MSG(success, "Failed creating output directory.");
    }

    const MapReduceSpec & m_spec;
    const std::vector<FileShard> & m_shards;

    std::unique_ptr<WorkerPoolManager> m_worker_pool;
    std::vector<std::unique_ptr<Task>> m_task_pool;


};



