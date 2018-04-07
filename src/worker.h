#pragma once

#include <thread>
#include <mutex>
#include <condition_variable>
#include <list>
#include <memory>
#include <utility>
#include <string>
#include <vector>
#include <map>

#include <iostream>
#include <sstream>
#include <fstream>

#include <boost/numeric/conversion/cast.hpp>
#include <boost/assert.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>
#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"

#include <mr_task_factory.h>
#include "mr_tasks.h"

#include "my_utils.h"


std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);



constexpr gpr_timespec kServerCqTimeout = make_milliseconds(500, kClockType);
constexpr unsigned kCallListGarbageCollectInterval = 16;

extern tbb::atomic<bool> s_terminate_worker;


template <class Func>
inline std::tuple<bool, WorkerErrorEnum, unsigned>
for_each_line_in_file_subrange(
      std::ifstream & ifs
    , const unsigned offset
    , unsigned length
    , Func do_line // must accept a mutable rvalue reference to a std::string
    )
{
    const char * fail_reason = nullptr;

    unsigned num_lines_read = 0;
    bool success = true;
    WorkerErrorEnum err_e = WorkerErrorEnum::kGood;

    // Get file length
    ifs.clear();
    ifs.seekg(0, ifs.end);
    const unsigned file_length = boost::numeric_cast<unsigned>(  std::max( (long)ifs.tellg(), 0l )  );

    // Translate zero read length request to entire file length
    if (success)
    {
        if (offset == 0 && length == 0)
        {
            length = file_length; // requesting offset == length == 0 means read whole file.
        }
    }

    // Fail directly if file has 0 length
    if (success)
    {
        if (file_length <= 0)
        {
            success = false;
            err_e = WorkerErrorEnum::kInputFileLengthError;
        }
    }

    // Fail if shard is out of file's range
    if (success)
    {
        success = (offset + length <= file_length);
        if (!success)
        {
            err_e = WorkerErrorEnum::kInputFileRangeError;
        }
    }


    // Check whether last character in the shard is newline or EOF.
    if (success)
    {
        ifs.clear();
        ifs.seekg( offset + length - 1 );
        auto c = ifs.get();

        std::ifstream::int_type cc = ifs.get();
        if ( !ifs.eof() )
        {
            success = (ifs.good()) && (c == '\n');
        }

        if (!success)
        {
            err_e = WorkerErrorEnum::kInputFileNewlineError;
        }
    }

    // Check offset-1 is newline character.
    // Skip this check if offset is 0
    if (success)
    {
        if (offset > 0)
        {
            ifs.clear();
            ifs.seekg( offset - 1 );
            auto c = ifs.get();
            success = (ifs.good()) && (c == '\n');
        }

        if (!success)
        {
            err_e = WorkerErrorEnum::kInputFileNewlineError;
        }
    }

    if (success)
    {
        ifs.clear();
        ifs.seekg( offset );

        std::string my_line;
        while ( (ifs.tellg() < offset + length) && ifs && success )
        {
            my_line.clear();

            std::getline(ifs, my_line);

            if (!my_line.empty())
            {
                std::tie(success, err_e) = do_line(std::move(my_line));
                if (success) ++num_lines_read;
            }
        }
    }

    if (success)
    {
        success = (ifs.tellg() >= offset + length) || ifs.eof();

        if (!success)
        {
            err_e = WorkerErrorEnum::kInputFileGetlineError;
        }
    }

    ifs.clear();
    ifs.seekg(0);

    return std::make_tuple(success, err_e, num_lines_read);
}

class CloseCallData // Does not mean "close call"...
{
public:
    CloseCallData(
        masterworker::WorkerService::AsyncService & service
      , grpc::ServerCompletionQueue & cq
    ) :
        m_responder(&m_context)
    {
        request_call(service, cq);
    }

    void send_reply()
    {
        // Call Finish
        m_reply.set_success(true);
        m_responder.Finish(m_reply, grpc::Status::OK, this);
    }


private:

    void request_call(
          masterworker::WorkerService::AsyncService & service
        , grpc::ServerCompletionQueue & cq
    )
    {
        service.RequestClose(
              &m_context
            , &m_request
            , &m_responder
            , &cq
            , &cq
            , this
        );
    }

    //Executor & m_executor;
    //masterworker::WorkerService::AsyncService & m_service;
    //grpc::ServerCompletionQueue & m_cq;

    // The m_responder depends on m_context,
    // so make m_context must appear before m_responder
    grpc::ServerContext m_context;
    grpc::ServerAsyncResponseWriter<masterworker::Ack> m_responder;

    masterworker::Ack m_request;
    masterworker::Ack m_reply;
};


class DispatchTaskToWorkerCallData
{
public:
    DispatchTaskToWorkerCallData(
        masterworker::WorkerService::AsyncService & service
      , grpc::ServerCompletionQueue & cq
    ) :
        m_responder(&m_context)
    {
        m_request.set_task_uid( std::numeric_limits<unsigned>::max() );
        request_call(service, cq);
    }

    void send_reply(bool success, WorkerErrorEnum err_e)
    {
        BOOST_ASSERT(!m_send_reply_time);
        m_send_reply_time = std::make_unique<gpr_timespec>(gpr_now(kClockType));

        // Assemble reply message
        m_reply = masterworker::TaskAck();
        m_reply.set_task_uid( m_request.task_uid() );
        m_reply.set_success( success );
        m_reply.set_error_enum( err_e );


        // Call Finish
        m_responder.Finish(m_reply, grpc::Status::OK, this);
    }

    void set_reply_returned()
    {
        m_reply_returned = true;

        // const unsigned task_uuid = get_task_uuid();
        // std::cout << "Reply returned for task " + std::to_string(task_uuid) + "\n" << std::flush;
    }

    bool has_sent_reply() const
    {
        return m_send_reply_time != nullptr;
    }

    bool should_destroy( const gpr_timespec & curr_time ) const
    {
        return m_reply_returned || send_reply_has_timed_out(curr_time);
    }

    unsigned get_task_uuid() const
    {
        return m_request.task_uid();
    }

    const masterworker::TaskRequest & get_request() const
    {
        return m_request;
    }

private:

    bool send_reply_has_timed_out( const gpr_timespec & curr_time ) const
    {
        if (m_send_reply_time)
        {
            gpr_timespec time_diff = curr_time - *m_send_reply_time;
            if (time_diff >= kIdleTimeout)
            {
                return true;
            }
        }
        return false;
    }

    void request_call(
          masterworker::WorkerService::AsyncService & service
        , grpc::ServerCompletionQueue & cq
    )
    {
        service.RequestDispatchTaskToWorker(
              &m_context
            , &m_request
            , &m_responder
            , &cq
            , &cq
            , this
        );
    }

    //Executor & m_executor;
    //masterworker::WorkerService::AsyncService & m_service;
    //grpc::ServerCompletionQueue & m_cq;

    // The m_responder depends on m_context,
    // so make m_context must appear before m_responder
    grpc::ServerContext m_context;
    grpc::ServerAsyncResponseWriter<masterworker::TaskAck> m_responder;

    masterworker::TaskRequest m_request;
    masterworker::TaskAck m_reply;

    std::unique_ptr<gpr_timespec> m_send_reply_time;
    bool m_reply_returned = false;
};


class Executor
{
public:
    Executor()
    {
        m_executor_thread = std::thread(  [this]() { executor_thread(); }  );
    }

    // If executor is available, pass the call to the executor thread. The thread will
    // be responsible for replying to the caller.
    //
    // If executor is busy, reject the call and send a reply directly.
    void async_execute_and_reply(DispatchTaskToWorkerCallData * const call)
    {
        bool success = try_set_call(call);
        if (!success)
        {
            std::cout << "Rejecting task " + std::to_string(call->get_task_uuid()) + "!\n" << std::flush;
            call->send_reply( false, WorkerErrorEnum::kWorkerBusyError );
        }
    }

    void wait()
    {
        m_executor_thread.join();
    }

private:

    // If the executor is free, occupy it by setting m_call pointer,
    // notify the executor thread, and return true. The executor
    // thread will pick the call up
    //
    // Else return false.
    bool try_set_call(DispatchTaskToWorkerCallData * const call)
    {
        BOOST_ASSERT(call != nullptr);
        ScopedLock lock(m_call_ptr_mutex);

        DispatchTaskToWorkerCallData * snapshot = m_call;
        if (!snapshot)
        {
            m_call = call;
            m_call_ptr_cv.notify_one();
            return true;
        }

        return false;
    }

    void executor_thread()
    {
        while(!s_terminate_worker)
        {
            DispatchTaskToWorkerCallData * call = wait_for_call();
            if (!call)
            {
                continue;
            }

            std::cout << "Accepting task " + std::to_string(call->get_task_uuid()) + "!\n" << std::flush;

            auto status = execute_task_internal(call);

            std::cout << "Task " + std::to_string(call->get_task_uuid()) + " execution " +
                (status.first ? "succeeded" : "failed") + "! error_enum=" +
                worker_error_enum_to_string(status.second) + "\n" << std::flush;

            call->send_reply(status.first, status.second);

            unset_call(call);
        }
    }

    // Wait until a call arrives, then return the
    // arrived call pointer value.
    DispatchTaskToWorkerCallData * wait_for_call()
    {
        ScopedLock lock(m_call_ptr_mutex);

        while (!m_call)
        {
            m_call_ptr_cv.wait_for(lock, std::chrono::milliseconds(100));

            if (s_terminate_worker)
            {
                return nullptr;
            }
        }

        return m_call;
    }

    // Invoke the mapper or reducer
    std::pair<bool, WorkerErrorEnum> execute_task_internal(DispatchTaskToWorkerCallData * call)
    {
        BOOST_ASSERT(call);

        // Artificially slow things down
        // std::this_thread::sleep_for(std::chrono::seconds(1));

        const auto & request = call->get_request();
        TaskType task_type = request.task_type();

        if (task_type == kMapTaskType)
        {
            return execute_map_task(call);
        }
        else if (task_type == kReduceTaskType)
        {
            return execute_reduce_task(call);
        }
        else
        {
            // Failed due to unknown task type!
            return std::make_pair(false, WorkerErrorEnum::kTaskTypeError);
        }
    }

    std::pair<bool, WorkerErrorEnum> execute_map_task(DispatchTaskToWorkerCallData * call)
    {
        // Perform a series of steps. If one step fails, exit directly while printing a message.
        // If all steps succeeds, don't print anything.

        const auto & request = call->get_request();
        TaskType task_type = request.task_type();
        const std::string & user_id = request.user_id();
        const unsigned task_uuid = call->get_task_uuid();

        BOOST_ASSERT(task_type == kMapTaskType);

        // Get mapper
        auto mapper_ptr = get_mapper_from_task_factory(user_id);
        if (!mapper_ptr)
        {
            return std::make_pair(false, WorkerErrorEnum::kTaskProcessorNotFoundError);
        }

        // Read each record in the input shard, and write to output file
        for (const auto & shard : request.input_file())
        {
            const std::string & filename = shard.filename();
            unsigned offset = shard.offset();
            unsigned length = shard.length();


            std::ifstream ifs(filename);

            bool success = false;
            WorkerErrorEnum err_e = WorkerErrorEnum::kUnknownError;
            unsigned num_lines_read = 0;
            std::tie(success, err_e, num_lines_read) = for_each_line_in_file_subrange(ifs, offset, length,
                [&](const std::string & line)
                {
                    mapper_ptr->map(line);
                    return std::make_pair(true, WorkerErrorEnum::kGood);
                }
            );

            if (!success)
            {
                return std::make_pair(false, err_e);
            }
        }

        return std::make_pair(true, WorkerErrorEnum::kGood);
    }

    std::pair<bool, WorkerErrorEnum> execute_reduce_task(DispatchTaskToWorkerCallData * call)
    {
        const auto & request = call->get_request();
        TaskType task_type = request.task_type();
        const std::string & user_id = request.user_id();
        const unsigned task_uuid = call->get_task_uuid();

        BOOST_ASSERT(task_type == kReduceTaskType);

        auto reducer_ptr = get_reducer_from_task_factory(user_id);
        if (!reducer_ptr)
        {
            return std::make_pair(false, WorkerErrorEnum::kTaskProcessorNotFoundError);
        }

        // Start building an ordered map from keys to all values of every key.
        std::map< std::string, std::vector<std::string> > kv_set;

        for (const auto & shard : request.input_file())
        {
            const std::string & filename = shard.filename();
            unsigned offset = shard.offset();
            unsigned length = shard.length();


            std::ifstream ifs(filename);

            bool success = false;
            WorkerErrorEnum err_e = WorkerErrorEnum::kUnknownError;
            unsigned num_lines_read = 0;
            std::tie(success, err_e, num_lines_read) = for_each_line_in_file_subrange(ifs, offset, length,
                //// Start Lambda Expression ////
                [&](const std::string & line)
                {
                    std::vector<std::string> kv;
                    boost::split(kv, line,
                        [](char c){ return c == kDelimiter; }, boost::token_compress_on);

                    if (kv.size() != 2)
                    {
                        return std::make_pair(false, WorkerErrorEnum::kReduceTokenizeError);
                    }

                    auto iter =
                        kv_set.emplace(
                            std::piecewise_construct,
                            std::forward_as_tuple( std::move(kv[0]) ),
                            std::forward_as_tuple()
                        ).first;

                    iter->second.push_back( std::move(kv[1]) );

                    // Not a real return: we're in a lambda expression right now.
                    return std::make_pair(true, WorkerErrorEnum::kGood);
                }
                //// End Lambda Expression ////
            );

            // Failed building the map. Early exit here.
            if (!success)
            {
                return std::make_pair(false, err_e);
            }
        }

        {
            bool scanned_first = false;
            size_t prev_hash = 0;
            bool success = true;
            for (const auto & p : kv_set)
            {
                if (!scanned_first)
                {
                    scanned_first = true;
                    prev_hash = compute_hash_for_key(p.first);
                }
                else
                {
                    size_t curr_hash = compute_hash_for_key(p.first);
                    success = (curr_hash == prev_hash);
                    prev_hash = curr_hash;

                    if (!success)
                    {
                        break;
                    }
                }
            }

            if (!success)
            {
                return std::make_pair(false, WorkerErrorEnum::kReduceKeyHashConsistencyError);
            }
        }

        for (const auto & p : kv_set)
        {
            reducer_ptr->reduce(p.first, p.second);
        }

        return std::make_pair(true, WorkerErrorEnum::kGood);
    }


    // Simply unset the call and assert call pointer is the expected value.
    void unset_call(DispatchTaskToWorkerCallData * const call)
    {
        ScopedLock lock(m_call_ptr_mutex);

        DispatchTaskToWorkerCallData * snapshot = m_call;
        BOOST_ASSERT(snapshot == call);

        m_call = nullptr;
    }

    DispatchTaskToWorkerCallData * m_call = nullptr; // Used to indicate whether is busy or not.
    std::mutex m_call_ptr_mutex;
    std::condition_variable m_call_ptr_cv;

    std::thread m_executor_thread;

};

class Server
{
public:
    using CqNextStatus = grpc::CompletionQueue::NextStatus;

    Server(const std::string & addr)
    {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
        builder.RegisterService(&m_service);

        m_cq = builder.AddCompletionQueue();
        m_server = builder.BuildAndStart();

        s_terminate_worker = false;
        m_executor = std::make_unique<Executor>();

        std::cout << "Waiting for configuration on " + addr + "\n" << std::flush;
        wait_for_config();
        setup_work_directory();

        std::cout << "Start accepting tasks on " + addr + "\n" << std::flush;
        start_listening_loop();
    }

    ~Server()
    {
        s_terminate_worker = true;
        m_executor->wait();

        m_cq->Shutdown();
        m_server->Shutdown();

        void * tag;
        bool ok;
        for (;;)
        {
            gpr_timespec deadline = gpr_now(kClockType) + kServerCqTimeout;
            auto status = m_cq->AsyncNext(&tag, &ok, deadline);
            if (status == CqNextStatus::SHUTDOWN)
            {
                break;
            }
        }
    }



private:

    void wait_for_config()
    {
        // States to keep
        grpc::ServerContext context;
        grpc::ServerAsyncResponseWriter<masterworker::Ack> responder(&context);
        masterworker::WorkerConfig config;

        // Request a call
        m_service.RequestSetConfig(
              &context
            , &config
            , &responder
            , m_cq.get()
            , m_cq.get()
            , this
        );

        // Accept a call
        {
            void * tag = nullptr;
            bool ok = false;
            bool success = m_cq->Next(&tag, &ok);

            BOOST_ASSERT_MSG(success && ok && tag == this, "Failed receiving worker configuration");
        }

        // Install the configuration!
        {
            WorkerConfig::install_config( std::move(config) );
        }

        // Return the call
        masterworker::Ack ack;
        {
            ack.set_success(true);
            responder.Finish(ack, grpc::Status::OK, this);
        }


        // Make sure the call will be received
        {
            void * tag = nullptr;
            bool ok = false;
            bool success = m_cq->Next(&tag, &ok);

            BOOST_ASSERT_MSG(success && ok && tag == this, "Failed acknowledging worker configuration");
        }
    }

    void setup_work_directory() const
    {
        const auto * config = WorkerConfig::get_inst();
        BOOST_ASSERT(config);

        const auto & work_dir = config->m_output_dir;

        bool success = boost::filesystem::is_directory(work_dir);
        BOOST_ASSERT_MSG(success, (work_dir + "does not exist!").c_str());
    }


    void start_listening_loop()
    {
        // Create a list to store all active DispatchTaskToWorkerCallData objects.
        // We can go through the list regularly to collect timed-out calls that
        // never return from completion queue


        // Create the first DispatchTaskToWorkerCallData object and call the constructor.
        // The constructor will open the worker up for incoming calls.
        m_call_list.emplace_back(m_service, *m_cq);
        gpr_timespec last_event = gpr_now(kClockType);

        CloseCallData close_call(m_service, *m_cq);

        // Loop waiting on the completion queue.
        // The queue will return either an incoming request,
        // or a finished outgoing reply.
        //
        // For incoming request, if the executor is free we delegate executor
        // to do the work then send a reply back with success.
        // If the executor is busy, we send a reply back with failure right away.
        //
        // For finished outgoing request, we need to clean it up from the m_call_list.
        //
        // At regular interval, also need to clean up DispatchTaskToWorkerCallData objects on the m_call_list
        // that are too old, but we already sent a reply back.
        // These will probably never be cleaned up cuz they're stale.
        for (unsigned i = 1; ; ++i)
        {
            // Garbage collection
            if ( i == kCallListGarbageCollectInterval )
            {
                i = 0;

                const gpr_timespec curr_time = gpr_now(kClockType);

                unsigned destruction_count = 0;
                for (auto iter = m_call_list.begin(); iter != m_call_list.end(); )
                {
                    if (iter->should_destroy(curr_time))
                    {
                        // std::cout << "Destroying request for Task "
                        //     + std::to_string(iter->get_task_uuid()) + "\n" << std::flush;
                        iter = m_call_list.erase(iter);
                        ++destruction_count;
                    }
                    else
                    {
                        ++iter;
                    }
                }
                if (destruction_count > 0)
                {
                    std::cout << "Destroyed " + std::to_string(destruction_count) + " requests. "
                        + std::to_string(m_call_list.size()) + " left pending.\n" << std::flush;
                }
            }

            // Process returned request on the completion queue
            void * tag = nullptr;
            bool ok = false;
            gpr_timespec deadline = gpr_now(kClockType) + kServerCqTimeout;

            CqNextStatus status = m_cq->AsyncNext(&tag, &ok, deadline);

            if ( status == CqNextStatus::GOT_EVENT && ok )
            {
                // Tag matches the close call. Close connection.
                if (tag == &close_call)
                {
                    // Master requests to close the session!
                    close_call.send_reply();

                    // Wait until it returns or sufficient amount of time has passed
                    for (;;)
                    {
                        gpr_timespec deadline = gpr_now(kClockType) + kIdleTimeout;

                        status = m_cq->AsyncNext(&tag, &ok, deadline);

                        // If the tag matches, exit directly
                        if ( status == CqNextStatus::GOT_EVENT && ok && (tag == &close_call) )
                        {
                            break;
                        }

                        // Waited for too long...Break any ways.
                        if ( status == CqNextStatus::TIMEOUT )
                        {
                            break;
                        }
                    }


                    break;
                }
                else
                {
                    last_event = gpr_now(kClockType);

                    DispatchTaskToWorkerCallData * call_data = static_cast<DispatchTaskToWorkerCallData *>(tag);

                    if (call_data->has_sent_reply())
                    {
                        std::cout << "Reply returned for task " + std::to_string(call_data->get_task_uuid()) + "\n" << std::flush;
                        // Case when a reply has been completed.
                        // We'll just mark it as completed so it'll be garbage-collected
                        call_data->set_reply_returned();
                    }
                    else
                    {
                        // Case when receiving an incoming call.
                        m_executor->async_execute_and_reply(call_data);

                        // Then invite a new call in
                        m_call_list.emplace_back(m_service, *m_cq);
                    }
                }
            }
            else
            {
                // Idled for too long. Close it out.
                if (gpr_now(kClockType) - last_event > kIdleTimeout)
                {
                    break;
                }
            }
        }
    }

    masterworker::WorkerService::AsyncService m_service;

    std::unique_ptr<grpc::Server> m_server;
    std::unique_ptr<grpc::ServerCompletionQueue> m_cq;

    std::unique_ptr<Executor> m_executor;
    std::list<DispatchTaskToWorkerCallData> m_call_list;

};

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
    This is a big task for this project, will test your understanding of map reduce */
class Worker
{

public:
    /* DON'T change the function signature of this constructor */
    /* CS6210_TASK: ip_addr_port is the only information you get when started.
        You can populate your other class data members here if you want */
    Worker(std::string address) :
        m_address( std::move(address) )
    {

    }

    /* DON'T change this function's signature */
    /* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks
    from Master, complete when given one and again keep looking for the next one.
    Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and
    BaseReduer's member BaseReducerInternal impl_ directly,
    so you can manipulate them however you want when running map/reduce tasks*/
    bool run()
    {

        for (;;)
        {
            Server server(m_address);
            std::cout << "Restarting server for a new session...\n" << std::flush;
        }

        return true;
    }

private:

    /* NOW you can add below, data members and member functions as per the need of your implementation*/
    std::string m_address;
    FaultHandlerSetter m_fault_handler_setter; // Hack...

};


