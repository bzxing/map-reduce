#pragma once

#include <thread>
#include <list>
#include <memory>
#include <utility>

#include <boost/numeric/conversion/cast.hpp>
#include <boost/assert.hpp>

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>
#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"

#include <mr_task_factory.h>
#include "mr_tasks.h"


using masterworker::TaskAck;
using masterworker::TaskRequest;


std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);


namespace
{
    static constexpr auto kClockType = GPR_CLOCK_MONOTONIC;
    static constexpr int kSendReplyTimeout = 5;
    static constexpr int kServerCqTimeout = 2;
    static constexpr unsigned kCallListGarbageCollectInterval = 16;

    inline gpr_timespec operator-(const gpr_timespec & a, const gpr_timespec & b)
    {
        BOOST_ASSERT(a.clock_type == b.clock_type);

        int32_t nano_diff = a.tv_nsec - b.tv_nsec;
        int64_t sec_diff = a.tv_sec - b.tv_sec;

        if (nano_diff < 0)
        {
            constexpr int32_t ns_in_sec = 1'000'000'000;
            nano_diff += ns_in_sec;
            --sec_diff;
        }

        gpr_timespec diff;
        diff.tv_sec = sec_diff;
        diff.tv_nsec = nano_diff;
        diff.clock_type = a.clock_type;
        return diff;
    }
}




class Executor
{
public:
    bool is_busy() const
    {
        return true;
    }

private:
};

class Server
{
    //// Begin class CallData ////
    class CallData
    {
    public:
        CallData(
            masterworker::WorkerService::AsyncService & service
          , grpc::ServerCompletionQueue & cq
        ) :
            m_responder(&m_context)
        {
            m_request.set_task_uid( std::numeric_limits<unsigned>::max() );
            request_call(service, cq);
        }

        void send_reply(bool success)
        {
            BOOST_ASSERT(!m_send_reply_time);
            m_send_reply_time = std::make_unique<gpr_timespec>(gpr_now(kClockType));

            // Assemble reply message
            m_reply = TaskAck();
            m_reply.set_task_uid( m_request.task_uid() );
            m_reply.set_success(success);

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

    private:

        bool send_reply_has_timed_out( const gpr_timespec & curr_time ) const
        {
            if (m_send_reply_time)
            {
                gpr_timespec time_diff = curr_time - *m_send_reply_time;
                if (time_diff.tv_sec >= kSendReplyTimeout)
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
        grpc::ServerAsyncResponseWriter<TaskAck> m_responder;

        TaskRequest m_request;
        TaskAck m_reply;

        std::unique_ptr<gpr_timespec> m_send_reply_time;
        bool m_reply_returned = false;
    };
    //// End class CallData ////



public:

    Server(const std::string & addr)
    {
        m_listening_thread = std::thread([&]() { start_listening_loop(addr); });
    }

    void start_listening_loop(const std::string & addr)
    {
        using CqNextStatus = grpc::CompletionQueue::NextStatus;

        grpc::ServerBuilder builder;
        builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
        builder.RegisterService(&m_service);

        m_cq = builder.AddCompletionQueue();
        m_server = builder.BuildAndStart();

        // Create a list to store all active CallData objects.
        // We can go through the list regularly to collect timed-out calls that
        // never return from completion queue
        std::list<CallData> call_list;

        // Create the first CallData object and call the constructor.
        // The constructor will open the worker up for incoming calls.
        call_list.emplace_back(m_service, *m_cq);

        std::cout << "Start listening on " + addr + "\n" << std::flush;


        // Loop waiting on the completion queue.
        // The queue will return either an incoming request,
        // or a finished outgoing reply.
        //
        // For incoming request, if the executor is free we delegate executor
        // to do the work then send a reply back with success.
        // If the executor is busy, we send a reply back with failure right away.
        //
        // For finished outgoing request, we need to clean it up from the call_list.
        //
        // At regular interval, also need to clean up CallData objects on the call_list
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
                for (auto iter = call_list.begin(); iter != call_list.end(); )
                {
                    if (iter->should_destroy(curr_time))
                    {
                        // std::cout << "Destroying request for Task "
                        //     + std::to_string(iter->get_task_uuid()) + "\n" << std::flush;
                        iter = call_list.erase(iter);
                        ++destruction_count;
                    }
                    else
                    {
                        ++iter;
                    }
                }

                std::cout << "Destroyed " + std::to_string(destruction_count) + " requests. "
                    + std::to_string(call_list.size()) + " left pending.";
            }

            // Process returned request on the completion queue
            void * tag = nullptr;
            bool ok = false;
            gpr_timespec deadline = gpr_now(kClockType);
            deadline.tv_sec += kServerCqTimeout;

            CqNextStatus status = m_cq->AsyncNext(&tag, &ok, deadline);

            if ( status == CqNextStatus::GOT_EVENT && ok )
            {
                CallData * call_data = static_cast<CallData *>(tag);

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
                    // We'll first sent the reply back
                    call_data->send_reply(true); // Mock a success here
                    std::cout << "Sent reply for task " + std::to_string(call_data->get_task_uuid()) + "\n" << std::flush;

                    // Then invite a new call in
                    call_list.emplace_back(m_service, *m_cq);
                }
            }
        }
    }

    void wait()
    {
        m_listening_thread.join();
    }

private:
    masterworker::WorkerService::AsyncService m_service;

    std::unique_ptr<grpc::Server> m_server;
    std::unique_ptr<grpc::ServerCompletionQueue> m_cq;

    Executor m_executor;

    std::thread m_listening_thread;
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
        // auto mapper = get_mapper_from_task_factory("cs6210");
        // mapper->map("I m just a 'dummy', a \"dummy line\"");
        // auto reducer = get_reducer_from_task_factory("cs6210");
        // reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));

        Server server(m_address);

        server.wait();
        return true;
    }

private:
    /* NOW you can add below, data members and member functions as per the need of your implementation*/
    std::string m_address;
    FaultHandlerSetter m_fault_handler_setter; // Hack...

};


