#pragma once

#include <thread>

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>
#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"

#include <mr_task_factory.h>
#include "mr_tasks.h"


std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

class Executor
{
public:
    bool is_busy() const
    {
        return true;
    }

private:
};

// class WorkerServiceImpl final : public WorkerService::Service
// {
//  grpc::Service RoutineName(
//           grpc::ServerContext * context
//         , const masterworker::TaskRequest & request
//         , masterworker::TaskAck * reply
//  ) override
//  {

//      return grpc::Status::OK;
//  }
// };

class Listener
{
    class CallData
    {
        enum class Status
        {
            kRequesting,
            kFinishing,
            kDone
        };

    public:
        CallData(
            Executor & executor
            , masterworker::WorkerService::AsyncService & service
            , grpc::ServerCompletionQueue & cq
        ) :
              m_executor(executor)
            , m_service(service)
            , m_cq(cq)
            , responder(&m_context)
        {

        }

    private:
        Executor & m_executor;
        masterworker::WorkerService::AsyncService & m_service;
        grpc::ServerCompletionQueue & m_cq;

        // The responder depends on context, so make it ordered this way.
        grpc::ServerContext m_context;
        grpc::ServerAsyncResponseWriter<masterworker::TaskAck> responder;

        masterworker::TaskRequest request;
        masterworker::TaskAck reply;
        Status m_status = Status::kRequesting;

    };

public:

    Listener(const std::string & addr)
    {
        m_listening_thread = std::thread([&]() { start_listening_loop(addr); });
    }

    void start_listening_loop(const std::string & addr)
    {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
        builder.RegisterService(&m_service);

        m_cq = builder.AddCompletionQueue();
        m_server = builder.BuildAndStart();


        for (;;)
        {

        }
    }

    void wait()
    {
        m_listening_thread.join();
    }

private:
    std::thread m_listening_thread;

    masterworker::WorkerService::AsyncService m_service;

    std::unique_ptr<grpc::ServerCompletionQueue> m_cq;
    std::unique_ptr<grpc::Server> m_server;

    Executor m_executor;
};

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
    This is a big task for this project, will test your understanding of map reduce */
class Worker
{

public:
    /* DON'T change the function signature of this constructor */
    /* CS6210_TASK: ip_addr_port is the only information you get when started.
        You can populate your other class data members here if you want */
    Worker(std::string ip_addr_port)
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
        /*  Below 5 lines are just examples of how you will call map and reduce
            Remove them once you start writing your own logic */
        std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
        auto mapper = get_mapper_from_task_factory("cs6210");
        mapper->map("I m just a 'dummy', a \"dummy line\"");
        auto reducer = get_reducer_from_task_factory("cs6210");
        reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));
        return true;
    }

private:
    /* NOW you can add below, data members and member functions as per the need of your implementation*/


};


