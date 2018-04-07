#pragma once

#include <boost/assert.hpp>

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>
#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"

using ScopedLock = std::unique_lock<std::mutex>;
constexpr auto kClockType = GPR_CLOCK_MONOTONIC;

using TaskType = typename masterworker::TaskRequest::TaskType;
constexpr TaskType kMapTaskType = masterworker::TaskRequest::kMap;
constexpr TaskType kReduceTaskType = masterworker::TaskRequest::kReduce;

constexpr char kDelimiter = ' ';



using WorkerErrorEnum = masterworker::WorkerErrorEnum ;

inline const char * worker_error_enum_to_string(WorkerErrorEnum e)
{
    switch(e)
    {
    case WorkerErrorEnum::kUnknownError:
        return "kUnknownError";
    case WorkerErrorEnum::kGood:
        return "kGood";
    case WorkerErrorEnum::kInputFileLengthError:
        return "kInputFileLengthError";
    case WorkerErrorEnum::kInputFileRangeError:
        return "kInputFileRangeError";
    case WorkerErrorEnum::kInputFileNewlineError:
        return "kInputFileNewlineError";
    case WorkerErrorEnum::kInputFileGetlineError:
        return "kInputFileGetlineError";
    case WorkerErrorEnum::kTaskTypeError:
        return "kTaskTypeError";
    case WorkerErrorEnum::kTaskProcessorNotFoundError:
        return "kTaskProcessorNotFoundError";
    case WorkerErrorEnum::kWorkerBusyError:
        return "kWorkerBusyError";
    case WorkerErrorEnum::kReceptionError:
        return "kReceptionError";
    case WorkerErrorEnum::kReduceTokenizeError:
        return "kReduceTokenizeError";
    case WorkerErrorEnum::kReduceKeyHashConsistencyError:
        return "kReduceKeyHashConsistencyError";
    default:
        return "_incomplete_switch_statements_";
    }
}

class WorkerConfig
{
    struct PrivateCtorKey {};

public:
    static void install_config( masterworker::WorkerConfig && config )
    {
        s_inst.reset();
        s_inst = std::make_unique<WorkerConfig>(
              PrivateCtorKey()
            , std::move( *config.mutable_output_dir() )
            , config.num_output_files()
            , config.worker_uid()
        );
        std::cout << "Configuration success!\n" << std::flush;

    }

    static const WorkerConfig * get_inst()
    {
        return s_inst.get();
    }

    WorkerConfig(
          const PrivateCtorKey &
        , std::string && output_dir
        , unsigned num_output_files
        , unsigned worker_uid
    ) :
          m_output_dir( std::move(output_dir) )
        , m_num_output_files( num_output_files )
        , m_worker_uid( worker_uid )
    {
        BOOST_ASSERT(!m_output_dir.empty());
        BOOST_ASSERT(m_num_output_files > 0);
    }

    // Outside code only have read only access
    // so making this public isn't a concern
    std::string m_output_dir;
    unsigned m_num_output_files = 1;
    unsigned m_worker_uid = 0;

private:

    static std::unique_ptr<WorkerConfig> s_inst;
};

inline size_t compute_hash_for_key(const std::string & key)
{
    const WorkerConfig * config = WorkerConfig::get_inst();
    BOOST_ASSERT(config);

    std::hash<std::string> hash_fn;

    size_t hash = hash_fn(key) % (config->m_num_output_files);
    return hash;
}

inline gpr_timespec & operator+=(gpr_timespec & a, const gpr_timespec & b)
{
    BOOST_ASSERT(a.clock_type == b.clock_type);
    constexpr int32_t ns_in_sec = 1'000'000'000;

    a.tv_nsec += b.tv_nsec;
    a.tv_sec += b.tv_sec;

    if (a.tv_nsec >= ns_in_sec)
    {
        a.tv_nsec -= ns_in_sec;
        ++a.tv_sec;
    }

    return a;
}

inline gpr_timespec operator+(const gpr_timespec & a, const gpr_timespec & b)
{
    auto aa = a;
    aa += b;
    return aa;
}

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

inline constexpr gpr_timespec make_milliseconds(int32_t mills, gpr_clock_type clock_type = kClockType)
{
    BOOST_ASSERT(mills >= 0);
    constexpr int32_t mills_in_sec = 1'000;
    constexpr int32_t nanos_in_mills = 1'000'000;

    gpr_timespec time {0, 0, clock_type};
    time.clock_type = clock_type;
    time.tv_sec = mills / mills_in_sec;
    time.tv_nsec = (mills % mills_in_sec) * nanos_in_mills;

    return time;
}

inline bool operator<(const gpr_timespec & a, const gpr_timespec & b)
{
    BOOST_ASSERT(a.clock_type == b.clock_type);

    if (a.tv_sec != b.tv_sec)
    {
        return a.tv_sec < b.tv_sec;
    }
    else if (a.tv_nsec != b.tv_nsec)
    {
        return a.tv_nsec < b.tv_nsec;
    }
    else
    {
        return false;
    }
}

inline bool operator==(const gpr_timespec & a, const gpr_timespec & b)
{
    BOOST_ASSERT(a.clock_type == b.clock_type);

    return (a.tv_sec == b.tv_sec)
        && (a.tv_nsec == b.tv_nsec);
}

inline bool operator!=(const gpr_timespec & a, const gpr_timespec & b)
{
    return !(a == b);
}

inline bool operator<=(const gpr_timespec & a, const gpr_timespec & b)
{
    return (a < b) || (a == b);
}

inline bool operator>(const gpr_timespec & a, const gpr_timespec & b)
{
    return !(a <= b);
}

inline bool operator>=(const gpr_timespec & a, const gpr_timespec & b)
{
    return (a == b) || (a > b);
}

constexpr gpr_timespec kIdleTimeout = make_milliseconds(15'000, kClockType);

