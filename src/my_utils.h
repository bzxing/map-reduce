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
