#pragma once

#define BOOST_ENABLE_ASSERT_HANDLER
#include <boost/assert.hpp>

#include <string>
#include <iostream>
#include <sstream>

#include <csignal>
#include <cstdlib>
#include <cstring>

#include <unistd.h>
#include <execinfo.h>
#include <dlfcn.h>

#include <tbb/atomic.h>

//// Begin class FaultHandlerSetter ////
class FaultHandlerSetter
{
    static constexpr const char * SignalToString(int sig)
    {
        switch (sig)
        {
        case SIGTERM:
            return "SIGTERM";
        case SIGSEGV:
            return "SIGSEGV";
        case SIGINT:
            return "SIGINT";
        case SIGILL:
            return "SIGILL";
        case SIGABRT:
            return "SIGABRT";
        case SIGFPE:
            return "SIGFPE";
        default:
            return "Unknown";
        }
    }

    // Linux only
    static std::string GetExeName()
    {
        constexpr int kBufSize = 1024;
        char buf[kBufSize] = {0};

        ssize_t size = readlink("/proc/self/exe", buf, kBufSize);
        BOOST_ASSERT_MSG(buf[kBufSize-1] == 0, "buf not NULL-terminated!");
        BOOST_ASSERT_MSG(size > 0, "Nothing returned");

        // Find the iter to first character after the last "/"
        std::string path(buf);

        auto riter = path.rbegin();
        for (; riter != path.rend(); ++riter)
        {
            if (*riter == '/')
            {
                break;
            }
        }

        std::string retval(riter.base(), path.end());
        return retval;
    }


    // Linux only
    static void PrintStackTrace()
    {
        constexpr int kMaxStackDepth = 1024;

        // Retrieve call stack addresses
        void * addr_array[kMaxStackDepth] = {0};
        const int addr_array_size = backtrace(addr_array, kMaxStackDepth);

        if (addr_array_size >= kMaxStackDepth)
        {
            std::cerr << "Warning: In function PrintStackTrace, the complete call stack couldn't fit into array addr_array\n" << std::flush;
        }

        // Translate to symbols and print to stderr

        const std::string exe_name = GetExeName();
        std::cerr << ("Current exe: " + exe_name + "\n") << std::flush;

        BOOST_ASSERT(!exe_name.empty());


        char ** symbol_array = backtrace_symbols(addr_array, addr_array_size);

        std::cerr << "Stack Trace:\n" << std::flush;
        for (int i = 0; i < addr_array_size; ++i)
        {
            std::ostringstream oss;

            oss << "#" + std::to_string(i) + " ";

            {
                const char * line = symbol_array[i];
                oss << std::string(line) << " : ";
            }

            {
                Dl_info info;
                dladdr(addr_array[i], &info);
                if (info.dli_sname)
                    oss << info.dli_sname << " ";
            }

            {
                std::ostringstream sys_cmd;
                sys_cmd << "addr2line " << addr_array[i] << " -C -f -i -p -e " << exe_name;

                FILE * fp = popen(sys_cmd.str().c_str(), "r");
                BOOST_ASSERT(fp);

                for (;;)
                {
                    constexpr int kBufSize = 1024;
                    char buf[kBufSize] = {0};

                    char * rbuf = fgets(buf, kBufSize, fp);
                    if (rbuf)
                    {
                        std::string fileline(rbuf);
                        while (fileline.back() == '\n')
                        {
                            fileline.pop_back();
                        }
                        oss << fileline << " ";
                    }
                    else
                    {
                        break;
                    }
                }

                pclose(fp);
            }

            oss << std::endl;
            std::cerr << oss.str() << std::flush;
        }

        free(symbol_array);
    }

    static void FaultHandler(int signum)
    {
        // Avoid recursion
        std::signal(signum, SIG_DFL);

        const std::string sig_name = std::to_string(signum) + " " + SignalToString(signum);

        std::cerr << ("FaultHandler starts for signal: " + sig_name + "\n") << std::flush;

        PrintStackTrace();

        std::raise(signum);
    }

public:
    FaultHandlerSetter()
    {
        static tbb::atomic<bool> already_set = false;
        bool was_already_set = already_set.fetch_and_store(true);

        if (!was_already_set)
        {
            std::cout << "Setting FaultHandler!!\n" << std::flush;
            // The function address is different from multiple translation units, but meh.
            std::signal(SIGSEGV, &FaultHandler);
            std::signal(SIGABRT, &FaultHandler);
            std::signal(SIGILL, &FaultHandler);
            std::signal(SIGBUS, &FaultHandler);
            std::signal(SIGFPE, &FaultHandler);
            std::signal(SIGINT, &FaultHandler);
        }
    }
};
//// End class FaultHandlerSetter ////

namespace boost
{
    inline void assertion_failed_msg(
        char const * expr, char const * msg, char const * func, char const * file, long line)
    {
        std::ostringstream oss;
        oss << "Assertion failed at file " << file << " line " << line << std::endl;
        oss << "Function: " << func << std::endl;
        oss << "Expression: " << expr << std::endl;
        oss << "Message: " << msg << std::endl;

        std::cerr << oss.str() << std::flush;
        std::abort();
    }

    inline void assertion_failed(
        char const * expr, char const * func, char const * file, long line)
    {
        assertion_failed_msg(expr, "", func, file, line);
    }
}
