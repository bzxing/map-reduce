#pragma once

#include <string>
#include <vector>
#include <unordered_map>

#include <algorithm>
#include <functional>
#include <regex>

#include <fstream>
#include <iostream>
#include <sstream>

#include <boost/numeric/conversion/cast.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/assert.hpp>

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


/* CS6210_TASK: Create your data structure here for storing spec from the config file */
class MapReduceSpec
{
    struct ParseLineResult
    {
        bool success = true;
        std::string error_description = "Success";
    };

    using FieldParseFunc = std::function< ParseLineResult(MapReduceSpec &, const std::string &) >;

    struct FieldParseData
    {
        unsigned parse_count = 0;
        FieldParseFunc parse_func;
    };



public:
    bool validate() const
    {
        return true;
    }

    MapReduceSpec() {}

    MapReduceSpec(const std::string & config_filename)
    {
        std::ifstream ifs(config_filename);

        unsigned iline = 1;
        unsigned success_count = 0;
        for (std::string line; std::getline(ifs, line); ++iline)
        {
            if (!line.empty())
            {
                std::regex expr("^\\s*([A-Za-z_]+)\\s*=(.*)");
                std::smatch what;
                bool match_success = std::regex_search(line, what, expr);
                if (match_success)
                {
                    const std::string & field = what[1];
                    const std::string & arg = what[2];
                    std::cout << "Parsing config: \"" + field + "\" \"" + arg + "\"\n" << std::flush;

                    // Parse the line
                    ParseLineResult parse_result = parse_line(field, arg);
                    if (!parse_result.success)
                    {
                        std::string err_msg(
                              "Config file error at line " + std::to_string(iline)
                            + " of file \"" + config_filename + "\": "
                            + parse_result.error_description
                        );
                        const char * err_msg_cstr = err_msg.c_str();
                        BOOST_ASSERT_MSG(false, err_msg_cstr);
                    }

                    ++success_count;
                }

            }
        }

        for (const auto & p : m_field_lookup)
        {
            if (p.second.parse_count == 0)
            {
                std::string err_msg(
                      "Config file error in \""
                    + config_filename + "\": setting for field \""
                    + p.first + "\" is missing"
                );
                const char * err_msg_cstr = err_msg.c_str();
                BOOST_ASSERT_MSG(false, err_msg_cstr);
            }
        }

        std::cout << "Parsing succeeded!\n" << std::flush;
    }

private:

    static ParseLineResult generic_parse_unsigned(const std::string & arg, unsigned & target)
    {
        try
        {
            target = boost::numeric_cast<unsigned>(std::stoul(arg));
        }
        catch (...)
        {
            return ParseLineResult{false, "Failed converting string \"" + arg + "\" to unsigned"};
        }

        return ParseLineResult();
    }

    static ParseLineResult generic_parse_string_list(
          const std::string & arg
        , std::vector<std::string> & target
        , bool discard_whitespace = true)
    {
        target.clear();

        boost::split( target, arg, boost::is_any_of(","), boost::token_compress_on );

        if (discard_whitespace)
        {
            for (std::string & substr : target)
            {
                boost::algorithm::trim(substr);
            }

            target.erase(
                  std::remove_if(
                      target.begin(), target.end()
                    , [](const std::string & substr){ return substr.empty(); })
                , target.end());
        }

        if (target.empty())
        {
            return ParseLineResult{false, "Cannot find any substring in the list."};
        }

        return ParseLineResult();
    }

    static ParseLineResult generic_parse_string(
          const std::string & arg
        , std::string & target
        , bool discard_whitespace = true)
    {
        target = arg;

        if (discard_whitespace)
        {
            boost::algorithm::trim(target);
        }

        if (target.empty())
        {
            return ParseLineResult{false, "String argument is empty."};
        }

        return ParseLineResult();
    }

    ParseLineResult parse_line(const std::string & field, const std::string & arg)
    {
        auto iter = m_field_lookup.find(field);
        if (iter == m_field_lookup.end())
        {
            return ParseLineResult{false, "Unrecognizable field: \"" + field + "\""};
        }

        unsigned parse_count = iter->second.parse_count++;
        if (parse_count != 0)
        {
            return ParseLineResult{false, "Field: \"" + field + "\" appeared more than once"};
        }

        auto func = iter->second.parse_func;
        return func(*this, arg);
    }


    ParseLineResult parse_n_workers(const std::string & arg)
    {
        return generic_parse_unsigned(arg, m_num_workers);
    }
    ParseLineResult parse_n_output_files(const std::string & arg)
    {
        return generic_parse_unsigned(arg, m_output_files);
    }
    ParseLineResult parse_map_kilobytes(const std::string & arg)
    {
        return generic_parse_unsigned(arg, m_max_kilobytes);
    }
    ParseLineResult parse_worker_ipaddr_ports(const std::string & arg)
    {
        return generic_parse_string_list(arg, m_worker_addresses);
    }
    ParseLineResult parse_input_files(const std::string & arg)
    {
        return generic_parse_string_list(arg, m_input_filenames);
    }
    ParseLineResult parse_output_dir(const std::string & arg)
    {
        return generic_parse_string(arg, m_output_directories);
    }
    ParseLineResult parse_user_id(const std::string & arg)
    {
        return generic_parse_string(arg, m_user_id);
    }


    unsigned m_num_workers = 0;
    unsigned m_output_files = 0;
    unsigned m_max_kilobytes = 0;
    std::string m_user_id;
    std::string m_output_directories;
    std::vector<std::string> m_worker_addresses;
    std::vector<std::string> m_input_filenames;

    std::unordered_map<std::string, FieldParseData> m_field_lookup {
        { "n_workers",           {0, &MapReduceSpec::parse_n_workers} },
        { "worker_ipaddr_ports", {0, &MapReduceSpec::parse_worker_ipaddr_ports} },
        { "input_files",         {0, &MapReduceSpec::parse_input_files} },
        { "output_dir",          {0, &MapReduceSpec::parse_output_dir} },
        { "n_output_files",      {0, &MapReduceSpec::parse_n_output_files} },
        { "map_kilobytes",       {0, &MapReduceSpec::parse_map_kilobytes} },
        { "user_id",             {0, &MapReduceSpec::parse_user_id} }
    };
};


class SpecReader
{

};

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string & config_filename, MapReduceSpec& mr_spec)
{
    mr_spec = MapReduceSpec(config_filename);

    return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec & mr_spec) {
    return true;
}
