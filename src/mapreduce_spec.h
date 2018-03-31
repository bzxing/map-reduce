#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

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
#include <boost/range/iterator_range.hpp>
#include <boost/filesystem.hpp>

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
    using ValidateFunc = std::function< bool(const MapReduceSpec &) >;

    struct FieldParseData
    {
        unsigned parse_count = 0;
        FieldParseFunc parse_func;
        ValidateFunc validate_func;
    };

public:

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
                    //std::cout << "Parsing config: \"" + field + "\" \"" + arg + "\"\n" << std::flush;

                    // Parse the line
                    ParseLineResult parse_result = parse_line(field, arg);
                    if (!parse_result.success)
                    {
                        std::string err_msg(
                              "Config Parse Error: at line " + std::to_string(iline)
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

        std::cout << "Config Parsing Succeeded!\n" << std::flush;
    }

    bool is_valid() const
    {
        for (const auto & field_data : m_field_lookup)
        {
            auto validate_func = field_data.second.validate_func;
            if (!validate_func(*this))
            {
                return false;
            }
        }
        return true;
    }

    auto get_input_file_range() const
    {
        return boost::make_iterator_range(m_input_files.begin(), m_input_files.end());
    }

    auto get_worker_range() const
    {
        return boost::make_iterator_range(m_workers.begin(), m_workers.end());
    }

    unsigned get_num_workers() const
    {
        return boost::numeric_cast<unsigned>(m_workers.size());
    }

    unsigned get_map_kilobytes() const
    {
        return m_map_kilobytes;
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
        std::cout << "Config Parse Warning: Field n_workers is ignored\n" << std::flush;
        return ParseLineResult();
    }
    ParseLineResult parse_n_output_files(const std::string & arg)
    {
        return generic_parse_unsigned(arg, m_output_files);
    }
    ParseLineResult parse_map_kilobytes(const std::string & arg)
    {
        return generic_parse_unsigned(arg, m_map_kilobytes);
    }
    ParseLineResult parse_worker_ipaddr_ports(const std::string & arg)
    {
        return generic_parse_string_list(arg, m_workers);
    }
    ParseLineResult parse_input_files(const std::string & arg)
    {
        return generic_parse_string_list(arg, m_input_files);
    }
    ParseLineResult parse_output_dir(const std::string & arg)
    {
        return generic_parse_string(arg, m_output_dir);
    }
    ParseLineResult parse_user_id(const std::string & arg)
    {
        return generic_parse_string(arg, m_user_id);
    }

    bool validate_n_workers() const
    {
        // This field is ignored. No validation needed.
        return true;
    }

    bool validate_n_output_files() const
    {
        bool success = m_output_files > 0;
        if (!success)
        {
            std::cout << "Config Validation Error: n_output_files must be greater than 0\n" << std::flush;
        }
        return success;
    }

    bool validate_map_kilobytes() const
    {
        bool success = m_output_files > 0;
        if (!success)
        {
            std::cout << "Config Validation Error: map_kilobytes must be greater than 0\n" << std::flush;
        }
        return success;
    }

    bool validate_user_id() const
    {
        bool success = !m_user_id.empty();
        if (!success)
        {
            std::cout << "Config Validation Error: user_id must not be empty\n" << std::flush;
        }
        return success;
    }

    bool validate_output_directories() const
    {
        bool success = !m_output_dir.empty();
        if (!success)
        {
            std::cout << "Config Validation Error: output_directories must not be empty\n" << std::flush;
        }
        return success;
    }

    bool validate_worker_addresses() const
    {
        if (m_workers.empty())
        {
            std::cout << "Config Validation Error: m_workers must not be empty\n" << std::flush;
            return false;
        }
        for (const std::string & str : m_workers)
        {
            if (str.empty())
            {
                std::cout << "Config Validation Error: m_workers must not have empty fields\n" << std::flush;
                return false;
            }
        }
        std::unordered_set<std::string> uniquifier;
        for (const std::string & str : m_workers)
        {
            bool success = uniquifier.insert(str).second;
            if (!success)
            {
                std::cout << "Config Validation Error: m_workers must not have duplicate fields\n" << std::flush;
                return false;
            }
        }
        return true;
    }

    bool validate_input_files() const
    {
        if (m_input_files.empty())
        {
            std::cout << "Config Validation Error: input_files must not be empty\n" << std::flush;
            return false;
        }

        // Check file exists
        for (const std::string & input_filename : m_input_files)
        {
            if (input_filename.empty())
            {
                std::cout << "Config Validation Error: input_files must not have empty fields\n" << std::flush;
                return false;
            }

            bool file_found = boost::filesystem::is_regular_file(input_filename);
            if (!file_found)
            {
                std::string err_msg = "Config Validation Error: input_files contain filename \"" + input_filename
                    + "\" which does not exist or is not a regular file.\n";
                std::cout << err_msg << std::flush;
                return false;
            }
        }

        return true;
    }

    unsigned m_output_files = 0;
    unsigned m_map_kilobytes = 0;
    std::string m_user_id;
    std::string m_output_dir;
    std::vector<std::string> m_workers;
    std::vector<std::string> m_input_files;

    std::map<std::string, FieldParseData> m_field_lookup {
         { "n_workers",           {0, &MapReduceSpec::parse_n_workers,           &MapReduceSpec::validate_n_workers } }
        ,{ "worker_ipaddr_ports", {0, &MapReduceSpec::parse_worker_ipaddr_ports, &MapReduceSpec::validate_worker_addresses } }
        ,{ "input_files",         {0, &MapReduceSpec::parse_input_files,         &MapReduceSpec::validate_input_files } }
        ,{ "output_dir",          {0, &MapReduceSpec::parse_output_dir,          &MapReduceSpec::validate_output_directories } }
        ,{ "n_output_files",      {0, &MapReduceSpec::parse_n_output_files,      &MapReduceSpec::validate_n_output_files } }
        ,{ "map_kilobytes",       {0, &MapReduceSpec::parse_map_kilobytes,       &MapReduceSpec::validate_map_kilobytes } }
        ,{ "user_id",             {0, &MapReduceSpec::parse_user_id,             &MapReduceSpec::validate_user_id } }
    };
};

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string & config_filename, MapReduceSpec & mr_spec)
{
    mr_spec = MapReduceSpec(config_filename);

    return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec & mr_spec)
{
    return mr_spec.is_valid();
}
