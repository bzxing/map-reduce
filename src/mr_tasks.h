#pragma once

#include <string>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <memory>
#include <utility>
#include <functional>

#include <boost/assert.hpp>
#include <boost/filesystem.hpp>

#include "masterworker.pb.h"

class WorkerConfig
{
    struct PrivateCtorKey {};

public:
    static void install_config( masterworker::WorkerConfig && config )
    {
        BOOST_ASSERT_MSG(!s_inst, "Not designed to install worker configuration twice!");
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

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal
{
public:
	/* DON'T change this function's signature */
	/* CS6210_TASK Implement this function */
	BaseMapperInternal() {}

	/* DON'T change this function's signature */
	/* CS6210_TASK Implement this function */
	void emit(const std::string& key, const std::string& val)
	{
		BOOST_ASSERT( string_is_legal(key) );
		BOOST_ASSERT( string_is_legal(val) );

		std::ofstream ofs(
			  compute_target_filepath(key)
			, std::ios::app
			);

		ofs << key << "\t" << val << "\n";
	}
private:

	static bool string_is_legal(const std::string& str)
	{
		return std::none_of(str.begin(), str.end(), [](char c){ return c == '\t' || c == '\n'; });
	}

	static size_t compute_hash_for_key(const std::string & key)
	{
		const WorkerConfig * config = WorkerConfig::get_inst();
		BOOST_ASSERT(config);

		std::hash<std::string> hash_fn;

		size_t hash = hash_fn(key) % (config->m_num_output_files);
		return hash;
	}

	// Filename would be: key_hash_worker_uid.mapped
	static std::string compute_target_filepath(const std::string & key)
	{
		const WorkerConfig * config = WorkerConfig::get_inst();
		BOOST_ASSERT(config);

		std::string filename;
		filename += std::to_string(compute_hash_for_key(key));
		filename += "_";
		filename += std::to_string(config->m_worker_uid);
		filename += ".mapped";

		boost::filesystem::path path = config->m_output_dir;
		path /= filename;

		return path.string();
	}

};




/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structure as per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal
{

	/* DON'T change this function's signature */
	/* CS6210_TASK Implement this function */
	BaseReducerInternal()
	{

	}

	/* DON'T change this function's signature */

	/* CS6210_TASK Implement this function */
	void emit(const std::string& key, const std::string& val)
	{
		std::cout << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;
	}

	/* NOW you can add below, data members and member functions as per the need of your implementation*/
};







