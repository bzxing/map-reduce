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
#include "my_utils.h"


inline bool string_is_legal(const std::string& str)
{
	return std::none_of(str.begin(), str.end(), [](char c){ return c == kDelimiter || c == '\n'; });
}

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

		std::ofstream ofs( compute_target_filepath(key), std::ios::app );

		ofs << key << kDelimiter << val << std::endl;
	}
private:



	// Filename would be: key_hash_worker_uid.mapped
	static std::string compute_target_filepath(const std::string & key)
	{
		const WorkerConfig * config = WorkerConfig::get_inst();
		BOOST_ASSERT(config);

		std::string filename;
		filename += "map_k";
		filename += std::to_string(compute_hash_for_key(key));
		filename += "_w";
		filename += std::to_string(config->m_worker_uid);


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
		BOOST_ASSERT( string_is_legal(key) );
		BOOST_ASSERT( string_is_legal(val) );

		std::ofstream ofs( compute_target_filepath(key), std::ios::app );

		ofs << key << kDelimiter << val << std::endl;
	}

private:
	static std::string compute_target_filepath(const std::string& key)
	{
		const WorkerConfig * config = WorkerConfig::get_inst();
		BOOST_ASSERT(config);

		std::string filename;
		filename += "reduce_k";
		filename += std::to_string(compute_hash_for_key(key));
		filename += "_w";
		filename += std::to_string(config->m_worker_uid);

		boost::filesystem::path path = config->m_output_dir;
		path /= filename;

		return path.string();
	}

	/* NOW you can add below, data members and member functions as per the need of your implementation*/
};







