#pragma once

#include <vector>

#include <initializer_list>

#include <sstream>
#include <iostream>
#include <fstream>

#include "mapreduce_spec.h"

#include <boost/filesystem.hpp>
#include <boost/numeric/conversion/cast.hpp>
#include <boost/assert.hpp>


/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard
{
	std::string filename;
	unsigned offset = 0;
	unsigned shard_length = 0;
	unsigned file_length = 0;
};


namespace
{

	unsigned determine_file_length(const std::string & filename)
	{
		BOOST_ASSERT_MSG(
			boost::filesystem::is_regular_file(filename),
			("Input File Error: File " + filename + " is not a regular file!").c_str());
		return boost::numeric_cast<unsigned>
				(boost::filesystem::file_size(filename));
	}

	void print_file_shards(std::ostream & os, const std::vector<FileShard> & shards)
	{
		std::ostringstream oss;
		oss << "shard_id"
			<< "\t" << "filename"
			<< "\t" << "offset"
			<< "\t" << "shard_length"
			<< "\t" << "file_length"
			<< "\n";
		for (size_t i = 0; i < shards.size(); ++i)
		{
			const auto & shard = shards[i];
			oss << i
				<< "\t" << shard.filename
				<< "\t" << shard.offset
				<< "\t" << shard.shard_length
				<< "\t" << shard.file_length
				<< "\n";
		}
		os << oss.str() << std::flush;
	}

	std::vector<FileShard> make_file_shards(const MapReduceSpec & mr_spec)
	{
		std::vector<FileShard> shard_vec;
		const unsigned target_shard_length = mr_spec.get_map_kilobytes() * 1024u;

		for ( const auto & filename : mr_spec.get_input_file_range() )
		{
			// Iteratively split this ffile
			const unsigned file_length = determine_file_length(filename);
			unsigned current_offset = 0;

			while (current_offset < file_length)
			{
				const unsigned remaining_file_length = file_length - current_offset;

				// If next remaining length is less than half of target shard length,
				// then include it in current shard

				auto func_determine_shard_length = [](
					  unsigned target_shard_length
					, unsigned remaining_file_length
					)
				{
					// If remaining is less than 1.5x of target shard length, use the remaining length
					if (  remaining_file_length < ( target_shard_length + (target_shard_length / 2) )  )
					{
						return remaining_file_length;
					}

					// Else, use the target shard length
					return target_shard_length;
				};

				const unsigned actual_shard_length = func_determine_shard_length(
					target_shard_length, remaining_file_length);

				BOOST_ASSERT_MSG(current_offset + actual_shard_length <= file_length,
					"Shard range overshooting file length!");
				shard_vec.push_back({filename, current_offset, actual_shard_length, file_length});

				current_offset += actual_shard_length;
			}
		}

		return shard_vec;
	}

}


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */
inline bool shard_files(const MapReduceSpec & mr_spec, std::vector<FileShard> & file_shards)
{
	file_shards = make_file_shards(mr_spec);
	std::cout << "File Sharding Completes! Results:\n" << std::flush;
	print_file_shards(std::cout, file_shards);

	return true;
}
