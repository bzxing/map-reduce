#pragma once

#include <string>
#include <iostream>

#include <memory>
#include <utility>

#include <boost/assert.hpp>

class TaskEnv
{
public:

	template <class ...Args>
	static const TaskEnv * create_inst(Args &&... args)
	{
		BOOST_ASSERT(!s_inst);
		s_inst = std::make_unique<TaskEnv>( std::forward<Args>(args)... );
		return s_inst.get();
	}

	static const TaskEnv * get_inst()
	{
		return s_inst.get();
	}

private:

	TaskEnv()
	{

	}

	static std::unique_ptr<TaskEnv> s_inst;

};

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal
{

	/* DON'T change this function's signature */
	/* CS6210_TASK Implement this function */
	BaseMapperInternal() {}

	/* DON'T change this function's signature */
	/* CS6210_TASK Implement this function */
	void emit(const std::string& key, const std::string& val)
	{
		std::cout << "Dummy emit by BaseMapperInternal: " << key << ", " << val << std::endl;
	}

	/* NOW you can add below, data members and member functions as per the need of your implementation*/
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







