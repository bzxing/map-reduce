#pragma once

#include <string>
#include <iostream>

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




