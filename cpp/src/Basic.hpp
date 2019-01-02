#ifndef BASIC_HPP_
#define BASIC_HPP_
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include "CassandraConnector.hpp"

class Basic : public CassandraConnector
{
	struct Basic_
	{
	  cass_bool_t bln;
	  cass_float_t flt;
	  cass_double_t dbl;
	  cass_int32_t i32;
	  cass_int64_t i64;
	};

	typedef struct Basic_ SBasic;

	CassError insert_into_basic(const char* key, const SBasic* basic);
	CassError select_from_basic(const char* key, SBasic* basic);

protected:
	/** Inner logic. */
	void _run();

public:
	Basic() : CassandraConnector()
	{}
};
#endif
