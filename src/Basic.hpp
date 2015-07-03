#ifndef BASIC_HPP_
#define BASIC_HPP_

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "cassandra.h"

class Basic
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

	void print_error(CassFuture* future);

	CassCluster* create_cluster();

	CassError connect_session(CassSession* session, const CassCluster* cluster);

	CassError execute_query(CassSession* session, const char* query);

	CassError insert_into_basic(CassSession* session, const char* key, const SBasic* basic);

	CassError select_from_basic(CassSession* session, const char* key, SBasic* basic);

public:
	/**
	 * @brief unique instance accessor as in Meyer's singleton
	 * @return a reference to the only instance of this class
	 */
	static Basic& instance()
	{
		static Basic t;
		return t;
	}

	int run();
};

#endif /* BASIC_HPP_ */
