#include "Basic.hpp"

CassError Basic::insert_into_basic(const char* key, const SBasic* basic)
{
	CassError rc = CASS_OK;
	CassStatement* statement = NULL;
	CassFuture* future = NULL;
	const char* query = "INSERT INTO examples.basic (key, bln, flt, dbl, i32, i64) VALUES (?, ?, ?, ?, ?, ?);";

	statement = cass_statement_new(query, 6);

	cass_statement_bind_string(statement, 0, key);
	cass_statement_bind_bool(statement, 1, basic->bln);
	cass_statement_bind_float(statement, 2, basic->flt);
	cass_statement_bind_double(statement, 3, basic->dbl);
	cass_statement_bind_int32(statement, 4, basic->i32);
	cass_statement_bind_int64(statement, 5, basic->i64);

	future = cass_session_execute(m_session, statement);
	cass_future_wait(future);

	rc = cass_future_error_code(future);
	if (rc != CASS_OK)
		print_error(future);

	cass_future_free(future);
	cass_statement_free(statement);

	return rc;
}

CassError Basic::select_from_basic(const char* key, SBasic* basic)
{
	CassError rc = CASS_OK;
	CassStatement* statement = NULL;
	CassFuture* future = NULL;
	const char* query = "SELECT * FROM examples.basic WHERE key = ?";

	statement = cass_statement_new(query, 1);

	cass_statement_bind_string(statement, 0, key);

	future = cass_session_execute(m_session, statement);
	cass_future_wait(future);

	rc = cass_future_error_code(future);
	if (rc != CASS_OK)
	{
		print_error(future);
	}
	else
	{
		const CassResult* result = cass_future_get_result(future);
		CassIterator* iterator = cass_iterator_from_result(result);

		if (cass_iterator_next(iterator))
		{
		const CassRow* row = cass_iterator_get_row(iterator);
		cass_value_get_bool(cass_row_get_column(row, 1), &basic->bln);
		cass_value_get_double(cass_row_get_column(row, 2), &basic->dbl);
		cass_value_get_float(cass_row_get_column(row, 3), &basic->flt);
		cass_value_get_int32(cass_row_get_column(row, 4), &basic->i32);
		cass_value_get_int64(cass_row_get_column(row, 5), &basic->i64);
		}
		cass_result_free(result);
		cass_iterator_free(iterator);
	}
	cass_future_free(future);
	cass_statement_free(statement);

	return rc;
}

void Basic::_run()
{
	SBasic input = { cass_true, 0.001f, 0.0002, 1, 2 };
	SBasic output;

	execute_query("CREATE KEYSPACE examples WITH replication = { \
				'class': 'SimpleStrategy', 'replication_factor': '3' };");


	execute_query("CREATE TABLE examples.basic (key text, \
												bln boolean, \
												flt float, dbl double,\
												i32 int, i64 bigint, \
												PRIMARY KEY (key));");

	insert_into_basic("test", &input);
	select_from_basic("test", &output);

	assert(input.bln == output.bln);
	assert(input.flt == output.flt);
	assert(input.dbl == output.dbl);
	assert(input.i32 == output.i32);
	assert(input.i64 == output.i64);
}
