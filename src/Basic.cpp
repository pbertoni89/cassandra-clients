#include "Basic.hpp"

void Basic::print_error(CassFuture* future)
{
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassCluster* Basic::create_cluster()
{
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, "127.0.0.1");
  return cluster;
}

CassError Basic::connect_session(CassSession* session, const CassCluster* cluster)
{
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect(session, cluster);

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK)
  {
	print_error(future);
  }
  cass_future_free(future);

  return rc;
}

CassError Basic::execute_query(CassSession* session, const char* query)
{
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(query, 0);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK)
  {
	print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError Basic::insert_into_basic(CassSession* session, const char* key, const SBasic* basic)
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

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK)
  {
	print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError Basic::select_from_basic(CassSession* session, const char* key, SBasic* basic)
{
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  const char* query = "SELECT * FROM examples.basic WHERE key = ?";

  statement = cass_statement_new(query, 1);

  cass_statement_bind_string(statement, 0, key);

  future = cass_session_execute(session, statement);
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

int Basic::run()
{
  CassCluster* cluster = create_cluster();
  CassSession* session = cass_session_new();
  CassFuture* close_future = NULL;

  SBasic input = { cass_true, 0.001f, 0.0002, 1, 2 };
  SBasic output;

  if (connect_session(session, cluster) != CASS_OK) {
	cass_cluster_free(cluster);
	cass_session_free(session);
	return -1;
  }

  execute_query(session,
				"CREATE KEYSPACE examples WITH replication = { \
						   'class': 'SimpleStrategy', 'replication_factor': '3' };");


  execute_query(session,
				"CREATE TABLE examples.basic (key text, \
											  bln boolean, \
											  flt float, dbl double,\
											  i32 int, i64 bigint, \
											  PRIMARY KEY (key));");

  insert_into_basic(session, "test", &input);
  select_from_basic(session, "test", &output);

  assert(input.bln == output.bln);
  assert(input.flt == output.flt);
  assert(input.dbl == output.dbl);
  assert(input.i32 == output.i32);
  assert(input.i64 == output.i64);

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
