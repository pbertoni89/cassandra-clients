#ifndef PATQUERY
#define PATQUERY
#include <stdio.h>
#include <iostream>
#include <cassandra.h>
#include "Foo.cpp"

class PatQuery
{
protected:
	const char* m_query;
	CassStatement* m_statement;
	CassFuture* m_result_future;
	const CassResult* m_result;
	CassIterator* m_rows;
	CassSession* m_session;
	bool m_is_setup;

	virtual void _query();

public:

	PatQuery(CassSession* session, const char* query)
	{
		m_session = session;
		m_query = query;
		m_is_setup = false;

		m_statement = cass_statement_new(m_query, 0);
		m_result_future = cass_session_execute(m_session, m_statement);

		if (cass_future_error_code(m_result_future) == CASS_OK)
		{
			/* Retrieve result set and iterate over the rows */
			m_result = cass_future_get_result(m_result_future);
			m_rows = cass_iterator_from_result(m_result);
			m_is_setup= true;
		}
	}

	virtual ~PatQuery()
	{
		cass_result_free(m_result);
		cass_iterator_free(m_rows);
		cass_statement_free(m_statement);
		cass_future_free(m_result_future);
	}

	void query()
	{
		if (m_is_setup)
			_query();
		else
		{
			const char* message;
			size_t message_length;
			cass_future_error_message(m_result_future, &message, &message_length);
			fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message_length, message);
		}
	}

	CassFuture* get_result()
	{
		return m_result_future;
	}

	bool is_setup()
	{
		return m_is_setup;
	}
};
#endif
