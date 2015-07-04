#include "CassandraConnector.hpp"

CassandraConnector::CassandraConnector()
{
	m_connect_future = NULL;
	m_disconnect_future = NULL;
	m_cluster = cass_cluster_new();
	m_session = cass_session_new();
}

CassandraConnector::~CassandraConnector()
{
	cass_future_free(m_connect_future);
	cass_cluster_free(m_cluster);
	cass_session_free(m_session);
}

int CassandraConnector::run()
{
	CassFuture* connect_future = NULL;
	m_cluster = cass_cluster_new();
	m_session = cass_session_new();

	if (this->connect())
	{
		_run();
	}
	else
		print_error(connect_future);

	disconnect();
	return 0;
}

void CassandraConnector::_run()
{
	std::cout << "OVERRIDE ME !" << std::endl;
}

bool CassandraConnector::connect()
{
	bool is_connected = true;
	/* Add contact points */
	cass_cluster_set_contact_points(m_cluster, "127.0.0.1");
	/* Provide the cluster object as configuration to connect the session */
	m_connect_future = cass_session_connect(m_session, m_cluster);

	/* This operation will block until the result is ready */
	if (cass_future_error_code(m_connect_future) != CASS_OK)
	{
		print_error(m_connect_future);
		is_connected = false;
	}
	return is_connected;
}

void CassandraConnector::disconnect()
{
	m_disconnect_future = cass_session_close(m_session);
	cass_future_wait(m_disconnect_future);
	cass_future_free(m_disconnect_future);
	cass_session_free(m_session);
	cass_cluster_free(m_cluster);
}

CassError CassandraConnector::execute_query(const char* query)
{
	CassError rc = CASS_OK;
	CassFuture* future = NULL;

	/* Queries are executed using CassStatement objects.
	 * Statements encapsulate the query string and the query parameters.
	 * Query parameters are not supported by ealier versions of Cassandra (1.2 and below)
	 * and values need to be inlined in the query string itself.
	 */
	CassStatement* statement = cass_statement_new(query, 0);

	future = cass_session_execute(m_session, statement);
	cass_future_wait(future);

	/* This will block until the query has finished */
	rc = cass_future_error_code(future);
	if (rc != CASS_OK)
		print_error(future);
	/*else
		printf("Query result: %s\n", cass_error_desc(rc));*/

	cass_future_free(future);
	/* Statement objects can be freed immediately after being executed */
	cass_statement_free(statement);
	return rc;
}

void CassandraConnector::print_error(CassFuture* future)
{
	const char* message;
	size_t message_length;
	cass_future_error_message(future, &message, &message_length);
	fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}
