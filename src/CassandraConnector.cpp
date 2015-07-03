#ifndef CASSANDRA_CONNECTOR
#define CASSANDRA_CONNECTOR
#include "PatQuery.cpp"

class CassandraConnector
{
	CassFuture* m_connect_future;
	CassCluster* m_cluster;
	CassSession* m_session;
	CassFuture* m_disconnect_future;

public:
	CassandraConnector()
	{
		m_connect_future = NULL;
		m_disconnect_future = NULL;
		m_cluster = cass_cluster_new();
		m_session = cass_session_new();
	}

	~CassandraConnector()
	{
		cass_future_free(m_connect_future);
		cass_cluster_free(m_cluster);
		cass_session_free(m_session);
	}

	void connect()
	{
		/* Add contact points */
		cass_cluster_set_contact_points(m_cluster, "127.0.0.1");
		m_connect_future = cass_session_connect(m_session, m_cluster);

		if (cass_future_error_code(m_connect_future) != CASS_OK)
		{
			/* Handle error */
			const char* message;
			size_t message_length;
			cass_future_error_message(m_connect_future, &message, &message_length);
			fprintf(stderr, "Unable to connect: '%.*s'\n", (int) message_length, message);
		}
	}

	/** Close the session */
	void disconnect()
	{
		m_disconnect_future = cass_session_close(m_session);
		cass_future_wait(m_disconnect_future);
		cass_future_free(m_disconnect_future);
	}

	CassSession* get_session()
	{
		return m_session;
	}
};
#endif
