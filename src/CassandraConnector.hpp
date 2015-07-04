#ifndef CASSANDRA_CONNECTOR_HPP
#define CASSANDRA_CONNECTOR_HPP
#include <cassandra.h>
#include <stdio.h>

class CassandraConnector
{
protected:
	/* The driver is designed so that no operation will force an application to block.
	 * Operations that would normally cause the application to block,
	 * such as connecting to a cluster or running a query,
	 * instead return a CassFuture object that can be waited on, polled, or used to register a callback.
	 *
	 * NOTE: The API can also be used synchronously by waiting on or immediately attempting to get the result from a future.
	 */
	CassFuture* m_connect_future;
	CassFuture* m_disconnect_future;
	CassCluster* m_cluster;
	CassSession* m_session;

	/** Inner logic. Must be overriden */
	virtual void _run();
	static void print_error(CassFuture* future);

public:

	CassandraConnector();
	virtual ~CassandraConnector();

	int run();

	bool connect();
	void disconnect();

	CassError execute_query(const char* query);
};
#endif
