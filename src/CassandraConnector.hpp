#ifndef CASSANDRA_CONNECTOR_HPP
#define CASSANDRA_CONNECTOR_HPP
#include <cassandra.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <vector>
#include <string>
#define CASSANDRA_CONNECTOR_NAME "CassandraConnector: "
/* 1 MegaByte of information can be transmitted over a single connection. */
#define CASSANDRA_CLUSTER_HIGH_WATERMARK 1048576

const std::string keyspace_name = "blockmon";
const std::string table_data_ckps_name = "table_data_ckps";
const std::string data_ckps_canonical_name = keyspace_name + "." + table_data_ckps_name;
const std::string table_last_ckps_name = "table_last_ckps";
const std::string last_ckps_canonical_name = keyspace_name + "." + table_last_ckps_name;
const int replication_factor = 2;

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
	/** Useful safeguard for improper calls. */
	bool m_is_connected;

	//static int BUFFER_BYTE_SIZE = 102400;
	//BYTES_NEEDED = 128;
	//BYTES_CODE = 4;

	/** Inner logic. Must be overriden */
	virtual void _run();
	static void print_error(CassFuture* future);

	/**
		CREATE KEYSPACE blockmon WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

		USE blockmon;
	 */
	void set_keyspace();
	/**
	 	CREATE TABLE blockmon.table_data_ckps (
	 	node_id text, dist_name text, comp_name text, ckp_id text, state text,
	 	PRIMARY KEY (node_id, dist_name, comp_name, ckp_id));

	 	CREATE TABLE blockmon.table_last_ckps (
	 	dist_name text, last_ckp_id text,
	 	PRIMARY KEY (dist_name));
	 */
	void create_tables();

	/** Sets last_ckp_id of dist_name, into table of last_ckps */
	CassError set_last_ckp(std::string dist_name, std::string ckp_id);
	/** Inner encapsulation. E.G.
	 	 INSERT INTO blockmon.table_data_ckps (node_id, dist_name, comp_name, ckp_id, state) VALUES ('1', 'wc1', 'src', 'ABCD', 'provaa');
	 */
	CassError _save_state(std::string node_id, std::string dist_name, std::string comp_name, std::string ckp_id, std::vector<unsigned char>* state);

	/* inner encapsulation */
	CassError _execute_query(CassStatement* statement);

	CassError execute_query(const char* query);

public:

	CassandraConnector();
	virtual ~CassandraConnector();

	int run();

	bool connect();
	void disconnect();

	/** API to save a state. It update the last_ckp_id, too
	 * @return true iff state is correctly saved */
	bool save_state(std::string node_id, std::string dist_name, std::string comp_name, std::string ckp_id, std::vector<unsigned char>* state);
	/** API to recover a state. It fetch the last_ckp_id, too
	 * @return true iff state is correctly recovered */
	bool recover_state(std::string node_id, std::string dist_name, std::string comp_name, std::string ckp_id, char** state, long long int& size);
	/** Show content of table_data_ckps */
	void list_data_ckps();
	/** Show content of table_last_ckps */
	void list_last_ckps();
	/** Create keyspace and tables */
	void create_db();
	/** Drop and create keyspace and tables */
	void recreate_db();
	/** Drop keyspace and tables */
	void drop_db();
	/** Retrieve the last_ckp saved for the given dist_name. */
	bool get_last_ckp(std::string dist_name, std::string &last_ckp_id);
};
#endif
