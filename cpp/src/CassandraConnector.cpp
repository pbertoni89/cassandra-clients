#include "CassandraConnector.hpp"

CassandraConnector::CassandraConnector()
{
	m_connect_future = NULL;
	m_disconnect_future = NULL;
	m_cluster = cass_cluster_new();
	m_session = cass_session_new();
	m_is_connected = false;
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
	/* Increase the maximum bunch of data that can be transmitted on a single connection. This because states often excees the default 64KB value */
	cass_cluster_set_write_bytes_high_water_mark(m_cluster, (unsigned)CASSANDRA_CLUSTER_HIGH_WATERMARK);

	/* This operation will block until the result is ready */
	if (cass_future_error_code(m_connect_future) != CASS_OK)
	{
		print_error(m_connect_future);
		is_connected = false;
	}
	else
	{
		m_is_connected = true;
	}
	return is_connected;
}

void CassandraConnector::disconnect()
{
	m_disconnect_future = cass_session_close(m_session);
	cass_future_wait(m_disconnect_future);
	cass_future_free(m_disconnect_future);
	//cass_session_free(m_session);	// TODO investigate. causes Aborted (core dumped)
	//cass_cluster_free(m_cluster);	// TODO investigate. causes Assertion `new_ref_count >= 1' failed.
}

void CassandraConnector::recreate_db()
{
	drop_db();
	create_db();
}

void CassandraConnector::create_db()
{
	set_keyspace();
	create_tables();
}

void CassandraConnector::drop_db()
{
	std::stringstream query;
	query << "DROP KEYSPACE " << keyspace_name;
	this->execute_query(query.str().c_str());
	std::cout << CASSANDRA_CONNECTOR_NAME << "Cassandra DB has been cleaned." << std::endl;
}

void CassandraConnector::set_keyspace()
{
	std::cout << CASSANDRA_CONNECTOR_NAME << "creating new keyspace `" + keyspace_name + "`" << std::endl;
	std::stringstream query_create;
	query_create << 	"CREATE KEYSPACE " << keyspace_name << " WITH replication " <<
				"= {'class':'SimpleStrategy', 'replication_factor':" <<
				replication_factor << "};";
	this->execute_query(query_create.str().c_str());

	std::stringstream query_use;
	query_use << 	"USE " << keyspace_name << ";";
	this->execute_query(query_use.str().c_str());
}

void CassandraConnector::create_tables()
{
	std::cout << CASSANDRA_CONNECTOR_NAME << "creating new table `" + table_data_ckps_name + "`" << std::endl;
	std::stringstream query_data;
	query_data << "CREATE TABLE " << data_ckps_canonical_name << " (" <<
				"node_id text," <<
				"dist_name text," <<
				"comp_name text," <<
				"ckp_id text," <<
				"state text," <<
				"PRIMARY KEY (node_id, dist_name, comp_name, ckp_id));";
	this->execute_query(query_data.str().c_str());

	std::cout << CASSANDRA_CONNECTOR_NAME << "creating new table `" + table_last_ckps_name + "`" << std::endl;
	std::stringstream query_last;
	query_last << "CREATE TABLE " << last_ckps_canonical_name << " (" <<
				"dist_name text," <<
				"last_ckp_id text," <<
				"PRIMARY KEY (dist_name));";
	this->execute_query(query_last.str().c_str());
}

CassError CassandraConnector::execute_query(const char* query)
{
	/* Queries are executed using CassStatement objects.
	 * Statements encapsulate the query string and the query parameters.
	 * Query parameters are not supported by ealier versions of Cassandra (1.2 and below)
	 * and values need to be inlined in the query string itself. */
	CassStatement* statement = cass_statement_new(query, 0);

	return _execute_query(statement);
}

CassError CassandraConnector::_execute_query(CassStatement* statement)
{
	CassError rc = CASS_OK;
	CassFuture* future = cass_session_execute(m_session, statement);
	cass_future_wait(future);

	/* This will block until the query has finished */
	rc = cass_future_error_code(future);
	if (rc != CASS_OK)
		print_error(future);
	/*else
	{
		THIS IS THE RIGHT PLACE TO TAKE ACTIONS ON THE QUERY. This method is general-purpose, so it will do nothing
		printf("Query result: %s\n", cass_error_desc(rc));
	}
	*/

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

bool CassandraConnector::save_state(std::string node_id, std::string dist_name, std::string comp_name, std::string ckp_id,
									std::vector<unsigned char>* state)
{
	if (m_is_connected)
		return 	(_save_state(node_id, dist_name, comp_name, ckp_id, state) == CASS_OK)
				&&
				(set_last_ckp(dist_name, ckp_id) == CASS_OK);
	else
		return false;
}

bool CassandraConnector::recover_state(	std::string node_id, std::string dist_name, std::string comp_name ,std::string ckp_id,
										char** state, long long int& size)
{
	if (m_is_connected)
		std::cout << CASSANDRA_CONNECTOR_NAME << "Implement me. I'm recover_state()" << std::endl;
	return false;
}

CassError CassandraConnector::set_last_ckp(std::string dist_name, std::string ckp_id)
{
	std::stringstream query;
	query << "INSERT INTO " << last_ckps_canonical_name << " (dist_name, last_ckp_id) VALUES (?, ?);";

	CassStatement* statement = cass_statement_new(query.str().c_str(), 2);
	cass_statement_bind_string(statement, 0, dist_name.c_str());
	cass_statement_bind_string(statement, 1, ckp_id.c_str());

	return _execute_query(statement);
}

CassError CassandraConnector::_save_state(std::string node_id, std::string dist_name, std::string comp_name, std::string ckp_id,
									std::vector<unsigned char>* state)
{
	std::stringstream query;
	query << "INSERT INTO " << data_ckps_canonical_name << " (node_id, dist_name, comp_name, ckp_id, state) VALUES (?, ?, ?, ?, ?);";
	std::string _state = std::string(state->begin(), state->end());

	CassStatement* statement = cass_statement_new(query.str().c_str(), 5);
	cass_statement_bind_string(statement, 0, node_id.c_str());
	cass_statement_bind_string(statement, 1, dist_name.c_str());
	cass_statement_bind_string(statement, 2, comp_name.c_str());
	cass_statement_bind_string(statement, 3, ckp_id.c_str());
	cass_statement_bind_string(statement, 4, _state.c_str());

	return _execute_query(statement);
}

void CassandraConnector::list_data_ckps()
{
	CassError rc = CASS_OK;
	CassStatement* statement = NULL;
	CassFuture* future = NULL;
	std::stringstream query;
	query << "SELECT * FROM " << data_ckps_canonical_name;

	statement = cass_statement_new(query.str().c_str(), 0);
	future = cass_session_execute(m_session, statement);
	cass_future_wait(future);

	const char * node_id, * dist_name, * comp_name, * ckp_id, * state;
	size_t len_node_id, len_dist_name, len_comp_name, len_ckp_id, len_state;

	rc = cass_future_error_code(future);
	if (rc != CASS_OK)
	{
		print_error(future);
	}
	else
	{
		const CassResult* result = cass_future_get_result(future);
		CassIterator* iterator = cass_iterator_from_result(result);

		printf("\n%-5s\t%-30s\t%-30s\t%-10s\t%-10s\n", "node_id", "dist_name", "comp_name", "ckp_id", "state");

		while(cass_iterator_next(iterator))
		{
			const CassRow* row = cass_iterator_get_row(iterator);

			cass_value_get_string(cass_row_get_column(row, 0), &node_id, &len_node_id);
			cass_value_get_string(cass_row_get_column(row, 1), &dist_name, &len_dist_name);
			cass_value_get_string(cass_row_get_column(row, 2), &comp_name, &len_comp_name);
			cass_value_get_string(cass_row_get_column(row, 3), &ckp_id, &len_ckp_id);
			cass_value_get_string(cass_row_get_column(row, 4), &state, &len_state);
			char state_descr[10];
			sprintf(state_descr, "%d", (int)len_state);
			//printf("STATE %s\n", state);
			printf("%-5s\t%-30s\t%-30s\t%-10s\t%-10s\n", node_id, dist_name, comp_name, ckp_id, state_descr);
		}
		printf("\n");
		cass_result_free(result);
		cass_iterator_free(iterator);
	}
	cass_future_free(future);
	cass_statement_free(statement);
}

void CassandraConnector::list_last_ckps()
{
	CassError rc = CASS_OK;
	CassStatement* statement = NULL;
	CassFuture* future = NULL;
	std::stringstream query;
	query << "SELECT * FROM " << last_ckps_canonical_name;

	statement = cass_statement_new(query.str().c_str(), 0);
	future = cass_session_execute(m_session, statement);
	cass_future_wait(future);

	const char * dist_name, * last_ckp_id;
	size_t len_dist_name, len_last_ckp_id;

	rc = cass_future_error_code(future);
	if (rc != CASS_OK)
	{
		print_error(future);
	}
	else
	{
		const CassResult* result = cass_future_get_result(future);
		CassIterator* iterator = cass_iterator_from_result(result);

		printf("\n%-30s\t%-10s\n", "dist_name", "ckp_id");

		while(cass_iterator_next(iterator))
		{
			const CassRow* row = cass_iterator_get_row(iterator);

			cass_value_get_string(cass_row_get_column(row, 0), &dist_name, &len_dist_name);
			cass_value_get_string(cass_row_get_column(row, 1), &last_ckp_id, &len_last_ckp_id);
			printf("%-30s\t%-10s\n", dist_name, last_ckp_id);
		}
		printf("\n");
		cass_result_free(result);
		cass_iterator_free(iterator);
	}
	cass_future_free(future);
	cass_statement_free(statement);
}

bool CassandraConnector::get_last_ckp(std::string dist_name, std::string &last_ckp_id)
{
	CassError rc = CASS_OK;
	CassStatement* statement = NULL;
	CassFuture* future = NULL;
	std::stringstream query;
	query << "SELECT * FROM " << last_ckps_canonical_name << " WHERE dist_name = '" << dist_name << "'";
	std::cout << CASSANDRA_CONNECTOR_NAME << "Query is " << query.str() << std::endl;
	bool is_found = false;

	statement = cass_statement_new(query.str().c_str(), 0);
	future = cass_session_execute(m_session, statement);
	cass_future_wait(future);

	const char * last_ckp_buffer;
	size_t len_last_ckp_id;

	rc = cass_future_error_code(future);
	if (rc != CASS_OK)
	{
		print_error(future);
	}
	else
	{
		const CassResult* result = cass_future_get_result(future);
		CassIterator* iterator = cass_iterator_from_result(result);

		if(cass_iterator_next(iterator))
		{
			const CassRow* row = cass_iterator_get_row(iterator);
			cass_value_get_string(cass_row_get_column(row, 1), &last_ckp_buffer, &len_last_ckp_id);
			last_ckp_id = std::string(last_ckp_buffer);
			is_found = true;
			std::cout << CASSANDRA_CONNECTOR_NAME << "YES checkpoint `" << last_ckp_id << "` found for dist_name = " << dist_name << std::endl;
		}
		else
		{
			std::cout << CASSANDRA_CONNECTOR_NAME << "NO checkpoint found for dist_name = " << dist_name << ". WHAT should I do ?" << std::endl;
		}
		cass_result_free(result);
		cass_iterator_free(iterator);
	}
	cass_future_free(future);
	cass_statement_free(statement);
	return is_found;
}
