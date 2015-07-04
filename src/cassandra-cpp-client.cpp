//============================================================================
// Name        : cassandra-cpp-client.cpp
// Author      : Patrizio Bertoni
// Version     :
// Copyright   : Your copyright notice
// Description : Hello World in C++, Ansi-style
//============================================================================

#include "CassandraConnector.hpp"
#include /*Select*/"KeyspacesQuery.cpp"
#include "Basic.hpp"
#include "SchemaMeta.hpp"
using namespace std;

int main()
{

// 1
/*
	CassandraConnector connector;
	connector.connect();
	CassSession* session = connector.get_session();
	KeySpacesQuery ksq(session);
	ksq.query();
	connector.disconnect();
*/

// 2
/*
	Basic b;
	b.run();
*/

// 3
	SchemaMeta sm;
	sm.run();

	return 0;
}
