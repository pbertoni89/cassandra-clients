//============================================================================
// Name        : cassandra-cpp-client.cpp
// Author      : Patrizio Bertoni
// Version     :
// Copyright   : Your copyright notice
// Description : Hello World in C++, Ansi-style
//============================================================================

#include <iostream>
#include "CassandraConnector.cpp"
#include /*Select*/"KeyspacesQuery.cpp"
#include "Basic.hpp"
#include "SchemaMeta.hpp"
using namespace std;

int main()
{
	CassandraConnector connector;
	connector.connect();
	CassSession* session = connector.get_session();
	/*Select*/KeySpacesQuery ksq(session);
	ksq.query();
	connector.disconnect();

	Basic::instance().run();
	SchemaMeta::instance().run();

	return 0;
}
