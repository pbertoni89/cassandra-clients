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
#include <getopt.h>
#include <unistd.h>
using namespace std;

int main(int argc, char* argv[])
{
	int option_char;
	int d_flag = 0, e_flag = 0, b_flag = 0, s_flag = 0;

	while ((option_char = getopt(argc, argv, "debs")) != EOF)
	{
		switch (option_char)
		{
		case 'd':
			d_flag = 1;
			break;
		case 'e':
			e_flag = 1;
			break;
		case 'b':
			b_flag = 1;
			break;
		case 's':
			s_flag = 1;
			break;
		default:
			fprintf (stderr, "usage: %s [debsk]\n", argv[0]);
		}
	}

	CassandraConnector connector;

	if(connector.connect())
	{
		if(d_flag)
		{
			connector.drop_db();
		}
		else if(e_flag)
		{
			connector.recreate_db();
			std::vector<unsigned char> foo_states;
			connector.save_state("1", "wc1", "src", "ABCD", &foo_states);
			connector.list_data_ckps();
			connector.save_state("1", "wc1", "dst", "ABCD", &foo_states);
			connector.list_data_ckps();
			connector.save_state("1", "wc2", "src", "EA17", &foo_states);
			connector.list_data_ckps();
			connector.save_state("2", "wc2", "src", "EA17", &foo_states);
			connector.list_data_ckps();
			connector.save_state("1", "wc2", "dst", "EA17", &foo_states);
			connector.list_data_ckps();
			connector.list_last_ckps();
		}
		else if(b_flag)
		{
			Basic b;
			b.run();
		}
		else if(s_flag)
		{
			SchemaMeta sm;
			sm.run();
		}

		connector.disconnect();
	}
	else
		std::cout << "Connection to Cassandra Cluster failed " << std::endl;
	return 0;
}
