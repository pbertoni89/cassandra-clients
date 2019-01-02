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
	int g_flag = 0, d_flag = 0, e_flag = 0, b_flag = 0, l_flag = 0, s_flag = 0;

	while ((option_char = getopt(argc, argv, "gdebls")) != EOF)
	{
		switch (option_char)
		{
		case 'g':
			g_flag = 1;
			break;
		case 'd':
			d_flag = 1;
			break;
		case 'e':
			e_flag = 1;
			break;
		case 'b':
			b_flag = 1;
			break;
		case 'l':
			l_flag = 1;
			break;
		case 's':
			s_flag = 1;
			break;
		default:
			fprintf (stderr,
					"usage: %s [gdebls]\n\tg - get last ckp\n\td - drop db\n\te - insert example rows\n\tb - run basic\n\tl - list db\n\ts run schema meta\n", argv[0]);
		}
	}

	CassandraConnector connector;

	if(connector.connect())
	{
		if(g_flag)
		{
			std::string tmp_last_ckp;
			std::string m_dist_name = "wc1";
			bool is_found_last_ckp = false;

			is_found_last_ckp = connector.get_last_ckp(m_dist_name, tmp_last_ckp);
			if (is_found_last_ckp)
				std::cout << "I've found an old checkpoint to recover! id: `" << tmp_last_ckp << "`" << std::endl;
			else
				std::cout << "NOT found an old checkpoint to recover!" << std::endl;
		}
		else if(d_flag)
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
		else if(l_flag)
		{
			connector.list_data_ckps();
			connector.list_last_ckps();
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
