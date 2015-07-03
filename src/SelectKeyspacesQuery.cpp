#ifndef SELECTKEYSPACESQUERY
#define SELECTKEYSPACESQUERY
#include "PatQuery.cpp"

class SelectKeyspacesQuery : public PatQuery
{
	void _query()
	{
		std::cout << "Now listing known keyspaces" << std::endl;
		while (cass_iterator_next(m_rows))
		{
			const CassRow* row = cass_iterator_get_row(m_rows);
			const CassValue* value = cass_row_get_column_by_name(row, "keyspace_name");

			const char* keyspace_name;
			size_t keyspace_name_length;
			cass_value_get_string(value, &keyspace_name, &keyspace_name_length);
			printf("keyspace_name: '%.*s'\n", (int) keyspace_name_length, keyspace_name);
		}
	}

public:
	SelectKeyspacesQuery(CassSession* session) :
		PatQuery(session,	"SELECT keyspace_name "
							"FROM system.schema_keyspaces;")
	{}
};
#endif
