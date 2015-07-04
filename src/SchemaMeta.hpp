#ifndef SCHEMAMETA_HPP_
#define SCHEMAMETA_HPP_
#include "CassandraConnector.hpp"

class SchemaMeta : public CassandraConnector
{
	void print_keyspace(const char* keyspace);
	void print_table(const char* keyspace, const char* table);

	void print_schema_value(const CassValue* value);
	void print_schema_list(const CassValue* value);
	void print_schema_map(const CassValue* value);
	void print_schema_meta(const CassSchemaMeta* meta, int indent);
	void print_schema_meta_field(const CassSchemaMetaField* field, int indent);
	void print_schema_meta_fields(const CassSchemaMeta* meta, int indent);
	void print_schema_meta_entries(const CassSchemaMeta* meta, int indent);
	void print_indent(int indent);

protected:
	/** Inner logic. */
	void _run();

public:
	SchemaMeta() : CassandraConnector()
	{}
};
#endif
