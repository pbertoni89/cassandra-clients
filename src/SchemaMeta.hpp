/*
 * SchemaMeta.hpp
 *
 *  Created on: 3 Jul 2015
 *      Author: atreides
 */

#ifndef SCHEMAMETA_HPP_
#define SCHEMAMETA_HPP_

#include <stdio.h>
#include <cassandra.h>

class SchemaMeta
{
	/**
	 * @brief private class constructor, as in Meyer's singleton
	 */
	SchemaMeta()
	{}

	void print_keyspace(CassSession* session, const char* keyspace);
	void print_table(CassSession* session, const char* keyspace, const char* table);
	void print_error(CassFuture* future);

	CassError execute_query(CassSession* session, const char* query);

	void print_schema_value(const CassValue* value);
	void print_schema_list(const CassValue* value);
	void print_schema_map(const CassValue* value);
	void print_schema_meta(const CassSchemaMeta* meta, int indent);
	void print_schema_meta_field(const CassSchemaMetaField* field, int indent);
	void print_schema_meta_fields(const CassSchemaMeta* meta, int indent);
	void print_schema_meta_entries(const CassSchemaMeta* meta, int indent);
	void print_indent(int indent);

public:
	/**
	 * @brief unique instance accessor as in Meyer's singleton
	 * @return a reference to the only instance of this class
	 */
	static SchemaMeta& instance()
	{
		static SchemaMeta t;
		return t;
	}

	int run();
};

#endif /* SCHEMAMETA_HPP_ */
