#!/bin/bash
echo "BlockmonWithCheckpoint | cassandra-cpp-client"
echo "CPP"
sdiff -s /home/patrizio/BlockmonWithCheckpoint/core/checkpoint/CassandraConnector.cpp /home/patrizio/cassandra-cpp-client/src/CassandraConnector.cpp
echo "HPP"
sdiff -s /home/patrizio/BlockmonWithCheckpoint/core/checkpoint/CassandraConnector.hpp /home/patrizio/cassandra-cpp-client/src/CassandraConnector.hpp
