#!/bin/bash
reset
mvn -f pom.xml exec:java -Dexec.mainClass=com.pbertoni.cassandra.CassandraConnector -Dexec.args="10.20.10.246"
