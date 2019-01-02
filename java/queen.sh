#!/bin/bash
reset
mvn compile
mvn -f pom.xml exec:java -Dexec.mainClass=com.pbertoni.cassandra.CassandraConnector -Dexec.args="192.168.0.50"
