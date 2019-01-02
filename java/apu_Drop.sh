#!/bin/bash
reset
mvn -f pom.xml exec:java -Dexec.mainClass=com.pbertoni.cassandra.CassandraConsole -Dexec.args="10.20.10.246 drop"
