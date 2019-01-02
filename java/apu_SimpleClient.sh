#!/bin/bash
reset
mvn -e compile
mvn -X -f pom.xml exec:java -Dexec.mainClass=com.pbertoni.cassandra.SimpleClient -Dexec.args="10.20.10.246"
