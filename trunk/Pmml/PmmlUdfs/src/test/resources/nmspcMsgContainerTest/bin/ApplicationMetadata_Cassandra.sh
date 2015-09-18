#!/bin/bash

ipport="8998"

if [ "$1" != "debug" ]; then
	java -jar /tmp/drdigital/bin/MetadataAPI-1.0 --config /tmp/drdigital/input/nmspcMsgContainerTest/metadata/config/MetadataAPIConfig_Cassandra.properties
else	
	java -Xdebug -Xrunjdwp:transport=dt_socket,address="$ipport",server=y -jar /tmp/drdigital/bin/MetadataAPI-1.0 --config /tmp/drdigital/input/nmspcMsgContainerTest/metadata/config/MetadataAPIConfig_Cassandra.properties
fi

