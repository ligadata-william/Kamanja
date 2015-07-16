#!/bin/bash

ipport="8998"

if [ "$1" != "debug" ]; then
	java -jar /tmp/KamanjaInstall/bin/MetadataAPI-1.0 --config /tmp/KamanjaInstall/input/application-3/metadata/config/MetadataAPIConfig.properties
else	
	java -Xdebug -Xrunjdwp:transport=dt_socket,address="$ipport",server=y -jar /tmp/KamanjaInstall/bin/MetadataAPI-1.0 --config /tmp/KamanjaInstall/input/application-3/metadata/config/MetadataAPIConfig.properties
fi

