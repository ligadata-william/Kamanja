#!/bin/bash

# Start the engine with hashdb backed metadata configuration.  The zookeeper and your queue software should be running
ipport="8998"

if [ "$1" != "debug" ]; then
	java -Dlog4j.configuration=file:{InstallDirectory}/config/log4j.properties -XX:+CMSClassUnloadingEnabled -jar {InstallDirectory}/bin/KamanjaManager-1.0 --config {InstallDirectory}/config/Engine1Config.properties
else
	java -Xdebug -Xrunjdwp:transport=dt_socket,address="$ipport",server=y -Dlog4j.configuration=file:{InstallDirectory}/config/log4j.properties -XX:+CMSClassUnloadingEnabled -jar {InstallDirectory}/bin/KamanjaManager-1.0 --config {InstallDirectory}/config/Engine1Config.properties
fi
