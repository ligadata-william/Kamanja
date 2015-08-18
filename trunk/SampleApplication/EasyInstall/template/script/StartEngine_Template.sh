#!/bin/bash
KAMANJA_BASEPATH=$(brew --prefix)/Cellar/kamanja/1.1.0

# Start the engine with hashdb backed metadata configuration.  The zookeeper and your queue software should be running
ipport="8998"

if [ "$1" != "debug" ]; then
	java -Dlog4j.configuration=file:$KAMANJA_BASEPATH/config/log4j.properties -jar $KAMANJA_BASEPATH/bin/KamanjaManager-1.0 --config $KAMANJA_BASEPATH/config/Engine1Config.properties
else
	java -Xdebug -Xrunjdwp:transport=dt_socket,address="$ipport",server=y -Dlog4j.configuration=file:$KAMANJA_BASEPATH/config/log4j.properties -jar $KAMANJA_BASEPATH/bin/KamanjaManager-1.0 --config $KAMANJA_BASEPATH/config/Engine1Config.properties
fi
