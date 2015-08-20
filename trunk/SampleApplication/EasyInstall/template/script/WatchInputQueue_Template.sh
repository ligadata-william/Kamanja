#!/bin/sh
{KafkaInstallDir}/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic testin_1 --from-beginning
