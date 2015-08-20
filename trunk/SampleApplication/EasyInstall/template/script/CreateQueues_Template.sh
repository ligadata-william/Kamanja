#!/bin/sh
{KafkaInstallDir}/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic testin_1
{KafkaInstallDir}/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic testout_1
{KafkaInstallDir}/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic teststatus_1
