#!/bin/sh
if [ "$KAFKA_HOME" = "" ]; then
    echo "Please set KAFKA_HOME"
    exit 1
fi
<<<<<<< HEAD
$KAFKA_HOME/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic testout_1 --from-beginning
=======
$KAFKA_HOME/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic testout_1 --from-beginning
>>>>>>> 44fc11ba7559dd5d484f569ad365ca2c5f9078af
