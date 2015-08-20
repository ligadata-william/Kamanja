#!/usr/bin/env bash
KAMANJA_BASEPATH=$(brew --prefix)/Cellar/kamanja/1.1.0
if [ "$#" -eq 1 ]; then
INPUTFILE=$@
else
count=0
FILEDIR=$KAMANJA_BASEPATH/input/SampleApplications/data
for entry in "$FILEDIR"/*
do
count=$((count+1))
  echo "$count: $entry"
  LISTOFFILES[count-1]=$entry
done
read -p "Please select from the above options: " useroption
OPTION=useroption-1
INPUTFILE=${LISTOFFILES[OPTION]}
fi
echo "User selected: $INPUTFILE"
java -jar /usr/local/bin/SimpleKafkaProducer-0.1.0 --gz true --topics "testin_1" --threads 1 --topicpartitions 8 --brokerlist "localhost:9092" --files $INPUTFILE   --partitionkeyidxs "1" --format CSV
