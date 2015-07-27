#!/usr/bin/env bash

#if config not provided, check if there is a cached version. If there is good else ask for config file. If config file provided add it and override the old one if present.

IFS=' --' read -a array <<< $@


for index in "${!array[@]}"
do
if [ "${array[index]}" = "config" ]; then
     echo "${array[index+1]}"
fi
done

#java -jar /Users/dhaval/Documents/Workspace/KamanjaScript/Kamanja/trunk/MetadataAPI/target/scala-2.10/MetadataAPI-1.0 $@
