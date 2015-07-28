#!/usr/bin/env bash

#if config not provided, check if there is a cached version. If there is good else ask for config file. If config file provided add it and override the old one if present.

IFS=' ' read -a array <<< $@

isConfigProvided=false
for index in "${!array[@]}"
do
#if config provided, use it and add to cache.
if [ "${array[index]}" = "--config" ]; then
      echo " metadata api config file provided"
        echo "${array[index+1]}"
     CONFIGLOCATION=`echo "${array[index+1]}"`
     echo config location "$CONFIGLOCATION"
     echo "$CONFIGLOCATION" > metadataapilocation.config
     isConfigProvided=true
     scriptInput="$scriptInput ${array[index]}"
     else
   #  echo "${array[index]}"
     scriptInput="$scriptInput ${array[index]}"
fi
done
#if not provided. Check if cached.
if [ "$isConfigProvided" = false ]; then
    echo "Checking cached copy"
    if [ -f metadataapilocation.config ]; then
        echo "metadata api picked from cache!"
        CONFIGLOCATION=`cat metadataapilocation.config`

        scriptInput="$scriptInput --config $CONFIGLOCATION"
    else
        echo "Please provide a metadata api config file with tag --config"
    fi
fi
echo jar input is  $scriptInput
java -jar /Users/dhaval/Documents/Workspace/KamanjaScript/Kamanja/trunk/MetadataAPI/target/scala-2.10/MetadataAPI-1.0 $scriptInput
