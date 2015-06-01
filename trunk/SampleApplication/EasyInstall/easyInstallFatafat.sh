#!/bin/bash

set -e

installPath=$1
srcPath=$2
ivyPath=$3
KafkaRootDir=$4

if [ ! -d "$installPath" ]; then
        echo "Not valid install path supplied.  It should be a directory that can be written to and whose current content is of no value (will be overwritten) "
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi

if [ ! -d "$srcPath" ]; then
        echo "Not valid src path supplied.  It should be the trunk directory containing the jars, files, what not that need to be supplied."
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi

if [ ! -d "$ivyPath" ]; then
        echo "Not valid ivy path supplied.  It should be the ivy path for dependency the jars."
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi

if [ ! -d "$KafkaRootDir" ]; then
        echo "Not valid Kafka path supplied."
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi

installPath=$(echo $installPath | sed 's/[\/]*$//')
srcPath=$(echo $srcPath | sed 's/[\/]*$//')
ivyPath=$(echo $ivyPath | sed 's/[\/]*$//')

# *******************************
# Clean out prior installation
# *******************************
rm -Rf $installPath

# *******************************
# Make the directories as needed
# *******************************
mkdir -p $installPath/bin
mkdir -p $installPath/lib
mkdir -p $installPath/lib/system
mkdir -p $installPath/lib/application
mkdir -p $installPath/storage
mkdir -p $installPath/logs
mkdir -p $installPath/config
mkdir -p $installPath/documentation
mkdir -p $installPath/output
mkdir -p $installPath/workingdir
mkdir -p $installPath/template
mkdir -p $installPath/template/config
mkdir -p $installPath/template/script
mkdir -p $installPath/input
mkdir -p $installPath/input/application-2-Healthcare
mkdir -p $installPath/input/application-2-Healthcare/bin
mkdir -p $installPath/input/application-2-Healthcare/data
mkdir -p $installPath/input/application-2-Healthcare/metadata
mkdir -p $installPath/input/application-2-Healthcare/metadata/config
mkdir -p $installPath/input/application-2-Healthcare/metadata/container
mkdir -p $installPath/input/application-2-Healthcare/metadata/function
mkdir -p $installPath/input/application-2-Healthcare/metadata/message
mkdir -p $installPath/input/application-2-Healthcare/metadata/model
mkdir -p $installPath/input/application-2-Healthcare/metadata/script
mkdir -p $installPath/input/application-2-Healthcare/metadata/type
mkdir -p $installPath/input/application-2-Healthcare/template
mkdir -p $installPath/input/application-2-Healthcare/metadata/outputmsg

# application-1-HelloWorld
mkdir -p $installPath/input/application-1-HelloWorld
mkdir -p $installPath/input/application-1-HelloWorld/bin
mkdir -p $installPath/input/application-1-HelloWorld/data
mkdir -p $installPath/input/application-1-HelloWorld/metadata
mkdir -p $installPath/input/application-1-HelloWorld/metadata/config
mkdir -p $installPath/input/application-1-HelloWorld/metadata/container
mkdir -p $installPath/input/application-1-HelloWorld/metadata/function
mkdir -p $installPath/input/application-1-HelloWorld/metadata/message
mkdir -p $installPath/input/application-1-HelloWorld/metadata/model
mkdir -p $installPath/input/application-1-HelloWorld/metadata/script
mkdir -p $installPath/input/application-1-HelloWorld/metadata/type
mkdir -p $installPath/input/application-1-HelloWorld/template
mkdir -p $installPath/input/application-2-Healthcare/metadata/outputmsg
# application-1-HelloWorld

bin=$installPath/bin
systemlib=$installPath/lib/system
applib=$installPath/lib/application

echo $installPath
echo $srcPath
echo $bin

# *******************************
# Build fat-jars
# *******************************

echo "clean, package and assemble $srcPath ..."

cd $srcPath
sbt package FatafatManager/assembly MetadataAPI/assembly KVInit/assembly MethodExtractor/assembly SimpleKafkaProducer/assembly NodeInfoExtract/assembly ExtractData/assembly MetadataAPIService/assembly

# recreate eclipse projects
#echo "refresh the eclipse projects ..."
#cd $srcPath
#sbt eclipse

# Move them into place
echo "copy the fat jars to $installPath ..."

cd $srcPath
cp Utils/KVInit/target/scala-2.10/KVInit* $bin
cp MetadataAPI/target/scala-2.10/MetadataAPI* $bin
cp FatafatManager/target/scala-2.10/FatafatManager* $bin
cp Pmml/MethodExtractor/target/scala-2.10/MethodExtractor* $bin
cp Utils/SimpleKafkaProducer/target/scala-2.10/SimpleKafkaProducer* $bin
cp Utils/ExtractData/target/scala-2.10/ExtractData* $bini
cp MetadataAPIService/target/scala-2.10/MetadataAPIService* $bin

# *******************************
# Copy jars required (more than required if the fat jars are used)
# *******************************

# Base Types and Functions, InputOutput adapters, and original versions of things
echo "copy all Fatafat jars and the jars upon which they depend to the $systemlib"

bash SampleApplication/EasyInstall/allDeps.scala --systemlib $systemlib BaseTypes BaseFunctions Serialize ZooKeeperClient ZooKeeperListener FatafatBase FatafatManager KafkaSimpleInputOutputAdapters FileSimpleInputOutputAdapters SimpleEnvContextImpl Storage Metadata MessageDef OutputMsgDef LoadtestCommon LoadtestRunner LoadtestMaster Loadtest PmmlRuntime PmmlCompiler PmmlUdfs MethodExtractor MetadataBootstrap MetadataAPI MetadataAPIService MetadataAPIServiceClient SimpleKafkaProducer KVInit ZooKeeperLeaderLatch JsonDataGen NodeInfoExtract Controller SimpleApacheShiroAdapter AuditAdapters FatafatData CustomUdfLib ExtractData 

# sample configs
#echo "copy sample configs..."
cp $srcPath/Utils/KVInit/src/main/resources/*cfg $systemlib

# Generate keystore file
#echo "generating keystore..."
#keytool -genkey -keyalg RSA -alias selfsigned -keystore $installPath/config/keystore.jks -storepass password -validity 360 -keysize 2048


# *******************************
# COPD messages data prep
# *******************************

# Prepare test messages and copy them into place

echo "Prepare test messages and copy them into place..."
cd $srcPath/Utils/KVInit/src/main/resources
cp copd_demo.csv.gz $installPath/input/application-2-Healthcare/data

cd $srcPath/SampleApplication/Medical/SampleData
cp *.csv $installPath/input/application-2-Healthcare/data

# application-1-HelloWorld
cd $srcPath/SampleApplication/HelloWorld/data
cp * $installPath/input/application-1-HelloWorld/data
# application-1-HelloWorld

# *******************************
# Copy documentation files
# *******************************
cd $srcPath/Documentation
cp -rf * $installPath/documentation

# *******************************
# copy models, messages, containers, config, scripts, types  messages data prep
# *******************************

cp $srcPath/FatafatManager/src/main/resources/log4j.properties $installPath/config

# Not copying anything from here
# cd $srcPath/SampleApplication/Medical/Configs
# cp * $installPath/input/application-2-Healthcare/metadata/config

cd $srcPath/SampleApplication/Medical/MessagesAndContainers/Fixed/Containers
cp * $installPath/input/application-2-Healthcare/metadata/container

# application-1-HelloWorld
cd $srcPath/SampleApplication/HelloWorld/container
cp * $installPath/input/application-1-HelloWorld/metadata/container
# application-1-HelloWorld

cd $srcPath/SampleApplication/Medical/Functions
cp * $installPath/input/application-2-Healthcare/metadata/function

cd $srcPath/SampleApplication/Medical/MessagesAndContainers/Fixed/Messages
cp * $installPath/input/application-2-Healthcare/metadata/message

cd $srcPath/SampleApplication/Medical/OutputMsg
cp * $installPath/input/application-2-Healthcare/metadata/outputmsg

# application-1-HelloWorld
cd $srcPath/SampleApplication/HelloWorld/message
cp * $installPath/input/application-1-HelloWorld/metadata/message
# application-1-HelloWorld

cd $srcPath/SampleApplication/Medical/Models
cp *.* $installPath/input/application-2-Healthcare/metadata/model

# application-1-HelloWorld
cd $srcPath/SampleApplication/HelloWorld/model
cp * $installPath/input/application-1-HelloWorld/metadata/model
# application-1-HelloWorld

cd $srcPath/SampleApplication/Medical/Types
cp * $installPath/input/application-2-Healthcare/metadata/type

cd $srcPath/SampleApplication/Medical/template
cp -rf * $installPath/input/application-2-Healthcare/template

# application-1-HelloWorld
cd $srcPath/SampleApplication/HelloWorld/template
cp -rf * $installPath/input/application-1-HelloWorld/template
# application-1-HelloWorld

cd $srcPath/SampleApplication/EasyInstall/template
cp -rf * $installPath/template

cd $srcPath/SampleApplication/EasyInstall
cp SetPaths.sh $installPath/bin/

bash $installPath/bin/SetPaths.sh $KafkaRootDir

chmod 0700 $installPath/input/application-2-Healthcare/bin/*sh

echo "FataFat install complete..."
