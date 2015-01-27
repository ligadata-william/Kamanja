#!/bin/bash

installPath=$1
srcPath=$2
ivyPath=$3
KafkaRootDir=$4

if [ ! -d "$installPath" ]; then
        echo "No install path supplied.  It should be a directory that can be written to and whose current content is of no value (will be overwritten) "
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies>"
        exit 1
fi

if [ ! -d "$srcPath" ]; then
        echo "No src path supplied.  It should be the trunk directory containing the jars, files, what not that need to be supplied."
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies>"
        exit 1
fi

if [ ! -d "$ivyPath" ]; then
        echo "No ivy path supplied.  It should be the ivy path for dependency the jars."
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies>"
        exit 1
fi

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
mkdir -p $installPath/input/application1
mkdir -p $installPath/input/application1/data
mkdir -p $installPath/input/application1/metadata
mkdir -p $installPath/input/application1/metadata/config
mkdir -p $installPath/input/application1/metadata/container
mkdir -p $installPath/input/application1/metadata/function
mkdir -p $installPath/input/application1/metadata/message
mkdir -p $installPath/input/application1/metadata/model
mkdir -p $installPath/input/application1/metadata/script
mkdir -p $installPath/input/application1/metadata/type

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
sbt clean package OnLEPManager/assembly MetadataAPI/assembly KVInit/assembly MethodExtractor/assembly SimpleKafkaProducer/assembly NodeInfoExtract/assembly

# recreate eclipse projects
#echo "refresh the eclipse projects ..."
#cd $srcPath
#sbt eclipse

# Move them into place
echo "copy the fat jars to $installPath ..."

cd $srcPath
cp Utils/KVInit/target/scala-2.10/KVInit* $bin
cp MetadataAPI/target/scala-2.10/MetadataAPI* $bin
cp OnLEPManager/target/scala-2.10/OnLEPManager* $bin
cp Pmml/MethodExtractor/target/scala-2.10/MethodExtractor* $bin
cp Utils/SimpleKafkaProducer/target/scala-2.10/SimpleKafkaProducer* $bin

# *******************************
# Copy jars required (more than required if the fat jars are used)
# *******************************

# Base Types and Functions, InputOutput adapters, and original versions of things
echo "copy Base Types and Functions, InputOutput adapters..."
cp $srcPath/BaseFunctions/target/scala-2.10/basefunctions_2.10-0.1.0.jar $systemlib
cp $srcPath/BaseTypes/target/scala-2.10/basetypes_2.10-0.1.0.jar $systemlib
cp $srcPath/InputOutputAdapters/FileSimpleInputOutputAdapters/target/scala-2.10/filesimpleinputoutputadapters_2.10-1.0.jar $systemlib
cp $srcPath/InputOutputAdapters/KafkaSimpleInputOutputAdapters/target/scala-2.10/kafkasimpleinputoutputadapters_2.10-1.0.jar $systemlib
cp $srcPath/EnvContexts/SimpleEnvContextImpl/target/scala-2.10/simpleenvcontextimpl_2.10-1.0.jar $systemlib
cp $srcPath/MetadataBootstrap/Bootstrap/target/scala-2.10/bootstrap_2.10-1.0.jar $systemlib

# Storage jars
echo "copy Storage jars..."
cp $srcPath/Storage/target/scala-2.10/storage_2.10-0.0.0.2.jar $systemlib

# Metadata jars
echo "copy Metadata jars..."
cp $srcPath/Metadata/target/scala-2.10/metadata_2.10-1.0.jar $systemlib
cp $srcPath/MessageDef/target/scala-2.10/messagedef_2.10-1.0.jar $systemlib
cp $srcPath/MetadataAPI/target/scala-2.10/metadataapi_2.10-1.0.jar $systemlib
cp $srcPath/Pmml/MethodExtractor/target/scala-2.10/methodextractor_2.10-1.0.jar $systemlib

# OnLEP jars
echo "copy OnLEP jars..."
cp $srcPath/OnLEPBase/target/scala-2.10/onlepbase_2.10-1.0.jar $systemlib
cp $srcPath/OnLEPManager/target/scala-2.10/onlepmanager_2.10-1.0.jar $systemlib

# Pmml compile and runtime jars
echo "copy Pmml compile and runtime jars..."
cp $srcPath/Pmml/PmmlRuntime/target/scala-2.10/pmmlruntime_2.10-1.0.jar $systemlib
cp $srcPath/Pmml/PmmlUdfs/target/scala-2.10/pmmludfs_2.10-1.0.jar $systemlib
cp $srcPath/Pmml/PmmlCompiler/target/scala-2.10/pmmlcompiler_2.10-1.0.jar $systemlib

# sample configs
#echo "copy sample configs..."
cp $srcPath/Utils/KVInit/src/main/resources/*cfg $systemlib

# other jars 
echo "copy other jars..."
cp $srcPath/../externals/log4j/log4j-1.2.17.jar $systemlib
cp $srcPath/Utils/Serialize/target/scala-2.10/serialize_2.10-1.0.jar $systemlib

# env context jars
echo "env context jars..."
cp $ivyPath/cache/asm/asm/jars/asm-3.1.jar $systemlib
cp $ivyPath/cache/com.codahale.metrics/metrics-core/bundles/metrics-core-3.0.2.jar $systemlib
cp $ivyPath/cache/com.yammer.metrics/metrics-core/jars/metrics-core-2.2.0.jar $systemlib
cp $ivyPath/cache/com.datastax.cassandra/cassandra-driver-core/bundles/cassandra-driver-core-2.0.2.jar $systemlib
cp $ivyPath/cache/com.esotericsoftware.kryo/kryo/bundles/kryo-2.21.jar $systemlib
cp $ivyPath/cache/com.esotericsoftware.minlog/minlog/jars/minlog-1.2.jar $systemlib
cp $ivyPath/cache/com.esotericsoftware.reflectasm/reflectasm/jars/reflectasm-1.07-shaded.jar $systemlib
cp $ivyPath/cache/com.fasterxml.jackson.core/jackson-annotations/bundles/jackson-annotations-2.3.0.jar $systemlib
cp $ivyPath/cache/com.fasterxml.jackson.core/jackson-core/bundles/jackson-core-2.3.1.jar $systemlib
cp $ivyPath/cache/com.fasterxml.jackson.core/jackson-databind/bundles/jackson-databind-2.3.1.jar $systemlib
cp $ivyPath/cache/com.github.stephenc.findbugs/findbugs-annotations/jars/findbugs-annotations-1.3.9-1.jar $systemlib
cp $ivyPath/cache/com.google.code.findbugs/jsr305/jars/jsr305-1.3.9.jar $systemlib
cp $ivyPath/cache/com.google.collections/google-collections/jars/google-collections-1.0.jar $systemlib
cp $ivyPath/cache/com.google.guava/guava/bundles/guava-16.0.1.jar $systemlib
cp $ivyPath/cache/com.google.protobuf/protobuf-java/bundles/protobuf-java-2.5.0.jar $systemlib
cp $ivyPath/cache/com.google.protobuf/protobuf-java/bundles/protobuf-java-2.6.0.jar $systemlib
cp $ivyPath/cache/com.jamesmurty.utils/java-xmlbuilder/jars/java-xmlbuilder-0.4.jar $systemlib
cp $ivyPath/cache/com.jcraft/jsch/jars/jsch-0.1.42.jar $systemlib
cp $ivyPath/cache/com.ning/compress-lzf/bundles/compress-lzf-0.9.1.jar $systemlib
cp $ivyPath/cache/com.novocode/junit-interface/jars/junit-interface-0.11-RC1.jar $systemlib
cp $ivyPath/cache/com.sleepycat/je/jars/je-4.0.92.jar $systemlib
cp $ivyPath/cache/com.sun.jersey/jersey-core/bundles/jersey-core-1.9.jar $systemlib
cp $ivyPath/cache/com.sun.jersey/jersey-json/bundles/jersey-json-1.9.jar $systemlib
cp $ivyPath/cache/com.sun.jersey/jersey-server/bundles/jersey-server-1.9.jar $systemlib
cp $ivyPath/cache/com.sun.xml.bind/jaxb-impl/jars/jaxb-impl-2.2.3-1.jar $systemlib
cp $ivyPath/cache/com.thoughtworks.paranamer/paranamer/jars/paranamer-2.3.jar $systemlib
cp $ivyPath/cache/com.thoughtworks.paranamer/paranamer/jars/paranamer-2.6.jar $systemlib
cp $ivyPath/cache/com.twitter/chill-java/jars/chill-java-0.3.6.jar $systemlib
cp $ivyPath/cache/com.twitter/chill_2.10/jars/chill_2.10-0.3.6.jar $systemlib
cp $ivyPath/cache/commons-beanutils/commons-beanutils-core/jars/commons-beanutils-core-1.8.0.jar $systemlib
cp $ivyPath/cache/commons-beanutils/commons-beanutils/jars/commons-beanutils-1.7.0.jar $systemlib
cp $ivyPath/cache/commons-cli/commons-cli/jars/commons-cli-1.2.jar $systemlib
cp $ivyPath/cache/commons-codec/commons-codec/jars/commons-codec-1.9.jar $systemlib
cp $ivyPath/cache/commons-collections/commons-collections/jars/commons-collections-3.2.1.jar $systemlib
cp $ivyPath/cache/commons-configuration/commons-configuration/jars/commons-configuration-1.6.jar $systemlib
cp $ivyPath/cache/commons-dbcp/commons-dbcp/jars/commons-dbcp-1.2.2.jar $systemlib
cp $ivyPath/cache/commons-digester/commons-digester/jars/commons-digester-1.8.jar $systemlib
cp $ivyPath/cache/commons-el/commons-el/jars/commons-el-1.0.jar $systemlib
cp $ivyPath/cache/commons-httpclient/commons-httpclient/jars/commons-httpclient-3.1.jar $systemlib
cp $ivyPath/cache/commons-io/commons-io/jars/commons-io-2.4.jar $systemlib
cp $ivyPath/cache/commons-lang/commons-lang/jars/commons-lang-2.6.jar $systemlib
cp $ivyPath/cache/commons-logging/commons-logging/jars/commons-logging-1.1.3.jar $systemlib
cp $ivyPath/cache/commons-net/commons-net/jars/commons-net-3.1.jar $systemlib
cp $ivyPath/cache/commons-pool/commons-pool/jars/commons-pool-1.5.2.jar $systemlib
cp $ivyPath/cache/io.netty/netty/bundles/netty-3.9.0.Final.jar $systemlib
cp $ivyPath/cache/javax.activation/activation/jars/activation-1.1.jar $systemlib
cp $ivyPath/cache/javax.servlet.jsp/jsp-api/jars/jsp-api-2.1.jar $systemlib
cp $ivyPath/cache/javax.servlet/servlet-api/jars/servlet-api-2.5.jar $systemlib
cp $ivyPath/cache/javax.xml.bind/jaxb-api/jars/jaxb-api-2.2.2.jar $systemlib
cp $ivyPath/cache/javax.xml.stream/stax-api/jars/stax-api-1.0-2.jar $systemlib
cp $ivyPath/cache/jline/jline/jars/jline-0.9.94.jar $systemlib
cp $ivyPath/cache/joda-time/joda-time/jars/joda-time-2.3.jar $systemlib
cp $ivyPath/cache/junit/junit/jars/junit-4.11.jar $systemlib
cp $ivyPath/cache/log4j/log4j/bundles/log4j-1.2.17.jar $systemlib
cp $ivyPath/cache/net.java.dev.jets3t/jets3t/jars/jets3t-0.9.0.jar $systemlib
cp $ivyPath/cache/net.java.dev.jna/jna/jars/jna-3.2.7.jar $systemlib
cp $ivyPath/cache/org.apache.avro/avro/jars/avro-1.7.4.jar $systemlib
cp $ivyPath/cache/org.apache.commons/commons-compress/jars/commons-compress-1.4.1.jar $systemlib
cp $ivyPath/cache/org.apache.commons/commons-math3/jars/commons-math3-3.1.1.jar $systemlib
cp $ivyPath/cache/org.apache.hadoop/hadoop-annotations/jars/hadoop-annotations-2.4.1.jar $systemlib
cp $ivyPath/cache/org.apache.hadoop/hadoop-auth/jars/hadoop-auth-2.4.1.jar $systemlib
cp $ivyPath/cache/org.apache.hadoop/hadoop-common/jars/hadoop-common-2.4.1.jar $systemlib
cp $ivyPath/cache/org.apache.hbase/hbase-client/jars/hbase-client-0.98.4-hadoop2.jar $systemlib
cp $ivyPath/cache/org.apache.hbase/hbase-common/jars/hbase-common-0.98.4-hadoop2.jar $systemlib
cp $ivyPath/cache/org.apache.hbase/hbase-protocol/jars/hbase-protocol-0.98.4-hadoop2.jar $systemlib
cp $ivyPath/cache/org.apache.httpcomponents/httpclient/jars/httpclient-4.2.5.jar $systemlib
cp $ivyPath/cache/org.apache.httpcomponents/httpcore/jars/httpcore-4.2.4.jar $systemlib
cp $ivyPath/cache/org.apache.zookeeper/zookeeper/jars/zookeeper-3.4.6.jar $systemlib
cp $ivyPath/cache/org.cloudera.htrace/htrace-core/jars/htrace-core-2.04.jar $systemlib
cp $ivyPath/cache/org.codehaus.jackson/jackson-core-asl/jars/jackson-core-asl-1.8.8.jar $systemlib
cp $ivyPath/cache/org.codehaus.jackson/jackson-jaxrs/jars/jackson-jaxrs-1.8.3.jar $systemlib
cp $ivyPath/cache/org.codehaus.jackson/jackson-mapper-asl/jars/jackson-mapper-asl-1.8.8.jar $systemlib
cp $ivyPath/cache/org.codehaus.jackson/jackson-xc/jars/jackson-xc-1.8.3.jar $systemlib
cp $ivyPath/cache/org.codehaus.jettison/jettison/bundles/jettison-1.1.jar $systemlib
cp $ivyPath/cache/org.hamcrest/hamcrest-core/jars/hamcrest-core-1.3.jar $systemlib
cp $ivyPath/cache/org.jdom/jdom/jars/jdom-1.1.jar $systemlib
cp $ivyPath/cache/org.joda/joda-convert/jars/joda-convert-1.6.jar $systemlib
cp $ivyPath/cache/org.json4s/json4s-ast_2.10/jars/json4s-ast_2.10-3.2.9.jar $systemlib
cp $ivyPath/cache/org.json4s/json4s-core_2.10/jars/json4s-core_2.10-3.2.9.jar $systemlib
cp $ivyPath/cache/org.json4s/json4s-jackson_2.10/jars/json4s-jackson_2.10-3.2.9.jar $systemlib
cp $ivyPath/cache/org.json4s/json4s-native_2.10/jars/json4s-native_2.10-3.2.9.jar $systemlib
cp $ivyPath/cache/org.mapdb/mapdb/bundles/mapdb-1.0.6.jar $systemlib
cp $ivyPath/cache/org.mortbay.jetty/jetty-util/jars/jetty-util-6.1.26.jar $systemlib
cp $ivyPath/cache/org.mortbay.jetty/jetty/jars/jetty-6.1.26.jar $systemlib
cp $ivyPath/cache/org.objenesis/objenesis/jars/objenesis-1.2.jar $systemlib
cp $ivyPath/cache/org.ow2.asm/asm-commons/jars/asm-commons-4.0.jar $systemlib
cp $ivyPath/cache/org.ow2.asm/asm-tree/jars/asm-tree-4.0.jar $systemlib
cp $ivyPath/cache/org.ow2.asm/asm/jars/asm-4.0.jar $systemlib
cp $ivyPath/cache/org.scala-lang/scalap/jars/scalap-2.10.0.jar $systemlib
cp $ivyPath/cache/org.scala-sbt/test-interface/jars/test-interface-1.0.jar $systemlib
cp $ivyPath/cache/org.scalamacros/quasiquotes_2.10.4/jars/quasiquotes_2.10.4-2.0.0-M6.jar $systemlib
cp $ivyPath/cache/org.scalatest/scalatest_2.10/bundles/scalatest_2.10-2.2.0.jar $systemlib
cp $ivyPath/cache/org.slf4j/slf4j-api/jars/slf4j-api-1.7.7.jar $systemlib
cp $ivyPath/cache/org.slf4j/slf4j-log4j12/jars/slf4j-log4j12-1.7.5.jar $systemlib
cp $ivyPath/cache/org.tukaani/xz/jars/xz-1.0.jar $systemlib
cp $ivyPath/cache/org.xerial.snappy/snappy-java/bundles/snappy-java-1.0.4.1.jar $systemlib
cp $ivyPath/cache/tomcat/jasper-compiler/jars/jasper-compiler-5.5.23.jar $systemlib
cp $ivyPath/cache/tomcat/jasper-runtime/jars/jasper-runtime-5.5.23.jar $systemlib
cp $ivyPath/cache/voldemort/voldemort/jars/voldemort-0.96.jar $systemlib
cp $ivyPath/cache/xmlenc/xmlenc/jars/xmlenc-0.52.jar $systemlib
cp $ivyPath/cache/commons-pool/commons-pool/jars/commons-pool-1.5.2.jar $systemlib
cp $ivyPath/cache/com.twitter/chill_2.10/jars/chill_2.10-0.3.6.jar $systemlib
cp $ivyPath/cache/org.apache.kafka/kafka_2.10/jars/*.jar $systemlib
cp $ivyPath/cache/net.sf.jopt-simple/jopt-simple/jars/jopt-simple-3.2.jar $systemlib
cp $ivyPath/cache/com.101tec/zkclient/jars/zkclient-0.3.jar $systemlib


# *******************************
# COPD messages data prep
# *******************************

# Prepare test messages and copy them into place

echo "Prepare test messages and copy them into place..."
cd $srcPath/Utils/KVInit/src/main/resources
cp copd_demo.csv.gz $installPath/input/application1/data

# *******************************
# copy models, messages, containers, config, scripts, types  messages data prep
# *******************************

cp $srcPath/OnLEPManager/src/main/resources/log4j.properties $installPath/config

cd $srcPath/SampleApplication/Medical/Configs
cp * $installPath/input/application1/metadata/config

cd $srcPath/SampleApplication/Medical/MessagesAndContainers/Fixed/Containers
cp * $installPath/input/application1/metadata/container

cd $srcPath/SampleApplication/Medical/Functions
cp * $installPath/input/application1/metadata/function

cd $srcPath/SampleApplication/Medical/MessagesAndContainers/Fixed/Messages
cp * $installPath/input/application1/metadata/message

cd $srcPath/SampleApplication/Medical/Models
cp *.* $installPath/input/application1/metadata/model

cd $srcPath/SampleApplication/Medical/Types
cp * $installPath/input/application1/metadata/type

cd $srcPath/SampleApplication/EasyInstall/template
cp -rf * $installPath/template

cd $srcPath/SampleApplication/EasyInstall
cp SetPaths.sh $installPath/bin/

cd $installPath/bin
./SetPaths.sh $KafkaRootDir

echo "installOnLEP complete..."