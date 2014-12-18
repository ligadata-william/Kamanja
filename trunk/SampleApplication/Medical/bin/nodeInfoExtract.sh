#!/bin/bash

# nodeInfoExtract.sh provisionally checks the arguments supplied, builds the current classpath for the NodeInfoExtract
# application, and invokes NodeInfoExtract that will build a bunch of configuration information needed to install the
# OnLEP on a cluster defined in the MetadataAPIConfig's metadata store.

name1=${1}
val1=${2}
name2=${3}
val2=${4}
name3=${5}
val3=${6}
name4=${7}
val4=${8}
name5=${9}
val5=${10}
name6=${11}
val6=${12}

if [ "$#" -lt 12 ]; then
	echo "Insufficient arguments"
	echo "usage:"
	echo "	nodeInfoExtract.sh --MetadataAPIConfig  <metadataAPICfgPath> --NodeConfigPath <onlepCfgPath> --ipFileName <ipFileName> --ipPathPairFileName <ipPathPairFileName> --workDir <nodeID workDir> --ipIdCfgTargPathQuartetFileName <nodeID-Config file name> "
	exit 1
fi

if [[ "$name1" != "--MetadataAPIConfig" && "$name1" != "--NodeConfigPath" && "$name1" != "--ipPathPairFileName" && "$name1" != "--ipFileName"  && "$name1" != "--workDir"  && "$name1" != "--ipIdCfgTargPathQuartetFileName" ]]; then
	echo "Bad arguments"
	echo "usage:"
	echo "	nodeInfoExtract.sh --MetadataAPIConfig  <metadataAPICfgPath> --NodeConfigPath <onlepCfgPath> --ipFileName <ipFileName> --ipPathPairFileName <ipPathPairFileName> --workDir <nodeID workDir> --ipIdCfgTargPathQuartetFileName <nodeID-Config file name> "
	exit 1
fi

# 1) Build the current classpath required for the installer's node info extract tool 
echo "...Build the current classpath for the NodeInfoExtract using sbtProjDependencies.scala script"
sbtProjDependencies.scala --sbtDeps "`sbt 'show InstallScript/fullClasspath' | grep 'List(Attributed'`" --emitCp t >/tmp/installercp.txt
installerCp=`cat /tmp/installercp.txt`

# 2) Issue the command 
#echo "Command = scala -cp "$installerCp" com.ligadata.installer.NodeInfoExtract \"$name1\" \"$val1\" $name2 \"$val2\" \"$name3\" \"$val3\" \"$name4\" \"$val4\""
echo "...Extract node information from the supplied OnLEP config file and/or metadata store"
java -cp "$installerCp" com.ligadata.installer.NodeInfoExtract "$name1" "$val1" "$name2" "$val2" "$name3" "$val3" "$name4" "$val4" "$name5" "$val5" "$name6" "$val6" 

#java -Xdebug -Xrunjdwp:transport=dt_socket,address=8998,server=y -cp $installerCp com.ligadata.installer.NodeInfoExtract "$name1" "$val1" "$name2" "$val2" "$name3" "$val3" "$name4" "$val4" "$name5" "$val5" "$name6" "$val6"

