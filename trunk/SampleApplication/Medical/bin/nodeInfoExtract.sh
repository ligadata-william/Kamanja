#!/bin/bash



name1=$1
val1=$2
name2=$3
val2=$4
name3=$5
val3=$6
name4=$7
val4=$8

if [ "$#" -lt 8 ]; then
	echo "Insufficient arguments"
	echo "usage:"
	echo "	nodeInfoExtract.sh --MetadataAPIConfig  <metadataAPICfgPath> --NodeConfigPath <onlepCfgPath> --ipFileName <ipFileName> --ipPathPairFileName <ipPathPairFileName>"
	exit 1
fi

if [[ "$name1" != "--MetadataAPIConfig" && "$name1" != "--NodeConfigPath" && "$name1" != "--ipPathPairFileName" && "$name1" != "--ipFileName" ]]; then
	echo "Bad arguments"
	echo "usage:"
	echo "	nodeInfoExtract.sh --MetadataAPIConfig  <metadataAPICfgPath> --NodeConfigPath <onlepCfgPath> --ipFileName <ipFileName> --ipPathPairFileName <ipPathPairFileName>"
	exit 1
fi

# 1 Build the current classpath required for the installer's node info extract tool 
echo "...Build the current classpath for the Installer using sbtProjDependencies.scala script"
sbtProjDependencies.scala --sbtDeps "`sbt 'show InstallScript/fullClasspath' | grep 'List(Attributed'`" --emitCp t >/tmp/installercp.txt
installerCp=`cat /tmp/installercp.txt`

# 2) Issue the command 
#echo "Command = scala -cp "$installerCp" com.ligadata.installer.NodeInfoExtract \"$name1\" \"$val1\" $name2 \"$val2\" \"$name3\" \"$val3\" \"$name4\" \"$val4\""
echo "...Extract node information from the supplied OnLEP config file and/or metadata store"
java -cp "$installerCp" com.ligadata.installer.NodeInfoExtract "$name1" "$val1" "$name2" "$val2" "$name3" "$val3" "$name4" "$val4"

#java -Xdebug -Xrunjdwp:transport=dt_socket,address=8998,server=y -cp $installerCp com.ligadata.installer.NodeInfoExtract "$name1" "$val1" "$name2" "$val2" "$name3" "$val3" "$name4" "$val4"

