#!/bin/bash

# startOnLEPCluster.sh
#
#	NOTE: This script must currently be run from a trunk directory that contains the InstallScript project:

#	example: startOnLEPCluster.sh --MetadataAPIConfig SampleApplication/Medical/Configs/MetadataAPIConfig.properties --NodeConfigPath SampleApplication/Medical/Configs/Engine2BoxConfigV1.json 
#

name1=$1
val1=$2
name2=$3
val2=$4

if [ "$#" -lt 4 ]; then
	echo "Insufficient arguments"
	echo "usage:"
	echo "	$0 --MetadataAPIConfig  <metadataAPICfgPath> --NodeConfigPath <onlepCfgPath> "
	exit 1
fi

if [[ "$name1" != "--MetadataAPIConfig" && "$name1" != "--NodeConfigPath" ]]; then
	echo "Bad arguments"
	echo "usage:"
	echo "	$0  --MetadataAPIConfig  <metadataAPICfgPath> --NodeConfigPath <onlepCfgPath> "
	exit 1
fi



# 1) determine which machines and installation directories are to get the build from the metadata and OnLEP config
# A number of files are produced, all in the working dir. For cluster start only the quartet file is used
workDir="/tmp"
ipFile="ip.txt"
ipPathPairFile="ipPath.txt"
ipIdCfgTargPathQuartetFileName="ipIdCfgTarg.txt"
echo "...extract node information for the cluster to be installed from the Metadata and OnLEP config supplied"
echo "...Command = nodeInfoExtract.sh $name1 \"$val1\" $name2 \"$val2\"  --workDir \"$workDir\" --ipFileName \"$ipFile\" --ipPathPairFileName \"$ipPathPairFile\" --ipIdCfgTargPathQuartetFileName \"$ipIdCfgTargPathQuartetFileName\""
nodeInfoExtract.sh "$name1" "$val1" "$name2" "$val2" --workDir "$workDir" --ipFileName "$ipFile" --ipPathPairFileName "$ipPathPairFile" --ipIdCfgTargPathQuartetFileName "$ipIdCfgTargPathQuartetFileName" 


#echo "/tmp/node2.cfg" | sed 's/.*\/\(.*\)/\1/g'
# Start the cluster nodes using the information extracted from the metadata and supplied config.
echo "...start the OnLEP cluster "
exec 12<&0 # save current stdin
exec < "$workDir/$ipIdCfgTargPathQuartetFileName"
while read LINE; do
    machine=$LINE
    read LINE
    id=$LINE
    read LINE
    cfgFile=$LINE
    read LINE
    targetPath=$LINE
    echo "quartet = $machine, $id, $cfgFile, $targetPath"
    echo "...On machine $machine, starting OnLEPManager with configuration $cfgFile for nodeId $id to $machine:$targetPath"
    scp "$cfgFile" "$machine:$targetPath/"
	ssh -T $machine  <<-EOF
	        cd $targetPath
	        nodeCfg=`echo $cfgFile | sed 's/.*\/\(.*\)/\1/g'`
	        java -jar ./OnLEPManager-1.0 --config "./$cfgFile" &
EOF
done
exec 0<&12 12<&-

echo


# 8) clean up
# echo "...clean up "
# exec 12<&0 # save current stdin
# exec < "$workDir/$ipPathPairFile"
# while read LINE; do
#     machine=$LINE
#     read LINE
#     targetPath=$LINE
# 	ssh -T $machine  <<-EOF
# 	        rm -Rf $workDir/$workDirSansSlash
#			rm -f "$workDir/$tarName"
# EOF
# done
# exec 0<&12 12<&-


