#!/bin/bash

# clusterInstallOnLEP.sh
#
#	run this script from the trunk directory that contains the release source:

#	example: clusterInstallOnLEP.sh --MetadataAPIConfig SampleApplication/Medical/Configs/MetadataAPIConfig.properties --NodeConfigPath SampleApplication/Medical/Configs/Engine2BoxConfigV1.json 
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



# 1 build the installation in the staging directory
dirName="OnLEPClusterInstall"  #if you change this name, you must modify the NodeInfoExtract.scala --classpath
stagingDir="/tmp/$dirName"
mkdir -p "$stagingDir"
echo "...build the OnLEP installation directory in $stagingDir"
installOnLEP_Medical.sh "$stagingDir" `pwd`


# 2) determine which machines and installation directories are to get the build from the metadata and OnLEP config
ipPath="/tmp/ip.txt"
ipPathPair="/tmp/ipPath.txt"
echo "...extract node information for the cluster to be installed from the Metadata and OnLEP config supplied"
echo "...Command = nodeInfoExtract.sh \"$name1\" \"$val1\" $name2 \"$val2\" --ipFileName $ipPath --ipPathPairFileName $ipPathPair "
nodeInfoExtract.sh "$name1" "$val1" "$name2" "$val2"  --ipFileName "$ipPath" --ipPathPairFileName "$ipPathPair" 

# 3) compress staging dir and tar it
echo "...compress and tar the installation directory $stagingDir to $tarName"
dtPrefix="OnLEP`date +"%Y%b%d"`"
tarName="$dtPrefix.$dirName.bz2"
tar cjf "/tmp/$tarName" "$stagingDir"

# 4) Push the tarballs to each machine defined in the supplied configuration
echo "...copy the tarball to the machines in this cluster"
exec 12<&0 # save current stdin
exec < "$ipPath"
while read LINE; do
    machine=$LINE
    echo "...copying $tarName to $machine"
    scp "/tmp/$tarName" "$machine:/tmp/$tarName"
done
exec 0<&12 12<&-

echo

# 5) untar/decompress tarballs there and move them into place
echo "...for each directory specified on each machine participating in the cluster, untar and decompress the software and mv it into position"
exec 12<&0 # save current stdin
exec < "$ipPathPair"
while read LINE; do
    machine=$LINE
    read LINE
    targetPath=$LINE
	ssh -T $machine  <<-EOF
	        cd /tmp
	        tar xjf $tarName
	        rm -Rf $targetPath
	        mkdir -p $targetPath
	        mv tmp/$dirName/* $targetPath/
EOF
done
exec 0<&12 12<&-

#	        rm -Rf $tarName $dirName
