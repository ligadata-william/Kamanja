#!/bin/bash

# FataFatClusterInstall.sh

#   Based upon either the metadata at the location found specified in the MetadataAPI config supplied, or, if a 
#   node config file was specified, from that file... install the FataFat software.  The software is 
#   built with the easy installer script and then gzipped and tar'd when the KafkaInstallPath is supplied.
#   If the TarballPath option is given, the tarball path in the value will be distributed.  If both Kafka path and
#   TarballPath are given, script issues usage message and exits.
#
#   Build examples:
#       a) Using the node config file Engine2BoxConfigV1.json 
#       FataFatClusterInstall.sh  --MetadataAPIConfig SampleApplication/Medical/Configs/MetadataAPIConfig.properties 
#                               --NodeConfigPath SampleApplication/Medical/Configs/Engine2BoxConfigV1.json 
#                               --KafkaInstallPath ~/tarballs/kafka/2.10/kafka_2.10-0.8.1.1
#       b) Using the metadata found in the metadata store specified by the MetadataAPIConfig.properties
#       FataFatClusterInstall.sh  --MetadataAPIConfig SampleApplication/Medical/Configs/MetadataAPIConfig.properties 
#                               --KafkaInstallPath ~/tarballs/kafka/2.10/kafka_2.10-0.8.1.1
#
#   TarballPath distribution examples (when the tarball has been built outside this script):
#       a) Using the node config file Engine2BoxConfigV1.json 
#       FataFatClusterInstall.sh  --MetadataAPIConfig SampleApplication/Medical/Configs/MetadataAPIConfig.properties 
#                               --NodeConfigPath SampleApplication/Medical/Configs/Engine2BoxConfigV1.json 
#                               --TarballPath ~/tarballs/Fatafat-01.00.0001.tgz
#       b) Using the metadata found in the metadata store specified by the MetadataAPIConfig.properties
#       FataFatClusterInstall.sh  --MetadataAPIConfig SampleApplication/Medical/Configs/MetadataAPIConfig.properties 
#                               --TarballPath ~/tarballs/Fatafat-01.00.0001.tgz
#
#   In the "a)" examples, a cluster configuration is presumably presented in the NodeConfigPath file (i.e, the cluster decl
#   is new).  In the "b)" examples, the cluster config info is retrieved from the metadata store.
#
#   If you supply all of the options, you will be asked to try again.  An alternate working directory may be supplied.  This 
#   directory is used by this script to store the built software when compiling as well as the control files that are generated
#   by the NodeInfoExtract.  By default, "/tmp" is used.  If a special one is supplied as the WorkingDir paramater value, it
#   must exist.  The user account must have CRUD access to its content.
#
#   If a cluster config is not present there when no NodeConfigPath argument is presented, an exception is thrown.  In fact
#   if the cluster map returned by the metadata api is empty, one is thrown in any case.  The other reason is that the 
#   engine cluster config is messed up in the NodeConfigPath file supplied.
#
#

Usage()
{
    echo "Usage if building from source:"
    echo "  $0 --MetadataAPIConfig  <metadataAPICfgPath> --KafkaInstallPath <kafka location> [--NodeConfigPath <engine config path --WorkingDir <alt working dir>  ]"
    echo "Usage if deploying tarball:"
    echo "  $0 --MetadataAPIConfig  <metadataAPICfgPath> --TarName <tarball path> [--NodeConfigPath <engine config path --WorkingDir <alt working dir>  ]"
}

scalaversion="2.10"
name1=$1

# Check 1: Is this even close to reasonable?
if [[ "$#" -eq 4  || "$#" -eq 6  || "$#" -eq 8 ]]; then
    echo 
else 
    echo "Incorrect number of arguments"
    Usage
    exit 1
fi

# Check 2: Is this even close to reasonable?
if [[ "$name1" != "--MetadataAPIConfig" && "$name1" != "--NodeConfigPath"  && "$name1" != "--KafkaInstallPath"   && "$name1" != "--TarballPath"  && "$name1" != "--WorkingDir" ]]; then
	echo "Unreasonable number of arguments... as few as 2 and as many as 4 may be supplied."
    Usage
	exit 1
fi

# Check 3: Is this even close to reasonable?
currDirPath=`pwd`
currDir=`echo "$currDirPath" | sed 's/.*\/\(.*\)/\1/g'`
if [ "$currDir" != "trunk" ]; then
    echo "Currently this script must be run from the trunk directory of a valid local git repo that has had NodeInfoExtract fat jar built."
    echo "It uses the NodeInfoExtract application found in the trunk/Utils/NodeInfoExtract/target/scala-$scalaversion/ "
    echo "This application extracts the node information from the metadata and/or engine node config supplied here. "
    echo "currDir = $currDir"
    Usage
    exit 1
fi


# Collect the named parameters 
metadataAPIConfig=""
kafkaInstallPath=""
nodeConfigPath=""
tarballPath=""
nodeCfgGiven=""
workDir="/tmp"
subDir="FataFat"
stagingDirName="ToBeInstalled" 

while [ "$1" != "" ]; do
    case $1 in
        --MetadataAPIConfig )   shift
                                metadataAPIConfig=$1
                                ;;
        --KafkaInstallPath )   shift
                                kafkaInstallPath=$1
                                ;;
        --NodeConfigPath )      shift
                                nodeConfigPath=$1
                                nodeCfgGiven="true enough"
                                ;;
        --TarballPath )         shift
                                tarballPath=$1
                                ;;
        --WorkingDir )          shift
                                workDir=$1
                                ;;
        * )                     echo "Argument $1 is invalid named parameter."
                                Usage
                                exit 1
                                ;;
    esac
done


# Check 4: Is this even close to reasonable?
if [ -n "$TarballPath" && -n "$KafkaInstallPath" ]; then
    echo "Either install from source or use the tarball specification... just don't do both on same run."
    Usage
    exit 1
fi

# Check 5: if the working directory was given, make sure it is full qualified
workDirHasLeadSlash=`echo "$workDir" | grep '^\/.*'
if [ -n "$workDirHasLeadSlash" ]; then
    echo "The WorkingDir must be a fully qualified path."
    Usage
    exit 1
fi

# Check 6: working directory must exist
if [ -d "$workDir" ]; then
    echo "The WorkingDir must exist and be a directory."
    Usage
    exit 1
fi

if [ -n "$tarballPath" ]; then
    # Check 7: tarball path must exist
    if [ -f "$tarballPath" ]; then
        echo "The TarballPath must exist and be a regular file."
        Usage
        exit 1
    fi

    # Check 8: tarball path must be readable
    if [ -r "$tarballPath" ]; then
        echo "The TarballPath must be readable."
        Usage
        exit 1
    fi
fi

if [ -n "$KafkaInstallPath" ]; then
    # Check 9: Is Kafka legit?
    if [ -d "$KafkaInstallPath" ]; then
        echo "KafkaInstallPath must exist."
        Usage
        exit 1
    fi
    # Check 10: Is Kafka legit?
    if [ -f "$KafkaInstallPath/bin/kafka-server-start.sh" ]; then
        echo "KafkaInstallPath doesn't look right... where is bin/kafka-server-start.sh?"
        Usage
        exit 1
    fi
fi


# Check N: more checks could probably be added ... 


# Skip the build if tarballPath was supplied 
dtPrefix="FataFat`date +"%Y%b%d"`"
tarName="$dtPrefix.bz2"

if [ -z "$tarballPath" ]; then
    # 1 build the installation in the staging directory
    stagingDir="$workDir/$stagingDirName"
    mkdir -p "$stagingDir"
    echo "...build the FataFat installation directory in $stagingDir"
    installOnLEP_Medical.sh "$stagingDir" `pwd`

    # 2) compress staging dir and tar it
    echo "...compress and tar the installation directory $stagingDir to $tarName"
    tar cjf "$workDir/$tarName" "$stagingDir"

    tarballPath="$workDir/$tarName"
else
    # get the tarball file name 
    tarName=`echo "$tarballPath" | sed 's/.*\/\(.*\)/\1/g'`
fi

# 3) determine which machines and installation directories are to get the build from the metadata and FataFat config
# A number of files are produced, all in the working dir.
ipFile="ip.txt"
ipPathPairFile="ipPath.txt"
ipIdCfgTargPathQuartetFileName="ipIdCfgTarg.txt"

trunkDir=`pwd`
nodeInfoExtractDir="$trunkDir/Utils/NodeInfoExtract/target/scala-$scalaversion"
echo "...extract node information for the cluster to be installed from the Metadata and OnLEP config supplied"
if  [ -n "$nodeCfgGiven" ]; then
    echo "...Command = $nodeInfoExtractDir/NodeInfoExtract-1.0 --MetadataAPIConfig \"$metadataAPIConfig\" --NodeConfigPath \"$nodeConfigPath\"  --workDir \"$workDir\" --ipFileName \"$ipFile\" --ipPathPairFileName \"$ipPathPairFile\" --ipIdCfgTargPathQuartetFileName \"$ipIdCfgTargPathQuartetFileName\""
    "$nodeInfoExtractDir"/NodeInfoExtract-1.0 --MetadataAPIConfig \"$metadataAPIConfig\" --NodeConfigPath \"$nodeConfigPath\" --workDir "$workDir" --ipFileName "$ipFile" --ipPathPairFileName "$ipPathPairFile" --ipIdCfgTargPathQuartetFileName "$ipIdCfgTargPathQuartetFileName"
else # info is assumed to be present in the supplied metadata store... see trunk/utils/NodeInfoExtract for details 
    echo "...Command = $nodeInfoExtractDir/NodeInfoExtract-1.0 --MetadataAPIConfig \"$metadataAPIConfig\" --workDir \"$workDir\" --ipFileName \"$ipFile\" --ipPathPairFileName \"$ipPathPairFile\" --ipIdCfgTargPathQuartetFileName \"$ipIdCfgTargPathQuartetFileName\""
        "$nodeInfoExtractDir"/NodeInfoExtract-1.0 --MetadataAPIConfig \"$metadataAPIConfig\" --workDir "$workDir" --ipFileName "$ipFile" --ipPathPairFileName "$ipPathPairFile" --ipIdCfgTargPathQuartetFileName "$ipIdCfgTargPathQuartetFileName"
fi

# 4) Push the tarballs to each machine defined in the supplied configuration
echo "...copy the tarball to the machines in this cluster"
exec 12<&0 # save current stdin
exec < "$workDir/$ipFile"
while read LINE; do
    machine=$LINE
    echo "...copying $tarName to $machine"
    ssh $machine "mkdir -p $workDir"
    scp "$tarballPath" "$machine:$workDir/$tarName"
done
exec 0<&12 12<&-

echo

# 5) untar/decompress tarballs there and move them into place
echo "...for each directory specified on each machine participating in the cluster, untar and decompress the software to $workDir/$subDir/$stagingDirName... then move to corresponding target path"
exec 12<&0 # save current stdin
exec < "$workDir/$ipPathPairFile"
while read LINE; do
    machine=$LINE
    read LINE
    targetPath=$LINE
	ssh -T $machine  <<-EOF
	        cd $workDir
	        rm -Rf $subDir
	        tar xjf $tarName
	        rm -Rf $targetPath
	        mkdir -p $targetPath
	        cp -R $subDir/$stagingDirName/* $targetPath/
EOF
done
exec 0<&12 12<&-

echo

# 6) Push the node$nodeId.cfg file to each cluster node's working directory.
echo "...copy the node$nodeId.cfg files to the machines' ($workDir/$subDir/$stagingDirName) for this cluster "
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
    echo "...copying $cfgFile for nodeId $id to $machine:$targetPath"
    scp "$cfgFile" "$machine:$targetPath/"
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
# 	        rm -Rf $workDir/$subDir
#			rm -f "$workDir/$tarName"
# EOF
# done
# exec 0<&12 12<&-


