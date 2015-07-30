#!/usr/bin/env bash
input="/Users/dhaval/Documents/Workspace/KamanjaScript/Kamanja/trunk/SampleApplication/HelloWorld/message/HelloWorld_Msg_Def.json"
config="/Users/dhaval/Documents/Workspace/KamanjaEasyInstall/config/ClusterCfgMetadataAPIConfig.properties"
action="addMessage"
tput setaf 3
echo
echo "                   Performing multiple metadata operations . . . . ."
echo
tput sgr0
echo "Adding message $input!"
echo
./kamanja.sh --action $action  --input $input  --config $config > /dev/null

if [ $? -eq 1 ]; then
    echo "`tput setaf 1`Failed to add the message:  $input "
    echo
    cat resultdescription.temp
    tput sgr0
    echo
    else
    echo
    echo "`tput setaf 2`Successfully added the message: $input"
    echo
    cat resultdescription.temp
    echo
    tput sgr0
fi
rm resultdescription.temp

#------------------------------------------------------------------------------------------------------------------------------------------------------
echo
input="/Users/dhaval/Documents/Workspace/KamanjaScript/Kamanja/trunk/SampleApplication/HelloWorld/message/HelloWorld_Msg_Def.json"
config="/Users/dhaval/Documents/Workspace/KamanjaEasyInstall/config/ClusterCfgMetadataAPIConfig.properties"
action="addMessage"
echo "Adding message $input!"
echo
./kamanja.sh --action $action  --input $input  --config $config > /dev/null
if [ $? -eq 1 ]; then
    echo
    echo "`tput setaf 1`Failed to add the message:  $input"
    echo
    cat resultdescription.temp
    echo
    tput sgr0
    else
    echo
    echo "`tput setaf 2`Successfully added the message: $input "
    echo
    cat resultdescription.temp
    echo
   `tput sgr0`
fi
rm resultdescription.temp


