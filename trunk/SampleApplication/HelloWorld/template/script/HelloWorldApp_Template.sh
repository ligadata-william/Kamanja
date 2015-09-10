#!/bin/sh

KAMANJA_HOME={InstallDirectory}

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/input/SampleApplications/metadata/config/MetadataAPIConfig_HelloWorld.properties upload cluster config $KAMANJA_HOME/config/ClusterConfig.json

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/input/SampleApplications/metadata/config/MetadataAPIConfig_HelloWorld.properties add message $KAMANJA_HOME/input/SampleApplications/metadata/message/Message_Definition_HelloWorld.json

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/input/SampleApplications/metadata/config/MetadataAPIConfig_HelloWorld.properties add model pmml $KAMANJA_HOME/input/SampleApplications/metadata/model/PMML_Model_HelloWorld.xml
