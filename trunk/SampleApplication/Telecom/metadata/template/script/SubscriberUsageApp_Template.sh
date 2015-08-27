#!/bin/sh

KAMANJA_HOME={InstallDirectory}

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/input/SampleApplications/metadata/config/MetadataAPIConfig_HelloWorld.properties upload engine config $KAMANJA_HOME/config/ClusterConfig.json

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/input/SampleApplications/metadata/config/MetadataAPIConfig_HelloWorld.properties add container $KAMANJA_HOME/input/SampleApplications/metadata/container/AccountAggregatedUsage_Telecom.json

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/input/SampleApplications/metadata/config/MetadataAPIConfig_HelloWorld.properties add container $KAMANJA_HOME/input/SampleApplications/metadata/container/AccountInfo_Telecom.json

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/input/SampleApplications/metadata/config/MetadataAPIConfig_HelloWorld.properties add container $KAMANJA_HOME/input/SampleApplications/metadata/container/SubscriberAggregatedUsage_Telecom.json

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/input/SampleApplications/metadata/config/MetadataAPIConfig_HelloWorld.properties add container $KAMANJA_HOME/input/SampleApplications/metadata/container/SubscriberGlobalPreferences_Telecom.json

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/input/SampleApplications/metadata/config/MetadataAPIConfig_HelloWorld.properties add container $KAMANJA_HOME/input/SampleApplications/metadata/container/SubscriberInfo_Telecom.json

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/input/SampleApplications/metadata/config/MetadataAPIConfig_HelloWorld.properties add container $KAMANJA_HOME/input/SampleApplications/metadata/container/SubscriberPlans_Telecom.json

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/input/SampleApplications/metadata/config/MetadataAPIConfig_HelloWorld.properties add message $KAMANJA_HOME/input/SampleApplications/metadata/message/SubscriberUsage_Telecom.json

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/input/SampleApplications/metadata/config/MetadataAPIConfig_HelloWorld.properties upload compile config $KAMANJA_HOME/input/SampleApplications/metadata/config/SubscriberUsageAlertCompileCfg_Telecom.json

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/input/SampleApplications/metadata/config/MetadataAPIConfig_HelloWorld.properties add model java $KAMANJA_HOME/input/SampleApplications/metadata/model/SubscriberUsageAlert_Telecom.java DEPENDSON subscriberusagealert
