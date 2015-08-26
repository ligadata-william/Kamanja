#!/bin/sh

KAMANJA_BASEPATH={InstallDirectory}

$KAMANJA_BASEPATH/bin/kamanja $KAMANJA_BASEPATH/input/SampleApplications/metadata/config/MetadataAPIConfig_Medical.properties upload engine config $KAMANJA_BASEPATH/config/ClusterConfig.json

$KAMANJA_BASEPATH/bin/kamanja $KAMANJA_BASEPATH/input/SampleApplications/metadata/config/MetadataAPIConfig_Medical.properties add container $KAMANJA_BASEPATH/input/SampleApplications/metadata/container/CoughCodes_Medical.json

$KAMANJA_BASEPATH/bin/kamanja $KAMANJA_BASEPATH/input/SampleApplications/metadata/config/MetadataAPIConfig_Medical.properties add container $KAMANJA_BASEPATH/input/SampleApplications/metadata/container/DyspnoeaCodes_Medical.json

$KAMANJA_BASEPATH/bin/kamanja $KAMANJA_BASEPATH/input/SampleApplications/metadata/config/MetadataAPIConfig_Medical.properties add container $KAMANJA_BASEPATH/input/SampleApplications/metadata/container/EnvCodes_Medical.json

$KAMANJA_BASEPATH/bin/kamanja $KAMANJA_BASEPATH/input/SampleApplications/metadata/config/MetadataAPIConfig_Medical.properties add container $KAMANJA_BASEPATH/input/SampleApplications/metadata/container/SmokeCodes_Medical.json

$KAMANJA_BASEPATH/bin/kamanja $KAMANJA_BASEPATH/input/SampleApplications/metadata/config/MetadataAPIConfig_Medical.properties add container $KAMANJA_BASEPATH/input/SampleApplications/metadata/container/SputumCodes_Medical.json

$KAMANJA_BASEPATH/bin/kamanja $KAMANJA_BASEPATH/input/SampleApplications/metadata/config/MetadataAPIConfig_Medical.properties add message $KAMANJA_BASEPATH/input/SampleApplications/metadata/message/beneficiary_Medical.json

$KAMANJA_BASEPATH/bin/kamanja $KAMANJA_BASEPATH/input/SampleApplications/metadata/config/MetadataAPIConfig_Medical.properties add message $KAMANJA_BASEPATH/input/SampleApplications/metadata/message/hl7_Medical.json

$KAMANJA_BASEPATH/bin/kamanja $KAMANJA_BASEPATH/input/SampleApplications/metadata/config/MetadataAPIConfig_Medical.properties add message $KAMANJA_BASEPATH/input/SampleApplications/metadata/message/inpatientclaim_Medical.json

$KAMANJA_BASEPATH/bin/kamanja $KAMANJA_BASEPATH/input/SampleApplications/metadata/config/MetadataAPIConfig_Medical.properties add message $KAMANJA_BASEPATH/input/SampleApplications/metadata/message/outpatientclaim_Medical.json

$KAMANJA_BASEPATH/bin/kamanja $KAMANJA_BASEPATH/input/SampleApplications/metadata/config/MetadataAPIConfig_Medical.properties upload compile config $KAMANJA_BASEPATH/input/SampleApplications/metadata/config/Java_ModelConfig_Medical.json

$KAMANJA_BASEPATH/bin/kamanja $KAMANJA_BASEPATH/input/SampleApplications/metadata/config/MetadataAPIConfig_Medical.properties add model java $KAMANJA_BASEPATH/input/SampleApplications/metadata/model/COPDRiskAssessment.java

