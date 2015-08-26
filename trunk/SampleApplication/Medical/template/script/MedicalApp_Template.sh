#!/bin/sh

KAMANJA_BASEPATH={InstallDirectory}

$KAMANJA_BASEPATH/bin/kamanja add container $KAMANJA_BASEPATH/input/SampleApplications/metadata/container/CoughCodes_Medical.json

$KAMANJA_BASEPATH/bin/kamanja add container $KAMANJA_BASEPATH/input/SampleApplications/metadata/container/DyspnoeaCodes_Medical.json

$KAMANJA_BASEPATH/bin/kamanja add container $KAMANJA_BASEPATH/input/SampleApplications/metadata/container/EnvCodes_Medical.json

$KAMANJA_BASEPATH/bin/kamanja add container $KAMANJA_BASEPATH/input/SampleApplications/metadata/container/SmokeCodes_Medical.json

$KAMANJA_BASEPATH/bin/kamanja add container $KAMANJA_BASEPATH/input/SampleApplications/metadata/container/SputumCodes_Medical.json

$KAMANJA_BASEPATH/bin/kamanja add message $KAMANJA_BASEPATH/input/SampleApplications/metadata/message/beneficiary_Medical.json

$KAMANJA_BASEPATH/bin/kamanja add message $KAMANJA_BASEPATH/input/SampleApplications/metadata/message/hl7_Medical.json

$KAMANJA_BASEPATH/bin/kamanja add message $KAMANJA_BASEPATH/input/SampleApplications/metadata/message/inpatientclaim_Medical.json

$KAMANJA_BASEPATH/bin/kamanja add message $KAMANJA_BASEPATH/input/SampleApplications/metadata/message/outpatientclaim_Medical.json

$KAMANJA_BASEPATH/bin/kamanja upload compile config $KAMANJA_BASEPATH/input/SampleApplications/metadata/config/Java_ModelConfig_Medical.json

$KAMANJA_BASEPATH/bin/kamanja add model java $KAMANJA_BASEPATH/input/SampleApplications/metadata/model/COPDRiskAssessment.java

$KAMANJA_BASEPATH/bin/kamanja upload engine config $KAMANJA_BASEPATH/config/ClusterConfig.json
