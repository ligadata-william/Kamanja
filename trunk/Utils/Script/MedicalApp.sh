#!/bin/sh

kamanja add container /usr/local/Cellar/kamanja/1.1.0/input/SampleApplications/metadata/container/CoughCodes_Medical.json

kamanja add container /usr/local/Cellar/kamanja/1.1.0/input/SampleApplications/metadata/container/DyspnoeaCodes_Medical.json

kamanja add container /usr/local/Cellar/kamanja/1.1.0/input/SampleApplications/metadata/container/EnvCodes_Medical.json

kamanja add container /usr/local/Cellar/kamanja/1.1.0/input/SampleApplications/metadata/container/SmokeCodes_Medical.json

kamanja add container /usr/local/Cellar/kamanja/1.1.0/input/SampleApplications/metadata/container/SputumCodes_Medical.json

kamanja add message /usr/local/Cellar/kamanja/1.1.0/input/SampleApplications/metadata/message/beneficiary_Medical.json

kamanja add message /usr/local/Cellar/kamanja/1.1.0/input/SampleApplications/metadata/message/hl7_Medical.json

kamanja add message /usr/local/Cellar/kamanja/1.1.0/input/SampleApplications/metadata/message/inpatientclaim_Medical.json

kamanja add message /usr/local/Cellar/kamanja/1.1.0/input/SampleApplications/metadata/message/outpatientclaim_Medical.json

kamanja upload compile config /usr/local/Cellar/kamanja/1.1.0/input/SampleApplications/metadata/config/Java_ModelConfig_Medical.json

kamanja add model java /usr/local/Cellar/kamanja/1.1.0/input/SampleApplications/metadata/model/COPDRiskAssessment.java

kamanja upload engine config /usr/local/Cellar/kamanja/1.1.0/config/ClusterConfig.json

kamanja kvinit /usr/local/Cellar/kamanja/1.1.0/input/SampleApplications/bin/InitKvStores_Medical.sh