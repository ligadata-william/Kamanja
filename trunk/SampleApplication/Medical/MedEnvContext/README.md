This is an implementation of the EnvContext trait designed to support the Medical projects that use MapDb persistent stores.

It uses the MapDb to persist these containers used for managing the COPD et al models:

1) CoughCodeSet_100
2) EnvCodeSet_100
3) SmokeCodeSet_100
4) SputumCodeSet_100
5) Icd9DiagnosisCodeMap_100


ESSENTIAL:

The MedPOCEnvContext will try to load each container supplied by the OnLEPManager to it.  If there are keys found in the store, they will be loaded into respective Map[String,BaseContainer] maps.

scala -Dlog4j.configuration=file:/somepath/log4j.properties -cp ...  --kvname CoughCodeSet_100 --kvpath /tmp/OnLEPInstall --csvpath /<yourpath>/trunk/SampleApplication/Medical/MedEnvContext/src/main/resources/coughCodes.csv  --keyfieldname icd9Code

scala -Dlog4j.configuration=file:/somepath/log4j.properties -cp ...  ---kvname EnvCodeSet_100 --kvpath /tmp/OnLEPInstall --csvpath /<yourpath>/trunk/SampleApplication/Medical/MedEnvContext/src/main/resources/envExposureCodes.csv  --keyfieldname icd9Code

scala -Dlog4j.configuration=file:/somepath/log4j.properties -cp ...  ---kvname SmokeCodeSet_100 --kvpath /tmp/OnLEPInstall --csvpath /<yourpath>/trunk/SampleApplication/Medical/MedEnvContext/src/main/resources/smokingCodes.csv  --keyfieldname icd9Code

scala -Dlog4j.configuration=file:/somepath/log4j.properties -cp ...  ---kvname SputumCodeSet_100 --kvpath /tmp/OnLEPInstall --csvpath /<yourpath>/trunk/SampleApplication/Medical/MedEnvContext/src/main/resources/Icd9DiagnosisCodes.csv  --keyfieldname icd9Code

scala -Dlog4j.configuration=file:/somepath/log4j.properties -cp ...  --kvname Icd9DiagnosisCodeMap_100 --kvpath /tmp/OnLEPInstall --csvpath /<yourpath>/trunk/SampleApplication/Medical/MedEnvContext/src/main/resources/Icd9DiagnosisCodes.csv  --keyfieldname icd9Code

This EnvContext implementation is a provisional one for near term.  A productized version will be implemented that can cope with mapdb, cassandra and other things at some point in the near future.

