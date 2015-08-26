KAMANJA_BASEPATH={InstallDirectory}

java -jar $KAMANJA_BASEPATH/bin/KVInit-1.0 --kvname System.SputumCodes        --config $KAMANJA_BASEPATH/config/Engine1Config.properties --csvpath $KAMANJA_BASEPATH/input/SampleApplications/data/sputumCodes_Medical.csv       --keyfieldname icd9Code
java -jar $KAMANJA_BASEPATH/bin/KVInit-1.0 --kvname System.SmokeCodes         --config $KAMANJA_BASEPATH/config/Engine1Config.properties --csvpath $KAMANJA_BASEPATH/input/SampleApplications/data/smokingCodes_Medical.csv      --keyfieldname icd9Code
java -jar $KAMANJA_BASEPATH/bin/KVInit-1.0 --kvname System.EnvCodes           --config $KAMANJA_BASEPATH/config/Engine1Config.properties --csvpath $KAMANJA_BASEPATH/input/SampleApplications/data/envExposureCodes_Medical.csv  --keyfieldname icd9Code
java -jar $KAMANJA_BASEPATH/bin/KVInit-1.0 --kvname System.CoughCodes         --config $KAMANJA_BASEPATH/config/Engine1Config.properties --csvpath $KAMANJA_BASEPATH/input/SampleApplications/data/coughCodes_Medical.csv        --keyfieldname icd9Code
java -jar $KAMANJA_BASEPATH/bin/KVInit-1.0 --kvname System.DyspnoeaCodes      --config $KAMANJA_BASEPATH/config/Engine1Config.properties --csvpath $KAMANJA_BASEPATH/input/SampleApplications/data/dyspnoea_Medical.csv          --keyfieldname icd9Code
