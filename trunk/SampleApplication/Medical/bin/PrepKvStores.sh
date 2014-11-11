#!/bin/bash

installPath=$1
srcPath=$2

if [ ! -d "$installPath" ]; then
        echo "No install path supplied. Usage: PrepKvStores.sh <installPath> <sourcePath>"
        exit 1
fi

if [ ! -d "$srcPath" ]; then
        echo "No source path supplied. Usage: PrepKvStores.sh <installPath> <sourcePath>"
        exit 1
fi

export ONLEPLIBPATH=$installPath

for file in $installPath/*.jar;
do
	echo "Adding $file to classpath..."
	CLASSPATH=$CLASSPATH:$file
done

echo "Prepare the test kvstore - Dyspnea Codes map..."

java -cp $installPath/dyspnoeacodes.jar -jar $ONLEPLIBPATH/KVInit-1.0 --classname com.ligadata.edifecs.DyspnoeaCodes_100 --kvpath $ONLEPLIBPATH/kvstores/ --csvpath $srcPath/SampleApplication/Medical/MedEnvContext/src/main/resources/dyspnoea.csv --keyfieldname icd9Code

echo "Prepare the test kvstore - Environmental Exposure Codes map..."

java -cp $installPath/envcodes.jar -jar $ONLEPLIBPATH/KVInit-1.0 --classname com.ligadata.edifecs.EnvCodes_100 --kvpath $ONLEPLIBPATH/kvstores/ --csvpath $srcPath/SampleApplication/Medical/MedEnvContext/src/main/resources/envExposureCodes.csv --keyfieldname icd9Code

echo "Prepare the test kvstore - Sputum Codes map..."

java -cp $installPath/sputumcodes.jar -jar $ONLEPLIBPATH/KVInit-1.0 --classname com.ligadata.edifecs.SputumCodes_100 --kvpath $ONLEPLIBPATH/kvstores/ --csvpath $srcPath/SampleApplication/Medical/MedEnvContext/src/main/resources/sputumCodes.csv --keyfieldname icd9Code

echo "Prepare the test kvstore - Cough Codes map..."

java -cp $installPath/coughcodes.jar -jar $ONLEPLIBPATH/KVInit-1.0 --classname com.ligadata.edifecs.CoughCodes_100 --kvpath $ONLEPLIBPATH/kvstores/ --csvpath $srcPath/SampleApplication/Medical/MedEnvContext/src/main/resources/coughCodes.csv --keyfieldname icd9Code

echo "Prepare the test kvstore - Smoking Codes map..."

java -cp $installPath/smokecodes.jar -jar $ONLEPLIBPATH/KVInit-1.0 --classname com.ligadata.edifecs.SmokeCodes_100 --kvpath $ONLEPLIBPATH/kvstores/ --csvpath $srcPath/SampleApplication/Medical/MedEnvContext/src/main/resources/smokingCodes.csv --keyfieldname icd9Code