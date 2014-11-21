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

echo "Prepare the test kvstore - CustomerPreferences map..."

# AlertHistory.csv  AlertParameters.csv  CustomerPreferences.csv  kvDimensionalData.xls  TukTier.csv

java -jar $ONLEPLIBPATH/KVInit-1.0 --kvname System.CustomerPreferences --classname com.ligadata.OnLEPBankPoc.CustomerPreferences_100 --kvpath $ONLEPLIBPATH/kvstores/ --csvpath $srcPath/SampleApplication/Bank/SampleData/EnvContextContainerData/dyspnoea.csv --keyfieldname ENT_ACC_NUM

echo "Prepare the test kvstore - AlertHistory map..."

java -jar $ONLEPLIBPATH/KVInit-1.0 --kvname System.AlertHistory --classname com.ligadata.OnLEPBankPoc.AlertHistory_100 --kvpath $ONLEPLIBPATH/kvstores/ --csvpath $srcPath/SampleApplication/Bank/SampleData/EnvContextContainerData/AlertHistory.csv --keyfieldname ENT_ACC_NUM

echo "Prepare the test kvstore - AlertParameters map..."

java -jar $ONLEPLIBPATH/KVInit-1.0 --kvname System.AlertParameters --classname com.ligadata.OnLEPBankPoc.AlertParameters_100 --kvpath $ONLEPLIBPATH/kvstores/ --csvpath $srcPath/SampleApplication/Bank/SampleData/EnvContextContainerData/AlertParameters.csv --keyfieldname ALERT

echo "Prepare the test kvstore - TukTier map..."

java -jar $ONLEPLIBPATH/KVInit-1.0 --kvname System.TukTier --classname com.ligadata.OnLEPBankPoc.TukTier_100 --kvpath $ONLEPLIBPATH/kvstores/ --csvpath $srcPath/SampleApplication/Bank/SampleData/EnvContextContainerData/TukTier.csv --keyfieldname TIERSET_ID


