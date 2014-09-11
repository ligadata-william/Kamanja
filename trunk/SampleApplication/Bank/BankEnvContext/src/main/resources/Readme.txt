

Building the containers with mapdb and the AlchemyKV tool is essential in order to effectively use the BankPoc.cfg in the same folder where this file is found.  The BankPOCEnvContext will try to load each container supplied by the OnLEPManager to it.  If there are keys found in the store, they will be loaded into respective Map[String,BaseContainer] maps.

scala -Dlog4j.configuration=file:/somepath/log4j.properties -cp ... com.ligadata.alchemy.kvinit.AlchemyKV --kvname AlertHistory_100 --kvpath /tmp/OnLEPNew --csvpath /yourpath/svn/ligadata/trunk/MetadataBootstrap/AlchemyKVInit/src/main/resources/EnvContextContainerData/AlertHistory.csv  --keyfieldname ENT_ACC_NUM

scala -Dlog4j.configuration=file:/somepath/log4j.properties -cp ... com.ligadata.alchemy.kvinit.AlchemyKV ---kvname AlertParameters_100 --kvpath /tmp/OnLEPNew --csvpath /yourpath/svn/ligadata/trunk/MetadataBootstrap/AlchemyKVInit/src/main/resources/EnvContextContainerData/AlertParameters.csv  --keyfieldname ALERT

scala -Dlog4j.configuration=file:/somepath/log4j.properties -cp ... com.ligadata.alchemy.kvinit.AlchemyKV --kvname CustomerPreferences_100 --kvpath /tmp/OnLEPNew --csvpath /yourpath/svn/ligadata/trunk/MetadataBootstrap/AlchemyKVInit/src/main/resources/EnvContextContainerData/CustomerPreferences.csv  --keyfieldname ACCOUNT_NUMBER

This EnvContext implementation is a provisional one for near term.  A productized version will be implemented that can cope with mapdb, et al representations at some point in the near future.

