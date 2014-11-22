
Bank Application

In this directory tree is the scripts, types, messages, container data, and configurations needed to 
run the Bank example.

Basic steps are:

a) Place the scripts (installOnLEP.sh, PrepKvStores_Bank.sh, PrepMsgData_Bank.sh) on your PATH, giving them executable permissions if necessary.

b) Install the software. Two parameters are required: the target installation directory and the github trunk directory for your enlistment.  For example,

	installOnLEP.sh /tmp/OnLEPInstall ~/github/Bank/RTD/trunk

c) Initialize the container kv stores that are required (same arguments as in b)

	PrepKvStores_Bank.sh /tmp/OnLEPInstall ~/github/Bank/RTD/trunk

d) Install the test message data ( (same arguments as in b)

	PrepMsgData_Bank.sh /tmp/OnLEPInstall ~/github/Bank/RTD/trunk

e) Configure the MetadataAPI configuration file sample in Configs to reflect the locations of the json files for models, types,
messages, containers, and UDFs.

f) Install any additional UDFs as needed (Note: a core library is provided that requires no installation)

	java -jar /tmp/OnLEPInstall/MetadataAPI-1.0 --config ./MetadataAPIConf.json

g) Install the message, container, and model metadata and implementation jars for the Bank by using the MetadataAPI application

	java -jar /tmp/OnLEPInstall/MetadataAPI-1.0 --config ./MetadataAPIConf.json

g) Run the OnLEPManager with one of the configuration files:

	java -jar /tmp/OnLEPInstall/OnLEPManager-1.0 --config ./BankPOC.cfg
	java -jar /tmp/OnLEPInstall/OnLEPManager-1.0 --config ./BankPOC_LBOD123EB2.cfg
	java -jar /tmp/OnLEPInstall/OnLEPManager-1.0 --config ./BankPOC_OD123.cfg
	java -jar /tmp/OnLEPInstall/OnLEPManager-1.0 --config ./BankPOC_OD123EB12.cfg

