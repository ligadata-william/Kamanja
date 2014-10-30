set installPath=%1
set srcPath=%2

IF %1 == "" (
        echo "No install path supplied.  It should be a directory that can be written to and whose current content is of no value (will be overwritten) "
        echo "%0 <install path> <src tree trunk directory>"
        exit 1
)
IF %2 == "" (
        echo "No src path supplied.  It should be the trunk directory containing the jars, files, what not that need to be supplied."
        echo "%0 <install path> <src tree trunk directory>"
        exit 1
)

set ONLEPLIBPATH=%installPath%

REM *******************************
REM Clean out prior installation
REM *******************************
rm -Rf %ONLEPLIBPATH%

REM *******************************
REM Make the directories as needed
REM *******************************
mkdir -p %ONLEPLIBPATH%/msgdata
mkdir -p %ONLEPLIBPATH%/kvstores
mkdir -p %ONLEPLIBPATH%/logs

REM *******************************
REM Build fat-jars
REM *******************************

echo "clean, package and assemble %srcPath% ..."

cd %srcPath%
call sbt clean 
call sbt package 
call sbt OnLEPManager/assembly 
call sbt MetadataAPI/assembly 
call sbt KVInit/assembly 

REM recreate eclipse projects
REM echo "refresh the eclipse projects ..."
REM cd %srcPath%
REM sbt eclipse

REM Move them into place
echo "xcopy the fat jars to %ONLEPLIBPATH% ..."

cd %srcPath%
xcopy SampleApplication/Tools/KVInit/target/scala-2.10/KVInit* %ONLEPLIBPATH%
xcopy MetadataAPI/target/scala-2.10/MetadataAPI* %ONLEPLIBPATH%
xcopy OnLEPManager/target/scala-2.10/OnLEPManager* %ONLEPLIBPATH%

REM *******************************
REM xcopy jars required (more than required if the fat jars are used)
REM *******************************

REM Base Types and Functions, InputOutput adapters, and original versions of things
echo "xcopy Base Types and Functions, InputOutput adapters..."
xcopy %srcPath%/BaseFunctions/target/scala-2.10/basefunctions_2.10-0.1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%/BaseTypes/target/scala-2.10/basetypes_2.10-0.1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%/InputOutputAdapters/FileSimpleInputOutputAdapters/target/scala-2.10/filesimpleinputoutputadapters_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%/InputOutputAdapters/KafkaSimpleInputOutputAdapters/target/scala-2.10/kafkasimpleinputoutputadapters_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%/EnvContexts/SimpleEnvContextImpl/target/scala-2.10/simpleenvcontextimpl_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%/MetadataBootstrap/Bootstrap/target/scala-2.10/bootstrap_2.10-1.0.jar %ONLEPLIBPATH%

REM Storage jars
echo "xcopy Storage jars..."
xcopy %srcPath%/Storage/target/scala-2.10/storage_2.10-0.0.0.2.jar %ONLEPLIBPATH%

REM Metadata jars
echo "xcopy Metadata jars..."
xcopy %srcPath%/Metadata/target/scala-2.10/metadata_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%/MessageDef/target/scala-2.10/messagedef_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%/MetadataAPI/target/scala-2.10/metadataapi_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%/MetadataAPIService/target/scala-2.10/metadataapiservice_2.10-0.1.jar %ONLEPLIBPATH%
xcopy %srcPath%/MetadataAPIServiceClient/target/scala-2.10/metadataapiserviceclient_2.10-0.1.jar %ONLEPLIBPATH% : Didn't build... Not included anymore?
xcopy %srcPath%/Pmml/MethodExtractor/target/scala-2.10/methodextractor_2.10-1.0.jar %ONLEPLIBPATH%

REM OnLEP jars
echo "xcopy OnLEP jars..."
xcopy %srcPath%/OnLEPBase/target/scala-2.10/onlepbase_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%/OnLEPManager/target/scala-2.10/onlepmanager_2.10-1.0.jar %ONLEPLIBPATH%

REM Pmml compile and runtime jars
echo "xcopy Pmml compile and runtime jars..."
xcopy %srcPath%/Pmml/PmmlRuntime/target/scala-2.10/pmmlruntime_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%/Pmml/PmmlUdfs/target/scala-2.10/pmmludfs_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%/Pmml/PmmlCompiler/target/scala-2.10/pmmlcompiler_2.10-1.0.jar %ONLEPLIBPATH%

REM Med Application jars
echo "xcopy Med Application jars..."
xcopy %srcPath%/SampleApplication/Medical/MedicalBootstrap/target/scala-2.10/medicalbootstrap_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%/SampleApplication/Medical/MedEnvContext/target/scala-2.10/medenvcontext_2.10-1.0.jar %ONLEPLIBPATH%

REM model debug jars (if any)
echo "xcopy model debug jars (if any)..."
xcopy %srcPath%/modeldbg/COPD_000100/target/scala-2.10/copd_000100_2.10-1.0.jar %ONLEPLIBPATH%

REM sample configs
REMecho "xcopy sample configs..."
REMxcopy %srcPath%/SampleApplication/Medical/MedEnvContext/src/main/resources/*cfg %ONLEPLIBPATH%
xcopy %srcPath%/SampleApplication/Tools/KVInit/src/main/resources/*cfg %ONLEPLIBPATH%

REM other jars 
echo "xcopy other jars..."
xcopy %srcPath%/../externals/log4j/log4j-1.2.17.jar %ONLEPLIBPATH%

REM *******************************
REM COPD messages data prep
REM *******************************

REM Prepare test messages and xcopy them into place

echo "Prepare test messages and xcopy them into place..."
cd %srcPath%/SampleApplication/Tools/KVInit/src/main/resources
REM 7za -tgzip beneficiaries.csv.gz beneficiaries.csv
REM 7za -tgzip messages_new_format.csv.gz messages_new_format.csv
REM 7za -tgzip messages_old_format.csv.gz messages_old_format.csv
REM 7za -tgzip messages_new_format_all.csv.gz messages_new_format_all.csv

xcopy *gz %ONLEPLIBPATH%/msgdata/

echo "Prepare the test kvstore - Dyspnea Codes map..."

java -jar %ONLEPLIBPATH%/KVInit-1.0 --kvname com.ligadata.edifecs.DyspnoeaCodes_100 --kvpath %ONLEPLIBPATH%/kvstores/ --csvpath %srcPath%/SampleApplication/Medical/MedEnvContext/src/main/resources/dyspnoea.csv --keyfieldname icd9Code

echo "Prepare the test kvstore - Environmental Exposure Codes map..."

java -jar %ONLEPLIBPATH%/KVInit-1.0 --kvname com.ligadata.edifecs.EnvCodes_100 --kvpath %ONLEPLIBPATH%/kvstores/ --csvpath %srcPath%/SampleApplication/Medical/MedEnvContext/src/main/resources/envExposureCodes.csv --keyfieldname icd9Code

echo "Prepare the test kvstore - Sputum Codes map..."

java -jar %ONLEPLIBPATH%/KVInit-1.0 --kvname com.ligadata.edifecs.SputumCodes_100 --kvpath %ONLEPLIBPATH%/kvstores/ --csvpath %srcPath%/SampleApplication/Medical/MedEnvContext/src/main/resources/sputumCodes.csv --keyfieldname icd9Code

echo "Prepare the test kvstore - Cough Codes map..."

java -jar %ONLEPLIBPATH%/KVInit-1.0 --kvname com.ligadata.edifecs.CoughCodes_100 --kvpath %ONLEPLIBPATH%/kvstores/ --csvpath %srcPath%/SampleApplication/Medical/MedEnvContext/src/main/resources/coughCodes.csv --keyfieldname icd9Code

echo "Prepare the test kvstore - Smoking Codes map..."

java -jar %ONLEPLIBPATH%/KVInit-1.0 --kvname com.ligadata.edifecs.SmokeCodes_100 --kvpath %ONLEPLIBPATH%/kvstores/ --csvpath %srcPath%/SampleApplication/Medical/MedEnvContext/src/main/resources/smokingCodes.csv --keyfieldname icd9Code

REM *******************************
REM All that is left is to run the OnLEPManager
REM *******************************

REM no debug
REM java -jar %ONLEPLIBPATH%/OnLEPManager-1.0 --config /tmp/OnLEPInstall/COPD.cfg

REM debug version intended for eclipse attached debugging
REM java -Xdebug -Xrunjdwp:transport=dt_socket,address=8998,server=y -jar %ONLEPLIBPATH%/OnLEPManager-1.0 --config /tmp/OnLEPInstall/COPD.cfg


echo "installOnLEP complete..."
