jar_full_path=$(which jar)

if [ "$?" != "0" ]; then
	echo "Not found java home directory."
	exit 1
fi

scala_full_path=$(which scala)

if [ "$?" != "0" ]; then
	echo "Not found java home directory."
	exit 1
fi

java_home=$(dirname $(dirname $jar_full_path))
scala_home=$(dirname $(dirname $scala_full_path))
install_dir=$(dirname $(dirname $(readlink -f "$0")))

java_home_repl=$(echo $java_home | sed 's/\//\\\//g')
scala_home_repl=$(echo $scala_home | sed 's/\//\\\//g')
install_dir_repl=$(echo $install_dir | sed 's/\//\\\//g')

# changing path in script files
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/script/InitKvStores_Template.sh > $install_dir/bin/InitKvStores.sh
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/script/StartEngine_Template.sh > $install_dir/bin/StartEngine.sh
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/script/StartMetadataAPI_Template.sh > $install_dir/bin/StartMetadataAPI.sh
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/script/PushSampleDataToKafka_Template.sh > $install_dir/bin/PushSampleDataToKafka.sh

# changing path in config files
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/config/ClusterConfig_Template.json > $install_dir/config/ClusterConfig.json
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/config/EngineConfig_Template.properties > $install_dir/config/Engine1Config.properties
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/config/MetadataAPIConfig_Template.properties > $install_dir/input/application1/metadata/config/MetadataAPIConfig.properties

# Expecting 1st Parameter as Kafka Install directory
if [ "$#" -ne 1 ] || ! [ -d "$1" ]; then
	echo "WARN: Not given/found Kafka install directory. Not going to replace {KafkaInstallDir} in CreateQueues.sh"
else
	kafkatopics="$1/bin/kafka-topics.sh"
	if [ ! -f "$kafkatopics" ]
		echo "WARN: Not found bin/kafka-topics.sh in given Kafka install directory $1. Not going to replace {KafkaInstallDir} in CreateQueues.sh"
	else
		KafkaRootDir=$1
		KafkaRootDir_repl=$(echo $KafkaRootDir | sed 's/\//\\\//g')
		sed "s/{KafkaInstallDir}/$KafkaRootDir_repl/g" $install_dir/template/script/CreateQueues_Template.sh > $install_dir/bin/CreateQueues.sh
		echo "found Kafka install directory"
	fi
fi

chmod 777 $install_dir/bin/*.*
