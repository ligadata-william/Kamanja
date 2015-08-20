KAMANJA_BASEPATH=`cat /tmp/kamanja.location`

java -jar $KAMANJA_BASEPATH/bin/KVInit-1.0 --kvname System.GlobalPreferences  --config $KAMANJA_BASEPATH/config/Engine1Config.properties --csvpath $KAMANJA_BASEPATH/input/SampleApplications/data/GlobalPreferences_Finance.dat  --keyfieldname PrefType
java -jar $KAMANJA_BASEPATH/bin/KVInit-1.0 --kvname System.CustPreferences    --config $KAMANJA_BASEPATH/config/Engine1Config.properties --csvpath $KAMANJA_BASEPATH/input/SampleApplications/data/CustPreferences_Finance.dat    --keyfieldname custId
java -jar $KAMANJA_BASEPATH/bin/KVInit-1.0 --kvname System.CustomerInfo       --config $KAMANJA_BASEPATH/config/Engine1Config.properties --csvpath $KAMANJA_BASEPATH/input/SampleApplications/data/CustomerInfo_Finance.dat       --keyfieldname custId
