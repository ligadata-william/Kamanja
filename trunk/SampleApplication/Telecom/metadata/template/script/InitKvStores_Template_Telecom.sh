KAMANJA_BASEPATH=`cat /tmp/kamanja.location`
java -jar $KAMANJA_BASEPATH/bin/KVInit-1.0 --kvname System.AccountAggregatedUsage          --config $KAMANJA_BASEPATH/config/Engine1Config.properties --csvpath $KAMANJA_BASEPATH/input/SampleApplications/data/AccountAggregatedUsage_Telecom.dat           --keyfieldname actNo
java -jar $KAMANJA_BASEPATH/bin/KVInit-1.0 --kvname System.AccountInfo                     --config $KAMANJA_BASEPATH/config/Engine1Config.properties --csvpath $KAMANJA_BASEPATH/input/SampleApplications/data/AccountInfo_Telecom.dat                      --keyfieldname actNo
java -jar $KAMANJA_BASEPATH/bin/KVInit-1.0 --kvname System.SubscriberAggregatedUsage       --config $KAMANJA_BASEPATH/config/Engine1Config.properties --csvpath $KAMANJA_BASEPATH/input/SampleApplications/data/SubscriberAggregatedUsage_Telecom.dat        --keyfieldname msisdn
java -jar $KAMANJA_BASEPATH/bin/KVInit-1.0 --kvname System.SubscriberGlobalPreferences     --config $KAMANJA_BASEPATH/config/Engine1Config.properties --csvpath $KAMANJA_BASEPATH/input/SampleApplications/data/SubscriberGlobalPreferences_Telecom.dat      --keyfieldname PrefType
java -jar $KAMANJA_BASEPATH/bin/KVInit-1.0 --kvname System.SubscriberInfo                  --config $KAMANJA_BASEPATH/config/Engine1Config.properties --csvpath $KAMANJA_BASEPATH/input/SampleApplications/data/SubscriberInfo_Telecom.dat                   --keyfieldname msisdn
<<<<<<< HEAD
java -jar $KAMANJA_BASEPATH/bin/KVInit-1.0 --kvname System.SubscriberPlans                 --config $KAMANJA_BASEPATH/config/Engine1Config.properties --csvpath $KAMANJA_BASEPATH/input/SampleApplications/data/SubscriberPlans_Telecom.dat                  --keyfieldname planName
=======
java -jar $KAMANJA_BASEPATH/bin/KVInit-1.0 --kvname System.SubscriberPlans                 --config $KAMANJA_BASEPATH/config/Engine1Config.properties --csvpath $KAMANJA_BASEPATH/input/SampleApplications/data/SubscriberPlans_Telecom.dat                  --keyfieldname planName
>>>>>>> 44fc11ba7559dd5d484f569ad365ca2c5f9078af
