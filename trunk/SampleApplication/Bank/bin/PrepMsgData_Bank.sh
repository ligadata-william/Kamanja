#!/bin/bash

installPath=$1
srcPath=$2

echo
echo "The install path is $installPath"
echo "the git trunk source path is $srcPath"
echo

if [ ! -d "$installPath" ]; then
        echo "No install path supplied.  It should be a directory that can be written to and whose current content is of no value (will be overwritten) "
        echo "$0 <install path> <src tree trunk directory>"
        exit 1
fi

if [ ! -d "$srcPath" ]; then
        echo "No src path supplied.  It should be the trunk directory containing the jars, files, what not that need to be supplied."
        echo "$0 <install path> <src tree trunk directory>"
        exit 1
fi


export ONLEPLIBPATH=$installPath


# *******************************
# messages data prep
# *******************************

# Prepare test messages and copy them into place

echo "Prepare test messages and copy them into place..."
cd $srcPath/SampleApplication/Bank/SampleData
gzip -c OneCustomerTransactionsAlerts_LBOD123EB2.csv > $ONLEPLIBPATH/msgdata/OneCustomerTransactionsAlerts_LBOD123EB2.csv.gz
gzip -c OneCustomerTransactionsAlerts_OD123.csv > $ONLEPLIBPATH/msgdata/OneCustomerTransactionsAlerts_OD123.csv.gz
gzip -c OneCustomerTransactionsAlerts_OD123EB12.csv > $ONLEPLIBPATH/msgdata/OneCustomerTransactionsAlerts_OD123EB12.csv.gz

# *******************************
# All that is left is to run the OnLEPManager
# *******************************

# no debug
# java -jar $ONLEPLIBPATH/OnLEPManager-1.0 --config /tmp/OnLEPInstall/COPD.cfg

# debug version intended for eclipse attached debugging
# java -Xdebug -Xrunjdwp:transport=dt_socket,address=8998,server=y -jar $ONLEPLIBPATH/OnLEPManager-1.0 --config /tmp/OnLEPInstall/COPD.cfg


echo "PrepMsgData_Bank.sh complete..."
