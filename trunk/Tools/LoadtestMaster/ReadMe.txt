Loadtest

The testprogram consists of two elements the runner and master. The runner will execute on behalf of the master.

Start the runner with the local IP
Start the master with the DBHost and runner IP's

./LoadtestRunner-0.0.0.1 --ip 10.100.0.22
./LoadtestRunner-0.0.0.1 --ip 10.100.0.21
./LoadtestMaster-0.0.0.1 --confs Tools/LoadtestMaster/src/main/resources/runner.json 10.100.0.21 10.100.0.22


Supported scenarios
===================

0 - run insert
1 - run insert, retrieve data after N ms and update record
2 - run reads, insert, read and update - 3:1:1 ratio

Run the LoadtestRunner
======================

  -i, --ip  <arg>   IP address to listen on
      --help        Show help message

Run the LoadtestMaster
======================

  -c, --confs  <arg>                      Configuations to run (default = )
      --consistency-level-delete  <arg>   Consistency level for Delete
                                          (default = ANY)
      --consistency-level-read  <arg>     Consistency level for read
                                          (default = ONE)
      --consistency-level-write  <arg>    Consistency level for write
                                          (default = ANY)
  -d, --dbhost  <arg>                     Database address (default = localhost)
      --maxsize  <arg>                    Maximal size (default = 256)
      --minsize  <arg>                    Minimal size (default = 128)
  -m, --msg  <arg>                        Number of messages (default = 10001)
  -r, --resultfile  <arg>                 Where to store the results
                                          (default = result.csv)
  -s, --scenario  <arg>                   Scenario to run (default = 1)
  -w, --workers  <arg>                    Number of workers (default = 10)
      --help                              Show help message

 trailing arguments:
  hosts (required)    (default = List(localhost))

Json configuration
==================

see sample runner.conf

attributes are:

Name : int
Scenario : int
Workers : int
MessagesInTheSystem : int
Messages : int
MinMessage : int
MaxMessage : int
DoStorage : boolean
TruncateStore : boolean
MsgDelayMSec : int
StatusEverySec : int

connectioninfo: 

name => value 

Nice to have
============

Abort of the master is not stopping the runner until they finished the given task

Build test binaries
===================

sbt
project LoadtestRunner
assembly
project LoadtestMaster
assembly

Copy "binaries"

cp Tools/LoadtestRunner/target/scala-2.11/LoadtestRunner-0.0.0.1 /srv/data/build/ci/ligadata/upload
cp Tools/LoadtestMaster/target/scala-2.11/LoadtestMaster-0.0.0.1 /srv/data/build/ci/ligadata/upload
