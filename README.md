Fatafat Home
===
###What is Fatafat?

Fatafat is a real-time processing and decisioning (RTD) framework. It is built natively on several open-source, Apache projects. It comprises a real-time computation engine, ingestion streaming operators, DAG rules as PMML models, a console for interfaces, search and resource management, REST APIs and pre-built connectors, and out-of-the-box integration with Hadoop, data stores like HBase or Cassandra and messaging systems like Kafka or MQ.

The FatafatTM framework is open source with many features that support use cases for developers:

1. **Built bottom-up in Scala**; a concise powerful, language.
2. **Compiles predictive models** saved as PMML into Scala and JAR files, then saves into data store.
Move complex analytic models into production from weeks to days.
3. **Performs real-time message processing** (i.e. from Kafka), **real-time scoring** with PMML models,
and feeds outputs to application or messaging systems. It provides the benefits of both Storm’s
stateless, event streaming and Spark Streaming’s stateful, distributed processing.
4. **Performance and Scale**. Delivers 100% more message throughput than Storm with DAG execution
of PMML models on a single node. Runs hundreds of models concurrently on the same node.
5. **Run event processing one-time only by enforcing concurrency and frequency**. Eliminates errors
and false alerts from running the same transactions at once or running transactions repeatedly.
6. **Fault tolerant**. Uses Apache Kafka for messaging, and Apache Zookeeper to provide fault
tolerance, processor isolation, cluster coordination service, and resource management.
7. **Handles complex record routing** in large parallelized implementations.
8. **Find and fix problems fast** with DevOps. View service logs to troubleshoot.
9. **Pluggable**. Provides pluggable APIs that enables FatafatTM to run with other messaging systems and
execution environments.
10. **Analytics**. FatafatTM exposes services and data for analytic, reporting and scheduling tools.
11. **Supports other high level languages and abstractions** for implementation (PMML, Cassandra,
Hbase, DSL)
12. **Quickly deploy on multi-node clusters**. Build, ship and run applications with
minimal downtime.
13. **Vibrant Community**. Easy to deploy, test samples, developer guides and community to answer your
questions and make contributions.

###Getting Started

####Prerequisites:

* CentOS/RedHat/OS X (virtual machine for Windows)

* ~ 400 MB for installation (3 GB if building from source)
 
* Have sudoer access

* JDK 1.7.1 or higher ([download](http://www.oracle.com/technetwork/java/javase/downloads/index.html))

* Scala v2.10.4 ([download] (http://www.scala-lang.org/download/2.10.4.html). Be sure to add scala to $PATH)

* On Linux [download sbt](http://www.scala-sbt.org/download.html), [download zookeeper](http://zookeeper.apache.org/releases.html#download), and [download kafka 2.10-0.8.1.1](http://kafka.apache.org/) [for Mac OS see instructions [here](https://github.com/ligaDATA/Fatafat/wiki/Appendix-A-SetupGuide)]

####Quick Start

1. [DOWNLOAD](http://www.ligadata.com/releases/bin/fatafat_install_v101.zip) latest Fatafat binaries

2. Unzip the downloaded file

    a. On Mac OS, double click the zip file if it’s not already unzipped
    
    b. On Windows, use ‘winrar’ or ‘winzip’ to unzip it 
    
    c. On Linux, use 'unzip fatafat_install_v101.zip -d destination_folder'. (Use “sudo apt-get install unzip” if unzip not found)
    
3. Start with [Installing and Running Fatafat] (https://github.com/ligaDATA/FatafatDocs/wiki/Installing-and-Running-Fatafat-Quickly)

###Getting Involved

     
###[Documentation](https://github.com/ligaDATA/Fatafat/wiki/Documentation)
