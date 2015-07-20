######What is Kamanja&#8482;?

Kamanja&#8482; is a real-time processing and decisioning (RTD) framework. It is built natively on several open-source, Apache&#8482; projects. It comprises a real-time computation engine, ingestion streaming operators, [DAG](https://github.com/ligaDATA/Kamanja/wiki/Glossary#d) rules as [PMML](http://www.ibm.com/developerworks/library/ba-ind-PMML1/) models, a console for interfaces, search and resource management, [REST](http://rest.elkstein.org/2008/02/what-is-rest.html) [API](https://github.com/ligaDATA/Kamanja/wiki/Glossary#a)s and pre-built connectors, and out-of-the-box integration with [Apache&#8482; Hadoop&#174;](https://hadoop.apache.org/), data stores such as [Apache HBase&#8482;](http://hbase.apache.org/) or [Apache Cassandra&#8482;](http://cassandra.apache.org/), and messaging systems such as [Apache Kafka](http://kafka.apache.org/) or [IBM&#174; MQ](http://www-03.ibm.com/software/products/en/ibm-mq).

The Kamanja framework is open source with many features that support your use cases:

1. **Built bottom-up in [Scala](http://www.scala-lang.org/what-is-scala.html)**; a concise, powerful, language.
2. **Compiles predictive models**; saves as PMML into Scala and [JAR](https://github.com/ligaDATA/Kamanja/wiki/Glossary#j) files, then saves into data store.
Moves complex analytic models into production from weeks to days.
3. **Performs real-time message processing** (that is, from Kafka), **real-time scoring** with PMML models,
and feeds outputs to application or messaging systems. It provides the benefits of both [Apache Storm](https://storm.apache.org/)’s stateless, event streaming, and [Apache Spark Streaming](https://spark.apache.org/streaming/)’s stateful, distributed processing.
4. **Performance and Scale**. Delivers 100% more message throughput than Storm with DAG execution of PMML models on a single node. Runs hundreds of models concurrently on the same node.
5. **Runs event processing one-time only by enforcing concurrency and frequency**. Eliminates errors
and false alerts from running the same transactions at once or running transactions repeatedly.
6. **Fault tolerant**. Uses Kafka for messaging and [Apache ZooKeeper](https://zookeeper.apache.org/) to provide fault tolerance, processor isolation, cluster coordination service, and resource management.
7. **Handles complex record routing** in large parallelized implementations.
8. **Finds and fixes problems fast** with [DevOps](http://theagileadmin.com/what-is-devops/). Views service logs to troubleshoot.
9. **Pluggable**. Provides pluggable APIs that enable Kamanja to run with other messaging systems and
execution environments.
10. **Analytics**. Kamanja exposes services and data for analytic, reporting, and scheduling tools.
11. **Supports other high-level languages and abstractions** for implementation (PMML, Cassandra,
Hbase, [DSL](https://github.com/ligaDATA/Kamanja/wiki/Glossary#d)).
12. **Quickly deploys on multi-node clusters**. Builds, ships, and runs applications with
minimal downtime.
13. **Vibrant Community**. Easy to deploy, provides test samples, developer guides, and community forums to answer your questions and make contributions.

[Proceed to wiki homepage](https://github.com/ligaDATA/Kamanja/wiki)



