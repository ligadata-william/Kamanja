package com.ligadata.KamanjaShell

import scala.actors.threadpool.{ Executors, ExecutorService }

import java.util.logging.Logger
import kafka.admin.AdminUtils
import kafka.common.TopicExistsException
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import java.util.Properties
import sys.process._
/**
 * @author danielkozin
 */
object KafkaCommands {
  val tmpKPath = "/Users/danielkozin/kafka_2.10-0.8.2.1"  
   
  // Logger stuff
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  
  var knownTopics: scala.collection.mutable.Map[String,Int] = scala.collection.mutable.Map[String,Int]()
  
  /**
   * Create a topics requested by the suer.  
   */
  def createTopic(opts: InstanceContext, topicName: String, numPartitions: Int, replicationFactor: Int): Unit = {
    
    var kamanjaHome = opts.getKamanjaHome
    var default_rFactor = 1
    val zkClient:ZkClient = new ZkClient(opts.makeZkConnectString, 3000, 3000, ZKStringSerializer)
    val topicConfig = new Properties()
    try {
      logger.info("KAMANJA_SHELL_KAFKA-KAFKA: Creating Topic: " + topicName)
      AdminUtils.createTopic(zkClient, topicName, numPartitions, default_rFactor, topicConfig)
      Thread sleep 3000
      logger.info("KAMANJA_SHELL_KAFKA: Topic Created: " + topicName)
      
      if (!knownTopics.contains(topicName))
        knownTopics(topicName) = numPartitions
    }
    catch {
      case e: TopicExistsException => {
        if (!knownTopics.contains(topicName))
          knownTopics(topicName) = numPartitions
        logger.info("KAMANJA_SHELL: Topic " + topicName + " already created. Continuing...")
      }
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    finally{
      if(zkClient != null) {
        zkClient.close()
      }
    } 

   // val kafkaCommand = Seq("sh","-c", s"$tmpKPath/bin/kafka-topics.sh --create --zookeeper "+opts.makeZkConnectString+" --replication-factor "+default_rFactor+" --partitions "+numPartitions+" --topic " + topicName)  
   // println(kafkaCommand)
   // kafkaCommand.!
    
  }
  
  /**
   * Push some date to a specified QUEUE
   */
  def pushMessages(exec: ExecutorService, opts: InstanceContext, topicName:String, partitionIndex: String,  sourceFile: String, sourceCodeFormat: String = "CSV", isGZ: String = "false"): Unit = {
    var brokerList = opts.makeKafkaConnectString
   // var numPart = knownTopics(topicName)
    var numPart = 8
    var numOfThd = 1
    // "\" --partitionkeyidxs \""+ partitionIndex +
    // java -jar /bin/Kamanja/bin/SimpleKafkaProducer-0.1.0 --gz false --topics testin_1 --threads 1 --topicpartitions 8 --brokerlist localhost:9092 --files /Users/danielkozin/kamanjaApps/BOFA/data/sampleData.csv --partitionkeyidx 1 --format CSV
    var kafkaCommand = "java -jar "+ opts.getKamanjaHome +"/bin/SimpleKafkaProducer-0.1.0 --gz "+ isGZ +" --topics \"" + topicName + " --threads "+ numOfThd + " --topicpartitions " +numPart+ " --brokerlist \""+ brokerList + "\" --files \""+ sourceFile + " --format " + sourceCodeFormat
    println("Executing ")
    println(kafkaCommand)
    exec.execute(new Runnable() {
      override def run() = {
        //var retVal = Process(kafkaCommand).!
        val buffer = new StringBuffer() 
        val lines =  kafkaCommand lines_! ProcessLogger(buffer append _)
        println("Executing -COMPLETED ")
      }
    })  
    
    // wait for the command to finish
    Thread sleep 1200
  }
  
   /**
   * Start an internal KAFKA PROCESS.  Need to figure out the port available, then 
   * set up the right configuration for this local kafka instance.  
   * 
   */
   def startLocalKafka(opts: InstanceContext, exec: ExecutorService): Unit = {
      opts.kafkaPort = KShellUtils.getAvailablePort(opts.kafkaPort).toString
      var kprops = new java.util.Properties
      var kpFile: String = s"$tmpKPath/config/server.properties"       
      try {
        // Override the Kafka Configuration with our default parameters..
        // and save it in the 
        kprops.load(new java.io.FileInputStream(kpFile))
        kprops.setProperty("log.dirs", opts.getIPath + "/kafka/logs")
        kprops.setProperty("port", opts.kafkaPort.toString)
        kprops.setProperty("zookeeper.connect", "localhost:" + opts.zkPort)       
        KShellUtils.savePropFile(kprops, "server.properties" , opts.getIPath + "/kafka/conf")       
      } catch { 
        //TODO:  Handle this nicely.
        case e: Exception => e.printStackTrace()
      }  
   
      println("Auto-Starting Kafka at " + opts.kafkaLocation + ":" + opts.kafkaPort)  
      exec.execute(new Runnable() {
        override def run() = {
          val buffer = new StringBuffer() 
          var tPath = opts.getIPath
          var cmd = s"$tmpKPath/bin/kafka-server-start.sh $tPath/kafka/conf/server.properties"
          println(" STARTING KAFKA -> " + cmd)
          val kafkaCommand = Seq("sh","-c", cmd)  
          val lines = kafkaCommand lines_! ProcessLogger(buffer append _)
        }
      })    
      // wait for the command to finish
      Thread sleep 1200
  }
    
}