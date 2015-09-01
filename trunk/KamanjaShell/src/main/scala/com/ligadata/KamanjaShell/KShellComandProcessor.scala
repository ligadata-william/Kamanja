package com.ligadata.KamanjaShell

import scala.actors.threadpool.{ Executors, ExecutorService }


/**
 * @author danielkozin
 */
object KShellComandProcessor {

  //verbs
  val CREATE_VERB: String = "create"
  val PUSH_VERB: String = "push"
  
  // Subjects
  val KAFKA_TOPICS: String = "kafka_topic"
  val DATA: String = "data"
  
  // Options
  val TOPIC_NAME: String = "name"  
  val NUMBER_OF_PARTITIONS: String = "partitions"
  val REPLICATION_FACTOR: String = "replicationFactor"
  val TARGET_TOPIC: String = "topic"
  val SOURCE_FILE: String = "fromFile"
  val PARTITION_ON: String = "partitionId"
  
  
  // Process the command
  def processCommand (verb: String, subject: String, cOptions: scala.collection.mutable.Map[String,String], opts: InstanceContext, exec: ExecutorService) : String = {
    
    // CREATE (Kafka-Topics,)
    if (verb.equalsIgnoreCase(CREATE_VERB)) {
      // KAFKA_TOPICS
      if (subject.equalsIgnoreCase(KAFKA_TOPICS))  {
        var topicName = cOptions.getOrElse(TOPIC_NAME,"")
        var numOfPart = cOptions.getOrElse(NUMBER_OF_PARTITIONS,"")
        var repFactor = cOptions.getOrElse(REPLICATION_FACTOR,"")
        
        if (repFactor.size == 0)
          repFactor = "1"
         
        println("Kamanja Shell is executing CREATE TOPIC command for \nTOPIC NAME: " + topicName)
        try {
          KafkaCommands.createTopic(opts, topicName, numOfPart.toInt, repFactor.toInt)  
          println("CREATE TOPIC - SUCCESS")
          return "OK!!!!"
        } catch {
          case e: Exception => {e.printStackTrace}
        }
      }
    }
    
    // PUSH (Data)
    if (verb.equalsIgnoreCase(PUSH_VERB)) {
      // DATA
      if (subject.equalsIgnoreCase(DATA))  {
        var topic = cOptions.getOrElse(TARGET_TOPIC, "")
        var sourceFile = cOptions.getOrElse(SOURCE_FILE, "")
        var partitionOn =  cOptions.getOrElse(PARTITION_ON, "")
       
        KafkaCommands.pushMessages(exec, opts, topic, partitionOn, sourceFile)
       
        println("Kamanja Shell is executing PUSH DATA command into \nTOPIC NAME: " + topic)
        try {
          KafkaCommands.pushMessages(exec, opts, topic, partitionOn, sourceFile)
          println("PUSH DATA - SUCCESS")
          return "OK!!!!"
        } catch {
          case e: Exception => {e.printStackTrace}
        }
      }            
    }
    return ""
  }
}