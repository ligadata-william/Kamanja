package com.ligadata.KamanjaShell

import scala.actors.threadpool.{ Executors, ExecutorService }


/**
 * @author danielkozin
 */
object KShellComandProcessor {

  //verbs
  val CREATE_VERB: String = "create"
  val PUSH_VERB: String = "push"
  val CONFIGURE_VERB: String = "configure"
  val SET_VERB: String = "set"
  val START_VERB: String = "start"
  val ADD_VERB: String = "add"
  val REMOVE_VERB: String = "remove"
  val GET_VERB: String = "get"
  
  
  // Subjects
  val KAFKA_TOPICS: String = "kafka_topic"
  val DATA: String = "data"
  val ENGINE: String = "engine"
  val APP_PATH: String = "appPath"
  val MODEL: String = "model"
  val MESSAGE: String = "message"
  val CONTAINER: String = "container"
  
  // Options
  val TOPIC_NAME: String = "name"  
  val NUMBER_OF_PARTITIONS: String = "partitions"
  val REPLICATION_FACTOR: String = "replicationFactor"
  val TARGET_TOPIC: String = "topic"
  val SOURCE_FILE: String = "fromFile"
  val PARTITION_ON: String = "partitionId"
  val CONFIG: String = "config"
  val PATH: String = "path"
  val FILTER: String = "filter"
  val MODEL_CONFIG: String = "configName"
  
  //other
  val JAVA: String = "java"
  val SCALA :String = "scala"
  
  
  // Process the command
  def processCommand (verb: String, subject: String, cOptions: scala.collection.mutable.Map[String,String], opts: InstanceContext, exec: ExecutorService) : String = {
    
    // CREATE commands (Kafka-Topics,)
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
    
    // CONFIGURE commands
    if (verb.equalsIgnoreCase(CONFIGURE_VERB)) {
      // Engine
      if(subject.equalsIgnoreCase(ENGINE)) {
        var configFilePath = cOptions.getOrElse(CONFIG, "")       
        MetadataProxy.addClusterConfig(opts,configFilePath)       
      }
    }
    
    // SET commands
    if (verb.equalsIgnoreCase(SET_VERB)) {
      // Engine
      if(subject.equalsIgnoreCase(APP_PATH)) {
        var appPath = cOptions.getOrElse(PATH, "")       
        opts.setAppPath(appPath)     
      }
    }
    
    // PUSH commands (Data)
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
    
    // ADD Commands (container, message, model)
    if (verb.equalsIgnoreCase(ADD_VERB)) {
      // -Add Model
      if (subject.equalsIgnoreCase(MODEL)) {
        var makeFilePath: String = ""
        var containerPath = cOptions.getOrElse(PATH, "")
        val tokenizedPath = containerPath.split(".")
        
        // is this a Java model
        if (tokenizedPath(tokenizedPath.size - 1).equalsIgnoreCase(JAVA)) {
           makeFilePath = cOptions.getOrElse(MODEL_CONFIG, "")
        }
        // is this a scala Model
        else if (tokenizedPath(tokenizedPath.size - 1).equalsIgnoreCase(SCALA)) {
           makeFilePath = cOptions.getOrElse(MODEL_CONFIG, "")
        }
        // nope, must be a PMML one...
        else {
          
        }    
      }
      
      // -Add container
      if (subject.equalsIgnoreCase(CONTAINER)) {
        var containerPath = cOptions.getOrElse(PATH, "")
        MetadataProxy.addContainer(opts, containerPath, opts.getAppPath)
      }
      
      // -Add message
      if (subject.equalsIgnoreCase(MESSAGE)) {
        var messagePath = cOptions.getOrElse(PATH, "")
        MetadataProxy.addContainer(opts, messagePath, opts.getAppPath)        
      } 
    }
    

    
    return ""
  }
}