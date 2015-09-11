package com.ligadata.KamanjaShell

import scala.actors.threadpool.{ Executors, ExecutorService }
import com.ligadata.KamanjaManager._


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
  val UPDATE_VERB: String = "update"
  
  
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
  val ABSOLUTE_PATH: String = "isFullPath"
  val KEY: String = "key"
  
  //other
  val JAVA: String = "java"
  val SCALA :String = "scala"
  
  
  // Process the command
  def processCommand (verb: String, subject: String, cOptions: scala.collection.mutable.Map[String,String], opts: InstanceContext, exec: ExecutorService) : String = {
    
    //******************************************
    //*  CREATE commands (Kafka-Topics,)
    //******************************************    
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
    
    //******************************************
    // START commands (engine)
    //******************************************
    if (verb.equalsIgnoreCase(START_VERB)) {
      // Engine?
      if (subject.equalsIgnoreCase(ENGINE)) {
       EningeCommandProcessor.startEngineProcess(opts, exec)
      }     
    }
    
    //******************************************
    //* CONFIGURE commands
    //******************************************
    if (verb.equalsIgnoreCase(CONFIGURE_VERB)) {
      // Engine
      if(subject.equalsIgnoreCase(ENGINE)) {
        var configFilePath = cOptions.getOrElse(CONFIG, "")       
        MetadataProxy.addClusterConfig(opts,configFilePath)       
      }
    }
    
    //******************************************
    //* SET commands
    //******************************************
    if (verb.equalsIgnoreCase(SET_VERB)) {
      // Engine
      if(subject.equalsIgnoreCase(APP_PATH)) {
        var appPath = cOptions.getOrElse(PATH, "")       
        opts.setAppPath(appPath)     
      }
    }
    
    //***************************************
    // * PUSH commands (Data)
    //***************************************
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
    
    //*****************************************
    // ADD Commands (container, message, model)
    //*****************************************
    if (verb.equalsIgnoreCase(ADD_VERB)) {
      // -Add Model
      if (subject.equalsIgnoreCase(MODEL)) {
        var makeFilePath: String = ""
        var modelPath = constructPath(cOptions, opts, MODEL)
        val tokenizedPath = modelPath.split('.')
        
        // is this a Java model
        if (tokenizedPath(tokenizedPath.size - 1).equalsIgnoreCase(JAVA)) {
           makeFilePath = cOptions.getOrElse(MODEL_CONFIG, "")
           MetadataProxy.addNativeModel(opts, modelPath, makeFilePath,"java") 
        }
        // is this a scala Model
        else if (tokenizedPath(tokenizedPath.size - 1).equalsIgnoreCase(SCALA)) {       
           makeFilePath = cOptions.getOrElse(MODEL_CONFIG, "")
           MetadataProxy.addNativeModel(opts, modelPath, makeFilePath, "scala") 
        }
        // nope, must be a PMML one...
        else {
          MetadataProxy.addPmmlModel(opts, modelPath)   
        }    
      }
      
      // -Add container
      if (subject.equalsIgnoreCase(CONTAINER)) {
        var containerPath = constructPath(cOptions, opts, CONTAINER)
        MetadataProxy.addContainer(opts, containerPath)
      }
      
      // -Add message
      if (subject.equalsIgnoreCase(MESSAGE)) {
        var messagePath =  constructPath(cOptions, opts, MESSAGE) 
        MetadataProxy.addMessage(opts, messagePath)        
      } 
    }
    
    //*****************************************
    // Get Commands (container, message, model)
    //   if KEY keyword is specified then only
    //     a definition for that key is returned.
    //   ELSE
    //     keys for all definitions are returned..
    //*****************************************
    if (verb.equalsIgnoreCase(GET_VERB)) {
      // -Get container(s)
      if (subject.equalsIgnoreCase(CONTAINER)) {
        var key = cOptions.getOrElse(KEY, "")
        MetadataProxy.getContainer(opts, key)
      } 
      // -Get message(s)
      if (subject.equalsIgnoreCase(MESSAGE)) {
        var key = cOptions.getOrElse(KEY, "")
        MetadataProxy.getMessage(opts, key)
      } 
      // -Get model(s)
      if (subject.equalsIgnoreCase(MODEL)) {
        var key = cOptions.getOrElse(KEY, "")
        MetadataProxy.getModel(opts, key)
      }             
    }
    
    //*******************************************
    // REMOVE Commands (container, message, model)
    //*******************************************
    if (verb.equalsIgnoreCase(REMOVE_VERB)) {
      // - Remove container
      if(subject.equalsIgnoreCase(CONTAINER)) {
        var key =  cOptions.getOrElse(KEY, "")
        MetadataProxy.removeContainer(opts, key)
      }
   
      // - Remove message
      if(subject.equalsIgnoreCase(CONTAINER)) {
        var key =  cOptions.getOrElse(KEY, "")
        MetadataProxy.removeMessage(opts, key)
      }
      
    }

    
    return ""
  }
  
  /**
   * constructPath - this will construct a full path to the file where the metadata definition is specified.  If the SET APP command has been
   *                 issued, then the path for this file will be created as following
   *                 /<APP_PATH>/metadata/<TYPE(container, message, etc)>/<User parameter>  
   *                 so for example:  
   *                 if SET APP was set for /tmp/app1, and a container named myContainer.json is passed then the final path will be
   *                 /tmp/app1/metadata/container/myContainer.json.
   *                 
   *                 NOTE:  A user can also specify a "isFullPath:yes".  In which case, the user is telling us to take the PATH option as is.  
   */
  private def constructPath(cOptions: scala.collection.mutable.Map[String,String],  opts: InstanceContext, metadataType: String): String = {
    var pathOption = cOptions.getOrElse(PATH, "")
    var abPath = cOptions.getOrElse(ABSOLUTE_PATH, "")
    
    if (!abPath.equalsIgnoreCase("yes") && opts.getAppPath.length > 0) {
      pathOption = opts.getAppPath + "/metadata/" + metadataType  + "/" + pathOption
    }   
    return pathOption
  }
}