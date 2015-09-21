package com.ligadata.KamanjaShell

import java.util.logging.Logger
import java.net.Socket
import java.io.File
import java.util.Properties
import java.util.List
import java.io.InputStream
import scala.actors.threadpool.{ Executors, ExecutorService }
import scala.collection.mutable
import sys.process._
import jline._
import scala.collection.mutable._

/*class MyStringsCompleter(v1: String, v2: String, v3: String, v4: String) extends jline.Completor {

  var strings: Set[String] = Set[String]()

  def complete(buffer: String, cursor: Int, candidates: java.util.List[_]): Int = {
    println("\nTABBED DETECTED "+ buffer + " xxx " + cursor)
   // var testList = mutable.MutableList[String] = mutable.MutableList[String]()
    var tokens = buffer.split(" ")
    tokens.foreach(x => {println(x)})
    //testList.asJava
    //val x = testList.asJava
    candidates.append("blah")

    //testList.foreach(x=>{candidates.add(_)})
   // candidates.add("string".asInstanceOf[T])
    return -1
  }*/

//}
/**
 * The main entry point for the "kamanja" script to start up all the necessary processes required for 
 * Kamanja Engine deployment and execution
 * 
 * @author danielkozin
 */
object KamanjaShell {
 
  // temp stuff - probably need to be removed or modified.
  val tmpZKPath = "/tmp/zookeeper-3.4.6"
  val tmpKPath = "/tmp/kafka_2.10-0.8.1.1"
  
  // String constants.
  val EXIT: String = "exit"
  val PROMPT_FOR_NAME: String = "Enter the Kamanja Instance Name "
  val PROMPT_FOR_PATH: String = "Enter the Kamanja Instance Path "
  val PROMPT_FOR_PORT: String = "Enter the Kamanja Instance Port "
  
  // property names
  val KAMANJA_HOME_PROP: String = "KAMANJA_HOME"
  val JAVA_HOME_PROP: String = "JAVA_HOME"
  val SCALA_HOME_PROP: String = "SCALA_HOME"
  val JAR_PATHS_PROP: String = "JARPATHS"
  val JAR_TARGET_DIR_PROP: String = "JAR_TARGET_DIR"
  val COMPILER_WORK_DIR_PROP: String = "COMPILER_WORK_DIR"
  val ZOOKEEPER_CONNECT_STRING_PROP: String = "ZOOKEEPER_CONNECT_STRING"
  val ZNODE_PATH_PROP: String = "ZNODE_PATH"
  val API_LEADER_SELECTION_ZK_NODE: String = "API_LEADER_SELECTION_ZK_NODE"
  val NOTIFY_ENGINE_PROP: String = "NOTIFY_ENGINE"
  val API_LEADER_SELECTION_ZK_NOD_PROP: String = "API_LEADER_SELECTION_ZK_NOD"
  val MODEL_EXEC_LOG_PROP: String = "MODEL_EXEC_LOG"
  val CLASSPATH_PROP: String = "CLASSPATH"
  val SERVICE_HOST_PROP: String = "SERVICE_HOST"
  val SERVICE_PORT_PROP: String = "SERVICE_PORT"
  val MODEL_FILES_DIR_PROP: String = "MODEL_FILES_DIR"
  val FUNCTION_FILES_DIR_PROP: String = "FUNCTION_FILES_DIR"
  val MESSAGE_FILES_DIR_PROP: String = "MESSAGE_FILES_DIR"
  val CONTAINER_FILES_DIR_PROP: String = "CONTAINER_FILES_DIR"
  val TYPE_FILES_DIR_PROP: String = "TYPE_FILES_DIR"
  val OUTPUTMESSAGE_FILES_DIR_PROP: String = "OUTPUTMESSAGE_FILES_DIR"
  val CONFIG_FILES_DIR_PROP: String = "CONFIG_FILES_DIR"
  val CONCEPT_FILES_DIR_PROP: String = "CONCEPT_FILES_DIR"
  val NODE_ID_PROP: String = "nodeId"
  val METADATASCHEMANAME_PROP: String = "MetadataSchemaName"
  val METADATASTORETYPE_PROP: String = "MetadataStoreType"
  val METADATALOCATION_PROP: String = "MetadataLocation"
  val METADATADATASTORE_PROP: String = "MetadataDataStore"

   
  
  // Logger stuff
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  
  // some execution state info
  var isRunning: Boolean = true
  var isNode: Boolean = true  
  
  // Required parameters.
  var tempName: String = "???"
  var tempPath: String = _
  var tempPort: String = _
  var iOptions: Array[String] = Array[String]()
  
  // TO keep or not to keep.
  private[this] var _exec: ExecutorService = null
  private var opts: InstanceContext = null
  var console: ConsoleReader = new ConsoleReader
  KamanjaShellCommand.init();
  var kc: KamanjaCompleter = new KamanjaCompleter();
  console.addCompletor(kc);
  


  private def TestThisShit {
    // Partition the KVS into sub partitions.  Each subpartition will be executed on its own Executro Thread.  For now
    // we will divide the partitions into equal number of subsets based on the number of processors determined.
    val kvs = scala.collection.mutable.Map[Int, (String, String, String)]()
    kvs(1) = ("a","a","a")
    kvs(2) = ("a","a","a")
    kvs(3) = ("a","a","a")
    kvs(4) = ("a","a","a")
    kvs(5) = ("a","a","a")
    kvs(6) = ("a","a","a")
    kvs(7) = ("a","a","a")
    kvs(8) = ("a","a","a")
    kvs(9) = ("a","a","a")
    kvs(10) = ("a","a","a")
    kvs(11) = ("a","a","a")
    kvs(12) = ("a","a","a")
    kvs(13) = ("a","a","a")
    kvs(14) = ("a","a","a")
    kvs(15) = ("a","a","a")
    kvs(16) = ("a","a","a")
    kvs(17) = ("a","a","a")
    kvs(18) = ("a","a","a")
    kvs(19) = ("a","a","a")
    kvs(20) = ("a","a","a")
    kvs(21) = ("a","a","a")
    kvs(22) = ("a","a","a")
    kvs(23) = ("a","a","a")
    kvs(24) = ("a","a","a")
    kvs(25) = ("a","a","a")
    kvs(26) = ("a","a","a")
    kvs(27) = ("a","a","a")
    kvs(28) = ("a","a","a")
    kvs(29) = ("a","a","a")
    kvs(30) = ("a","a","a")

    println(kvs)

    val kvs_per_threads = scala.collection.mutable.Map[Int, scala.collection.mutable.Map[Int, (String, String, String)]]()

    var threads = Runtime.getRuntime.availableProcessors

    println(threads)
    var indx = 1
    var bucket = 0
    var currInBucket = 0
    var numberPerBucket = kvs.size / threads


    for (idnx <- 1 to threads) {
      kvs_per_threads(idnx) = scala.collection.mutable.Map[Int, (String, String, String)]()
      println("Creating Bucket")
    }


    kvs.foreach(quad => {
      bucket = (quad._1 % threads) + 1
      var temp = kvs_per_threads.get(bucket).get
      temp(quad._1) = quad._2
    })

    println(kvs_per_threads)
  }

  /**
   * Main entry point
   * @param args Array[String]
   */
  def main(args: Array[String]) {
    var line: String = ""
    var metadataParams = new Properties
    var runtimeParams = new Properties

    TestThisShit

    return
    try {  
      if (args.size == 0) {
        println("Either NODE or CLUSTER must be specified when starting Kamanja")
        return
      }

      // Starting the Kamanja Shell, initialize the shell and 
      parseInitialInput(args)
      // Get the options for this installations.
      opts = InstanceContext.getOptions(iOptions)
      opts.setIName(tempName)
      opts.setIPath(tempPath + "/node_" + tempName)
      opts.setIPort(tempPort)
      opts.setUserid("shellLocal")
      
      // set up Console prompt
      console.setDefaultPrompt("Kamanja Instance: NODE_" + opts.getIName + ">")
      //console.setPrompt("Kamanja Instance: NODE_" + opts.getIName + ">")
      // print("Kamanja Instance: NODE_" + opts.getIName + ">")
      
      // See if the directory already exists or not, if it needs to be created, create it along with the basic
      // configurations to be used.
      if (new java.io.File(opts.getIPath).exists)
        println("Node instance already exists")
      else {
        println("Create new node instance")  
        createNewNodeStructure
      }
      
      var myConfigFile = setupMetadataParams(metadataParams, opts)
      var myEngineFile = setupEngineParams(opts,runtimeParams,metadataParams)      
      
      _exec = Executors.newFixedThreadPool(10)
      
      // Check to see if Zookeeper needs to start.
      if (opts.needZk) 
         ZookeeperCommands.startLocalZookeeper(opts,_exec)
    
      // Check to see if Kafka is needed to be started
      if (opts.needKafka)  
        KafkaCommands.startLocalKafka(opts, _exec)
       
      // Initialize MetadataAPI object
      MetadataProxy.initMetadata(opts, myConfigFile)

        
      // Process all the OPTIONAL PARAMETERS
      
      // Give porcesses time to start upl
      Thread.sleep(3000)
      
      //go into a loop listening for commands.
    //  while (isRunning) {  
    //    println("...")
    //    println("Ready to process commands")
   //     val input = getLine()
   //     if (input.equalsIgnoreCase(this.EXIT)) isRunning = false else exectuteShellCommand(input)
        
   //     println("??????")
  //    }
      var keepProcessing: Boolean = true
      while (keepProcessing) {
        println("-->" + line)
        line = console.readLine
        if (line.equalsIgnoreCase(EXIT)) keepProcessing = false
        kc.commandCompleted()
      }
      
      // cleanup and bail
      cleanup
      return
    } catch {
      case e: Exception => {
          e.printStackTrace
          cleanup
          sys.exit
        }
      case t: Throwable => {
        t.printStackTrace
      }
    }
    
  }
  
  /**
   * Process incoming command from shell
   * @param incmd String
   */
  private def exectuteShellCommand(incmd: String): Unit = {
    
    var wasVerbSet = false
    var wasSubjectSet = false
    
    var count = 1
    var verb: String = ""
    var subject: String = ""
    var cmdParms: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map[String,String]()
    

    val shellCommand = incmd.split(" ").filter(x => x.length > 0 )
    shellCommand.foreach (term => {
      println(term) 
      if (count == 1) {
        verb = term
        wasVerbSet = true
      } 
      
      if (count == 2) {
        subject = term
        wasSubjectSet = true
      }
      
      if (count > 2) {
        var parms = term.split(":")
        if (parms.size != 2) {println("Bad Command Parameters"); return}
        cmdParms(parms(0)) = parms(1)
      }
           
      count = count + 1
    })
    
    if (!wasVerbSet || !wasSubjectSet) {
      println("Bad Command")
      return
    }
    
    
    KShellComandProcessor.processCommand(verb,subject,cmdParms,opts, _exec)
    
    return
  }
  
  /**
   * 
   */
  private def setupEngineParams(opts: InstanceContext, derivedParams: Properties, mdParams: Properties): String = {
    
    var nodeDir = opts.getIPath + "/config"
    
    // Set the node Id to default to the name fo the node.
     derivedParams.setProperty(NODE_ID_PROP, opts.getIName) 
     derivedParams.setProperty(METADATADATASTORE_PROP, mdParams.getProperty(METADATADATASTORE_PROP)) 
     
      KShellUtils.savePropFile(derivedParams,"engineConfig_"+opts.getIName+".properties",nodeDir)
     nodeDir + "/mdConfig_"+opts.getIName+".properties"
  }
  
  
  /**
   * create metadata parameters.
   */
  private def setupMetadataParams(derivedParams: Properties, opts: InstanceContext): String = {
     // This is a straightforward algorithm.  First, Find the KAMANJA_HOME.
     // Once KamanjaHome is set, get the MetadataAPI.property file located in KAMANJA_HOME/config
     // take any values that have been specified, and default to whatever hasn't been specified.
     // if a value is required but cannot be determined, FAIL and BAIL.
     var configDir: String = ""
     var jarPathDir: String = ""
     var jarTargetDir: String = ""
     var compilerWorkDir: String = ""
     var classPath: String = ""
     var nodeDir: String = opts.getIPath + "/config"
    
     // Step 1. -  Get KamanjaHome.  It must be set
     try {
        val kamanjaHome = sys.env("KAMANJA_HOME")
        opts.setKamanjaHome(kamanjaHome)
        derivedParams.setProperty(KAMANJA_HOME_PROP, kamanjaHome)
        configDir = kamanjaHome+"/config"
        jarPathDir = kamanjaHome+"/lib/system," + kamanjaHome+"/lib/application"
        jarTargetDir = kamanjaHome+"/lib/applications"
        compilerWorkDir = s"/tmp/Kamanja/lib/system/kamanjabase_2.10-1.0.jar:/tmp/Kamanja/lib/system/basefunctions_2.10-0.1.0.jar:/tmp/Kamanja/lib/system/metadata_2.10-1.0.jar:/tmp/Kamanja/lib/system/methodextractor_2.10-1.0.jar:/tmp/Kamanja/lib/system/pmmlcompiler_2.10-1.0.jar:/tmp/Kamanja/lib/system/bootstrap_2.10-1.0.jar:/tmp/Kamanja/lib/system/joda-time-2.3.jar:/tmp/Kamanja/lib/system/joda-convert-1.6.jar:/tmp/Kamanja/lib/system/basetypes_2.10-0.1.0.jar:/tmp/Kamanja/lib/system/pmmludfs_2.10-1.0.jar:/tmp/Kamanja/lib/system/pmmlruntime_2.10-1.0.jar:/tmp/Kamanja/lib/system/json4s-native_2.10-3.2.9.jar:/tmp/Kamanja/lib/system/json4s-core_2.10-3.2.9.jar:/tmp/Kamanja/lib/system/json4s-ast_2.10-3.2.9.jar:/tmp/Kamanja/lib/system/jackson-databind-2.3.1.jar:/tmp/Kamanja/lib/system/jackson-annotations-2.3.0.jar:/tmp/Kamanja/lib/system/json4s-jackson_2.10-3.2.9.jar:/tmp/Kamanja/lib/system/jackson-core-2.3.1.jar:/tmp/Kamanja/lib/system/log4j-1.2.17.jar:/tmp/Kamanja/lib/system/guava-18.0.jar:/tmp/Kamanja/lib/system/scala-library-2.10.4.jar:/tmp/Kamanja/lib/system/exceptions_2.10-1.0.jar:/tmp/Kamanja/lib/system/scala-reflect.jar"
        println(kamanjaHome)
     } catch {
       case nsee: java.util.NoSuchElementException => {
         logger.info("KAMANJA_HOME must be set.  Aborting")
         throw nsee
       }
     }
     
     // Get the config file.
     var mdProps = new java.util.Properties
     var mdFile: String = configDir+"/kamanjaMetadata.properties"       
     try {
       mdProps.load(new java.io.FileInputStream(mdFile))
     } catch {
       case fnfe: java.io.FileNotFoundException => {
         logger.info(s"Unable to find $mdFile in "+configDir)
         throw fnfe
       }
     }
     
     // ---- START SETTING PROPERTIES
     //
     // 1. get JAVA_HOME - if defined in config, use it, else, check JAVA_HOME, if not there, use the JRE
     try {
        var javaHome = mdProps.getProperty(JAVA_HOME_PROP)
        if(javaHome == null)
          javaHome = sys.env("JAVA_HOME")
        else
          println("JAVA_HOME found in file")
        derivedParams.setProperty(JAVA_HOME_PROP, javaHome)
        println(javaHome)
     } catch {
       case nsee: java.util.NoSuchElementException => {
         logger.info("JAVA_HOME is not set... attempting to resolve from system setting")
         val jreHome = new File(System.getProperty("java.home"))
         derivedParams.setProperty(JAVA_HOME_PROP, jreHome.toPath + "/bin")
          println(jreHome.toPath + "/bin")
         //throw nsee
       }
     }

     // 2. get SCALA_HOME... if not set  default to system.
     try {
        var scalaHome = mdProps.getProperty(SCALA_HOME_PROP)
        
        if(scalaHome == null)
          scalaHome = sys.env(SCALA_HOME_PROP)
        else
          println("SCALA found in file")
          
        derivedParams.setProperty(SCALA_HOME_PROP, scalaHome)
        println(scalaHome)
     } catch {
       case nsee: java.util.NoSuchElementException => {
         logger.info("SCALA_HOME is not set... attempting to resolve from system setting")
         //throw nsee
       }
     }
     

     
     // 3. JAR_PATHS
     if (mdProps.getProperty(JAR_PATHS_PROP) != null) 
       derivedParams.setProperty(JAR_PATHS_PROP, mdProps.getProperty(JAR_PATHS_PROP)) 
     else 
       derivedParams.setProperty(JAR_PATHS_PROP, jarPathDir)
       
     // 4. COMPILER_WORKING_DIR   
     if (mdProps.getProperty(COMPILER_WORK_DIR_PROP) != null)  
       derivedParams.setProperty(COMPILER_WORK_DIR_PROP, mdProps.getProperty(COMPILER_WORK_DIR_PROP)) // jarPathDir = mdProps.getProperty(COMPILER_WORK_DIR_PROP)
     else
       derivedParams.setProperty(COMPILER_WORK_DIR_PROP, compilerWorkDir)
       
     // 5. JAR_TARGET_DIR  
     if (mdProps.getProperty(JAR_TARGET_DIR_PROP) != null)  
       derivedParams.setProperty(JAR_TARGET_DIR_PROP, mdProps.getProperty(JAR_TARGET_DIR_PROP)) 
     else
       derivedParams.setProperty(JAR_TARGET_DIR_PROP, jarTargetDir)
       
     // 6. ZOOKEEPER_CONNECT_STRING        
     derivedParams.setProperty(ZOOKEEPER_CONNECT_STRING_PROP, opts.zkLocation+":"+opts.zkPort) 
    
     // 7 ZNODE_PATH  
     if (mdProps.getProperty(ZNODE_PATH_PROP) != null) 
       derivedParams.setProperty(ZNODE_PATH_PROP, mdProps.getProperty(ZNODE_PATH_PROP)) 
     else
       derivedParams.setProperty(ZNODE_PATH_PROP, "\\ligadata")  
 
 
     // 7 NOTIFY_ENGINE  
     if (mdProps.getProperty(NOTIFY_ENGINE_PROP) != null) 
       derivedParams.setProperty(NOTIFY_ENGINE_PROP, mdProps.getProperty(NOTIFY_ENGINE_PROP)) 
     else
       derivedParams.setProperty(NOTIFY_ENGINE_PROP, "YES")       
       
     // 8 API_LEADER_SELECTION_ZK_NODE 
     if (mdProps.getProperty(API_LEADER_SELECTION_ZK_NOD_PROP) != null) 
       derivedParams.setProperty(API_LEADER_SELECTION_ZK_NOD_PROP, mdProps.getProperty(API_LEADER_SELECTION_ZK_NOD_PROP)) 
     else
       derivedParams.setProperty(API_LEADER_SELECTION_ZK_NOD_PROP, "\\ligadata")       
       
     // 9. MODEL_EXEC_LOG  
     if (mdProps.getProperty(MODEL_EXEC_LOG_PROP) != null)       
       derivedParams.setProperty(MODEL_EXEC_LOG_PROP, mdProps.getProperty(MODEL_EXEC_LOG_PROP)) 
     else
       derivedParams.setProperty(MODEL_EXEC_LOG_PROP, "true") 
       
     // 10. CLASSPATH  
     if (mdProps.getProperty(CLASSPATH_PROP) != null)
       derivedParams.setProperty(CLASSPATH_PROP, mdProps.getProperty(CLASSPATH_PROP)) 
     else
       derivedParams.setProperty(CLASSPATH_PROP, classPath) 
       
     if (opts.getIPort.toInt > 0) {
        derivedParams.setProperty(SERVICE_HOST_PROP, "localhost") 
        derivedParams.setProperty(SERVICE_PORT_PROP, opts.getIPort)    
     }

     // 11. APPLICATION SPECIFIC DIRECTORIES.. just compy them, we cant assume anything with them
     if (mdProps.getProperty(MODEL_FILES_DIR_PROP) != null)  derivedParams.setProperty(MODEL_FILES_DIR_PROP, mdProps.getProperty(MODEL_FILES_DIR_PROP)) 
     if (mdProps.getProperty(TYPE_FILES_DIR_PROP) != null)  derivedParams.setProperty(TYPE_FILES_DIR_PROP, mdProps.getProperty(TYPE_FILES_DIR_PROP)) 
     if (mdProps.getProperty(FUNCTION_FILES_DIR_PROP) != null)  derivedParams.setProperty(FUNCTION_FILES_DIR_PROP, mdProps.getProperty(FUNCTION_FILES_DIR_PROP)) 
     if (mdProps.getProperty(CONCEPT_FILES_DIR_PROP) != null)  derivedParams.setProperty(CONCEPT_FILES_DIR_PROP, mdProps.getProperty(CONCEPT_FILES_DIR_PROP)) 
     if (mdProps.getProperty(MESSAGE_FILES_DIR_PROP) != null)  derivedParams.setProperty(MESSAGE_FILES_DIR_PROP, mdProps.getProperty(MESSAGE_FILES_DIR_PROP)) 
     if (mdProps.getProperty(OUTPUTMESSAGE_FILES_DIR_PROP) != null)  derivedParams.setProperty(OUTPUTMESSAGE_FILES_DIR_PROP, mdProps.getProperty(OUTPUTMESSAGE_FILES_DIR_PROP)) 
     if (mdProps.getProperty(CONTAINER_FILES_DIR_PROP) != null)  derivedParams.setProperty(CONTAINER_FILES_DIR_PROP, mdProps.getProperty(CONTAINER_FILES_DIR_PROP)) 
     if (mdProps.getProperty(CONFIG_FILES_DIR_PROP) != null)  derivedParams.setProperty(CONFIG_FILES_DIR_PROP, mdProps.getProperty(CONFIG_FILES_DIR_PROP)) 
      
     // 10. STORAGE STUFF  
     if (mdProps.getProperty(METADATADATASTORE_PROP) != null)
       derivedParams.setProperty(METADATADATASTORE_PROP, mdProps.getProperty(METADATADATASTORE_PROP)) 
     else
       throw new Exception("Metadata Store information must be set.")
       


     
     // Set the node Id to default to the name fo the node.
     derivedParams.setProperty(NODE_ID_PROP, opts.getIName) 
     KShellUtils.savePropFile(derivedParams,"mdConfig_"+opts.getIName+".properties",nodeDir)
     // Return..
     nodeDir + "/mdConfig_"+opts.getIName+".properties"
                                 
  }
    
 
  /**
   * Each node WILL have its "HOME" directory.  This directory will have
   *  a ZOOKEEPER, KAFKA and CONFIG folgers.
   *  
   *  ZOOKEEPER will have /DATA and /CONF folders
   *  KAFKA will have /LOGS and /CONF folders
   */
  private def createNewNodeStructure: Unit = {
    // Create the Root Node Directory.
    var topDir: File = new File(opts.getIPath)
    var isSuccess = topDir.mkdir

    // Create zookeeper dir for this node
    var zkDir = new File(opts.getIPath + "/zookeeper" )
    isSuccess = zkDir.mkdir
    var zkData = new File(opts.getIPath + "/zookeeper/data" )
    isSuccess = zkData.mkdir
    var zkConfig = new File(opts.getIPath + "/zookeeper/conf" )
    isSuccess = zkConfig.mkdir
    
    // Create kafka dir for this node
    var kafkaDir = new File(opts.getIPath + "/kafka" )
    isSuccess = kafkaDir.mkdir
    var kData = new File(opts.getIPath + "/kafka/logs" )
    isSuccess = kData.mkdir
    var kConfig = new File(opts.getIPath + "/kafka/conf" )
    isSuccess = kConfig.mkdir 
     
    // Create config dir
    var configDir = new File(opts.getIPath + "/config" )
    isSuccess = configDir.mkdir
  }
  
  
  /**
   *  This parses the initial, REQUIRED, input...  NAME, PATH and GLOBAL PORT numbers are mandatory
   *  and can be provided all at once, or if not provided from the initial CLI, user will be prompted
   */
  private def parseInitialInput(args: Array[String]): Unit = {
    
    // Second element is ALWAYS a NAME if it is provided.
    if (args.size < 2) tempName = getUserInput(args, this.PROMPT_FOR_NAME)  else tempName = args(1)
 
    // third element is ALWAYS a PATH to installation if it is provided.
    if (args.size < 3) tempPath = getUserInput(args, this.PROMPT_FOR_PATH)  else tempPath = args(2)
    
    // fourth element is ALWAYS a Global Port if it is provided.
    if (args.size < 4) tempPort = getUserInput(args, this.PROMPT_FOR_PORT)  else tempPort = args(3)
    
    // All the rest of the arguments are part of the OPTIONS and need will be processed later.
    if (args.size >= 4)
      iOptions = args.slice(4, args.length - 1)
    
  }
  
  /**
   *  Prompt user for input.
   */
  private def getUserInput(args: Array[String], prompt: String): String = {
    var tval: String = ""
    while (tval.length == 0)  { 
      tval = getLine(prompt)
    } 
    tval
  }
  
  /**
   * Get the prompt command.  Should be Name of the instance with ">"
   */
  private def getLine(msg: String = ""): String = {
    if (msg.length > 0) println(msg)
    print("Kamanja Instance: NODE_" + opts.getIName + ">")
    val t = readLine()
    t
  }
  
  /**
   * Show some information.
   */
  private def showInstanceParameters: Unit = {
    println("Kamanja " + {if (isNode) "Node" else "Cluster"} + " is running with the following parameters:")
    println("Name -> " + tempName)
    println("Path -> " + tempPath)
    println("Port -> " + tempPort)
    if (iOptions.size > 0)
      println("Options -> "+ iOptions.mkString(" "))
    else 
      println("No Options sepcified")
  }
  
  /** 
   * Cleanup - stop the Zookeeper, Kafka, Kamanja Runtime, and Kamanja Metadata 
   */
  private def cleanup: Unit = {
    
    // This should kill the Engine if it is running
    logger.info ("Cleaning up the shell sturctures...")
    
    // First things first, Clear Kamanja Metadata Object and the Runtime engine if it is running.
    logger.info("Stopping Metadata Process")
    try {
      MetadataProxy.shutdown(opts)
      EningeCommandProcessor.shutdown
    } catch {
      case e: Exception => e.printStackTrace()
    }
    logger.info("...")
    Thread.sleep(2000)
    
    logger.info("Stoping KAFKA ") 
    _exec.execute(new Runnable() {
      override def run() = {
        val cmd2 = Seq("sh","-c","ps ax | grep -i 'kafka\\.Kafka' | grep java | grep -v grep | awk '{print $1}' | xargs kill -SIGKILL")
        val mvCmdRc : Int = Process(cmd2).!       
      }
    })

    // Allow Kafka a few seconds to shutdown.
    logger.info("...")
    Thread.sleep(3000)

    // Shut down the zookeeper
    logger.info("Stoping ZOOKEEPER ")   
    _exec.execute(new Runnable() {
      override def run() = {
        val stopZK =  s"$tmpZKPath/bin/zkServer.sh stop".!      
      }
    })
    // Let zk a few seconds to shutdown
    Thread.sleep(2000)
    logger.info("...")
    

    // Clean up the executor
    if (_exec != null) {
       logger.info ("Cleaning up the shell sturctures")
       _exec.shutdown   
    }
     _exec = null
    logger.info ("Kamanja Shell shutdown Complete.")
  }
  
}