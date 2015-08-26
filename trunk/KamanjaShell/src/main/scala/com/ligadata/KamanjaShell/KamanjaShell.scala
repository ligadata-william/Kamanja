package com.ligadata.KamanjaShell

import java.util.logging.Logger
import java.net.Socket
import java.io.File
import java.io.InputStream
import sys.process._
import scala.actors.threadpool.{ Executors, ExecutorService }

/**
 * The main entry point for the "kamanja" script to start up all the necessary processes required for 
 * Kamanja Engine deployment and execution
 * 
 * @author danielkozin
 */
object KamanjaShell {
 
  val tmpZKPath = "/tmp/zookeeper-3.4.6"
  val tmpKPath = "/tmp/kafka_2.10-0.8.1.1"
  
  val EXIT: String = "exit"
  val PROMPT_FOR_NAME: String = "Enter the Kamanja Instance Name "
  val PROMPT_FOR_PATH: String = "Enter the Kamanja Instance Path "
  val PROMPT_FOR_PORT: String = "Enter the Kamanja Instance Port "
  // Logger stuff
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  
  // some state info
  var isRunning: Boolean = true
  var isNode: Boolean = true  
  // Required parameters.
  var iName: String = "???"
  var iPath: String = _
  var iPort: String = _
  var iOptions: Array[String] = Array[String]()

  var zkPort: String = ""
  
  private[this] var _exec: ExecutorService = null

  /**
   * Main entry point
   * @param args Array[String]
   */
  def main(args: Array[String]) {
   
    try {
      if (args.size == 0) {
        println("Either NODE or CLUSTER must be specified when starting Kamanja")
        return
      }
      // Starting the Kamanja Shell, initialize the shell and 
      parseInput(args)
      
      // See if the directory already exists or not, if it needs to be created, create it along with the basic
      // configurations to be used.
      if (new java.io.File(iPath).exists)
        println("Node instance already exists")
      else {
        println("Create new node instance")  
        createNewNodeStructure
      }
   
      
      _exec = Executors.newFixedThreadPool(4)
      
      // Get the options for this installations.
      var opts: InstanceOptions = InstanceOptions.getOptions(iOptions)
      
      // Check to see if Zookeeper needs to start.
      if (opts.needZk) {
        opts.zkPort = getAvailablePort(opts.zkPort).toString
  
        // Setup the zookeeper config.
        var zkProps = new java.util.Properties
        zkProps.setProperty("tickTime", "2000")
        zkProps.setProperty("dataDir", iPath + "/zookeeper/data")
        zkProps.setProperty("clientPort", opts.zkPort)
        savePropFile(zkProps, "zoo.cfg" ,iPath + "/zookeeper/conf")
        
        // Start Zookeeper
        println("Auto-Starting Zookeeper at " + opts.zkLocation + ":" + opts.zkPort)
        _exec.execute(new Runnable() {
          override def run() = {
            val result =  s"$tmpZKPath/bin/zkServer.sh start".!
          }
        })
      }
      
      // Check to see if Kafka is needed to be started
      if (opts.needKafka) {
 
        opts.kafkaPort = getAvailablePort(opts.kafkaPort).toString
        var kprops = new java.util.Properties
        var kpFile: String = "/tmp/kafka_2.10-0.8.1.1/config/server.properties"       
        try {
          // Override the Kafka Configuration with our default parameters..
          // and save it in the 
          kprops.load(new java.io.FileInputStream(kpFile))
          kprops.setProperty("log.dirs", iPath + "/kafka/logs")
          kprops.setProperty("port", opts.kafkaPort.toString)
          kprops.setProperty("zookeeper.connect", "localhost:" + opts.zkPort)       
          savePropFile(kprops, "server.properties" ,iPath + "/kafka/conf")       
        } catch { 
          //TODO:  Handle this nicely.
          case e: Exception => e.printStackTrace()
        }  
   
        println("Auto-Starting Kafka at " + opts.kafkaLocation + ":" + opts.kafkaPort)  
        _exec.execute(new Runnable() {
          override def run() = {
            
           val buffer = new StringBuffer() 
           val kafkaCommand = Seq("sh","-c",s"$tmpKPath/bin/kafka-server-start.sh $iPath/kafka/conf/server.properties  > /tmp/mylog")
         //  val res: Int = Process(kafkaCommand).!
         //  val cmd =  s"$tmpKPath/bin/kafka-server-start.sh $iPath/kafka/conf/server.properties"
           val lines = kafkaCommand lines_! ProcessLogger(buffer append _)    
          }
        })
      }         
      
      // Give porcesses time to start upl
      Thread.sleep(3000)
      
      //go into a loop listening for commands.
      while (isRunning) {  
        println("...")
        println("Ready to process commands")
        val input = getLine()
        if (input.equalsIgnoreCase(this.EXIT)) isRunning = false else exectuteShellCommand(input)
      }
      
      // cleanup and bail
      cleanup
      return
    } catch {
      case e: Exception => {
          e.printStackTrace()
          cleanup
          sys.exit
        }
      case t: Throwable => {
        t.printStackTrace()
      }
    }
    
  }
  
  /**
   * Process incoming command from shell
   * @param incmd String
   */
  private def exectuteShellCommand(incmd: String): Unit = {

    
  }
 
  private def createNewNodeStructure: Unit = {
    // Create the Root Node Directory.
    var topDir: File = new File(iPath)
    var isSuccess = topDir.mkdir

    // Create zookeeper dir for this node
    var zkDir = new File(iPath + "/zookeeper" )
    isSuccess = zkDir.mkdir
    var zkData = new File(iPath + "/zookeeper/data" )
    isSuccess = zkData.mkdir
    var zkConfig = new File(iPath + "/zookeeper/conf" )
    isSuccess = zkConfig.mkdir
    
    // Create kafka dir for this node
    var kafkaDir = new File(iPath + "/kafka" )
    isSuccess = kafkaDir.mkdir
    var kData = new File(iPath + "/kafka/logs" )
    isSuccess = kData.mkdir
    var kConfig = new File(iPath + "/kafka/conf" )
    isSuccess = kConfig.mkdir 
     
    // Create config dir
    var configDir = new File(iPath + "/config" )
    isSuccess = configDir.mkdir
  }
  
  /**
   *  Scan the next 100 ports
   */
  private def getAvailablePort(port: String): Int = {
    var finalPort = port.toInt
    var stopSearch = false
    var socket: java.net.Socket = null
    
    // Scan until you find the open port or you run out 100 ports.
    while (!stopSearch && finalPort < port.toInt + 100) {
      try {   
        // A little weird, but if this does not cause an exception,
        // that means that the port is open, and being used by someone.
        socket = new java.net.Socket("localhost", finalPort)
        socket.close
        finalPort = finalPort + 1 
      } catch {
        case e: Exception => {
          // Port is not opened.. Use it!
          if (socket != null) socket.close
          stopSearch = true
        }
      }           
    }
    if (stopSearch) return finalPort else return -1
  }
   
  /**
   * save a file to a given directory.  Used to create Zookeepr and Kafka  Configs to
   * start internal processes.
   */
  private def savePropFile(props: java.util.Properties, fname:String, path: String) : Unit = {
    var cfile: File = new File(path +"/"+fname)
    var fileOut = new java.io.FileOutputStream(cfile)
    props.store(fileOut, "Kamanja Generated Configuration file")
    fileOut.close
  }
  
  
  /**
   * 
   */
  private def parseInput(args: Array[String]): Unit = {
    
    // Second element is ALWAYS a NAME if it is provided.
    if (args.size < 2) iName = getUserInput(args, this.PROMPT_FOR_NAME)  else iName = args(1)
 
    // third element is ALWAYS a PATH to installation if it is provided.
    if (args.size < 3) iPath = getUserInput(args, this.PROMPT_FOR_PATH)  else iPath = args(2)
    
    // fourth element is ALWAYS a Global Port if it is provided.
    if (args.size < 4) iPort = getUserInput(args, this.PROMPT_FOR_PORT)  else iPort = args(3)
    
    // All the rest of the arguments are part of the OPTIONS and need will be processed later.
    if (args.size >= 4)
      iOptions = args.slice(4, args.length - 1)
    
  }
  
  /**
   * 
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
    print("Kamanja Instance: " + iName + ">")
    val t = readLine()
    t
  }
  
  /**
   * Show some information.
   */
  private def showInstanceParameters: Unit = {
    println("Kamanja " + {if (isNode) "Node" else "Cluster"} + " is running with the following parameters:")
    println("Name -> " + iName)
    println("Path -> " + iPath)
    println("Port -> " + iPort)
    if (iOptions.size > 0)
      println("Options -> "+ iOptions.mkString(" "))
    else 
      println("No Options sepcified")
  }
  
  /** 
   * Cleanup - stop the Zookeeper, Kafka, Kamanja Runtime, and Kamanja Metadata 
   */
  private def cleanup: Unit = {
    println("Stopping ZooKeeper")
    val stopZK =  s"$tmpZKPath/bin/zkServer.sh stop".! 
    
    println("Stoping KAFKA ")
    val cmd2 = Seq("sh","-c","ps ax | grep -i 'kafka\\.Kafka' | grep java | grep -v grep | awk '{print $1}' | xargs kill -SIGKILL")
    val mvCmdRc : Int = Process(cmd2).!
    
    if (_exec != null)
      _exec.shutdown 
  }
  
}