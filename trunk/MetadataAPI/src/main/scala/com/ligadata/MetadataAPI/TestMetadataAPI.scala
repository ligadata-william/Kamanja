package com.ligadata.MetadataAPI

import java.io.{ByteArrayOutputStream, _}
import java.util

import com.datastax.driver.core.Cluster
import com.esotericsoftware.kryo.io.{Input, Output}
import com.ligadata.Serialize._
import com.ligadata.ZooKeeper._
import com.ligadata.keyvaluestore._
import com.ligadata.fatafat.metadata._
import com.ligadata.fatafat.metadataload.MetadataLoad
import com.twitter.chill.ScalaKryoInstantiator
import org.apache.log4j._
import org.apache.zookeeper.CreateMode
import org.scalatest.Assertions._

import scala.collection.mutable.ArrayBuffer
import scala.io._
import java.util.{Properties, Date}

case class MissingArgumentException(e: String) extends Throwable(e)

object TestMetadataAPI {

  private case class Key(namespace:String, name:String, version:String)
  private type OptionMap = Map[Symbol, Any]
  private val logger = Logger.getLogger(this.getClass)
  logger.setLevel(Level.OFF)

  private var containerFilesDir:String = _
  private var messageFilesDir:String = _
  private var modelFilesDir:String = _
  private var functionFilesDir:String = _
  private var typeFilesDir:String = _
  private var conceptFilesDir:String = _
  private var configFilesDir:String = _
  private var jarTargetDir:String = _

  def main(args: Array[String]){
    try{
      var myConfigFile:String = null
      if (args.length == 0) {
        println("Config File must be supplied, pass a config file as a command line argument:  --config /path/to/your/MetadataAPIConfig.properties")
        return
      }
      else{
        val options = nextOption(Map(), args.toList)
        val cfgfile = options.getOrElse('config, null)
        if (cfgfile == null) {
          logger.error("Need configuration file as parameter")
          println("Config File must be supplied, pass a config file as a command line argument:  --config /path/to/your/MetadataAPIConfig.properties")
          return
        }
        myConfigFile = cfgfile.asInstanceOf[String]
      }
      MetadataAPIImpl.InitMdMgrFromBootStrap(myConfigFile)
      initMetadataDirectories(myConfigFile)
      StartTest
    }catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    finally{
      MetadataAPIImpl.shutdown
    }
  }

  private def StartTest{
    try{
      val addModel = ()                   => { add("model") }
      val getModel = ()                   => { get("model") }
      val removeModel = ()                => { remove("model") }
      val updateModel = ()                => { update("model") }
      val activateModel = ()              => { activate("model") }
      val deactivateModel = ()            => { deactivate("model")}
      val addMessage = ()                 => { add("message") }
      val getMessage = ()                 => { get("message")}
      val removeMessage = ()              => { remove("message") }
      val updateMessage = ()              => { update("message") }
      val addContainer = ()               => { add("container") }
      val getContainer = ()               => { get("container") }
      val removeContainer = ()            => { remove("container") }
      val updateContainer = ()            => { update("container") }
      val addFunction         = ()        => { add("function") }
      val getFunction     = ()            => { get("function") }
      val removeFunction = ()             => { remove("function") }
      val updateFunction = ()             => { update("function") }
      val uploadJarFile = ()              => { add("jar") }
      val uploadEngineConfig = ()         => { add("config") }
      val dumpAllCfgObjects = ()          => { get("config") }

      val topLevelMenu = List(
        ("Add Model",addModel),
        ("Get Model",getModel),
        ("Remove Model",removeModel),
        ("Update Model",updateModel),
        ("Activate Model",activateModel),
        ("Deactivate Model",deactivateModel),
        ("Add Message",addMessage),
        ("Get Message",getMessage),
        ("Remove Message",removeMessage),
        ("Update Message", updateMessage),
        ("Add Container",addContainer),
        ("Get Container",getContainer),
        ("Remove Container",removeContainer),
        ("Update Container", updateContainer),
        ("Add Function",addFunction),
        ("Get Function", getFunction),
        ("Remove Function", removeFunction),
        ("Update Function", updateFunction),
        ("Add Jar",uploadJarFile),
        ("Add Cluster Configuration",uploadEngineConfig),
        ("Get Cluster Configuration",dumpAllCfgObjects))

      var done = false
      while ( done == false ){
        println("\n\nPick an API ")
        for((key,idx) <- topLevelMenu.zipWithIndex){println("[" + (idx+1) + "] " + key._1)}
        println("[" + (topLevelMenu.size+1) + "] Exit")
        print("\nEnter your choice: ")
        val choice:Int = readInt()
        if( choice <= topLevelMenu.size ){
          topLevelMenu(choice-1)._2.apply
        }
        else if( choice == topLevelMenu.size + 1 ){
          done = true
        }
        else{
          logger.error("Invalid Choice : " + choice)
        }
      }
    }catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  private def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--config" :: value :: tail =>
        nextOption(map ++ Map('config -> value), tail)
      case option :: tail => {
        logger.error("Unknown option " + option)
        sys.exit(1)
      }
    }
  }

  private def initMetadataDirectories(metadataConfigFile: String) : Unit = {
    val props: Properties = new Properties()
    var dirsNotFound = List[String]()
    props.load(new FileInputStream(new File(metadataConfigFile)))
    containerFilesDir = props.getProperty("CONTAINER_FILES_DIR")
    messageFilesDir = props.getProperty("MESSAGE_FILES_DIR")
    modelFilesDir = props.getProperty("MODEL_FILES_DIR")
    conceptFilesDir = props.getProperty("CONCEPT_FILES_DIR")
    functionFilesDir = props.getProperty("FUNCTION_FILES_DIR")
    typeFilesDir = props.getProperty("TYPE_FILES_DIR")
    configFilesDir = props.getProperty("CONFIG_FILES_DIR")
    jarTargetDir = props.getProperty("JAR_TARGET_DIR")

    if(!isValidDir(containerFilesDir)) {
      dirsNotFound = dirsNotFound :+ containerFilesDir
    }
    if(!isValidDir(messageFilesDir)) {
      dirsNotFound = dirsNotFound :+ messageFilesDir
    }
    if(!isValidDir(modelFilesDir)) {
      dirsNotFound = dirsNotFound :+ modelFilesDir
    }
    if(!isValidDir(functionFilesDir)) {
      dirsNotFound = dirsNotFound :+ functionFilesDir
    }
    if(!isValidDir(conceptFilesDir)) {
      dirsNotFound = dirsNotFound :+ conceptFilesDir
    }
    if(!isValidDir(typeFilesDir)) {
      dirsNotFound = dirsNotFound :+ typeFilesDir
    }
    if(!isValidDir(configFilesDir)) {
      dirsNotFound = dirsNotFound :+ configFilesDir
    }
    if(!isValidDir(jarTargetDir)) {
      dirsNotFound = dirsNotFound :+ jarTargetDir
    }

    if(dirsNotFound.length > 0) {
      logger.warn("The following metadata files paths were either not found or aren't directories:")
      dirsNotFound.foreach(f => {
        logger.warn("\t" + f)
      })
    }
  }

  private def isValidDir(dirName:String) : Boolean = {
    val iFile = new File(dirName)
    if (!iFile.exists && !iFile.isDirectory) {
      return false
    }
    else
     return true
  }

  private def listFilesInDir(directory: String, format:String): List[File] = {
    var files:List[File] = List[File]()
    format.toLowerCase() match {
      case "json" =>
        files = new File(directory).listFiles.filter(_.getName.endsWith(".json")).toList
      case "xml" =>
        files = new File(directory).listFiles.filter(_.getName.endsWith(".xml")).toList
      case "jar" =>
        files = new File(directory).listFiles.filter(_.getName.endsWith(".jar")).toList
      case _ => throw new IllegalArgumentException("format '" + format + "' is not accepted. Valid formats include 'json' and 'xml'.")
    }

    if (files.length == 0) {
      println("No files of format '" + format + "' were found in directory '" + directory + "'.")
      return List[File]()
    }

    var fileCount = 0
    files.foreach(file => {
      fileCount += 1
      println("[" + fileCount + "] " + file)
    })

    fileCount += 1
    println("[" + fileCount + "] Main Menu")

    return files
  }

  private def listKeys(metadataType:String, active:Boolean = true): List[String] = {
    var keys:List[String] = List[String]()
    metadataType.toLowerCase match {
      case "container" => keys = MetadataAPIImpl.GetAllContainersFromCache(active).toList
      case "message" => keys = MetadataAPIImpl.GetAllMessagesFromCache(active).toList
      case "model" => keys = MetadataAPIImpl.GetAllModelsFromCache(active).toList
      case "function" => keys = MetadataAPIImpl.GetAllFunctionsFromCache(active).toList
      case "type" => keys = MetadataAPIImpl.GetAllTypesFromCache(active).toList
      case "concept" => keys = MetadataAPIImpl.GetAllConceptsFromCache(active).toList
      case _ => throw new IllegalArgumentException("Invalid metadata type '" + metadataType + "'. Valid types are:\n\tcontainer\n\tmessage\n\tmodel\n\tfunction\n\ttype\n\tconcept")
    }

    if(keys.length == 0) {
      println("No '" + metadataType + "' keys were found in metadata")
      return List[String]()
    }

    var keyCount = 0
    keys.foreach(key => {
      keyCount += 1
      println("[" + keyCount + "] " + key)
    })

    keyCount += 1
    println("[" + keyCount + "] Main Menu")

    return keys
  }

  private def selectChoices(): List[Int] = {
    print("Please enter a number corresponding to your choice: ")
    val choice = readLine()
    print("\n")
    var valid = true
    var choices:List[Int] = List[Int]()

    var validChoices = List[Int]()

    try {
      choices = choice.filter(_ != '\n').split(',').filter(ch => (ch != null && ch != "")).map(_.trim.toInt).toList
    }
    catch {
      case _:Throwable => valid = false
    }

    choices.foreach(ch => {
      if(valid) {
        validChoices = validChoices :+ ch
      }
      else {
        println("Choice '" + ch + "' is invalid and has been discarded.")
      }
    })

    return validChoices
  }

  private def selectFiles(directory:String, format:String): List[String] = {
    val files = listFilesInDir(directory, format)
    if(files.length == 0) return List[String]()
    val choices = selectChoices
    var selectedFiles:List[String] = List[String]()

    choices.foreach(choice => {
      if(choice < 0 || choice > files.length + 1) {
        println("Invalid choice. Returning to main menu.")
        return List[String]()
      }
      else if(choice == files.length + 1) {
        println("Returning to main menu.")
        return List[String]()
      }
      else {
        selectedFiles = selectedFiles :+ files(choice - 1).getAbsolutePath
      }
    })

    selectedFiles
  }

  private def selectKeys(metadataType:String, active:Boolean = true): List[Key] = {
    val keys = listKeys(metadataType, active)
    if(keys.length == 0) return List[Key]()
    val choices = selectChoices
    var selectedKeys:List[String] = List[String]()
    var finalKeys:List[Key] = List[Key]()

    choices.foreach(choice => {
      if(choice < 0 || choice > keys.length + 1) {
        println("Invalid choice. Returning to main menu.")
        return List[Key]()
      }
      else if(choice == keys.length + 1) {
        println("Returning to main menu.")
        return List[Key]()
      }
      else {
        selectedKeys = selectedKeys :+ keys(choice -1)
      }
    })

    selectedKeys.foreach(key => {
      val splitKey = key.split("\\.")
      val keyNamespace = splitKey(0)
      val keyName = splitKey(1)
      val keyVersion = splitKey(2)
      finalKeys = finalKeys :+ new Key(keyNamespace, keyName, keyVersion)
    })

    return finalKeys
  }

  def add(metadataType: String): Unit = {
    var files: List[String] = List[String]()
    //var results:ArrayBuffer[(String,String,String)] = ArrayBuffer[(String,String,String)]()
    var results:List[String] = List[String]()

    metadataType.toLowerCase match {
      case "container" => {
        files = selectFiles(containerFilesDir, "json")
        if(files.length == 0) return
        files.foreach(file => {
          results = results :+ MetadataAPIImpl.AddContainer(Source.fromFile(file).mkString)
        })
      }
      case "message" => {
        files = selectFiles(messageFilesDir, "json")
        if(files.length == 0) return
        files.foreach(file => {
          results = results :+ MetadataAPIImpl.AddMessage(Source.fromFile(file).mkString)
        })
      }
      case "model" => {
        files = selectFiles(modelFilesDir, "xml")
        if(files.length == 0) return
        files.foreach(file => {
          results = results :+ MetadataAPIImpl.AddModel(Source.fromFile(file).mkString)
        })
      }
      case "function" => {
        files = selectFiles(functionFilesDir, "json")
        if(files.length == 0) return
        files.foreach(file => {
          results = results :+ MetadataAPIImpl.AddFunctions(Source.fromFile(file).mkString, "JSON")
        })
      }
      case "type" => {
        files = selectFiles(typeFilesDir, "json")
        if(files.length == 0) return
        files.foreach(file => {
          results = results :+ MetadataAPIImpl.AddTypes(Source.fromFile(file).mkString, "JSON")
        })
      }
      case "concept" => {
        files = selectFiles(conceptFilesDir, "json")
        if(files.length == 0) return
        files.foreach(file => {
          results = results :+ MetadataAPIImpl.AddConcepts(Source.fromFile(file).mkString, "JSON")
        })
      }
      case "config" => {
        files = selectFiles(configFilesDir, "json")
        if(files.length == 0) return
        if (files.length > 1) {
          println("Please select only one configuration file.")
          return
        }
        else {
          results = results :+ MetadataAPIImpl.UploadConfig(Source.fromFile(files(0)).mkString)
        }
      }
      case "jar" => {
        files = selectFiles(jarTargetDir, "jar")
        if(files.length == 0) return
        files.foreach(file => {
          results = results :+ MetadataAPIImpl.UploadJar(file)
        })
      }
    }

    if(results.length > 0) {
      println("Results as a json string =>")
      results.foreach(result => {
        println(result + "\n")
      })
    }
  }

  def get(metadataType:String): Unit = {
    var keys: List[Key] = List[Key]()
    if(metadataType.toLowerCase != "config"){
      keys = selectKeys(metadataType)
      if (keys.length == 0) {
        return
      }
    }
    var results: List[String] = List[String]()

    metadataType.toLowerCase match {
      case "container" => {
        keys.foreach(key => {
          results = results :+ MetadataAPIImpl.GetContainerDefFromCache(key.namespace, key.name, "JSON", key.version)
        })
      }
      case "message" => {
        keys.foreach(key => {
          results = results :+ MetadataAPIImpl.GetMessageDefFromCache(key.namespace, key.name, "JSON", key.version)
        })
      }
      case "model" => {
        keys.foreach(key => {
          results = results :+ MetadataAPIImpl.GetModelDefFromCache(key.namespace, key.name, "JSON", key.version)
        })
      }
      case "function" => {
        keys.foreach(key => {
          results = results :+ MetadataAPIImpl.GetFunctionDef(key.namespace, key.name, "JSON", key.version)
        })
      }
      case "type" => {
        keys.foreach(key => {
          results = results :+ MetadataAPIImpl.GetTypeDef(key.namespace, key.name, "JSON", key.version)
        })
      }
      case "concept" => {
        keys.foreach(key => {
          results = results :+ MetadataAPIImpl.GetConceptDef(key.namespace, key.name, "JSON", key.version)
        })
      }
      case "config" => {
        results = results :+ MetadataAPIImpl.GetAllClusterCfgs("JSON")
      }

    }

    if (results.length > 0) {
      println("Results as a json string =>")
      results.foreach(result => {
        println(result + "\n")
      })
    }
  }

  def update(metadataType:String): Unit = {
    var files: List[String] = List[String]()
    var results:List[String] = List[String]()

    metadataType.toLowerCase match {
      case "container" => {
        files = selectFiles(containerFilesDir, "json")
        files.foreach(file => {
          results = results :+ MetadataAPIImpl.UpdateContainer(Source.fromFile(file).mkString)
        })
      }
      case "message" => {
        files = selectFiles(messageFilesDir, "json")
        files.foreach(file => {
          results = results :+ MetadataAPIImpl.UpdateMessage(Source.fromFile(file).mkString)
        })
      }
      case "model" => {
        files = selectFiles(modelFilesDir, "xml")
        files.foreach(file => {
          results = results :+ MetadataAPIImpl.UpdateModel(Source.fromFile(file).mkString)
        })
      }
      case "function" => {
        files = selectFiles(functionFilesDir, "json")
        files.foreach(file => {
          results = results :+ MetadataAPIImpl.UpdateFunctions(Source.fromFile(file).mkString, "JSON")
        })
      }
      case "type" => {
        files = selectFiles(typeFilesDir, "json")
        files.foreach(file => {
          results = results :+ MetadataAPIImpl.UpdateType(Source.fromFile(file).mkString, "JSON")
        })
      }
      case "concept" => {
        files = selectFiles(conceptFilesDir, "json")
        files.foreach(file => {
          results = results :+ MetadataAPIImpl.UpdateConcepts(Source.fromFile(file).mkString, "JSON")
        })
      }
    }

    if(results.length > 0) {
      println("Results as a json string =>")
      results.foreach(result => {
        println(result + "\n")
      })
    }
  }

  def remove(metadataType:String, active:Boolean = true): Unit = {
    var keys: List[Key] = selectKeys(metadataType, active)
    if (keys.length == 0) {
      return
    }
    var results: List[String] = List[String]()

    metadataType.toLowerCase match {
      case "container" => {
        keys.foreach(key => {
          results = results :+ MetadataAPIImpl.RemoveContainer(key.name, key.version.toLong)
        })
      }
      case "message" => {
        keys.foreach(key => {
          results = results :+ MetadataAPIImpl.RemoveMessage(key.name, key.version.toLong)
        })
      }
      case "model" => {
        keys.foreach(key => {
          results = results :+ MetadataAPIImpl.RemoveModel(key.namespace, key.name, key.version.toLong)
        })
      }
      case "function" => {
        keys.foreach(key => {
          results = results :+ MetadataAPIImpl.RemoveFunction(key.namespace, key.name, key.version.toLong)
        })
      }
      case "type" => {
        keys.foreach(key => {
          results = results :+ MetadataAPIImpl.RemoveType(key.namespace, key.name, key.version.toLong)
        })
      }
      case "concept" => {
        keys.foreach(key => {
          results = results :+ MetadataAPIImpl.RemoveConcept(key.namespace, key.name, key.version.toLong)
        })
      }
      case _ => throw new IllegalArgumentException("Metadata type '" + metadataType + "' is not a valid type. Valid types: container, message, model, function, type and concept.")
    }

    if(results.length > 0) {
      println("Results as a json string =>")
      results.foreach(result => {
        println(result + "\n")
      })
    }
  }

  def activate(metadataType:String): Unit = {
    val keys: List[Key] = selectKeys(metadataType, false)
    if (keys.length == 0) {
      return
    }
    var results: List[String] = List[String]()

    metadataType.toLowerCase match {
      case "model" => {
        keys.foreach(key => {
          results = results :+ MetadataAPIImpl.ActivateModel(key.namespace, key.name, key.version.toLong)
        })
      }
      case _ => throw new IllegalArgumentException("Metadata type '" + metadataType + "' is not a valid type for activate. Valid types: model.")
    }

    if (results.length > 0) {
      println("Results as a json string =>")
      results.foreach(reuslts => {
        println(result + "\n")
      })
    }
  }

  def deactivate(metadataType:String): Unit = {
    val keys: List[Key] = selectKeys(metadataType)
    if (keys.length == 0) {
      return
    }
    var results: List[String] = List[String]()

    metadataType.toLowerCase match {
      case "model" => {
        keys.foreach(key => {
          results = results :+ MetadataAPIImpl.DeactivateModel(key.namespace, key.name, key.version.toLong)
        })
      }
      case _ => throw new IllegalArgumentException("Metadata type '" + metadataType + "' is not a valid type for deactivate. Valid types: model.")
    }

    if (results.length > 0) {
      println("Results as a json string =>")
      results.foreach(reuslts => {
        println(result + "\n")
      })
    }
  }
}