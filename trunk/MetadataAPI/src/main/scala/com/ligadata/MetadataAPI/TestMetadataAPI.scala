package com.ligadata.MetadataAPI

import org.scalatest.Assertions._
import scala.collection.mutable.{ArrayBuffer}
import com.ligadata.olep.metadata.ObjType._
import com.ligadata.olep.metadata._
import com.ligadata.olep.metadataload.MetadataLoad

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.ResultSet

import com.ligadata.keyvaluestore._
import com.ligadata.keyvaluestore.cassandra._

import com.ligadata._
import org.apache.log4j._
import java.util.Properties

import java.io._
import scala.io._
import com.ligadata.messagedef._
import com.ligadata.Compiler._

import com.twitter.chill.ScalaKryoInstantiator
import java.io.ByteArrayOutputStream
import com.esotericsoftware.kryo.io.{Input, Output}

import com.ligadata.Serialize._
import com.ligadata.ZooKeeper._
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

case class MissingArgumentException(e: String) extends Throwable(e)

object TestMetadataAPI{

  private type OptionMap = Map[Symbol, Any]

  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)

  var databaseOpen = false

  var serializer = SerializerManager.GetSerializer("kryo")

  def testDbConn{
    var hostnames = "localhost"
    var keyspace = "default"
    var table = "default"
  
    var clusterBuilder = Cluster.builder()
	
    clusterBuilder.addContactPoints(hostnames)
	
    val cluster = clusterBuilder.build()
    val session = cluster.connect(keyspace);
  }

  
  // Type defs
  
  def AddType {
    try {
      var dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("TYPE_FILES_DIR")
      if (dirName == null) {
        dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("GIT_ROOT") + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Types"
        logger.info("The environment variable TYPE_FILES_DIR is undefined. Setting to default " + dirName)
      }
      
      if (!IsValidDir(dirName)) {
        logger.fatal("Invalid Directory " + dirName)
        return
      }
      val typFiles = new java.io.File(dirName).listFiles.filter(_.getName.endsWith(".json"))
      if (typFiles.length == 0) {
        logger.fatal("No json type files exist in the directory " + dirName)
        return
      }
      
      println("\nSelect a Type Definition file:\n")
      
      var seq = 0
      typFiles.foreach(key => { seq += 1; println("[" + seq + "]" + key)})
      seq += 1
      println("[" + seq + "] Main Menu")
      
      print("\nEnter your choice: ")
      val choice:Int = readInt()
      
      if (choice == typFiles.length +1) {
        return
      }
      
      if (choice < 1 || choice > typFiles.length + 1) {
        logger.fatal("Invalid Choice: " + choice)
        return
      }
      
      val typDefFile = typFiles(choice - 1).toString
      
      logger.setLevel(Level.TRACE);
      val typStr = Source.fromFile(typDefFile).mkString
      MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
      println("Results as json string => \n" + MetadataAPIImpl.AddType(typStr, "JSON"))
    }
    catch {
      case e: AlreadyExistsException => {
        logger.error("Container already exists in metadata...")
      }
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def GetType {
    try{
      logger.setLevel(Level.TRACE);

      val typKeys = MetadataAPIImpl.GetAllKeys("TypeDef")
      if( typKeys.length == 0 ){
	      println("Sorry, No types available in the Metadata")
	      return
      }

      println("\nPick the type to be presented from the following list: ")
      var seq = 0
      typKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if ( choice < 1 || choice > typKeys.length ){
        println("Invalid choice " + choice + ",start with main menu...")
	      return
      }

      val typKey = typKeys(choice-1)

      val typKeyTokens = typKey.split("\\.")
      val typNameSpace = typKeyTokens(0)
      val typName = typKeyTokens(1)
      val typVersion = typKeyTokens(2)
      val typOpt = MetadataAPIImpl.GetType(typNameSpace,typName,typVersion,"JSON")
      
      typOpt match {
        case None => None
        case Some(ts) => 
          val apiResult = new ApiResult(0,"Successfully fetched typeDef",JsonSerializer.SerializeObjectToJson(ts)).toString()
          val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
          println("Result as Json String => \n" + resultData)
      }
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }
  
  def GetAllTypes{
    val apiResult = MetadataAPIImpl.GetAllTypes("JSON")
    val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
    println("Result as Json String => \n" + resultData)
    
  }

  def RemoveType {
    val loggerName = this.getClass.getName
    lazy val logger = Logger.getLogger(loggerName)
    
    try {
      logger.setLevel(Level.TRACE);

      val typKeys = MetadataAPIImpl.GetAllKeys("TypeDef")
      if( typKeys.length == 0 ){
        println("Sorry, there are no types available in the Metadata")
	      return
      }

      println("\nPick the Type to be deleted from the following list: ")
      var seq = 0
      typKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if ( choice < 1 || choice > typKeys.length ){
        println("Invalid choice " + choice + ",start with main menu...")
        return
      }

      val typKey = typKeys(choice-1)
      val typKeyTokens = typKey.split("\\.")
      val typNameSpace = typKeyTokens(0)
      val typName = typKeyTokens(1)
      val typVersion = typKeyTokens(2)
      val apiResult = MetadataAPIImpl.RemoveType(typNameSpace,typName,typVersion.toInt)

      val (statusCode1,resultData1) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + apiResult)
      
    }
    catch {
      case e: Exception => {
	    e.printStackTrace()
      }
    }
  }
  
  // End Type defs
  
  // TODO: Rewrite Update Type to allow a user to pick the file they wish to update a type from.
  def UpdateType = {
    val apiResult = MetadataAPIImpl.UpdateType(SampleData.sampleNewScalarTypeStr,"JSON")
    val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
    println("Result as Json String => \n" + resultData)
  }


  def AddFunction = {
    var apiResult = MetadataAPIImpl.AddFunction(SampleData.sampleFunctionStr,"JSON")
    var result = MetadataAPIImpl.getApiResult(apiResult)
    println("Result as Json String => \n" + result._2)
  }

  def RemoveFunction = {
    val apiResult = MetadataAPIImpl.RemoveFunction(MdMgr.sysNS,"my_min",100)
    val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
    println("Result as Json String => \n" + resultData)
  }

  def UpdateFunction = {
    val apiResult = MetadataAPIImpl.UpdateFunctions(SampleData.sampleFunctionStr,"JSON")
    val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
    println("Result as Json String => \n" + resultData)
  }

  def AddConcept = {
    var apiResult = MetadataAPIImpl.AddConcepts(SampleData.sampleConceptStr,"JSON")
    var result = MetadataAPIImpl.getApiResult(apiResult)
    println("Result as Json String => \n" + result._2)
  }

  def RemoveConcept = {
    val apiResult = MetadataAPIImpl.RemoveConcept("Ligadata.ProviderId.100")
    val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
    println("Result as Json String => \n" + resultData)
  }

  def RemoveConcepts = {
    val apiResult = MetadataAPIImpl.RemoveConcepts(Array("Ligadata.ProviderId.100"))
    val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
    println("Result as Json String => \n" + resultData)
  }

  def UpdateConcept = {
    val apiResult = MetadataAPIImpl.UpdateConcepts(SampleData.sampleConceptStr,"JSON")
    val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
    println("Result as Json String => \n" + resultData)
  }

  def AddDerivedConcept = {
    var apiResult = MetadataAPIImpl.AddDerivedConcept(SampleData.sampleDerivedConceptStr,"JSON")
    var result = MetadataAPIImpl.getApiResult(apiResult)
    println("Result as Json String => \n" + result._2)
  }

  def fileToString(filePath: String) :String = {
    val file = new java.io.File(filePath)
    val inStream = new FileInputStream(file)
    val outStream = new ByteArrayOutputStream
    try {
      var reading = true
      while ( reading ) {
	inStream.read() match {
          case -1 => reading = false
          case c => outStream.write(c)
	}
      }
      outStream.flush()
    }
    finally {
      inStream.close()
    }
    val pmmlStr =  new String(outStream.toByteArray())
    logger.trace(pmmlStr)
    pmmlStr
  }

  def GetMessage{
    try{
      logger.setLevel(Level.TRACE);

      val msgKeys = MetadataAPIImpl.GetAllKeys("MessageDef")

      if( msgKeys.length == 0 ){
	println("Sorry, No messages available in the Metadata")
	return
      }

      println("\nPick the message to be presented from the following list: ")

      var seq = 0
      msgKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if ( choice < 1 || choice > msgKeys.length ){
	println("Invalid choice " + choice + ",start with main menu...")
	return
      }
      val msgKey = msgKeys(choice-1)

      val msgKeyTokens = msgKey.split("\\.")
      val msgNameSpace = msgKeyTokens(0)
      val msgName = msgKeyTokens(1)
      val msgVersion = msgKeyTokens(2)

      val depModels = MetadataAPIImpl.GetDependentModels(msgNameSpace,msgName,msgVersion.toInt)
      logger.trace("DependentModels => " + depModels)
	
      val apiResult = MetadataAPIImpl.GetMessageDef(msgNameSpace,msgName,"JSON",msgVersion)

      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + resultData)
      
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def GetMessageFromCache{
    try{
      logger.setLevel(Level.TRACE);

      val msgKeys = MetadataAPIImpl.GetAllMessagesFromCache

      if( msgKeys.length == 0 ){
	println("Sorry, No messages available in the Metadata")
	return
      }

      println("\nPick the message to be presented from the following list: ")

      var seq = 0
      msgKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if ( choice < 1 || choice > msgKeys.length ){
	println("Invalid choice " + choice + ",start with main menu...")
	return
      }
      val msgKey = msgKeys(choice-1)

      val msgKeyTokens = msgKey.split("\\.")
      val msgNameSpace = msgKeyTokens(0)
      val msgName = msgKeyTokens(1)
      val msgVersion = msgKeyTokens(2)

      val depModels = MetadataAPIImpl.GetDependentModels(msgNameSpace,msgName,msgVersion.toInt)
      if( depModels.length > 0 ){
	depModels.foreach(mod => {
	  logger.trace("DependentModel => " + mod.FullNameWithVer)
	})
      }

      val apiResult = MetadataAPIImpl.GetMessageDefFromCache(msgNameSpace,msgName,"JSON",msgVersion)

      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + resultData)
      
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def GetContainerFromCache{
    try{
      logger.setLevel(Level.TRACE);

      val contKeys = MetadataAPIImpl.GetAllContainersFromCache

      if( contKeys.length == 0 ){
	println("Sorry, No containers available in the Metadata")
	return
      }

      println("\nPick the container to be presented from the following list: ")

      var seq = 0
      contKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if ( choice < 1 || choice > contKeys.length ){
	println("Invalid choice " + choice + ",start with main menu...")
	return
      }
      val contKey = contKeys(choice-1)

      val contKeyTokens = contKey.split("\\.")
      val contNameSpace = contKeyTokens(0)
      val contName = contKeyTokens(1)
      val contVersion = contKeyTokens(2)
      val apiResult = MetadataAPIImpl.GetContainerDefFromCache(contNameSpace,contName,"JSON",contVersion)

      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + resultData)
      
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def GetModel{
    val loggerName = this.getClass.getName
    lazy val logger = Logger.getLogger(loggerName)

    try{
      logger.setLevel(Level.TRACE);

      val modKeys = MetadataAPIImpl.GetAllKeys("ModelDef")
      if( modKeys.length == 0 ){
	println("Sorry, No models available in the Metadata")
	return
      }

      println("\nPick the model to be presented from the following list: ")
      var seq = 0
      modKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if ( choice < 1 || choice > modKeys.length ){
	println("Invalid choice " + choice + ",start with main menu...")
	return
      }

      val modKey = modKeys(choice-1)

      val modKeyTokens = modKey.split("\\.")
      val modNameSpace = modKeyTokens(0)
      val modName = modKeyTokens(1)
      val modVersion = modKeyTokens(2)
      val apiResult = MetadataAPIImpl.GetModelDef(modNameSpace,modName,"JSON",modVersion)

      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + resultData)

    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def GetModelFromCache{
    val loggerName = this.getClass.getName
    lazy val logger = Logger.getLogger(loggerName)

    try{
      logger.setLevel(Level.TRACE);

      val modKeys = MetadataAPIImpl.GetAllModelsFromCache(true)
      if( modKeys.length == 0 ){
	println("Sorry, No models available in the Metadata")
	return
      }

      println("\nPick the model to be presented from the following list: ")
      var seq = 0
      modKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if ( choice < 1 || choice > modKeys.length ){
	println("Invalid choice " + choice + ",start with main menu...")
	return
      }

      val modKey = modKeys(choice-1)

      val modKeyTokens = modKey.split("\\.")
      val modNameSpace = modKeyTokens(0)
      val modName = modKeyTokens(1)
      val modVersion = modKeyTokens(2)
      val apiResult = MetadataAPIImpl.GetModelDefFromCache(modNameSpace,modName,"JSON",modVersion)

      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + resultData)

    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def RemoveMessageFromStore{
    try{
      logger.setLevel(Level.TRACE);

      val msgKeys = MetadataAPIImpl.GetAllKeys("MessageDef")

      if( msgKeys.length == 0 ){
	println("Sorry, No messages available in the Metadata")
	return
      }

      println("\nPick the message to be deleted from the following list: ")
      var seq = 0
      msgKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if ( choice < 1 || choice > msgKeys.length ){
	println("Invalid choice " + choice + ",start with main menu...")
	return
      }

      val msgKey = msgKeys(choice-1)

      val msgKeyTokens = msgKey.split("\\.")
      val msgNameSpace = msgKeyTokens(0)
      val msgName = msgKeyTokens(1)
      val msgVersion = msgKeyTokens(2)
      val apiResult = MetadataAPIImpl.RemoveMessage(msgNameSpace,msgName,msgVersion.toInt)

      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + apiResult)
      
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def RemoveContainer{
    try{
      logger.setLevel(Level.TRACE);

      val contKeys = MetadataAPIImpl.GetAllContainersFromCache

      if( contKeys.length == 0 ){
	println("Sorry, No containers available in the Metadata")
	return
      }

      println("\nPick the container to be deleted from the following list: ")
      var seq = 0
      contKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if ( choice < 1 || choice > contKeys.length ){
	println("Invalid choice " + choice + ",start with main menu...")
	return
      }

      val contKey = contKeys(choice-1)

      val contKeyTokens = contKey.split("\\.")
      val contNameSpace = contKeyTokens(0)
      val contName = contKeyTokens(1)
      val contVersion = contKeyTokens(2)
      val apiResult = MetadataAPIImpl.RemoveContainer(contNameSpace,contName,contVersion.toInt)

      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + apiResult)
      
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def RemoveMessage{
    try{
      logger.setLevel(Level.TRACE);

      val msgKeys = MetadataAPIImpl.GetAllMessagesFromCache

      if( msgKeys.length == 0 ){
	println("Sorry, No messages available in the Metadata")
	return
      }

      println("\nPick the message to be deleted from the following list: ")
      var seq = 0
      msgKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if ( choice < 1 || choice > msgKeys.length ){
	println("Invalid choice " + choice + ",start with main menu...")
	return
      }

      val msgKey = msgKeys(choice-1)

      val msgKeyTokens = msgKey.split("\\.")
      val msgNameSpace = msgKeyTokens(0)
      val msgName = msgKeyTokens(1)
      val msgVersion = msgKeyTokens(2)
      val apiResult = MetadataAPIImpl.RemoveMessage(msgNameSpace,msgName,msgVersion.toInt)

      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + apiResult)
      
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def RemoveModel{
    try{
      logger.setLevel(Level.TRACE);

      val modKeys = MetadataAPIImpl.GetAllModelsFromCache(true)

      if( modKeys.length == 0 ){
	println("Sorry, No models available in the Metadata")
	return
      }

      println("\nPick the model to be deleted from the following list: ")
      var seq = 0
      modKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if ( choice < 1 || choice > modKeys.length ){
	println("Invalid choice " + choice + ",start with main menu...")
	return
      }

      val modKey = modKeys(choice-1)
      val modKeyTokens = modKey.split("\\.")
      val modNameSpace = modKeyTokens(0)
      val modName = modKeyTokens(1)
      val modVersion = modKeyTokens(2)
      val apiResult = MetadataAPIImpl.RemoveModel(modNameSpace,modName,modVersion.toInt)

      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + apiResult)
      
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def DeactivateModel{
    try{
      logger.setLevel(Level.TRACE);

      val modKeys = MetadataAPIImpl.GetAllModelsFromCache(true)

      if( modKeys.length == 0 ){
	println("Sorry, No models available in the Metadata")
	return
      }

      println("\nPick the model to be deleted from the following list: ")
      var seq = 0
      modKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if ( choice < 1 || choice > modKeys.length ){
	println("Invalid choice " + choice + ",start with main menu...")
	return
      }

      val modKey = modKeys(choice-1)
      val modKeyTokens = modKey.split("\\.")
      val modNameSpace = modKeyTokens(0)
      val modName = modKeyTokens(1)
      val modVersion = modKeyTokens(2)
      val apiResult = MetadataAPIImpl.DeactivateModel(modNameSpace,modName,modVersion.toInt)

      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + apiResult)
      
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def ActivateModel{
    try{
      logger.setLevel(Level.TRACE);

      val modKeys = MetadataAPIImpl.GetAllModelsFromCache(false)

      if( modKeys.length == 0 ){
	println("Sorry, No models available in the Metadata")
	return
      }

      println("\nPick the model to be deleted from the following list: ")
      var seq = 0
      modKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if ( choice < 1 || choice > modKeys.length ){
	println("Invalid choice " + choice + ",start with main menu...")
	return
      }

      val modKey = modKeys(choice-1)
      val modKeyTokens = modKey.split("\\.")
      val modNameSpace = modKeyTokens(0)
      val modName = modKeyTokens(1)
      val modVersion = modKeyTokens(2)
      val apiResult = MetadataAPIImpl.ActivateModel(modNameSpace,modName,modVersion.toInt)

      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + apiResult)
      
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def RemoveModelFromCache{
    try{
      logger.setLevel(Level.TRACE);

      val modKeys = MetadataAPIImpl.GetAllModelsFromCache(true)

      if( modKeys.length == 0 ){
	println("Sorry, No models available in the Metadata")
	return
      }

      println("\nPick the model to be deleted from the following list: ")
      var seq = 0
      modKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if ( choice < 1 || choice > modKeys.length ){
	println("Invalid choice " + choice + ",start with main menu...")
	return
      }

      val modKey = modKeys(choice-1)
      val modKeyTokens = modKey.split("\\.")
      val modNameSpace = modKeyTokens(0)
      val modName = modKeyTokens(1)
      val modVersion = modKeyTokens(2)
      val apiResult = MetadataAPIImpl.RemoveModel(modNameSpace,modName,modVersion.toInt)

      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + apiResult)
      
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def GetAllMessagesFromStore{
    try{
      logger.setLevel(Level.TRACE);
      val msgKeys = MetadataAPIImpl.GetAllKeys("MessageDef")
      if( msgKeys.length == 0 ){
	println("Sorry, No messages available in the Metadata")
	return
      }
      var seq = 0
      msgKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def GetAllModelsFromCache{
    try{
      logger.setLevel(Level.TRACE);
      val modKeys = MetadataAPIImpl.GetAllModelsFromCache(true)
      if( modKeys.length == 0 ){
	println("Sorry, No models available in the Metadata")
	return
      }

      var seq = 0
      modKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def GetAllMessagesFromCache{
    try{
      logger.setLevel(Level.TRACE);
      val msgKeys = MetadataAPIImpl.GetAllMessagesFromCache
      if( msgKeys.length == 0 ){
	println("Sorry, No messages are available in the Metadata")
	return
      }

      var seq = 0
      msgKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def GetAllContainersFromCache{
    try{
      logger.setLevel(Level.TRACE);
      val msgKeys = MetadataAPIImpl.GetAllContainersFromCache
      if( msgKeys.length == 0 ){
	println("Sorry, No containers are available in the Metadata")
	return
      }

      var seq = 0
      msgKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def GetAllModelsFromStore{
    try{
      logger.setLevel(Level.TRACE);
      val modKeys = MetadataAPIImpl.GetAllKeys("ModelDef")
      if( modKeys.length == 0 ){
	println("Sorry, No models are available in the Metadata")
	return
      }

      var seq = 0
      modKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key)})
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def AddContainer{
    try{
      var dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("CONTAINER_FILES_DIR")
      if ( dirName == null  ){
		dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("GIT_ROOT") + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Containers"
		logger.info("The environment variable CONTAINER_FILES_DIR is undefined, The directory defaults to " + dirName)
      }

      if ( ! IsValidDir(dirName) ){
		logger.fatal("Invalid Directory " + dirName)
		return
      }
      val contFiles = new java.io.File(dirName).listFiles.filter(_.getName.endsWith(".json"))
      if ( contFiles.length == 0 ){
		logger.fatal("No json container files in the directory " + dirName)
		return
      }

      println("\nPick a Container Definition file(s) from below choices\n")

      var seq = 0
      contFiles.foreach(key => { seq += 1; println("[" + seq + "] " + key)})
      seq += 1
      println("[" + seq + "] Main Menu")

      print("\nEnter your choices (separate with commas if more than 1 choice given): ")
      //val choice:Int = readInt()
      val choicesStr:String = readLine()

      var valid : Boolean = true
      var choices : List [Int] = List[Int]()
      var results : ArrayBuffer [(String,String,String)] = ArrayBuffer[(String,String,String)]()
      try {
    	  choices = choicesStr.filter(_!='\n').split(',').filter(ch => (ch != null && ch != "")).map(_.trim.toInt).toList
      } catch {
        case _:Throwable => valid = false
      }
      
      if (valid) {
    	  choices.foreach(choice => {
		       if( choice == contFiles.length + 1){
		    	   return
		       }
		       if( choice < 1 || choice > contFiles.length + 1 ){
					logger.fatal("Invalid Choice : " + choice)
					return
		       }
  		  
		       val contDefFile = contFiles(choice-1).toString
    		   logger.setLevel(Level.TRACE);
		       val contStr = Source.fromFile(contDefFile).mkString
    		   MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
    		   val res : String = MetadataAPIImpl.AddContainer(contStr,"JSON")
    		   results += Tuple3(choice.toString, contDefFile, res)
    	  })
      } else {
          logger.fatal("Invalid Choices... choose 1 or more integers from list separating multiple entries with a comma")
          return
      }
      
      results.foreach(triple => {
    	  val (choice,filePath,result) : (String,String,String) = triple
    	  println(s"Results for container [$choice] $filePath => \n$result")
      })

    }catch {
      case e: AlreadyExistsException => {
	  logger.error("Container Already in the metadata...." + e.getMessage())
      }
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def IsValidDir(dirName:String) : Boolean = {
    val fl = new java.io.File(dirName).listFiles
    if( fl != null) true else false
  }


  def AddMessage{
    try{

	  var dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("MESSAGE_FILES_DIR")
	  if ( dirName == null  ){
	dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("GIT_ROOT") + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Messages"
	logger.info("The environment variable MESSAGE_FILES_DIR is undefined, The directory defaults to " + dirName)
	  }

      if ( ! IsValidDir(dirName) ){
	logger.fatal("Invalid Directory " + dirName)
	return
      }

      val msgFiles = new java.io.File(dirName).listFiles.filter(_.getName.endsWith(".json"))
      if ( msgFiles.length == 0 ){
	logger.fatal("No json message files in the directory " + dirName)
	return
      }
      println("\nPick a Message Definition file(s) from below choices\n")

      var seq = 0
      msgFiles.foreach(key => { seq += 1; println("[" + seq + "] " + key)})
      seq += 1
      println("[" + seq + "] Main Menu")

      print("\nEnter your choices (separate with commas if more than 1 choice given): ")
      //val choice:Int = readInt()
      val choicesStr:String = readLine()

      var valid : Boolean = true
      var choices : List [Int] = List[Int]()
      var results : ArrayBuffer [(String,String,String)] = ArrayBuffer[(String,String,String)]()
      try {
    	  choices = choicesStr.filter(_!='\n').split(',').filter(ch => (ch != null && ch != "")).map(_.trim.toInt).toList
      } catch {
        case _:Throwable => valid = false
      }
      
      if (valid) {

    	  choices.foreach(choice => {
		       if( choice == msgFiles.length + 1){
		    	   return
		       }
		       if( choice < 1 || choice > msgFiles.length + 1 ){
					logger.fatal("Invalid Choice : " + choice)
					return
		       }
  		  
		       val msgDefFile = msgFiles(choice-1).toString
    		   logger.setLevel(Level.TRACE);
		       val msgStr = Source.fromFile(msgDefFile).mkString
    		   MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
    		   val res : String = MetadataAPIImpl.AddContainer(msgStr,"JSON")
    		   results += Tuple3(choice.toString, msgDefFile, res)
    	  })
      } else {
          logger.fatal("Invalid Choices... choose 1 or more integers from list separating multiple entries with a comma")
          return
      }
      
      results.foreach(triple => {
    	  val (choice,filePath,result) : (String,String,String) = triple
    	  println(s"Results for message [$choice] $filePath => \n$result")
      })
      
    }catch {
      case e: AlreadyExistsException => {
	  logger.error("Message Already in the metadata....")
      }
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def AddModel {
    try{
      var dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("MODEL_FILES_DIR")
      if ( dirName == null  ){
	dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("GIT_ROOT") + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Models"
	logger.info("The environment variable MODEL_FILES_DIR is undefined, The directory defaults to " + dirName)
      }
      val pmmlFiles = new java.io.File(dirName).listFiles.filter(_.getName.endsWith(".xml"))
      if ( pmmlFiles.length == 0 ){
	logger.fatal("No model files in the directory " + dirName)
	return
      }

      var pmmlFilePath = ""
      println("Pick a Model Definition file(pmml) from below choices")

      var seq = 0
      pmmlFiles.foreach(key => { seq += 1; println("[" + seq + "] " + key)})
      seq += 1
      println("[" + seq + "] Main Menu")

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if( choice == pmmlFiles.length + 1){
	return
      }
      if( choice < 1 || choice > pmmlFiles.length + 1 ){
	  logger.fatal("Invalid Choice : " + choice)
	  return
      }

      pmmlFilePath = pmmlFiles(choice-1).toString
      val pmmlStr = Source.fromFile(pmmlFilePath).mkString
      // Save the model
      MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
      println("Results as json string => \n" + MetadataAPIImpl.AddModel(pmmlStr))
    }catch {
      case e: AlreadyExistsException => {
	  logger.error("Model Already in the metadata....")
      }
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def LoadFunctionsFromAFile {
    try{
      var dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("FUNCTION_FILES_DIR")
      if ( dirName == null  ){
	dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("GIT_ROOT") + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Functions"
	logger.info("The environment variable FUNCTION_FILES_DIR is undefined, The directory defaults to " + dirName)
      }
      val functionFiles = new java.io.File(dirName).listFiles.filter(_.getName.endsWith(".json"))
      if ( functionFiles.length == 0 ){
	logger.fatal("No function files in the directory " + dirName)
	return
      }

      var functionFilePath = ""
      println("Pick a Function Definition file(function) from below choices")

      var seq = 0
      functionFiles.foreach(key => { seq += 1; println("[" + seq + "] " + key)})
      seq += 1
      println("[" + seq + "] Main Menu")

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if( choice == functionFiles.length + 1){
	return
      }
      if( choice < 1 || choice > functionFiles.length + 1 ){
	  logger.fatal("Invalid Choice : " + choice)
	  return
      }

      functionFilePath = functionFiles(choice-1).toString

      val functionStr = Source.fromFile(functionFilePath).mkString
      //MdMgr.GetMdMgr.truncate("FunctionDef")
      val apiResult = MetadataAPIImpl.AddFunctions(functionStr,"JSON")
      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + resultData)
    }catch {
      case e: AlreadyExistsException => {
	  logger.error("Function Already in the metadata....")
      }
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def DumpAllFunctionsAsJson{
    try{
      val apiResult = MetadataAPIImpl.GetAllFunctionDefs("JSON")
      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + resultData)
    } catch{
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def LoadConceptsFromAFile {
    try{
      var dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("CONCEPT_FILES_DIR")
      if ( dirName == null  ){
	dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("GIT_ROOT") + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Concepts"
	logger.info("The environment variable CONCEPT_FILES_DIR is undefined, The directory defaults to " + dirName)
      }
      val conceptFiles = new java.io.File(dirName).listFiles.filter(_.getName.endsWith(".json"))
      if ( conceptFiles.length == 0 ){
	logger.fatal("No concept files in the directory " + dirName)
	return
      }

      var conceptFilePath = ""
      println("Pick a Concept Definition file(concept) from below choices")

      var seq = 0
      conceptFiles.foreach(key => { seq += 1; println("[" + seq + "] " + key)})
      seq += 1
      println("[" + seq + "] Main Menu")

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if( choice == conceptFiles.length + 1){
	return
      }
      if( choice < 1 || choice > conceptFiles.length + 1 ){
	  logger.fatal("Invalid Choice : " + choice)
	  return
      }

      conceptFilePath = conceptFiles(choice-1).toString

      val conceptStr = Source.fromFile(conceptFilePath).mkString
      MdMgr.GetMdMgr.truncate("ConceptDef")
      val apiResult = MetadataAPIImpl.AddConcepts(conceptStr,"JSON")
      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + resultData)
    }catch {
      case e: AlreadyExistsException => {
	  logger.error("Concept Already in the metadata....")
      }
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def DumpAllConceptsAsJson{
    try{
      val apiResult = MetadataAPIImpl.GetAllConcepts("JSON")
      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + resultData)
    } catch{
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def LoadTypesFromAFile {
    try{
      var dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("TYPE_FILES_DIR")
      if ( dirName == null  ){
	dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("GIT_ROOT") + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Types"
	logger.info("The environment variable TYPE_FILES_DIR is undefined, The directory defaults to " + dirName)
      }
      val typeFiles = new java.io.File(dirName).listFiles.filter(_.getName.endsWith(".json"))
      if ( typeFiles.length == 0 ){
	logger.fatal("No type files in the directory " + dirName)
	return
      }

      var typeFilePath = ""
      println("Pick a Type Definition file(type) from below choices")

      var seq = 0
      typeFiles.foreach(key => { seq += 1; println("[" + seq + "] " + key)})
      seq += 1
      println("[" + seq + "] Main Menu")

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if( choice == typeFiles.length + 1){
	return
      }
      if( choice < 1 || choice > typeFiles.length + 1 ){
	  logger.fatal("Invalid Choice : " + choice)
	  return
      }

      typeFilePath = typeFiles(choice-1).toString

      val typeStr = Source.fromFile(typeFilePath).mkString
      //MdMgr.GetMdMgr.truncate("TypeDef")
      val apiResult = MetadataAPIImpl.AddTypes(typeStr,"JSON")
      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + resultData)
    }catch {
      case e: AlreadyExistsException => {
	  logger.error("Type Already in the metadata....")
      }
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def DumpAllTypesByObjTypeAsJson{
    try{
      val typeMenu = Map( 1 ->  "ScalarTypeDef",
			  2 ->  "ArrayTypeDef",
			  3 ->  "ArrayBufTypeDef",
			  4 ->  "SetTypeDef",
			  5 ->  "TreeSetTypeDef",
			  6 ->  "AnyTypeDef",
			  7 ->  "SortedSetTypeDef",
			  8 ->  "MapTypeDef",
			  9 ->  "HashMapTypeDef",
			  10 ->  "ImmutableMapTypeDef",
			  11 ->  "ListTypeDef",
			  12 ->  "QueueTypeDef",
			  13 ->  "TupleTypeDef")
      var selectedType = "com.ligadata.olep.metadata.ScalarTypeDef"
      var done = false
      while ( done == false ){
	println("\n\nPick a Type ")
	var seq = 0
	typeMenu.foreach(key => { seq += 1; println("[" + seq + "] " + typeMenu(seq))})
	seq += 1
	println("[" + seq + "] Main Menu")
	print("\nEnter your choice: ")
	val choice:Int = readInt()
	if( choice <= typeMenu.size ){
	  selectedType = "com.ligadata.olep.metadata." + typeMenu(choice)
	  done = true
	}
	else if( choice == typeMenu.size + 1 ){
	  done = true
	}
	else{
	  logger.error("Invalid Choice : " + choice)
	}
      }

      val apiResult = MetadataAPIImpl.GetAllTypesByObjType("JSON",selectedType)
      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => " + resultData)
    } catch{
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def initMsgCompilerBootstrap{
    MdMgr.GetMdMgr.truncate
    val mdLoader = new MetadataLoad (MdMgr.mdMgr, "","","","")
    mdLoader.initialize
  }

  def DumpMetadata{
    MdMgr.GetMdMgr.SetLoggerLevel(Level.TRACE)
    MdMgr.GetMdMgr.dump
    MdMgr.GetMdMgr.SetLoggerLevel(Level.ERROR)
  }    

  def initModCompilerBootstrap{
    MdMgr.GetMdMgr.truncate
    logger.setLevel(Level.ERROR);
    val mdLoader = new MetadataLoad(MdMgr.GetMdMgr, "","","","")
    mdLoader.initialize
  }

  def TestChill[T <: BaseElemDef](obj: List[T]) {
    val items = obj;

    logger.trace("Serializing " + obj.length + " objects ")
    val instantiator = new ScalaKryoInstantiator
    instantiator.setRegistrationRequired(false)
 
    val kryo = instantiator.newKryo()
    val baos = new ByteArrayOutputStream
    val output = new Output(baos, 4096)
    kryo.writeObject(output, items)
 
    val input = new Input(baos.toByteArray)
    val deser = kryo.readObject(input, classOf[List[T]])

    logger.trace("DeSerialized " + deser.length + " objects ")
    assert(deser.length == obj.length)
  }


  def TestKryoSerialize(configFile: String){
    MetadataAPIImpl.InitMdMgrFromBootStrap(configFile)
    val msgDefs = MdMgr.GetMdMgr.Types(true,true)
    msgDefs match{
      case None => {
	logger.trace("No Messages found ")
      }
      case Some(ms) => {
	val msa = ms.toArray
	TestChill(msa.toList)
      }
    }
  }


  def TestSerialize1(serializeType:String) = {
    //MetadataAPIImpl.InitMdMgrFromBootStrap
    var serializer = SerializerManager.GetSerializer(serializeType)
    serializer.SetLoggerLevel(Level.TRACE)
    val modelDefs = MdMgr.GetMdMgr.Models(true,true)
    modelDefs match{
      case None => {
	logger.trace("No Models found ")
      }
      case Some(ms) => {
	val msa = ms.toArray
	msa.foreach( m => {
	  val ba = serializer.SerializeObjectToByteArray(m)
	  MdMgr.GetMdMgr.ModifyModel(m.nameSpace,m.name,m.ver,"Remove")
	  val m1 = serializer.DeserializeObjectFromByteArray(ba,m.getClass().getName()).asInstanceOf[ModelDef]
	  val preJson  = JsonSerializer.SerializeObjectToJson(m);
	  val postJson = JsonSerializer.SerializeObjectToJson(m1);
	  logger.trace("Length of pre  Json string => " + preJson.length)
	  logger.trace("Length of post Json string => " + postJson.length)

	  logger.trace("Json Before Any Serialization => " + preJson)
	  logger.trace("Json After  Serialization/DeSerialization => " + postJson)
	  //assert(preJson == postJson)
	})
      }
    }
  }

  def TestGenericProtobufSerializer = {
    val serializer = new ProtoBufSerializer
    serializer.SetLoggerLevel(Level.TRACE)
    val a = MdMgr.GetMdMgr.MakeConcept("System","concept1","System","Int",1,false)
    //val ba = serializer.SerializeObjectToByteArray1(a)
    //val o = serializer.DeserializeObjectFromByteArray1(ba)
    //assert(JsonSerializer.SerializeObjectToJson(a) == JsonSerializer.SerializeObjectToJson(o.asInstanceOf[AttributeDef]))
  }

  def TestNotifyZooKeeper{
    val zkc = CreateClient.createSimple("localhost:2181")
    try{
      zkc.start()
      if(zkc.checkExists().forPath("/ligadata/models") == null ){
	zkc.create().withMode(CreateMode.PERSISTENT).forPath("/ligadata/models",null);
      }
      zkc.setData().forPath("/ligadata/models","Activate ModelDef-2".getBytes);
    }catch{
      case e:Exception => {
	e.printStackTrace()
      }
    }finally{
      zkc.close();
    }
  }


  def NotifyZooKeeperAddModelEvent{
    val modelDefs = MdMgr.GetMdMgr.Models(true,true)
    modelDefs match{
      case None => {
	logger.trace("No Models found ")
      }
      case Some(ms) => {
	val msa = ms.toArray
	val objList = new Array[BaseElemDef](msa.length)
	var i = 0
	msa.foreach( m => {objList(i) = m; i = i + 1})
	val operations = for (op <- objList) yield "Add"
	MetadataAPIImpl.NotifyEngine(objList,operations)
      }
    }
  }


  def NotifyZooKeeperAddMessageEvent{
    val msgDefs = MdMgr.GetMdMgr.Messages(true,true)
    msgDefs match{
      case None => {
	logger.trace("No Msgs found ")
      }
      case Some(ms) => {
	val msa = ms.toArray
	val objList = new Array[BaseElemDef](msa.length)
	var i = 0
	msa.foreach( m => {objList(i) = m; i = i + 1})
	val operations = for (op <- objList) yield "Add"
	MetadataAPIImpl.NotifyEngine(objList,operations)
      }
    }
  }

  def testSaveObject(key: String, value: String, store: DataStore){
    val serializer = SerializerManager.GetSerializer("kryo")
    var ba = serializer.SerializeObjectToByteArray(value)
    try{
      MetadataAPIImpl.UpdateObject(key,ba,store)
    }
    catch{
      case e:Exception => {
	logger.trace("Failed to save the object : " + e.getMessage())
      }
    }
  }

  def testDbOp{
    try{
      val serializer = SerializerManager.GetSerializer("kryo")
      testSaveObject("key1","value1",MetadataAPIImpl.oStore)
      var obj = MetadataAPIImpl.GetObject("key1",MetadataAPIImpl.oStore)
      var v = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[String]
      assert(v == "value1")
      testSaveObject("key1","value2",MetadataAPIImpl.oStore)
      obj = MetadataAPIImpl.GetObject("key1",MetadataAPIImpl.oStore)
      v = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[String]
      assert(v == "value2")
      testSaveObject("key1","value3",MetadataAPIImpl.oStore)
      obj = MetadataAPIImpl.GetObject("key1",MetadataAPIImpl.oStore)
      v = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[String]
      assert(v == "value3")
    }catch{
      case e:Exception => {
	e.printStackTrace()
      }
    }
  }

  def StartTest{
    try{
      val dumpMetadata = ()               => { DumpMetadata }
      val addModel = ()                   => { AddModel }
      val getModel = ()                   => { GetModelFromCache }
      val getAllModels = ()               => { GetAllModelsFromCache }
      val removeModel = ()                => { RemoveModel }
      val deactivateModel = ()            => { DeactivateModel }
      val activateModel = ()              => { ActivateModel }
      val addMessage = ()                 => { AddMessage }
      val getMessage = ()                 => { GetMessageFromCache }
      val getAllMessages = ()             => { GetAllMessagesFromCache }
      val removeMessage = ()              => { RemoveMessage }
      val addContainer = ()               => { AddContainer }
      val getContainer = ()               => { GetContainerFromCache }
      val getAllContainers = ()           => { GetAllContainersFromCache }
      val removeContainer = ()            => { RemoveContainer }
      val addType         = ()            => { AddType }
      val getType         = ()            => { GetType }
      val getAllTypes     = ()            => { GetAllTypes }
      val removeType      = ()            => { RemoveType }
      val addFunction         = ()        => { AddFunction }
      val removeFunction      = ()        => { RemoveFunction }
      val updateFunction         = ()     => { UpdateFunction }
      val addConcept         = ()         => { AddConcept }
      val removeConcept      = ()         => { RemoveConcept }
      val updateConcept         = ()      => { UpdateConcept }
      val dumpAllFunctionsAsJson = ()     => { DumpAllFunctionsAsJson }
      val loadFunctionsFromAFile = ()     => { LoadFunctionsFromAFile }
      val dumpAllConceptsAsJson = ()      => { DumpAllConceptsAsJson }
      val loadConceptsFromAFile = ()      => { LoadConceptsFromAFile }
      val dumpAllTypesByObjTypeAsJson = ()=> { DumpAllTypesByObjTypeAsJson }
      val loadTypesFromAFile = ()         => { LoadTypesFromAFile }

      val topLevelMenu = List(("Add Model",addModel),
			      ("Get Model",getModel),
			      ("Get All Models",getAllModels),
			      ("Remove Model",removeModel),
			      ("Deactivate Model",deactivateModel),
			      ("Activate Model",activateModel),
			      ("Add Message",addMessage),
			      ("Get Message",getMessage),
			      ("Get All Messages",getAllMessages),
			      ("Remove Message",removeMessage),
			      ("Add Container",addContainer),
			      ("Get Container",getContainer),
			      ("Get All Containers",getAllContainers),
			      ("Remove Container",removeContainer),
			      ("Add Type",addType),
			      ("Get Type",getType),
			      ("Get All Types",getAllTypes),
			      ("Remove Type",removeType),
			      ("Add Function",addFunction),
			      ("Remove Function",removeFunction),
			      ("Update Function",updateFunction),
			      ("Add Concept",addConcept),
			      ("Remove Concept",removeConcept),
			      ("Update Concept",updateConcept),
			      ("Load Concepts from a file",loadConceptsFromAFile),
			      ("Load Functions from a file",loadFunctionsFromAFile),
			      ("Load Types from a file",loadTypesFromAFile),
			      ("Dump All Metadata Keys",dumpMetadata),
			      ("Dump All Functions",dumpAllFunctionsAsJson),
			      ("Dump All Concepts",dumpAllConceptsAsJson),
			      ("Dump All Types By Object Type",dumpAllTypesByObjTypeAsJson))

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

  private def PrintUsage(): Unit = {
    logger.warn("    --config <configfilename>")
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

  def main(args: Array[String]){
    try{
      logger.setLevel(Level.TRACE);
      MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
      MdMgr.GetMdMgr.SetLoggerLevel(Level.TRACE)
      serializer.SetLoggerLevel(Level.TRACE)
      JsonSerializer.SetLoggerLevel(Level.TRACE)

      var myConfigFile = System.getenv("HOME") + "/MetadataAPIConfig.properties"
      if (args.length == 0) {
	logger.error("Config File defaults to " + myConfigFile)
	logger.error("One Could optionally pass a config file as a command line argument:  --config myConfig.properties")
	logger.error("The config file supplied is a complete path name of a config file similar to one in github/RTD/trunk/MetadataAPI/src/main/resources/MetadataAPIConfig.properties")
      }
      else{
	val options = nextOption(Map(), args.toList)
	val cfgfile = options.getOrElse('config, null)
	if (cfgfile == null) {
	  logger.error("Need configuration file as parameter")
	  throw new MissingArgumentException("Usage: configFile  supplied as --config myConfig.json")
	}
	myConfigFile = cfgfile.asInstanceOf[String]
      }
      MetadataAPIImpl.InitMdMgrFromBootStrap(myConfigFile)
      databaseOpen = true
      StartTest
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
    finally{
      // CloseDbStore Must be called for a clean exit
      if( databaseOpen ){
	MetadataAPIImpl.CloseDbStore
      }
      MetadataAPIImpl.CloseZKSession
    }
  }
}
