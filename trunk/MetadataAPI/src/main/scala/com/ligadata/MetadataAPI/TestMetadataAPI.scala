package com.ligadata.MetadataAPI

import org.scalatest.Assertions._

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

import java.io._
import scala.io._
import com.ligadata.messagedef._
import com.ligadata.Compiler._

object TestMetadataAPI{

  val sysNS = "System"		// system name space

  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)

  lazy val loggerFormat = "%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n"

  def testDbConn{
    var hostnames = "localhost"
    var keyspace = "default"
    var table = "default"
  
    var clusterBuilder = Cluster.builder()
	
    clusterBuilder.addContactPoints(hostnames)
	
    val cluster = clusterBuilder.build()
    val session = cluster.connect(keyspace);
  }

  def GetConnectionProperties: PropertyMap = {
    val connectinfo = new PropertyMap
    connectinfo+= ("connectiontype" -> "cassandra")
    connectinfo+= ("hostlist" -> "localhost") 
    connectinfo+= ("schema" -> "default")
    connectinfo+= ("table" -> "default")
    connectinfo
  }

  def dumpAllFunctions{
    val functions = MdMgr.GetMdMgr.Functions(true,true)
    functions match{
      case None => println("no functions available")
      case Some(fs) =>
	logger.trace(MetadataAPIImpl.toJson(fs.toArray))
	fs.foreach(f => {
	  logger.trace("Dump the function " +  f.name)
	  logger.trace("key => "   + f.typeString)
	})
    }
  }

  def testGetFunctions{
    val functions = MdMgr.GetMdMgr.Functions(true,true)
    functions match{
      case None => println("no functions available")
      case Some(fs) =>
	val functionList = fs.toList
	for( f <- functionList ){
	  logger.trace("Get the function " +  f.Name)
          var kv:KeyValuePair = new KeyValuePair
          var funcStr = MetadataAPIImpl.GetFunctionDef(f.FullName,"JSON","-1.0")
          logger.trace("FunctionDef => " + funcStr)
	}
    }
  }


  def testAddFunctions{
    MetadataAPIImpl.AddFunctions(SampleData.sampleFunctionsText,"JSON")
    dumpAllFunctions
  }

  def testAddTypes{
    MetadataAPIImpl.AddTypes(SampleData.sampleTypesStr,"JSON")
  }

  def testAddConcepts{
    MetadataAPIImpl.AddConcepts(SampleData.sampleConceptsStr,"JSON")
    //MetadataAPIImpl.dumpAttrbDefs
  }

  def testGetAllConcepts : String = {
    MetadataAPIImpl.GetAllConcepts("JSON")
  }

  def testGetAllFunctions : String = {
    MetadataAPIImpl.GetAllFunctionDefs("JSON")
  }

  def testAddMessage(msgDefFile: String){
    val msgStr = Source.fromFile(msgDefFile).mkString
    new MetadataLoad(MdMgr.GetMdMgr, null, "", "", "", "")
    MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
    logger.trace(MetadataAPIImpl.AddMessage(msgStr,"JSON"))
  }


  def testDeserializeMessage(msgDefFile: String){
    val msgStr = Source.fromFile(msgDefFile).mkString
    new MetadataLoad(MdMgr.GetMdMgr, null, "", "", "", "")
    MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
    var compProxy = new CompilerProxy
    compProxy.setLoggerLevel(Level.TRACE)
    var(classStr,msgDef) = compProxy.compileMessageDef(msgStr)
    val jsonStr = MetadataAPIImpl.toJson(msgDef)
    logger.trace(jsonStr)
    val newMsgDef = MetadataAPIImpl.parseMessageDef(jsonStr,"JSON")
    logger.trace(MetadataAPIImpl.toJson(newMsgDef))
  }

  def testAddRemoveDeletePmml = {
    MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
    logger.trace(MetadataAPIImpl.AddModel(SampleData.sampleModelInXml))
  }

  def testCompileMessageDef1 = {
    var compProxy = new CompilerProxy
    var(classStr,msgDef) = compProxy.compileMessageDef(SampleData.sampleMessageDef1)
  }

  def testAddRemoveUpdateType = {
    MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
    logger.trace(MetadataAPIImpl.AddType(SampleData.sampleIntTypeStr,"JSON"))
    logger.trace(MetadataAPIImpl.RemoveType("Int",1))
    logger.trace(MetadataAPIImpl.AddType(SampleData.sampleIntTypeStr,"JSON"))
    logger.trace(MetadataAPIImpl.UpdateType(SampleData.sampleUpdatedIntTypeStr,"JSON"))
    logger.trace(MetadataAPIImpl.RemoveType("Int",1))
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

  def testAddModel(pmmlFilePath: String,compiler: PmmlCompiler){
    // Read a model
    val pmmlStr = Source.fromFile(pmmlFilePath).mkString
    val (classStr:String,model:ModelDef) = compiler.compile(pmmlStr)
    val pmmlScalaFile = System.getenv("SCALA_SRC_TARGET_PATH") + "/" + model.name + ".pmml"
    val (jarFile,depJars) = compiler.createJar(classStr,
	    System.getenv("CLASSPATH"),
	    pmmlScalaFile,
	    //System.getenv("SCRIPT_PATH"),
	    System.getenv("JAR_TARGET_DIR"),
	    System.getenv("MANIFEST_PATH"),
	    System.getenv("SCALA_HOME"),
	    System.getenv("JAVA_HOME"),
	    false)
    model.jarName = jarFile
    model.ver     = 1
    if( model.modelType == null){
      model.modelType = "RuleSet"
    }

    // Save the model
    MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
    logger.trace(MetadataAPIImpl.AddModel(model))
    logger.trace(MetadataAPIImpl.GetAllModelDefs("JSON"))
    //logger.trace(MetadataAPIImpl.GetModelDef(model.name,"JSON",model.ver.toString))
    //logger.trace(MetadataAPIImpl.RemoveModel(model.name,model.ver))
    //logger.trace(MetadataAPIImpl.GetModelDef(model.name,"JSON",model.ver.toString))
  }


  def testDeserializeModel(pmmlFilePath: String,compiler: PmmlCompiler){
    // Read a model
    val pmmlStr = Source.fromFile(pmmlFilePath).mkString
    val (classStr:String,model:ModelDef) = compiler.compile(pmmlStr)
    val pmmlScalaFile = System.getenv("SCALA_SRC_TARGET_PATH") + "/" + model.name + ".pmml"
    val (jarFile,depJars) = compiler.createJar(classStr,
	    System.getenv("CLASSPATH"),
	    pmmlScalaFile,
	    //System.getenv("SCRIPT_PATH"),
	    System.getenv("JAR_TARGET_DIR"),
	    System.getenv("MANIFEST_PATH"),
	    System.getenv("SCALA_HOME"),
	    System.getenv("JAVA_HOME"),
	    false)
    model.jarName = jarFile
    model.nameSpace = sysNS
    model.ver     = 1
    if( model.modelType == null){
      model.modelType = "RuleSet"
    }
    // Save the model
    MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
    val jsonStr = MetadataAPIImpl.toJson(model)
    logger.trace(jsonStr)
    val newModDef = MetadataAPIImpl.parseModelDef(jsonStr,"JSON")
    logger.trace(MetadataAPIImpl.toJson(newModDef))
  }

  def AddMessage(msgDefFile: String){
    try{
      MetadataAPIImpl.OpenDbStore
      val msgStr = Source.fromFile(msgDefFile).mkString
      MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
      logger.trace(MetadataAPIImpl.AddMessage(msgStr,"JSON"))
      // logger.trace("Message was added just now...")
      MetadataAPIImpl.CloseDbStore
    }catch {
      case e: Exception => {
	MetadataAPIImpl.CloseDbStore
	e.printStackTrace()
      }
    }
  }

  def testCompileMessageDef3{
    try{
      MetadataAPIImpl.OpenDbStore

      logger.trace("add the message...")
      testAddMessage(System.getenv("HOME") + "/ligadata/trunk/MessageDef/messageTOutPatient.json")
      logger.trace("get the message that was added just now...")
      val results = MetadataAPIImpl.GetMessageDef("outpatientclaimtmsg","JSON","-1")

      logger.trace("get the messageDef string from APIResult instance")
      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(results)
      logger.trace(resultData)
      
      logger.trace("parse the message def from we got from persistence store")
      val msgDef = MetadataAPIImpl.parseMessageDef(resultData,"JSON")

      logger.trace("add the message again")
      logger.trace(MetadataAPIImpl.AddMessageDef(msgDef))
      MetadataAPIImpl.CloseDbStore
    }catch {
      case e: Exception => {
	MetadataAPIImpl.CloseDbStore
	e.printStackTrace()
      }
    }
  }

  def GetMessage{
    try{
      logger.setLevel(Level.TRACE);

      val msgKeys = MetadataAPIImpl.GetAllMessageKeys

      if( msgKeys.length == 0 ){
	println("Sorry, No messages available in the Metadata")
	return
      }

      println("\nPick the message to be presented from the following list: ")

      var seq = 0
      msgKeys.foreach(key => { seq += 1; println("[" + seq + "] " + MetadataAPIImpl.KeyAsStr(key))})

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if ( choice < 1 || choice > msgKeys.length ){
	println("Invalid choice " + choice + ",start with main menu...")
	return
      }
      val msgKey = MetadataAPIImpl.KeyAsStr(msgKeys(choice-1))

      val msgKeyTokens = msgKey.split("\\.")
      val msgNameSpace = msgKeyTokens(0)
      val msgName = msgKeyTokens(1)
      val msgVersion = msgKeyTokens(2)
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

      val modKeys = MetadataAPIImpl.GetAllModelKeys
      if( modKeys.length == 0 ){
	println("Sorry, No models available in the Metadata")
	return
      }

      println("\nPick the model to be presented from the following list: ")
      var seq = 0
      modKeys.foreach(key => { seq += 1; println("[" + seq + "] " + MetadataAPIImpl.KeyAsStr(key))})

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if ( choice < 1 || choice > modKeys.length ){
	println("Invalid choice " + choice + ",start with main menu...")
	return
      }

      val modKey = MetadataAPIImpl.KeyAsStr(modKeys(choice-1))

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

      val modKeys = MetadataAPIImpl.GetAllModelsFromCache
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

      val msgKeys = MetadataAPIImpl.GetAllMessageKeys

      if( msgKeys.length == 0 ){
	println("Sorry, No messages available in the Metadata")
	return
      }

      println("\nPick the message to be deleted from the following list: ")
      var seq = 0
      msgKeys.foreach(key => { seq += 1; println("[" + seq + "] " + MetadataAPIImpl.KeyAsStr(key))})

      print("\nEnter your choice: ")
      val choice:Int = readInt()

      if ( choice < 1 || choice > msgKeys.length ){
	println("Invalid choice " + choice + ",start with main menu...")
	return
      }

      val msgKey = MetadataAPIImpl.KeyAsStr(msgKeys(choice-1))

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

      val modKeys = MetadataAPIImpl.GetAllModelsFromCache

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


  def RemoveModelFromCache{
    try{
      logger.setLevel(Level.TRACE);

      val modKeys = MetadataAPIImpl.GetAllModelsFromCache

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
      val msgKeys = MetadataAPIImpl.GetAllMessageKeys
      if( msgKeys.length == 0 ){
	println("Sorry, No messages available in the Metadata")
	return
      }
      var seq = 0
      msgKeys.foreach(key => { seq += 1; println("[" + seq + "] " + MetadataAPIImpl.KeyAsStr(key))})
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }



  def GetAllModelsFromCache{
    try{
      logger.setLevel(Level.TRACE);
      val modKeys = MetadataAPIImpl.GetAllModelsFromCache
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
      val modKeys = MetadataAPIImpl.GetAllModelKeys
      if( modKeys.length == 0 ){
	println("Sorry, No models are available in the Metadata")
	return
      }

      var seq = 0
      modKeys.foreach(key => { seq += 1; println("[" + seq + "] " + MetadataAPIImpl.KeyAsStr(key))})
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def AddContainer{
    try{
      var dirName = System.getenv("CONTAINER_FILES_DIR")
      if ( dirName == null  ){
	dirName = System.getenv("HOME") + "/ligadata/trunk/MetadataAPI/src/test/SampleTestFiles/Containers"
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

      println("\nPick a Container Definition file from below choices\n")

      var seq = 0
      contFiles.foreach(key => { seq += 1; println("[" + seq + "] " + key)})
      seq += 1
      println("[" + seq + "] Main Menu")

      print("\nEnter your choice: ")
      val choice:Int = readInt()

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
      println("Results as json string => \n" + MetadataAPIImpl.AddContainer(contStr,"JSON"))
    }catch {
      case e: AlreadyExistsException => {
	  logger.error("Container Already in the metadata, update is not implemented yet")
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

      var dirName = System.getenv("MESSAGE_FILES_DIR")
      if ( dirName == null  ){
	dirName = System.getenv("HOME") + "/ligadata/trunk/MetadataAPI/src/test/SampleTestFiles/Messages"
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
      println("\nPick a Message Definition file from below choices\n")

      var seq = 0
      msgFiles.foreach(key => { seq += 1; println("[" + seq + "] " + key)})
      seq += 1
      println("[" + seq + "] Main Menu")

      print("\nEnter your choice: ")
      val choice:Int = readInt()

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
      println("Results as json string => \n" + MetadataAPIImpl.AddMessage(msgStr,"JSON"))
    }catch {
      case e: AlreadyExistsException => {
	  logger.error("Message Already in the metadata, update is not implemented yet")
      }
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def AddModel {
    try{
      var dirName = System.getenv("MODEL_FILES_DIR")
      if ( dirName == null  ){
	dirName = System.getenv("HOME") + "/ligadata/trunk/MetadataAPI/src/test/SampleTestFiles/Models"
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
      val compiler  = new PmmlCompiler(MdMgr.GetMdMgr, "ligadata", logger)
      val (classStr,model) = compiler.compile(pmmlStr)
      val pmmlScalaFile = System.getenv("SCALA_SRC_TARGET_PATH") + "/" + model.name + ".pmml"
      val (jarFile,depJars) = 
      compiler.createJar(classStr,
			 System.getenv("CLASSPATH"),
			 pmmlScalaFile,
			 System.getenv("JAR_TARGET_DIR"),
			 System.getenv("MANIFEST_PATH"),
			 System.getenv("SCALA_HOME"),
			 System.getenv("JAVA_HOME"),
			 false)
      model.jarName = jarFile
      if( model.ver == 0 ){
	model.ver     = 1
      }
      if( model.modelType == null){
	model.modelType = "RuleSet"
      }

      // Save the model
      MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
      println("Results as json string => \n" + MetadataAPIImpl.AddModel(model))
    }catch {
      case e: AlreadyExistsException => {
	  logger.error("Model Already in the metadata, update is not implemented yet")
      }
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def LoadFunctionsFromAFile {
    try{
      var dirName = System.getenv("FUNCTION_FILES_DIR")
      if ( dirName == null  ){
	dirName = System.getenv("HOME") + "/ligadata/trunk/MetadataAPI/src/test/SampleTestFiles/Functions"
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
      MdMgr.GetMdMgr.truncate("FunctionDef")
      val apiResult = MetadataAPIImpl.AddFunctions(functionStr,"JSON")
      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + resultData)
    }catch {
      case e: AlreadyExistsException => {
	  logger.error("Function Already in the metadata, update is not implemented yet")
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
      var dirName = System.getenv("CONCEPT_FILES_DIR")
      if ( dirName == null  ){
	dirName = System.getenv("HOME") + "/ligadata/trunk/MetadataAPI/src/test/SampleTestFiles/Concepts"
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
	  logger.error("Concept Already in the metadata, update is not implemented yet")
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

  def initMsgCompilerBootstrap{
    MdMgr.GetMdMgr.truncate
    val mdLoader = new com.ligadata.olep.metadataload.MetadataLoad (MdMgr.mdMgr, logger,"","","","")
    mdLoader.initialize
  }

  def dumpMetadata{
    MdMgr.GetMdMgr.SetLoggerLevel(Level.TRACE)
    MdMgr.GetMdMgr.dump
    MdMgr.GetMdMgr.SetLoggerLevel(Level.ERROR)
  }    

  def initModCompilerBootstrap{
    MdMgr.GetMdMgr.truncate
    logger.setLevel(Level.ERROR);
    val mdLoader = new com.ligadata.olep.metadataload.MetadataLoad (MdMgr.mdMgr, logger,"","","","")
    mdLoader.initialize
  }

  def StartTest{
    try{
      MdMgr.GetMdMgr.truncate
      val mdLoader = new com.ligadata.olep.metadataload.MetadataLoad (MdMgr.mdMgr, logger,"","","","")
      mdLoader.initialize
      MetadataAPIImpl.OpenDbStore
      MetadataAPIImpl.LoadObjectsIntoCache

      val objectDump = ()       => { dumpMetadata }
      val addModel = ()         => { AddModel }
      val getModel = ()         => { GetModelFromCache }
      val getAllModels = ()     => { GetAllModelsFromCache }
      val removeModel = ()      => { RemoveModel }
      val addMessage = ()       => { AddMessage }
      val getMessage = ()       => { GetMessageFromCache }
      val getAllMessages = ()   => { GetAllMessagesFromCache }
      val removeMessage = ()    => { RemoveMessage }
      val addContainer = ()     => { AddContainer }
      val getContainer = ()     => { GetContainerFromCache }
      val getAllContainers = () => { GetAllContainersFromCache }
      val removeContainer = ()  => { RemoveContainer }
      val dumpAllFunctionsAsJson = ()=> { DumpAllFunctionsAsJson }
      val loadFunctionsFromAFile = ()=> { LoadFunctionsFromAFile }
      val dumpAllConceptsAsJson = ()=> { DumpAllConceptsAsJson }
      val loadConceptsFromAFile = ()=> { LoadConceptsFromAFile }

      val topLevelMenu1 = Array( "Add Model","Get Model","Get All Models","Remove Model","Add Message","Get Message","Get All Messages","Remove Message","Add Container","Get Container","Get All Containers","Remove Container","Dump MetaData","Load Functions From a File","Dump All Functions","Load Concepts From a File","Dump All Concepts")
      val topLevelMenu2 = Map( 1 ->  addModel, 
			       2 ->  getModel,
			       3 ->  getAllModels,
			       4 ->  removeModel,
			       5 ->  addMessage,
			       6 ->  getMessage,
			       7 ->  getAllMessages,
			       8 ->  removeMessage,
			       9 ->  addContainer,
			       10 -> getContainer,
			       11 -> getAllContainers,
			       12 -> removeContainer,
			       13 -> objectDump,
			       14 -> loadFunctionsFromAFile,
			       15 -> dumpAllFunctionsAsJson,
			       16 -> loadConceptsFromAFile,
			       17 -> dumpAllConceptsAsJson)

      var done = false
      while ( done == false ){
	println("\n\nPick an API ")
	var seq = 0
	topLevelMenu1.foreach(key => { seq += 1; println("[" + seq + "] " + key)})
	seq += 1
	println("[" + seq + "] Exit")
	print("\nEnter your choice: ")
	val choice:Int = readInt()
	if( choice <= topLevelMenu2.size ){
	  topLevelMenu2(choice).apply
	}
	else if( choice == topLevelMenu2.size + 1 ){
	  done = true
	}
	else{
	  logger.error("Invalid Choice : " + choice)
	}
      }
      MetadataAPIImpl.CloseDbStore
    }catch {
      case e: Exception => {
	MetadataAPIImpl.CloseDbStore
	e.printStackTrace()
      }
    }
  }    

  def main(args: Array[String]){

    try{
      logger.setLevel(Level.TRACE);
      MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
      MdMgr.GetMdMgr.SetLoggerLevel(Level.INFO)
      //val mdLoader = new com.ligadata.olep.metadataload.MetadataLoad (MdMgr.mdMgr, logger,"","","","")
      //mdLoader.initialize
      //new MetadataLoad(MdMgr.GetMdMgr, null, "", "", "", "")
      // 
      // testDbConn
      //MetadataAPIImpl.testDbOp
      // testInitMdMgr

      // All of MetaDataAPI functions use datastore
      //var pMap = GetConnectionProperties
      StartTest

      //testAddMessage(System.getenv("HOME") + "/ligadata/trunk/MessageDef/ContainerFE.json")
      //val msgDefFile = System.getenv("HOME") + "/ligadata/trunk/MessageDef/ContainerFE.json"
      //val compProxy = new CompilerProxy
      //val msgDefStr = Source.fromFile(msgDefFile).mkString
      //val mgr = MdMgr.GetMdMgr
      //val msg = new MessageDefImpl()
      //val(classStr,msgDef) = msg.processMsgDef(msgDefStr, "JSON",mgr)

      // AddMessage(System.getenv("HOME") + "/ligadata/trunk/MessageDef/messageTOutPatient.json")
      // testCompileMessageDef3
      // testDeserializeMessage(System.getenv("HOME") + "/ligadata/trunk/MessageDef/messageTOutPatient.json")
      //testAddFunctions
      //testGetFunctions
      //dumpAllFunctions
      //logger.trace(testGetAllFunctions)
      //testCompilePmml
      //testCompileMessageDef2
      //testAddRemoveUpdateType
      //testAddRemoveDeletePmml
      //testAddTypes

      //testAddConcepts
      //testGetAllConcepts
      //var zkClient = new ZooKeeperClient("localhost:2181")
      //zkClient.close

      //val compiler : PmmlCompiler = new PmmlCompiler(MdMgr.GetMdMgr, "MetadataAPI", logger)
      //testAddModel(System.getenv("HOME") + "/ligadata/trunk/Pmml/Models/Banking/Alchemy.xml",compiler)
      //testDeserializeModel(System.getenv("HOME") + "/ligadata/trunk/Pmml/Models/Banking/Alchemy.xml",compiler)
      //testAddModel(System.getenv("HOME") + "/ligadata/trunk/Pmml/Models/Banking/DebitCardFraud.xml",compiler)
      //testAddModel(System.getenv("HOME") + "/ligadata/trunk/Pmml/Models/Banking/EmerBorrowing.xml")

      //val mgr = MetadataAPIImpl.InitMdMgr
      // CloseDbStore Must be called for a clean exit
      //MetadataAPIImpl.CloseDbStore
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }
}
