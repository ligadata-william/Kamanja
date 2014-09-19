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
import java.util.Properties

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
	logger.trace(MetadataAPIImpl.listToJson("Functions",fs.toArray))
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

  def testAddScalarTypes{
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
    val pmmlScalaFile = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SCALA_SRC_TARGET_PATH") + "/" + model.name + ".pmml"
    val (jarFile,depJars) = compiler.createJar(classStr,
	    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("CLASSPATH"),
	    pmmlScalaFile,
	    //MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SCRIPT_PATH"),
	    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR"),
	    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("MANIFEST_PATH"),
	    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SCALA_HOME"),
	    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAVA_HOME"),
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
    val pmmlScalaFile = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SCALA_SRC_TARGET_PATH") + "/" + model.name + ".pmml"
    val (jarFile,depJars) = compiler.createJar(classStr,
	    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("CLASSPATH"),
	    pmmlScalaFile,
	    //MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SCRIPT_PATH"),
	    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR"),
	    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("MANIFEST_PATH"),
	    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SCALA_HOME"),
	    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAVA_HOME"),
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
      testAddMessage(MetadataAPIImpl.GetMetadataAPIConfig.getProperty("GIT_ROOT") + "/RTD/trunk/MessageDef/messageTOutPatient.json")
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

      val msgKeys = MetadataAPIImpl.GetAllKeys("MessageDef")

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

      val modKeys = MetadataAPIImpl.GetAllKeys("ModelDef")
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

      val msgKeys = MetadataAPIImpl.GetAllKeys("MessageDef")

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
      val msgKeys = MetadataAPIImpl.GetAllKeys("MessageDef")
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
      val modKeys = MetadataAPIImpl.GetAllKeys("ModelDef")
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
	  logger.error("Container Already in the metadata....")
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
      val compiler  = new PmmlCompiler(MdMgr.GetMdMgr, "ligadata", logger)
      val (classStr,model) = compiler.compile(pmmlStr)
      val pmmlScalaFile = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SCALA_SRC_TARGET_PATH") + "/" + model.name + ".pmml"
      val (jarFile,depJars) = 
      compiler.createJar(classStr,
			 MetadataAPIImpl.GetMetadataAPIConfig.getProperty("CLASSPATH"),
			 pmmlScalaFile,
			 MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR"),
			 MetadataAPIImpl.GetMetadataAPIConfig.getProperty("MANIFEST_PATH"),
			 MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SCALA_HOME"),
			 MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAVA_HOME"),
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

  def DumpAllTypesAsJson{
    try{
      val apiResult = MetadataAPIImpl.GetAllTypes("JSON")
      val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
      println("Result as Json String => \n" + resultData)
    } catch{
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
			  10 ->  "ListTypeDef",
			  11 ->  "QueueTypeDef",
			  12 ->  "TupleTypeDef")
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
      val dumpAllTypesAsJson = ()=> { DumpAllTypesAsJson }
      val dumpAllTypesByObjTypeAsJson = ()=> { DumpAllTypesByObjTypeAsJson }
      val loadTypesFromAFile = ()=> { LoadTypesFromAFile }

      val topLevelMenu1 = Array( "Add Model","Get Model","Get All Models","Remove Model","Add Message","Get Message","Get All Messages","Remove Message","Add Container","Get Container","Get All Containers","Remove Container","Dump MetaData","Load Functions From a File","Dump All Functions","Load Concepts From a File","Dump All Concepts","Load Types From a File","Dump All Types","Dump All Types By Object Type")
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
			       17 -> dumpAllConceptsAsJson,
			       18 -> loadTypesFromAFile,
			       19 -> dumpAllTypesAsJson,
			       20 -> dumpAllTypesByObjTypeAsJson)

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
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }    

  def main(args: Array[String]){

    try{
      logger.setLevel(Level.TRACE);
      MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
      MdMgr.GetMdMgr.SetLoggerLevel(Level.TRACE)
      
      MetadataAPIImpl.readMetadataAPIConfig
      StartTest
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
    finally{
      // CloseDbStore Must be called for a clean exit
      MetadataAPIImpl.CloseDbStore
    }
  }
}
