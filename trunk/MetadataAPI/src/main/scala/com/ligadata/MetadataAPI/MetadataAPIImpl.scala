package com.ligadata.MetadataAPI

import java.util.Properties
import java.io._
import scala.Enumeration
import scala.io.Source._

import com.ligadata.olep.metadata.ObjType._
import com.ligadata.olep.metadata._
import com.ligadata.olep.metadata.MdMgr._
import com.ligadata.olep.metadataload.MetadataLoad

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.ResultSet

import com.ligadata.keyvaluestore._
import com.ligadata.keyvaluestore.mapdb._

import scala.util.parsing.json.JSON
import scala.util.parsing.json.{JSONObject, JSONArray}
import scala.collection.immutable.Map

import com.ligadata.messagedef._

import scala.xml.XML
import org.apache.log4j._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.zookeeper.{CreateMode, Watcher, WatchedEvent, ZooKeeper}
import org.apache.zookeeper.CreateMode._
import org.apache.zookeeper.KeeperException.NoNodeException
import org.apache.zookeeper.data.{ACL, Id, Stat}

import com.ligadata.Serialize._

case class JsonException(message: String) extends Exception(message)

case class TypeDef(MetadataType:String, NameSpace: String, Name: String, TypeTypeName:String, TypeNameSpace: String, TypeName:String, PhysicalName:String, var Version: String, JarName: String, DependencyJars: List[String], Implementation: String, Fixed: Option[Boolean], NumberOfDimensions :Option[Int], KeyTypeNameSpace: Option[String], KeyTypeName: Option[String], ValueTypeNameSpace: Option[String], ValueTypeName: Option[String], TupleDefinitions: Option[List[TypeDef]] )
case class TypeDefList(Types: List[TypeDef])

case class Argument(ArgName: String, ArgTypeNameSpace:String, ArgTypeName: String)
case class Function(NameSpace:String, Name:String, PhysicalName: String, ReturnTypeNameSpace: String, ReturnTypeName: String, Arguments: List[Argument], Version:String, JarName: String, DependantJars: List[String])
case class FunctionList(Functions: List[Function])

//case class Concept(NameSpace: String,Name: String, TypeNameSpace: String, TypeName: String,Version: String,Description: String, Author: String, ActiveDate: String)
case class Concept(NameSpace: String,Name: String, TypeNameSpace: String, TypeName: String,Version: String)
case class ConceptList(Concepts: List[Concept])

case class Attr(NameSpace: String,Name: String, Version: Int, Type: TypeDef)

case class MessageStruct(NameSpace: String,Name: String, FullName: String, Version: Int, JarName: String, PhysicalName: String, DependencyJars: List[String], Attributes: List[Attr])
case class MessageDefinition(Message: MessageStruct)
case class ContainerDefinition(Container: MessageStruct)

case class ModelInfo(NameSpace: String,Name: String,Version: String,ModelType: String, JarName: String,PhysicalName: String, DependencyJars: List[String], InputAttributes: List[Attr], OutputAttributes: List[Attr])
case class ModelDefinition(Model: ModelInfo)

case class APIResultInfo(statusCode:Int, statusDescription: String, resultData: String)
case class APIResultJsonProxy(APIResults: APIResultInfo)

case class UnsupportedObjectException(e: String) extends Throwable(e)
case class Json4sParsingException(e: String) extends Throwable(e)
case class FunctionListParsingException(e: String) extends Throwable(e)
case class FunctionParsingException(e: String) extends Throwable(e)
case class TypeDefListParsingException(e: String) extends Throwable(e)
case class TypeParsingException(e: String) extends Throwable(e)
case class TypeDefProcessingException(e: String) extends Throwable(e)
case class ConceptListParsingException(e: String) extends Throwable(e)
case class ConceptParsingException(e: String) extends Throwable(e)
case class MessageDefParsingException(e: String) extends Throwable(e)
case class ContainerDefParsingException(e: String) extends Throwable(e)
case class ModelDefParsingException(e: String) extends Throwable(e)
case class ApiResultParsingException(e: String) extends Throwable(e)
case class UnexpectedMetadataAPIException(e: String) extends Throwable(e)
case class ObjectNotFoundException(e: String) extends Throwable(e)
case class CreateStoreFailedException(e: String) extends Throwable(e)

case class MissingPropertyException(e: String) extends Throwable(e)
case class InvalidPropertyException(e: String) extends Throwable(e)
case class KryoSerializationException(e: String) extends Throwable(e)


class KeyValuePair extends IStorage{
  var key = new com.ligadata.keyvaluestore.Key
  var value = new com.ligadata.keyvaluestore.Value
  def Key = new com.ligadata.keyvaluestore.Key
  def Value = new com.ligadata.keyvaluestore.Value
  def Construct(k: com.ligadata.keyvaluestore.Key, v: com.ligadata.keyvaluestore.Value) = {
    key = k
    value = v
  }
}    


// The implementation class
object MetadataAPIImpl extends MetadataAPI{
    
  val sysNS = "System"		// system name space
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)

  lazy val serializer = SerializerManager.GetSerializer("kryo")

  lazy val metadataAPIConfig = new Properties()

  def GetMetadataAPIConfig: Properties = {
    metadataAPIConfig
  }

  def SetLoggerLevel(level: Level){
    logger.setLevel(level);
  }


  private var modelStore:      DataStore = _
  private var messageStore:    DataStore = _
  private var containerStore:  DataStore = _
  private var functionStore:   DataStore = _
  private var conceptStore:    DataStore = _
  private var typeStore:       DataStore = _
  private var otherStore:      DataStore = _
  
  def KeyAsStr(k: com.ligadata.keyvaluestore.Key): String = {
    val k1 = k.toArray[Byte]
    new String(k1)
  }

  def ValueAsStr(v: com.ligadata.keyvaluestore.Value): String = {
    val v1 = v.toArray[Byte]
    new String(v1)
  }

  def SaveObject(key: String, value: String, store: DataStore){
    object i extends IStorage{
      var k = new com.ligadata.keyvaluestore.Key
      var v = new com.ligadata.keyvaluestore.Value
      for(c <- key ){
	k += c.toByte
      }
      for(c <- value ){
	v += c.toByte
      }
      def Key = k
      def Value = v
      def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
    }
    val t = store.beginTx
    store.put(i)
    store.commitTx(t)
  }

  def SaveSerializedObject(key: String, value: Array[Byte], store: DataStore){
    object i extends IStorage{
      var k = new com.ligadata.keyvaluestore.Key
      var v = new com.ligadata.keyvaluestore.Value
      for(c <- key ){
	k += c.toByte
      }
      for(c <- value ){
	v += c
      }
      def Key = k
      def Value = v
      def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
    }
    val t = store.beginTx
    store.put(i)
    store.commitTx(t)
  }

  def SaveObject(obj: BaseElemDef){
    try{
      val key = obj.FullNameWithVer.toLowerCase
      val value = JsonSerializer.SerializeObjectToJson(obj)
      obj match{
	case o:ModelDef => {
          if( IsModelAlreadyExists(o) == false ){
	    logger.trace("Adding the model to the cache " + key)
	    MdMgr.GetMdMgr.AddModelDef(o)
	  }
	  SaveObject(key,value,modelStore)
	}
	case o:MessageDef => {
          if( IsMessageAlreadyExists(o) == false ){
	    logger.trace("Adding the message to the cache " + key)
	    MdMgr.GetMdMgr.AddMsg(o)
	  }
	  SaveObject(key,value,messageStore)
	}
	case o:ContainerDef => {
          if( IsContainerAlreadyExists(o) == false ){
	    logger.trace("Adding the container to the cache " + key)
	    MdMgr.GetMdMgr.AddContainer(o)
	  }
	  SaveObject(key,value,containerStore)
	}
	case o:FunctionDef => {
          val funcKey = o.typeString.toLowerCase
          if( IsFunctionAlreadyExists(o) == false ){
	    logger.trace("Adding the function to the cache " + funcKey)
	    MdMgr.GetMdMgr.AddFunc(o)
	  }
	  SaveObject(funcKey,value,functionStore)
	}
	case o:AttributeDef => {
          if( IsConceptAlreadyExists(o) == false ){
	    logger.trace("Adding the attribute to the cache " + key)
	    MdMgr.GetMdMgr.AddAttribute(o)
	  }
	  SaveObject(key,value,conceptStore)
	}
	case o:ScalarTypeDef => {
	  MdMgr.GetMdMgr.AddScalar(o)
	  SaveObject(key,value,typeStore)
	}
	case o:ArrayTypeDef => {
	  MdMgr.GetMdMgr.AddArray(o)
	  SaveObject(key,value,typeStore)
	}
	case o:ArrayBufTypeDef => {
	  MdMgr.GetMdMgr.AddArrayBuffer(o)
	  SaveObject(key,value,typeStore)
	}
	case o:ListTypeDef => {
	  MdMgr.GetMdMgr.AddList(o)
	  SaveObject(key,value,typeStore)
	}
	case o:QueueTypeDef => {
	  MdMgr.GetMdMgr.AddQueue(o)
	  SaveObject(key,value,typeStore)
	}
	case o:SetTypeDef => {
	  MdMgr.GetMdMgr.AddSet(o)
	  SaveObject(key,value,typeStore)
	}
	case o:TreeSetTypeDef => {
	  MdMgr.GetMdMgr.AddTreeSet(o)
	  SaveObject(key,value,typeStore)
	}
	case o:SortedSetTypeDef => {
	  MdMgr.GetMdMgr.AddSortedSet(o)
	  SaveObject(key,value,typeStore)
	}
	case o:MapTypeDef => {
	  MdMgr.GetMdMgr.AddMap(o)
	  SaveObject(key,value,typeStore)
	}
	case o:HashMapTypeDef => {
	  MdMgr.GetMdMgr.AddHashMap(o)
	  SaveObject(key,value,typeStore)
	}
	case o:TupleTypeDef => {
	  MdMgr.GetMdMgr.AddTupleType(o)
	  SaveObject(key,value,typeStore)
	}
	case o:ContainerTypeDef => {
	  MdMgr.GetMdMgr.AddContainerType(o)
	  SaveObject(key,value,typeStore)
	}
	case _ => {
	  logger.error("SaveObject is not implemented for objects of type " + obj.getClass.getName)
	}
      }
    }catch{
      case e:AlreadyExistsException => {
	logger.error("Failed to Save the object(" + obj.FullNameWithVer + "): It already exists")
      }
      case e:Exception => {
	logger.error("Failed to Save the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
    }
  }

  def DeleteObject(key: String,store: DataStore){
    var k = new com.ligadata.keyvaluestore.Key
    for(c <- key ){
      k += c.toByte
    }
    val t = store.beginTx
    store.del(k)
    store.commitTx(t)
  }

  def DeleteObject(obj: BaseElemDef){
    obj match{
      case o:FunctionDef => {
	DeleteObject(o.typeString.toLowerCase,functionStore)
	MdMgr.GetMdMgr.RemoveFunction(o.nameSpace,o.name,o.ver)
      }
      case o:ModelDef => {
	DeleteObject(o.FullNameWithVer.toLowerCase,modelStore)
	MdMgr.GetMdMgr.RemoveModel(o.nameSpace,o.name,o.ver)
      }
      case o:MessageDef => {
	DeleteObject(o.FullNameWithVer.toLowerCase,messageStore)
	MdMgr.GetMdMgr.RemoveMessage(o.nameSpace,o.name,o.ver)
      }
      case o:ContainerDef => {
	DeleteObject(o.FullNameWithVer.toLowerCase,containerStore)
	MdMgr.GetMdMgr.RemoveContainer(o.nameSpace,o.name,o.ver)
      }
      case o:AttributeDef => {
	DeleteObject(o.FullNameWithVer.toLowerCase,conceptStore)
	MdMgr.GetMdMgr.RemoveAttribute(o.nameSpace,o.name,o.ver)
      }
      case o:ScalarTypeDef => {
	DeleteObject(o.FullNameWithVer.toLowerCase,typeStore)
	MdMgr.GetMdMgr.RemoveType(o.nameSpace,o.name,o.ver)
      }
      case o:ArrayTypeDef => {
	DeleteObject(o.FullNameWithVer.toLowerCase,typeStore)
	MdMgr.GetMdMgr.RemoveType(o.nameSpace,o.name,o.ver)
      }
      case o:ArrayBufTypeDef => {
	DeleteObject(o.FullNameWithVer.toLowerCase,typeStore)
	MdMgr.GetMdMgr.RemoveType(o.nameSpace,o.name,o.ver)
      }
      case o:ListTypeDef => {
	DeleteObject(o.FullNameWithVer.toLowerCase,typeStore)
	MdMgr.GetMdMgr.RemoveType(o.nameSpace,o.name,o.ver)
      }
      case o:QueueTypeDef => {
	DeleteObject(o.FullNameWithVer.toLowerCase,typeStore)
	MdMgr.GetMdMgr.RemoveType(o.nameSpace,o.name,o.ver)
      }
      case o:SetTypeDef => {
	DeleteObject(o.FullNameWithVer.toLowerCase,typeStore)
	MdMgr.GetMdMgr.RemoveType(o.nameSpace,o.name,o.ver)
      }
      case o:TreeSetTypeDef => {
	DeleteObject(o.FullNameWithVer.toLowerCase,typeStore)
	MdMgr.GetMdMgr.RemoveType(o.nameSpace,o.name,o.ver)
      }
      case o:SortedSetTypeDef => {
	DeleteObject(o.FullNameWithVer.toLowerCase,typeStore)
	MdMgr.GetMdMgr.RemoveType(o.nameSpace,o.name,o.ver)
      }
      case o:MapTypeDef => {
	DeleteObject(o.FullNameWithVer.toLowerCase,typeStore)
	MdMgr.GetMdMgr.RemoveType(o.nameSpace,o.name,o.ver)
      }
      case o:HashMapTypeDef => {
	DeleteObject(o.FullNameWithVer.toLowerCase,typeStore)
	MdMgr.GetMdMgr.RemoveType(o.nameSpace,o.name,o.ver)
      }
      case o:TupleTypeDef => {
	DeleteObject(o.FullNameWithVer.toLowerCase,typeStore)
	MdMgr.GetMdMgr.RemoveType(o.nameSpace,o.name,o.ver)
      }
      case o:ContainerTypeDef => {
	DeleteObject(o.FullNameWithVer.toLowerCase,typeStore)
	MdMgr.GetMdMgr.RemoveType(o.nameSpace,o.name,o.ver)
      }
      case _ => {
	logger.error("DeleteObject is not implemented for objects of type " + obj.getClass.getName)
      }
    }
  }

  def GetObject(key: String,store: DataStore) : IStorage = {
    try{
      object o extends IStorage
      {
	var key = new com.ligadata.keyvaluestore.Key;
	var value = new com.ligadata.keyvaluestore.Value
	
	def Key = key
	def Value = value
	def Construct(k: com.ligadata.keyvaluestore.Key, v: com.ligadata.keyvaluestore.Value) = 
	{ 
	  key = k; 
	  value = v; 
	}
      } 
      
      var k = new com.ligadata.keyvaluestore.Key
      for(c <- key ){
	k += c.toByte
      }
      logger.trace("Get the object from store, key => " + KeyAsStr(k))
      store.get(k,o)
      //logger.trace("key => " + KeyAsStr(o.key) + ",value => " + ValueAsStr(o.value))
      o
    } catch {
      case e:Exception => {
	e.printStackTrace()
	throw new ObjectNotFoundException(e.getMessage())
      }
    }

  }

  def GetCassandraConnectionProperties: PropertyMap = {
    val connectinfo = new PropertyMap
    connectinfo+= ("connectiontype" -> "cassandra")
    connectinfo+= ("hostlist" -> "localhost") 
    connectinfo+= ("schema" -> "default")
    connectinfo+= ("table" -> "default")
    connectinfo+= ("ConsistencyLevelRead" -> "ONE")
    connectinfo
  }


  def GetMapDBConnectionProperties: PropertyMap = {
    val connectinfo = new PropertyMap
    connectinfo+= ("connectiontype" -> "hashmap")
    connectinfo+= ("path" -> "/tmp")
    connectinfo+= ("schema" -> "metadata")
    connectinfo+= ("table" -> "default")
    connectinfo
  }


  @throws(classOf[Json4sParsingException])
  @throws(classOf[ApiResultParsingException])
  def getApiResult(apiResultJson: String): (Int,String) = {
    // parse using Json4s
    try{
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(apiResultJson)
      //logger.trace("Parsed the json : " + apiResultJson)
      val apiResultInfo = json.extract[APIResultJsonProxy]
      (apiResultInfo.APIResults.statusCode,apiResultInfo.APIResults.resultData)
    } catch {
      case e:MappingException =>{
	e.printStackTrace()
	throw Json4sParsingException(e.getMessage())
      }
      case e:Exception => {
	e.printStackTrace()
	throw new ApiResultParsingException(e.getMessage())
      }
    }
  }

  @throws(classOf[CreateStoreFailedException])
  def GetDataStoreHandle(storeType:String, storeName:String,tableName:String) : DataStore = {
    try{
      var connectinfo = new PropertyMap
      connectinfo+= ("connectiontype" -> storeType)
      connectinfo+= ("table" -> tableName)
      storeType match{
	case "hashmap" => {
	  connectinfo+= ("path" -> "/tmp")
	  connectinfo+= ("schema" -> storeName)
	  connectinfo+= ("inmemory" -> "false")
	  connectinfo+= ("withtransaction" -> "true")
	}
	case "treemap" => {
	  connectinfo+= ("path" -> "/tmp")
	  connectinfo+= ("schema" -> storeName)
	  connectinfo+= ("inmemory" -> "false")
	  connectinfo+= ("withtransaction" -> "true")
	}
	case "cassandra" => {
	  connectinfo+= ("hostlist" -> "localhost") 
	  connectinfo+= ("schema" -> "metadata")
	  connectinfo+= ("ConsistencyLevelRead" -> "ONE")
	}
	case _ => {
	  throw new CreateStoreFailedException("The database type " + storeType + " is not supported yet ")
	}
      }
      KeyValueManager.Get(connectinfo)
    }catch{
      case e:Exception => {
	e.printStackTrace()
	throw new CreateStoreFailedException(e.getMessage())
      }
    }
  }
      

  @throws(classOf[CreateStoreFailedException])
  def OpenDbStore(storeType:String) {
    try{
      logger.info("Opening datastore")
      modelStore     = GetDataStoreHandle(storeType,"model_store","models")
      messageStore   = GetDataStoreHandle(storeType,"message_store","messages")
      containerStore = GetDataStoreHandle(storeType,"container_store","containers")
      functionStore  = GetDataStoreHandle(storeType,"function_store","functions")
      conceptStore   = GetDataStoreHandle(storeType,"concept_store","concepts")
      typeStore      = GetDataStoreHandle(storeType,"type_store","types")
      otherStore     = GetDataStoreHandle(storeType,"other_store","others")
    }catch{
      case e:CreateStoreFailedException => {
	e.printStackTrace()
	throw new CreateStoreFailedException(e.getMessage())
      }
      case e:Exception => {
	e.printStackTrace()
	throw new CreateStoreFailedException(e.getMessage())
      }
    }
  }

  def CloseDbStore {
    try{
      logger.info("Closing datastore")
      modelStore.Shutdown()
      messageStore.Shutdown()
      containerStore.Shutdown()
      functionStore.Shutdown()
      conceptStore.Shutdown()
      typeStore.Shutdown()
      otherStore.Shutdown()
    }catch{
      case e:Exception => {
	throw e;
      }
    }
  }

  def AddType(typeText:String, format:String): String = {
    try{
      val typ = JsonSerializer.parseType(typeText,"JSON")
      SaveObject(typ)
      var apiResult = new ApiResult(0,"Type was Added",typeText)
      apiResult.toString()
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to add a Type:",e.toString)
	apiResult.toString()
      }
    }   
  }

  def DumpTypeDef(typeDef: ScalarTypeDef){
    logger.trace("NameSpace => " + typeDef.nameSpace)
    logger.trace("Name => " + typeDef.name)
    logger.trace("Version => " + typeDef.ver)
    logger.trace("Description => " + typeDef.description)
    logger.trace("Implementation class name => " + typeDef.implementationNm)
    logger.trace("Implementation jar name => " + typeDef.jarName)
  }


  def AddType(typeDef: BaseTypeDef): String = {
    try{
      var key = typeDef.FullNameWithVer
      var value = JsonSerializer.SerializeObjectToJson(typeDef);
      logger.trace("key => " + key + ",value =>" + value);
      SaveObject(typeDef)
      var apiResult = new ApiResult(0,"Type was Added",value)
      apiResult.toString()
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to add a Type:",e.toString)
	apiResult.toString()
      }
    }   
  }

  def AddTypes(typesText:String, format:String): String = {
    try{
      if( format != "JSON" ){
	var apiResult = new ApiResult(0,"Not Implemented Yet","No Result")
	apiResult.toString()
      }
      else{
	var typeList = JsonSerializer.parseTypeList(typesText,"JSON")
	if (typeList.length  > 0) {
	  logger.trace("Found " + typeList.length + " type objects ")
	  typeList.foreach(typ => { 
	    SaveObject(typ) 
	    logger.trace("Type object name => " + typ.FullNameWithVer)
	  })
	  var apiResult = new ApiResult(0,"Types Are Added",typesText)
	  apiResult.toString()
	}
	else{
	  var apiResult = new ApiResult(0,"No Type objects are available","Couldn't find any Type Objects")
	  apiResult.toString()
	}	  
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to add Types: " + typesText,e.getMessage())
	apiResult.toString()
      }
    }
  }

  // Remove type for given TypeName and Version
  def RemoveType(typeNameSpace:String, typeName:String, version:Int): String = {
    try{
      var key = typeNameSpace + "." + typeName + "." + version.toString
      DeleteObject(key,typeStore)
      MdMgr.GetMdMgr.RemoveType(typeNameSpace,typeName,version)
      var apiResult = new ApiResult(0,"Type was Deleted",key)
      apiResult.toString()
    }catch {
      case e:ObjectNolongerExistsException =>{
	var apiResult = new ApiResult(-1,"Failed to delete the Type:",e.toString)
	apiResult.toString()
      }
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to delete the Type:",e.toString)
	apiResult.toString()
      }
    }
  }

  def UpdateType(typeJson:String, format:String): String = {
    var typeDef:BaseTypeDef = null
    try{
      implicit val jsonFormats: Formats = DefaultFormats
      typeDef = JsonSerializer.parseType(typeJson,"JSON")
      try{
	AddType(typeDef)
      }catch{
	case e:AlreadyExistsException => {
	  typeDef.ver += 1
	  AddType(typeDef)
	}
      }
      var apiResult = new ApiResult(0,"Type was updated",typeJson)
      apiResult.toString()
    } catch {
      case e:MappingException =>{
	logger.trace("Failed to parse the type, json => " + typeJson  + ",Error => " + e.getMessage())
	var apiResult = new ApiResult(-1,"Parsing Error: " + e.getMessage(),typeJson)
	apiResult.toString()
      }
      case e:AlreadyExistsException => {
	logger.trace("Failed to update the type, json => " + typeJson  + ",Error => " + e.getMessage())
	var apiResult = new ApiResult(-1,"Error: " + e.getMessage(),typeJson)
	apiResult.toString()
      }
      case e:Exception => {
	logger.trace("Failed to up the type, json => " + typeJson  + ",Error => " + e.getMessage())
	var apiResult = new ApiResult(-1,"Error: " + e.getMessage(),typeJson)
	apiResult.toString()
      }
    }
  }

  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  // Upload Jars into system. Dependency jars may need to upload first. Once we upload the jar, if we retry to upload it will throw an exception.
  def UploadImplementation(implPath:String): String = {
    var apiResult = new ApiResult(0,"Not Implemented Yet","No Result")
    apiResult.toString()
  }

  def DumpAttributeDef(attrDef: AttributeDef){
    logger.trace("NameSpace => " + attrDef.nameSpace)
    logger.trace("Name => " + attrDef.name)
    logger.trace("Type => " + attrDef.typeString)
  }

  def AddConcept(attributeDef: BaseAttributeDef): String = {
    val key = attributeDef.FullNameWithVer
    try{
      SaveObject(attributeDef)
      var apiResult = new ApiResult(0,"Concept was Added",key)
      apiResult.toString()
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to add the Concept," + key + ":",e.toString)
	apiResult.toString()
      }
    }
  }

  def RemoveConcept(concept: AttributeDef): String = {
    try{
      var key = concept.nameSpace + ":" + concept.name
      DeleteObject(concept)
      var apiResult = new ApiResult(0,"Concept was Deleted",key)
      apiResult.toString()
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to delete the Concept:",e.toString)
	apiResult.toString()
      }
    }
  }

  def RemoveConcept(key:String): String = {
    try{
      val c = MdMgr.GetMdMgr.Attributes(key,false,false)
      c match{
	case None => None
	  logger.trace("No concepts found ")
	  var apiResult = new ApiResult(-1,"Failed to Remove concepts","No Concepts Available")
	  apiResult.toString()
	case Some(cs) => 
	  val conceptArray = cs.toArray
	  conceptArray.foreach(concept => { DeleteObject(concept) })
	  var apiResult = new ApiResult(0,"Successfully Removed concept",JsonSerializer.SerializeObjectListToJson(conceptArray))
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to Remove the concept:",e.toString)
	apiResult.toString()
      }
    }
  }

  def AddFunction(functionDef: FunctionDef): String = {
    try{
      val key   = functionDef.FullNameWithVer
      val value = JsonSerializer.SerializeObjectToJson(functionDef)

      logger.trace("key => " + key + ",value =>" + value);
      SaveObject(functionDef)
      logger.trace("Added function " + key + " successfully ")
      val apiResult = new ApiResult(0,"Function was Added",value)
      apiResult.toString()
    }catch {
      case e:Exception =>{
	val apiResult = new ApiResult(-1,"Failed to add the functionDef:",e.toString)
	apiResult.toString()
      }
    }
  }

  def RemoveFunction(functionDef: FunctionDef): String = {
    try{
      var key = functionDef.typeString
      DeleteObject(functionDef)
      var apiResult = new ApiResult(0,"Function was Deleted",key)
      apiResult.toString()
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to delete the Function:",e.toString)
	apiResult.toString()
      }
    }
  }

  def RemoveFunction(nameSpace:String, functionName:String, version:Int): String = {
    try{
      var key = functionName + ":" + version
      DeleteObject(key,functionStore)
      var apiResult = new ApiResult(0,"Function was Deleted",key)
      apiResult.toString()
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to delete the Function:",e.toString)
	apiResult.toString()
      }
    }
  }

  def DumpFunctionDef(funcDef: FunctionDef){
    logger.trace("Name => " + funcDef.Name)
    for( arg <- funcDef.args ){
      logger.trace("arg_name => " + arg.name)
      logger.trace("arg_type => " + arg.Type.tType)
    }
    logger.trace("Json string => " + JsonSerializer.SerializeObjectToJson(funcDef))
  }

  def UpdateFunction(functionDef: FunctionDef): String = {
    val key = functionDef.typeString
    try{
      if( IsFunctionAlreadyExists(functionDef) ){
	functionDef.ver = functionDef.ver + 1
      }
      AddFunction(functionDef)
      var apiResult = new ApiResult(0,"Function was updated",key)
      apiResult.toString()
    } catch {
      case e:AlreadyExistsException => {
	logger.trace("Failed to update the function, key => " + key  + ",Error => " + e.getMessage())
	var apiResult = new ApiResult(-1,"Error: " + e.getMessage(),key)
	apiResult.toString()
      }
      case e:Exception => {
	logger.trace("Failed to up the type, json => " + key  + ",Error => " + e.getMessage())
	var apiResult = new ApiResult(-1,"Error: " + e.getMessage(),key)
	apiResult.toString()
      }
    }
  }

  def AddFunctions(functionsText:String, format:String): String = {
    try{
      if( format != "JSON" ){
	var apiResult = new ApiResult(0,"Not Implemented Yet","No Result")
	apiResult.toString()
      }
      else{
	var funcList = JsonSerializer.parseFunctionList(functionsText,"JSON")
	funcList.foreach(func => { 
	  SaveObject(func) 
	})
	var apiResult = new ApiResult(0,"Functions Are Added",functionsText)
	apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to add Functions: " + e.getMessage(),"FAILED")
	apiResult.toString()
      }
    }
  }


  def AddFunction(functionText:String, format:String): String = {
    try{
      if( format != "JSON" ){
	var apiResult = new ApiResult(0,"Not Implemented Yet","No Result")
	apiResult.toString()
      }
      else{
	var func = JsonSerializer.parseFunction(functionText,"JSON")
	SaveObject(func) 
	var apiResult = new ApiResult(0,"Function is Added",functionText)
	apiResult.toString()
      }
    } catch {
      case e:MappingException =>{
	logger.trace("Failed to parse the function, json => " + functionText  + ",Error => " + e.getMessage())
	var apiResult = new ApiResult(-1,"Parsing Error: " + e.getMessage(),functionText)
	apiResult.toString()
      }
      case e:AlreadyExistsException => {
	logger.trace("Failed to add the function, json => " + functionText  + ",Error => " + e.getMessage())
	var apiResult = new ApiResult(-1,"Error: " + e.getMessage(),functionText)
	apiResult.toString()
      }
      case e:Exception => {
	logger.trace("Failed to up the function, json => " + functionText  + ",Error => " + e.getMessage())
	var apiResult = new ApiResult(-1,"Error: " + e.getMessage(),functionText)
	apiResult.toString()
      }
    }
  }
  
  def UpdateFunctions(functionsText:String, format:String): String = {
    try{
      if( format != "JSON" ){
	var apiResult = new ApiResult(0,"Not Implemented Yet","No Result")
	apiResult.toString()
      }
      else{
	var funcList = JsonSerializer.parseFunctionList(functionsText,"JSON")
	funcList.foreach(func => { 
	  UpdateFunction(func) 
	})
	var apiResult = new ApiResult(0,"Functions Are Added",functionsText)
	apiResult.toString()
      }
    }catch {
      case e:MappingException =>{
	logger.trace("Failed to parse the function, json => " + functionsText  + ",Error => " + e.getMessage())
	var apiResult = new ApiResult(-1,"Parsing Error: " + e.getMessage(),functionsText)
	apiResult.toString()
      }
      case e:AlreadyExistsException => {
	logger.trace("Failed to add the function, json => " + functionsText  + ",Error => " + e.getMessage())
	var apiResult = new ApiResult(-1,"Error: " + e.getMessage(),functionsText)
	apiResult.toString()
      }
      case e:Exception => {
	logger.trace("Failed to up the function, json => " + functionsText  + ",Error => " + e.getMessage())
	var apiResult = new ApiResult(-1,"Error: " + e.getMessage(),functionsText)
	apiResult.toString()
      }
    }
  }


  def AddConcepts(conceptsText:String, format:String): String = {
    try{
      if( format != "JSON" ){
	var apiResult = new ApiResult(0,"Not Implemented Yet","No Result")
	apiResult.toString()
      }
      else{
	  var conceptList = JsonSerializer.parseConceptList(conceptsText,format)
	  conceptList.foreach(concept => { 
	    SaveObject(concept) 
	  })
	  var apiResult = new ApiResult(0,"Concepts Are Added",conceptsText)
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to add concets: " + e.getMessage(),"FAILED")
	apiResult.toString()
      }
    }
  }

  def UpdateConcept(concept: BaseAttributeDef): String = {
    val key = concept.FullNameWithVer
    try{
      if( IsConceptAlreadyExists(concept) ){
	concept.ver = concept.ver + 1
      }
      AddConcept(concept)
      var apiResult = new ApiResult(0,"Concept was updated",key)
      apiResult.toString()
    } catch {
      case e:AlreadyExistsException => {
	logger.trace("Failed to update the concept, key => " + key  + ",Error => " + e.getMessage())
	var apiResult = new ApiResult(-1,"Error: " + e.getMessage(),key)
	apiResult.toString()
      }
      case e:Exception => {
	logger.trace("Failed to update the concept, key => " + key  + ",Error => " + e.getMessage())
	var apiResult = new ApiResult(-1,"Error: " + e.getMessage(),key)
	apiResult.toString()
      }
    }
  }

  def UpdateConcepts(conceptsText:String, format:String): String = {
    try{
      if( format != "JSON" ){
	var apiResult = new ApiResult(0,"Not Implemented Yet","No Result")
	apiResult.toString()
      }
      else{
	var conceptList = JsonSerializer.parseConceptList(conceptsText,"JSON")
	conceptList.foreach(concept => { 
	  UpdateConcept(concept) 
	})
	var apiResult = new ApiResult(0,"Concepts Are updated",conceptsText)
	apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to update Concepts: " + e.getMessage(),"FAILED")
	apiResult.toString()
      }
    }
  }


  // RemoveConcepts take all concepts names to be removed as an Array
  def RemoveConcepts(concepts:Array[String]): String = {
    val json = ("ConceptList" -> concepts.toList)
    val jsonStr = pretty(render(json))
    try{
      concepts.foreach(c => { RemoveConcept(c) })
	var apiResult = new ApiResult(0,"Concepts Are Removed",jsonStr)
	apiResult.toString()
    }catch{
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to remove Concepts: " + e.getMessage(),jsonStr)
	apiResult.toString()
      }
    }
  }

  def AddContainerDef(contDef: ContainerDef): String = {
    try{
      var key = contDef.FullNameWithVer
      var value = JsonSerializer.SerializeObjectToJson(contDef)

      logger.trace("key => " + key + ",value =>" + value);

      SaveObject(contDef)
      var apiResult = new ApiResult(0,"ContDef was Added",value)
      apiResult.toString()
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to add the contDef:",e.toString)
	apiResult.toString()
      }
    }
  }

  def AddMessageDef(msgDef: MessageDef): String = {
    try{
      var key = msgDef.FullNameWithVer
      var value = JsonSerializer.SerializeObjectToJson(msgDef)

      logger.trace("key => " + key + ",value =>" + value);

      SaveObject(msgDef)
      var apiResult = new ApiResult(0,"MsgDef was Added",value)
      apiResult.toString()
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to add the msgDef:",e.toString)
	apiResult.toString()
      }
    }
  }


  def AddMessageDef1(msgDef: MessageDef): String = {
    try{
      var key = msgDef.FullNameWithVer
      var value = serializer.SerializeObjectToByteArray(msgDef)

      //logger.trace("key => " + key + ",value =>" + value);

      SaveSerializedObject(key,value,messageStore)
      MdMgr.GetMdMgr.AddMsg(msgDef)
      var apiResult = new ApiResult(0,"MsgDef was Added",key)
      apiResult.toString()
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to add the msgDef:",e.toString)
	apiResult.toString()
      }
    }
  }

  def AddMessage(messageText:String, format:String): String = {
    try{
      var compProxy = new CompilerProxy
      compProxy.setLoggerLevel(Level.TRACE)
      val(classStr,msgDef) = compProxy.compileMessageDef(messageText)
      msgDef match{
	case msg:MessageDef =>{
	  logger.trace(JsonSerializer.SerializeObjectToJson(msg))
	  AddMessageDef1(msg)
	}
	case cont:ContainerDef =>{
	  logger.trace(JsonSerializer.SerializeObjectToJson(cont))
	  AddContainerDef(cont)
	}
      }
    }
    catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to compile the msgDef:",e.toString)
	apiResult.toString()
      }
    }
  }

  def AddContainer(containerText:String, format:String): String = {
    try{
      var compProxy = new CompilerProxy
      compProxy.setLoggerLevel(Level.TRACE)
      val(classStr,msgDef) = compProxy.compileMessageDef(containerText)
      msgDef match{
	case msg:MessageDef =>{
	  logger.trace(JsonSerializer.SerializeObjectToJson(msg))
	  AddMessageDef(msg)
	}
	case cont:ContainerDef =>{
	  logger.trace(JsonSerializer.SerializeObjectToJson(cont))
	  AddContainerDef(cont)
	}
      }
    }
    catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to compile the containerDef:",e.toString)
	apiResult.toString()
      }
    }
  }

  def UpdateMessage(messageText:String, format:String): String = {
    try{
      var compProxy = new CompilerProxy
      compProxy.setLoggerLevel(Level.TRACE)
      val(classStr,msgDef) = compProxy.compileMessageDef(messageText)
      val key = msgDef.FullNameWithVer
      msgDef match{
	case msg:MessageDef =>{
	  if( IsMessageAlreadyExists(msg) ){
	    msg.ver = msg.ver + 1
	  }
	  AddMessageDef(msg)
	}
	case cont:ContainerDef =>{
	  if( IsContainerAlreadyExists(cont) ){
	    cont.ver = cont.ver + 1
	  }
	  AddContainerDef(cont)
	}
      }
      var apiResult = new ApiResult(0,"Container Definition was Updated",key)
      apiResult.toString()
    }
    catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to update the msgDef:",e.toString)
	apiResult.toString()
      }
    }
  }

  // Remove container with Container Name and Version Number
  def RemoveContainer(nameSpace:String,containerName:String, version:Int): String = {
    try{
      var key = nameSpace + "." + containerName + "." + version
      DeleteObject(key.toLowerCase,containerStore)
      MdMgr.GetMdMgr.RemoveContainer(nameSpace,containerName,version)
      var apiResult = new ApiResult(0,"Container Definition was Deleted",key)
      apiResult.toString()
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to delete the ContainerDef:",e.toString)
	apiResult.toString()
      }
    }
  }

  // Remove message with Message Name and Version Number
  def RemoveMessage(nameSpace:String,messageName:String, version:Int): String = {
    try{
      var key = nameSpace + "." + messageName + "." + version
      DeleteObject(key.toLowerCase,messageStore)
      MdMgr.GetMdMgr.RemoveMessage(nameSpace,messageName,version)
      var apiResult = new ApiResult(0,"Message Definition was Deleted",key)
      apiResult.toString()
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to delete the MessageDef:",e.toString)
	apiResult.toString()
      }
    }
  }

  // Remove message with Message Name and Version Number
  def RemoveMessage(messageName:String, version:Int): String = {
    RemoveMessage(sysNS,messageName,version)
  }


  // Remove model with Model Name and Version Number
  def RemoveModel(nameSpace:String, modelName:String, version:Int): String = {
    try{
      var key = nameSpace + "." + modelName + "." + version
      DeleteObject(key.toLowerCase,modelStore)
      MdMgr.GetMdMgr.RemoveModel(nameSpace,modelName,version)
      var apiResult = new ApiResult(0,"Model Definition was Deleted",key)
      apiResult.toString()
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to delete the ModelDef:",e.toString)
	apiResult.toString()
      }
    }
  }

  // Remove model with Model Name and Version Number
  def RemoveModel(modelName:String, version:Int): String = {
    RemoveModel(sysNS,modelName,version)
  }

  // Add Model (model def)
  def AddModel(model: ModelDef): String = {
    try{
      var key = model.FullNameWithVer
      var value = JsonSerializer.SerializeObjectToJson(model)

      //logger.trace("key => " + key + ",value =>" + value);

      SaveObject(model)

      //Notify(model)
      var apiResult = new ApiResult(0,"Model was Added",value)
      apiResult.toString()
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to add the model:",e.toString)
	apiResult.toString()
      }
    }
  }

  // Add Model (format XML)
  def AddModel(pmmlText:String): String = {
    try{
      var compProxy = new CompilerProxy
      compProxy.setLoggerLevel(Level.TRACE)
      var(classStr,modDef) = compProxy.compilePmml(pmmlText)
      AddModel(modDef)
    }
    catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to add the model:",e.toString)
	apiResult.toString()
      }
    }
  }

  def UpdateModel(pmmlText:String): String = {
    try{
      var compProxy = new CompilerProxy
      compProxy.setLoggerLevel(Level.TRACE)
      var(classStr,modDef) = compProxy.compilePmml(pmmlText)
      if( IsModelAlreadyExists(modDef) ){
	modDef.ver = modDef.ver + 1
      }
      AddModel(modDef)
    }
    catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to update the model:",e.toString)
	apiResult.toString()
      }
    }
  }

  // All available models(format JSON or XML) as a String
  def GetAllModelDefs(formatType:String) : String = {
    try{
      val modDefs = MdMgr.GetMdMgr.Models(true,true)
      modDefs match{
	case None => None
	  logger.trace("No Models found ")
	  var apiResult = new ApiResult(-1,"Failed to Fetch models","No Models Available")
	  apiResult.toString()
	case Some(ms) => 
	  val msa = ms.toArray
	  var apiResult = new ApiResult(0,"Successfully Fetched all models",JsonSerializer.SerializeObjectListToJson("Models",msa))
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to fetch all the models:",e.toString)
	apiResult.toString()
      }
    }
  }


  // All available messages(format JSON or XML) as a String
  def GetAllMessageDefs(formatType:String) : String = {
    try{
      val msgDefs = MdMgr.GetMdMgr.Messages(true,true)
      msgDefs match{
	case None => None
	  logger.trace("No Messages found ")
	  var apiResult = new ApiResult(-1,"Failed to Fetch messages","No Messages Available")
	  apiResult.toString()
	case Some(ms) => 
	  val msa = ms.toArray
	  var apiResult = new ApiResult(0,"Successfully Fetched all messages",JsonSerializer.SerializeObjectListToJson("Messages",msa))
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to fetch all the messages:",e.toString)
	apiResult.toString()
      }
    }
  }


  def GetAllModelsFromCache : Array[String] = {
    var modelList: Array[String] = new Array[String](0)
    try{
      val modDefs = MdMgr.GetMdMgr.Models(true,true)
      modDefs match{
	case None => None
	  logger.trace("No Models found ")
	  modelList
	case Some(ms) => 
	  val msa = ms.toArray
	  val modCount = msa.length
	  modelList = new Array[String](modCount)
	  for( i <- 0 to modCount - 1){
	    modelList(i) = msa(i).FullNameWithVer
	  }
	  modelList
      }
    }catch {
      case e:Exception =>{
	e.printStackTrace()
	throw new UnexpectedMetadataAPIException("Failed to fetch all the models:" + e.toString)
      }
    }
  }

  def GetAllMessagesFromCache : Array[String] = {
    var messageList: Array[String] = new Array[String](0)
    try{
      val msgDefs = MdMgr.GetMdMgr.Messages(true,true)
      msgDefs match{
	case None => None
	  logger.trace("No Messages found ")
	  messageList
	case Some(ms) => 
	  val msa = ms.toArray
	  val msgCount = msa.length
	  messageList = new Array[String](msgCount)
	  for( i <- 0 to msgCount - 1){
	    messageList(i) = msa(i).FullNameWithVer
	  }
	  messageList
      }
    }catch {
      case e:Exception =>{
	e.printStackTrace()
	throw new UnexpectedMetadataAPIException("Failed to fetch all the messages:" + e.toString)
      }
    }
  }

  def GetAllContainersFromCache : Array[String] = {
    var containerList: Array[String] = new Array[String](0)
    try{
      val contDefs = MdMgr.GetMdMgr.Containers(true,true)
      contDefs match{
	case None => None
	  logger.trace("No Containers found ")
	  containerList
	case Some(ms) => 
	  val msa = ms.toArray
	  val contCount = msa.length
	  containerList = new Array[String](contCount)
	  for( i <- 0 to contCount - 1){
	    containerList(i) = msa(i).FullNameWithVer
	  }
	  containerList
      }
    }catch {
      case e:Exception =>{
	e.printStackTrace()
	throw new UnexpectedMetadataAPIException("Failed to fetch all the containers:" + e.toString)
      }
    }
  }

  // Specific models (format JSON or XML) as an array of strings using modelName(without version) as the key
  def GetModelDef(nameSpace:String, objectName:String,formatType:String) : String = {
    try{
      val modDefs = MdMgr.GetMdMgr.Models(nameSpace,objectName,true,true)
      modDefs match{
	case None => None
	  logger.trace("No Models found ")
	  var apiResult = new ApiResult(-1,"Failed to Fetch models","No Models Available")
	  apiResult.toString()
	case Some(ms) => 
	  val msa = ms.toArray
	  var apiResult = new ApiResult(0,"Successfully Fetched all models",JsonSerializer.SerializeObjectListToJson("Models",msa))
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to fetch all the models:",e.toString)
	apiResult.toString()
      }
    }
  }


  // Specific models (format JSON or XML) as an array of strings using modelName(without version) as the key
  def GetModelDef(objectName:String,formatType:String) : String = {
      GetModelDef(sysNS,objectName,formatType)
  }

  // Specific model (format JSON or XML) as a String using modelName(with version) as the key
  def GetModelDefFromCache(nameSpace:String,name:String,formatType:String,version:String) : String = {
    try{
      var key = nameSpace + "." + name + "." + version
      val o = MdMgr.GetMdMgr.Model(nameSpace.toLowerCase,name.toLowerCase,version.toInt,true)
      o match{
	case None => None
	  logger.trace("model not found => " + key)
	  var apiResult = new ApiResult(-1,"Failed to Fetch the model",key)
	  apiResult.toString()
	case Some(m) => 
	  logger.trace("model found => " + m.asInstanceOf[ModelDef].FullNameWithVer)
	  var apiResult = new ApiResult(0,"Successfully Fetched the model",JsonSerializer.SerializeObjectToJson(m))
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to fetch the model:",e.toString)
	apiResult.toString()
      }
    }
  }


  // Specific message (format JSON or XML) as a String using messageName(with version) as the key
  def GetMessageDefFromCache(nameSpace:String, name:String,formatType:String,version:String) : String = {
    try{
      var key = nameSpace + "." + name + "." + version
      val o = MdMgr.GetMdMgr.Message(nameSpace.toLowerCase,name.toLowerCase,version.toInt,true)
      o match{
	case None => None
	  logger.trace("message not found => " + key)
	  var apiResult = new ApiResult(-1,"Failed to Fetch the message",key)
	  apiResult.toString()
	case Some(m) => 
	  logger.trace("message found => " + m.asInstanceOf[MessageDef].FullNameWithVer)
	  var apiResult = new ApiResult(0,"Successfully Fetched the message",JsonSerializer.SerializeObjectToJson(m))
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to fetch the message:",e.toString)
	apiResult.toString()
      }
    }
  }

  // Specific container (format JSON or XML) as a String using containerName(with version) as the key
  def GetContainerDefFromCache(nameSpace:String,name:String,formatType:String,version:String) : String = {
    try{
      var key = nameSpace + "." + name + "." + version
      val o = MdMgr.GetMdMgr.Container(nameSpace.toLowerCase,name.toLowerCase,version.toInt,true)
      o match{
	case None => None
	  logger.trace("container not found => " + key)
	  var apiResult = new ApiResult(-1,"Failed to Fetch the container",key)
	  apiResult.toString()
	case Some(m) => 
	  logger.trace("container found => " + m.asInstanceOf[ContainerDef].FullNameWithVer)
	  var apiResult = new ApiResult(0,"Successfully Fetched the container",JsonSerializer.SerializeObjectToJson(m))
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to fetch the container:",e.toString)
	apiResult.toString()
      }
    }
  }


  // Return Specific messageDef object using messageName(with version) as the key
  @throws(classOf[ObjectNotFoundException])
  def GetMessageDefInstanceFromCache(nameSpace:String, name:String,formatType:String,version:String) : MessageDef = {
    var key = nameSpace + "." + name + "." + version
    try{
      val o = MdMgr.GetMdMgr.Message(nameSpace.toLowerCase,name.toLowerCase,version.toInt,true)
      o match{
	case None => None
	  logger.trace("message not found => " + key)
	  throw new ObjectNotFoundException("Failed to Fetch the message:" + key)
	case Some(m) => 
	  m.asInstanceOf[MessageDef]
      }
    }catch {
      case e:Exception =>{
	throw new ObjectNotFoundException("Failed to Fetch the message:" + key + ":" + e.getMessage())
      }
    }
  }


  // check whether model already exists in metadata manager. Ideally,
  // we should never add the model into metadata manager more than once
  // and there is no need to use this function in main code flow
  // This is just a utility function being during these initial phases
  def IsModelAlreadyExists(modDef: ModelDef) : Boolean = {
    try{
      var key = modDef.nameSpace + "." + modDef.name + "." + modDef.ver
      val o = MdMgr.GetMdMgr.Model(modDef.nameSpace.toLowerCase,
				   modDef.name.toLowerCase,
				   modDef.ver,
				   false)
      o match{
	case None => None
	  logger.trace("model not in the cache => " + key)
	  return false;
	case Some(m) => 
	  logger.trace("model found => " + m.asInstanceOf[ModelDef].FullNameWithVer)
	  return true
      }
    }catch {
      case e:Exception =>{
	e.printStackTrace()
	throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }


  // check whether message already exists in metadata manager. Ideally,
  // we should never add the message into metadata manager more than once
  // and there is no need to use this function in main code flow
  // This is just a utility function being during these initial phases
  def IsMessageAlreadyExists(msgDef: MessageDef) : Boolean = {
    try{
      var key = msgDef.nameSpace + "." + msgDef.name + "." + msgDef.ver
      val o = MdMgr.GetMdMgr.Message(msgDef.nameSpace.toLowerCase,
				   msgDef.name.toLowerCase,
				   msgDef.ver,
				   false)
      o match{
	case None => None
	  logger.trace("message not in the cache => " + key)
	  return false;
	case Some(m) => 
	  logger.trace("message found => " + m.asInstanceOf[MessageDef].FullNameWithVer)
	  return true
      }
    }catch {
      case e:Exception =>{
	e.printStackTrace()
	throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }


  def IsContainerAlreadyExists(contDef: ContainerDef) : Boolean = {
    try{
      var key = contDef.nameSpace + "." + contDef.name + "." + contDef.ver
      val o = MdMgr.GetMdMgr.Container(contDef.nameSpace.toLowerCase,
				   contDef.name.toLowerCase,
				   contDef.ver,
				   false)
      o match{
	case None => None
	  logger.trace("container not in the cache => " + key)
	  return false;
	case Some(m) => 
	  logger.trace("container found => " + m.asInstanceOf[ContainerDef].FullNameWithVer)
	  return true
      }
    }catch {
      case e:Exception =>{
	e.printStackTrace()
	throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }


  def IsFunctionAlreadyExists(funcDef: FunctionDef) : Boolean = {
    try{
      var key = funcDef.typeString
      val o = MdMgr.GetMdMgr.Function(funcDef.nameSpace,
				      funcDef.name,
				      funcDef.args.toList.map(a => (a.aType.nameSpace,a.aType.name)),
				      funcDef.ver,
				      false)
      o match{
	case None => None
	  logger.trace("function not in the cache => " + key)
	  return false;
	case Some(m) => 
	  logger.trace("function found => " + m.asInstanceOf[FunctionDef].FullNameWithVer)
	  return true
      }
    }catch {
      case e:Exception =>{
	e.printStackTrace()
	throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }


  def IsConceptAlreadyExists(attrDef: BaseAttributeDef) : Boolean = {
    try{
      var key = attrDef.nameSpace + "." + attrDef.name + "." + attrDef.ver
      val o = MdMgr.GetMdMgr.Attribute(attrDef.nameSpace,
				      attrDef.name,
				      attrDef.ver,
				      false)
      o match{
	case None => None
	  logger.trace("concept not in the cache => " + key)
	  return false;
	case Some(m) => 
	  logger.trace("concept found => " + m.asInstanceOf[AttributeDef].FullNameWithVer)
	  return true
      }
    }catch {
      case e:Exception =>{
	e.printStackTrace()
	throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }


  def IsTypeAlreadyExists(typeDef: BaseTypeDef) : Boolean = {
    try{
      var key = typeDef.nameSpace + "." + typeDef.name + "." + typeDef.ver
      val o = MdMgr.GetMdMgr.Type(typeDef.nameSpace,
				      typeDef.name,
				      typeDef.ver,
				      false)
      o match{
	case None => None
	  logger.trace("Type not in the cache => " + key)
	  return false;
	case Some(m) => 
	  logger.trace("Type found => " + m.asInstanceOf[BaseTypeDef].FullNameWithVer)
	  return true
      }
    }catch {
      case e:Exception =>{
	e.printStackTrace()
	throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }


  // Specific message (format JSON or XML) as a String using messageName(with version) as the key
  def GetModelDef(nameSpace:String, objectName:String,formatType:String,version:String) : String  = {
    try{
      var key = nameSpace + '.' + objectName + "." + version
      var obj = GetObject(key.toLowerCase,modelStore)
      var apiResult = new ApiResult(0,"Model definition was Fetched",ValueAsStr(obj.Value))
      apiResult.toString()
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to fetch the Model Def:",e.toString)
	apiResult.toString()
      }
    }
  }

  def GetAllKeys(objectType: String) : Array[com.ligadata.keyvaluestore.Key] = {
    var keys = scala.collection.mutable.Set[com.ligadata.keyvaluestore.Key]()
    objectType match{
      case "TypeDef" => {
	typeStore.getAllKeys( {(key : Key) => keys.add(key) } )
      }
      case "FunctionDef" => {
	functionStore.getAllKeys( {(key : Key) => keys.add(key) } )
      }
      case "MessageDef" => {
	messageStore.getAllKeys( {(key : Key) => keys.add(key) } )
      }
      case "ContainerDef" => {
	containerStore.getAllKeys( {(key : Key) => keys.add(key) } )
      }
      case "Concept" => {
	conceptStore.getAllKeys( {(key : Key) => keys.add(key) } )
      }
      case "ModelDef" => {
	modelStore.getAllKeys( {(key : Key) => keys.add(key) } )
      }
      case _ => {
	logger.error("Unknown object type " + objectType + " in GetAllKeys function")
      }
    }
    keys.toArray
  }
    
  def LoadAllTypesIntoCache{
    try{
      val typeKeys = GetAllKeys("TypeDef")
      if( typeKeys.length == 0 ){
	logger.trace("Sorry, No types available in the Metadata")
	return
      }
      typeKeys.foreach(key => { 
	val typeKey = KeyAsStr(key)
	val obj = GetObject(typeKey.toLowerCase,typeStore)
	val typ = JsonSerializer.parseType(ValueAsStr(obj.Value),"JSON")
	if( typ != null ){
	  SaveObject(typ)
	}
      })
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def LoadAllConceptsIntoCache{
    try{
      val conceptKeys = GetAllKeys("Concept")
      if( conceptKeys.length == 0 ){
	logger.trace("Sorry, No concepts available in the Metadata")
	return
      }
      conceptKeys.foreach(key => { 
	val conceptKey = KeyAsStr(key)
	val obj = GetObject(conceptKey.toLowerCase,conceptStore)
	val concept = JsonSerializer.parseConcept(ValueAsStr(obj.Value),"JSON")
	SaveObject(concept)
      })
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def LoadAllFunctionsIntoCache{
    try{
      val functionKeys = GetAllKeys("FunctionDef")
      if( functionKeys.length == 0 ){
	logger.trace("Sorry, No functions available in the Metadata")
	return
      }
      functionKeys.foreach(key => { 
	val functionKey = KeyAsStr(key)
	val obj = GetObject(functionKey.toLowerCase,functionStore)
	val function = JsonSerializer.parseFunction(ValueAsStr(obj.Value),"JSON")
	SaveObject(function)
      })
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def LoadAllMessagesIntoCache{
    try{
      val msgKeys = GetAllKeys("MessageDef")
      if( msgKeys.length == 0 ){
	logger.trace("Sorry, No messages available in the Metadata")
	return
      }
      msgKeys.foreach(key => { 
	val msgKey = KeyAsStr(key)
	val obj = GetObject(msgKey.toLowerCase,messageStore)
	val ba = obj.Value.toArray[Byte]
	val o = serializer.DeserializeObjectFromByteArray(ba,new MessageDef)
	MdMgr.GetMdMgr.AddMsg(o)
      })
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def LoadAllContainersIntoCache{
    try{
      val contKeys = GetAllKeys("ContainerDef")
      if( contKeys.length == 0 ){
	logger.trace("Sorry, No containers available in the Metadata")
	return
      }
      contKeys.foreach(key => { 
	val contKey = KeyAsStr(key)
	val obj = GetObject(contKey.toLowerCase,containerStore)
	val contDef = JsonSerializer.parseContainerDef(ValueAsStr(obj.Value),"JSON")
	SaveObject(contDef)
      })
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def LoadAllModelsIntoCache{
    try{
      val modKeys = GetAllKeys("ModelDef")
      if( modKeys.length == 0 ){
	logger.trace("Sorry, No models available in the Metadata")
	return
      }
      modKeys.foreach(key => { 
	val modKey = KeyAsStr(key)
	val obj = GetObject(modKey.toLowerCase,modelStore)
	val modDef = JsonSerializer.parseModelDef(ValueAsStr(obj.Value),"JSON")
	SaveObject(modDef)
      })
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def LoadObjectsIntoCache{
    LoadAllModelsIntoCache
    LoadAllMessagesIntoCache
    LoadAllContainersIntoCache
    LoadAllFunctionsIntoCache
    LoadAllConceptsIntoCache
    LoadAllTypesIntoCache
  }

  def ListAllModels{
    try{
      logger.setLevel(Level.TRACE);

      val modKeys = MetadataAPIImpl.GetAllKeys("ModelDef")
      if( modKeys.length == 0 ){
	println("Sorry, No models available in the Metadata")
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

  // Specific messages (format JSON or XML) as a String using messageName(without version) as the key
  def GetMessageDef(objectName:String,formatType:String) : String  = {
    val nameSpace = MdMgr.sysNS
    GetMessageDefFromCache(nameSpace,objectName,formatType,"-1")
  }
  // Specific message (format JSON or XML) as a String using messageName(with version) as the key
  def GetMessageDef(nameSpace:String,objectName:String,formatType:String,version:String) : String  = {
      GetMessageDefFromCache(nameSpace,objectName,formatType,version)
  }

  // Specific message (format JSON or XML) as a String using messageName(with version) as the key
  def GetMessageDef(objectName:String,version:String, formatType:String) : String  = {
    val nameSpace = MdMgr.sysNS
    GetMessageDef(nameSpace,objectName,formatType,version)
  }

  // All available messages(format JSON or XML) as a String
  def GetAllFunctionDefs(formatType:String) : String = {
    try{
      val funcDefs = MdMgr.GetMdMgr.Functions(true,true)
      funcDefs match{
	case None => None
	  logger.trace("No Functions found ")
	  var apiResult = new ApiResult(-1,"Failed to Fetch functions",
					"No Functions Available")
	  apiResult.toString()
	case Some(fs) => 
	  val fsa = fs.toArray
	  var apiResult = new ApiResult(0,"Successfully Fetched all functions",JsonSerializer.SerializeObjectListToJson("Functions",fsa))
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to fetch all the functions:",e.toString)
	apiResult.toString()
      }
    }
  }

  def GetFunctionDef(nameSpace:String, objectName:String,formatType:String) : String = {
    try{
      val funcDefs = MdMgr.GetMdMgr.FunctionsAvailable(nameSpace,objectName)
      if ( funcDefs == null ){
	  logger.trace("No Functions found ")
	  var apiResult = new ApiResult(-1,"Failed to Fetch functions",
					"No Functions Available")
	  apiResult.toString()
      }
      else{
	  val fsa = funcDefs.toArray
	  var apiResult = new ApiResult(0,"Successfully Fetched all functions",JsonSerializer.SerializeObjectListToJson("Functions",fsa))
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to fetch all the functions:",e.toString)
	apiResult.toString()
      }
    }
  }
    
  // Specific messages (format JSON or XML) as a String using messageName(without version) as the key
  def GetFunctionDef(objectName:String,formatType:String) : String = {
    val nameSpace = MdMgr.sysNS
    GetFunctionDef(nameSpace,objectName,formatType)
  }

  // All available concepts as a String
  def GetAllConcepts(formatType:String) : String = {
    try{
      val concepts = MdMgr.GetMdMgr.Attributes(true,true)
      concepts match{
	case None => None
	  logger.trace("No concepts found ")
	  var apiResult = new ApiResult(-1,"Failed to Fetch concepts",
					"No Concepts Available")
	  apiResult.toString()
	case Some(cs) => 
	  val csa = cs.toArray
	  var apiResult = new ApiResult(0,"Successfully Fetched all concepts",JsonSerializer.SerializeObjectListToJson("Concepts",csa))
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to fetch all the concepts:",e.toString)
	apiResult.toString()
      }
    }
  }

  // A single concept as a string using name and version as the key
  def GetConcept(objectName:String,version:String,formatType:String) : String = {
    try{
      val concept = MdMgr.GetMdMgr.Attribute(MdMgr.sysNS,objectName,version.toInt,false)
      concept match{
	case None => None
	  logger.trace("No concepts found ")
	  var apiResult = new ApiResult(-1,"Failed to Fetch concepts",
					"No Concepts Available")
	  apiResult.toString()
	case Some(cs) => 
	  var apiResult = new ApiResult(0,"Successfully Fetched concepts",JsonSerializer.SerializeObjectToJson(cs))
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to fetch all the concepts:",e.toString)
	apiResult.toString()
      }
    }
  }

  // A list of concept(s) as a string using name 
  def GetConcept(objectName:String,formatType:String) : String = {
    try{
      val concepts = MdMgr.GetMdMgr.Attributes(MdMgr.sysNS,objectName,false,false)
      concepts match{
	case None => None
	  logger.trace("No concepts found ")
	  var apiResult = new ApiResult(-1,"Failed to Fetch concepts",
					"No Concepts Available")
	  apiResult.toString()
	case Some(cs) => 
	  val csa = cs.toArray
	  var apiResult = new ApiResult(0,"Successfully Fetched concepts",JsonSerializer.SerializeObjectListToJson("Concepts",csa))
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to fetch all the concepts:",e.toString)
	apiResult.toString()
      }
    }
  }

  // All available derived concepts(format JSON or XML) as a String
  def GetAllDerivedConcepts(formatType:String) : String = {
    try{
      val concepts = MdMgr.GetMdMgr.Attributes(true,true)
      concepts match{
	case None => None
	  logger.trace("No concepts found ")
	  var apiResult = new ApiResult(-1,"Failed to Fetch Derived concepts","No Derived Concepts Available")
	  apiResult.toString()
	case Some(cs) => 
	  val csa = cs.toArray.filter(t => {t.getClass.getName == "DerivedAttributeDef"})
	  if( csa.length > 0 ) {
	    var apiResult = new ApiResult(0,"Successfully Fetched all concepts",JsonSerializer.SerializeObjectListToJson("Concepts",csa))
	    apiResult.toString()
	  }
	  else{
	    var apiResult = new ApiResult(-1,"Failed to Fetch Derived concepts","No Derived Concepts Available")
	    apiResult.toString()
	  }
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to fetch all the derived concepts:",e.toString)
	apiResult.toString()
      }
    }
  }
  // A derived concept(format JSON or XML) as a string using name(without version) as the key
  def GetDerivedConcept(objectName:String,formatType:String) : String = {
    try{
      val concepts = MdMgr.GetMdMgr.Attributes(MdMgr.sysNS,objectName,false,false)
      concepts match{
	case None => None
	  logger.trace("No concepts found ")
	  var apiResult = new ApiResult(-1,"Failed to Fetch Derived concepts",
					"No Concepts Available")
	  apiResult.toString()
	case Some(cs) => 
	  val csa = cs.toArray.filter(t => {t.getClass.getName == "DerivedAttributeDef"})
	  if( csa.length > 0 ) {
	    var apiResult = new ApiResult(0,"Successfully Fetched all concepts",JsonSerializer.SerializeObjectListToJson("Concepts",csa))
	    apiResult.toString()
	  }
	  else{
	    var apiResult = new ApiResult(-1,"Failed to Fetch Derived concepts","No Derived Concepts Available")
	    apiResult.toString()
	  }
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to fetch the derived concepts:",e.toString)
	apiResult.toString()
      }
    }
  }
  // A derived concept(format JSON or XML) as a string using name and version as the key
  def GetDerivedConcept(objectName:String, version:String,formatType:String) : String = {
    try{
      val concept = MdMgr.GetMdMgr.Attribute(MdMgr.sysNS,objectName,version.toInt,false)
      concept match{
	case None => None
	  logger.trace("No concepts found ")
	  var apiResult = new ApiResult(-1,"Failed to Fetch Derived concepts","No Derived Concepts Available")
	  apiResult.toString()
	case Some(cs) => 
	  if ( cs.isInstanceOf[DerivedAttributeDef] ){
	    var apiResult = new ApiResult(0,"Successfully Fetched concepts",JsonSerializer.SerializeObjectToJson(cs))
	    apiResult.toString()
	  }
	  else{
	    logger.trace("No Derived concepts found ")
	    var apiResult = new ApiResult(-1,"Failed to Fetch Derived concepts","No Derived Concepts Available")
	    apiResult.toString()
	  }	    
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to fetch all the concepts:",e.toString)
	apiResult.toString()
      }
    }
  }

  // All available types(format JSON or XML) as a String
  def GetAllTypes(formatType:String) : String = {
    try{
      val typeDefs = MdMgr.GetMdMgr.Types(true,true)
      typeDefs match{
	case None => None
	  logger.trace("No typeDefs found ")
	  var apiResult = new ApiResult(-1,"Failed to Fetch typeDefs",
					"No Types Available")
	  apiResult.toString()
	case Some(ts) => 
	  val tsa = ts.toArray
	  logger.trace("Found " + tsa.length + " types ")
	  var apiResult = new ApiResult(0,"Successfully Fetched all typeDefs",JsonSerializer.SerializeObjectListToJson("Types",tsa))
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to fetch all the typeDefs:",e.getMessage())
	apiResult.toString()
      }
    }
  }


  // All available types(format JSON or XML) as a String
  def GetAllTypesByObjType(formatType:String,objType: String) : String = {
    logger.trace("Find the types of type " + objType)
    try{
      val typeDefs = MdMgr.GetMdMgr.Types(true,true)
      typeDefs match{
	case None => None
	  logger.trace("No typeDefs found ")
	  var apiResult = new ApiResult(-1,"Failed to Fetch typeDefs",
					"No Types Available")
	  apiResult.toString()
	case Some(ts) => 
	  val tsa = ts.toArray.filter(t => {t.getClass.getName == objType})
	  if ( tsa.length == 0 ){
	    var apiResult = new ApiResult(-1,"Failed to Fetch " + objType + " objects ","None Available")
	    apiResult.toString()
	  }
	  else{
	    var apiResult = new ApiResult(0,"Successfully Fetched all typeDefs",JsonSerializer.SerializeObjectListToJson("Types",tsa))
	    apiResult.toString()
	  }
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to fetch all the typeDefs:",e.toString)
	apiResult.toString()
      }
    }
  }

  // Get types for a given name
  def GetType(objectName:String,formatType:String) : String = {
    try{
      val typeDefs = MdMgr.GetMdMgr.Types(MdMgr.sysNS, objectName, false,false)
      typeDefs match{
	case None => None
	  logger.trace("No typeDefs found ")
	  var apiResult = new ApiResult(-1,"Failed to Fetch typeDefs","No Types Available")
	  apiResult.toString()
	case Some(ts) => 
	  val tsa = ts.toArray
	  logger.trace("Found " + tsa.length + " types ")
	  var apiResult = new ApiResult(0,"Successfully Fetched all typeDefs",JsonSerializer.SerializeObjectListToJson("Types",tsa))
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to fetch all the typeDefs:",e.getMessage())
	apiResult.toString()
      }
    }
  }

  def GetType(nameSpace:String, objectName:String, version: String, formatType:String) : Option[BaseTypeDef] = {
    try{
      val typeDefs = MdMgr.GetMdMgr.Types(MdMgr.sysNS, objectName, false,false)
      typeDefs match{
	case None => None
	case Some(ts) => 
	  val tsa = ts.toArray.filter(t => {t.ver == version.toInt})
	  tsa.length match{
	    case 0 => None
	    case 1 => Some(tsa(0))
	    case _ => { 
	      logger.trace("Found More than one type, Doesn't make sesne")
	      Some(tsa(0))
	    }
	  }
      }
    }catch {
      case e:Exception =>{
	logger.trace("Failed to fetch the typeDefs:" + e.getMessage())
	None
      }
    }
  }

  def testDbOp{
    try{
      OpenDbStore(GetMetadataAPIConfig.getProperty("DATABASE"))
      SaveObject("key2","value2",otherStore)
      SaveObject("key2","value2",otherStore)
      GetObject("key2",otherStore)
      CloseDbStore
    }catch{
      case e:Exception => {
	e.printStackTrace()
      }
    }
  }

  def dumpMetadataAPIConfig{

    //metadataAPIConfig.list(System.out)

    val e = metadataAPIConfig.propertyNames()
    while (e.hasMoreElements()) {
      val key = e.nextElement().asInstanceOf[String]
      val value = metadataAPIConfig.getProperty(key)
      logger.trace("Key : " + key + ", Value : " + value)
    }
  }
  
  @throws(classOf[MissingPropertyException])
  @throws(classOf[InvalidPropertyException])
  def readMetadataAPIConfig {
    try{
      val configFile = "MetadataAPI.properties"
      val input = MetadataAPIImpl.getClass.getClassLoader().getResourceAsStream(configFile)
      val prop = new Properties()
      prop.load(input)
      val root_dir = prop.getProperty("ROOT_DIR")
      if (root_dir == null ){
	throw new MissingPropertyException("The property ROOT_DIR must be defined in the config file " + configFile)
      }
      logger.trace("ROOT_DIR => " + root_dir)

      var database = prop.getProperty("DATABASE")
      if (database == null ){
	database = "hashmap"
      }
      logger.trace("database => " + database)

      var git_root = prop.getProperty("GIT_ROOT")
      if (git_root == null ){
	throw new MissingPropertyException("The property GIT_ROOT must be defined in the config file " + configFile)
      }
      git_root = git_root.replace("$ROOT_DIR",root_dir)
      logger.trace("GIT_ROOT => " + git_root)

      var jar_target_dir = prop.getProperty("JAR_TARGET_DIR")
      if (jar_target_dir == null ){
	throw new MissingPropertyException("The property JAR_TARGET_DIR must be defined in the config file " + configFile)
      }
      jar_target_dir = jar_target_dir.replace("$ROOT_DIR",root_dir)
      logger.trace("JAR_TARGET_DIR => " + jar_target_dir)

      var scala_home = prop.getProperty("SCALA_HOME")
      if (scala_home == null ){
	throw new MissingPropertyException("The property SCALA_HOME must be defined in the config file " + configFile)
      }
      scala_home = scala_home.replace("$ROOT_DIR",root_dir)
      logger.trace("SCALA_HOME => " + scala_home)


      var java_home = prop.getProperty("JAVA_HOME")
      if (java_home == null ){
	throw new MissingPropertyException("The property JAVA_HOME must be defined in the config file " + configFile)
      }
      java_home = java_home.replace("$ROOT_DIR",root_dir)
      logger.trace("JAVA_HOME => " + java_home)

      var manifest_path = prop.getProperty("MANIFEST_PATH")
      if (manifest_path == null ){
	throw new MissingPropertyException("The property MANIFEST_PATH must be defined in the config file " + configFile)
      }
      manifest_path = manifest_path.replace("$GIT_ROOT",git_root)
      logger.trace("MANIFEST_PATH => " + manifest_path)

      var classpath = prop.getProperty("CLASSPATH")
      if (classpath == null ){
	throw new MissingPropertyException("The property CLASSPATH must be defined in the config file " + configFile)
      }
      classpath = classpath.replace("$ROOT_DIR",root_dir)
      classpath = classpath.replace("$GIT_ROOT",git_root)
      classpath = classpath.replace("$SCALA_HOME",scala_home)
      logger.trace("CLASSPATH => " + classpath)

      metadataAPIConfig.setProperty("ROOT_DIR",root_dir)
      metadataAPIConfig.setProperty("DATABASE",database)
      metadataAPIConfig.setProperty("GIT_ROOT",git_root)
      metadataAPIConfig.setProperty("JAR_TARGET_DIR",jar_target_dir)
      metadataAPIConfig.setProperty("SCALA_HOME",scala_home)
      metadataAPIConfig.setProperty("JAVA_HOME",java_home)
      metadataAPIConfig.setProperty("MANIFEST_PATH",manifest_path)
      metadataAPIConfig.setProperty("CLASSPATH",classpath)
      
    } catch { 
      case e: Exception => 
	logger.error("Failed to load configuration: " + e.getMessage)
	sys.exit(1)
    }
  }


  def InitMdMgr {
    MdMgr.GetMdMgr.truncate
    val mdLoader = new com.ligadata.olep.metadataload.MetadataLoad (MdMgr.mdMgr, "","","","")
    mdLoader.initialize
    MetadataAPIImpl.readMetadataAPIConfig
    MetadataAPIImpl.OpenDbStore(GetMetadataAPIConfig.getProperty("DATABASE"))
    MetadataAPIImpl.LoadObjectsIntoCache
    MetadataAPIImpl.CloseDbStore
  }

  def InitMdMgrFromBootStrap{
    MdMgr.GetMdMgr.truncate
    val mdLoader = new com.ligadata.olep.metadataload.MetadataLoad (MdMgr.mdMgr,"","","","")
    mdLoader.initialize
    MetadataAPIImpl.readMetadataAPIConfig
    MetadataAPIImpl.OpenDbStore(GetMetadataAPIConfig.getProperty("DATABASE"))
  }
}
