package com.ligadata.MetadataAPI


import java.util.Properties
import java.io._
import scala.Enumeration
import scala.io._

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
import com.ligadata.keyvaluestore.cassandra._

import scala.util.parsing.json.JSON
import scala.util.parsing.json.{JSONObject, JSONArray}
import scala.collection.immutable.Map

import com.ligadata.messagedef._

import scala.xml.XML
import org.apache.log4j._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import com.ligadata.ZooKeeper._
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import com.ligadata.Serialize._
import com.ligadata.Utils._

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

case class ParameterMap(RootDir:String
    , GitRootDir: String
    , Database: String
    , DatabaseHost: String
    , DatabaseSchema: Option[String]
    , DatabaseLocation: Option[String]
	, JarTargetDir : String
	, ScalaHome : String
	, JavaHome : String
	, ManifestPath : String
	, ClassPath : String
	, NotifyEngine : String
	, ZnodePath :String
	, ZooKeeperConnectString : String
	, MODEL_FILES_DIR : Option[String]
	, TYPE_FILES_DIR : Option[String]
	, FUNCTION_FILES_DIR : Option[String]
	, CONCEPT_FILES_DIR : Option[String]
	, MESSAGE_FILES_DIR : Option[String]
	, CONTAINER_FILES_DIR : Option[String]
	, COMPILER_WORK_DIR : Option[String]
);

case class MetadataAPIConfig(APIConfigParameters: ParameterMap)

case class APIResultInfo(statusCode:Int, statusDescription: String, resultData: String)
case class APIResultJsonProxy(APIResults: APIResultInfo)

case class UnsupportedObjectException(e: String) extends Exception(e)
case class Json4sParsingException(e: String) extends Exception(e)
case class FunctionListParsingException(e: String) extends Exception(e)
case class FunctionParsingException(e: String) extends Exception(e)
case class TypeDefListParsingException(e: String) extends Exception(e)
case class TypeParsingException(e: String) extends Exception(e)
case class TypeDefProcessingException(e: String) extends Exception(e)
case class ConceptListParsingException(e: String) extends Exception(e)
case class ConceptParsingException(e: String) extends Exception(e)
case class MessageDefParsingException(e: String) extends Exception(e)
case class ContainerDefParsingException(e: String) extends Exception(e)
case class ModelDefParsingException(e: String) extends Exception(e)
case class ApiResultParsingException(e: String) extends Exception(e)
case class UnexpectedMetadataAPIException(e: String) extends Exception(e)
case class ObjectNotFoundException(e: String) extends Exception(e)
case class CreateStoreFailedException(e: String) extends Exception(e)
case class UpdateStoreFailedException(e: String) extends Exception(e)

case class LoadAPIConfigException(e: String) extends Exception(e)
case class MissingPropertyException(e: String) extends Exception(e)
case class InvalidPropertyException(e: String) extends Exception(e)
case class KryoSerializationException(e: String) extends Exception(e)
case class InternalErrorException(e: String) extends Exception(e)
case class TranIdNotFoundException(e: String) extends Exception(e)


// The implementation class
object MetadataAPIImpl extends MetadataAPI{
    
  lazy val sysNS = "System"		// system name space
  lazy val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  lazy val serializer = SerializerManager.GetSerializer("kryo")
  lazy val metadataAPIConfig = new Properties()
  var zkc:CuratorFramework = null
  val configFile = System.getenv("HOME") + "/MetadataAPIConfig.json"
  var propertiesAlreadyLoaded = false

  private var tableStoreMap : Map[String,DataStore] = Map()

  def CloseZKSession: Unit = {
    if( zkc != null ){
      zkc.close()
      zkc = null
    }
  }

  def InitZooKeeper: Unit = {
    logger.trace("Connect to zookeeper..")
    if( zkc != null ){
      // Zookeeper is already connected
      return
    }

    val zkcConnectString = GetMetadataAPIConfig.getProperty("ZOOKEEPER_CONNECT_STRING")
    val znodePath = GetMetadataAPIConfig.getProperty("ZNODE_PATH")
    logger.trace("Connect To ZooKeeper using " + zkcConnectString)
    try{
      CreateClient.CreateNodeIfNotExists(zkcConnectString, znodePath)
      zkc = CreateClient.createSimple(zkcConnectString)
    }catch{
	case e:Exception => {
	  throw new InternalErrorException("Failed to start a zookeeper session with(" +  zkcConnectString + "): " + e.getMessage())
	}
    }
  }
    

  def GetMetadataAPIConfig: Properties = {
    metadataAPIConfig
  }

  def SetLoggerLevel(level: Level){
    logger.setLevel(level);
  }


  private var metadataStore:   DataStore = _
  private var transStore:      DataStore = _
  private var modelStore:      DataStore = _
  private var messageStore:    DataStore = _
  private var containerStore:  DataStore = _
  private var functionStore:   DataStore = _
  private var conceptStore:    DataStore = _
  private var typeStore:       DataStore = _
  private var otherStore:      DataStore = _

  def oStore = otherStore
  
  def KeyAsStr(k: com.ligadata.keyvaluestore.Key): String = {
    val k1 = k.toArray[Byte]
    new String(k1)
  }

  def ValueAsStr(v: com.ligadata.keyvaluestore.Value): String = {
    val v1 = v.toArray[Byte]
    new String(v1)
  }


  def GetObject(key: com.ligadata.keyvaluestore.Key,store: DataStore) : IStorage = {
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
      
      var k = key
      logger.trace("Get the object from store, key => " + KeyAsStr(k))
      store.get(k,o)
      o
    } catch {
      case e:KeyNotFoundException => {
	logger.trace("KeyNotFound Exception: Error => " + e.getMessage())
	throw new ObjectNotFoundException(e.getMessage())
      }    
      case e:Exception => {
	e.printStackTrace()
	logger.trace("General Exception: Error => " + e.getMessage())
	throw new ObjectNotFoundException(e.getMessage())
      }
    }
  }


  def GetObject(key: String,store: DataStore) : IStorage = {
    try{
      var k = new com.ligadata.keyvaluestore.Key
      for(c <- key ){
	k += c.toByte
      }
      GetObject(k,store)
    }
  }


  def SaveObject(key: String, value: Array[Byte], store: DataStore){
    val t = store.beginTx
    object obj extends IStorage{
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
    try{
      store.put(obj)
      store.commitTx(t)
    }
    catch{
      case e:Exception => {
	logger.trace("Failed to insert/update object for : " + key)
	store.endTx(t)
	throw new UpdateStoreFailedException("Failed to insert/update object for : " + key)
      }
    }
  }


  def SaveObjectList(keyList: Array[String], valueList: Array[Array[Byte]], store: DataStore){
    var i = 0
    val t = store.beginTx
    var storeObjects = new Array[IStorage](keyList.length)
    keyList.foreach( key => {
      var value = valueList(i)
      object obj extends IStorage{
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
      storeObjects(i) = obj
      i = i + 1
    })
    try{
      store.putBatch(storeObjects)
      store.commitTx(t)
    }
    catch{
      case e:Exception => {
	logger.trace("Failed to insert/update object for : " + keyList.mkString(","))
	store.endTx(t)
	throw new UpdateStoreFailedException("Failed to insert/update object for : " + keyList.mkString(","))
      }
    }
  }

  // If tables are different, an internal utility function
  def getTable(obj: BaseElemDef) : String = {
    obj match {
      case o:ModelDef => {
	"models"
      }
      case o:MessageDef => {
	"messages"
      }
      case o:ContainerDef => {
	"containers"
      }
      case o:FunctionDef => {
	"functions"
      }
      case o:AttributeDef => {
	"concepts"
      }
      case o:BaseTypeDef => {
	"types"
      }	
      case _ => {
	logger.error("getTable is not implemented for objects of type " + obj.getClass.getName)
	throw new InternalErrorException("getTable is not implemented for objects of type " + obj.getClass.getName)
      }
    }
  }

  def getObjectType(obj: BaseElemDef): String = {
    val className = obj.getClass().getName();
    className.split("\\.").last
  }

  // 
  // The following batch function is useful when we store data in single table
  // If we use Storage component library, note that table itself is associated with a single
  // database connection( which itself can be mean different things depending on the type
  // of datastore, such as cassandra, hbase, etc..)
  // 
  def SaveObjectList(objList: Array[BaseElemDef], store: DataStore){
    logger.trace("Save " + objList.length + " objects in a single transaction ")
    val tranId = GetNewTranId
    var keyList = new Array[String](objList.length)
    var valueList = new Array[Array[Byte]](objList.length)
    try{
      var i = 0;
      objList.foreach(obj => {
	obj.tranId = tranId
	val key = (getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
	var value = serializer.SerializeObjectToByteArray(obj)
	keyList(i) = key
	valueList(i) = value
	i = i + 1
      })
      SaveObjectList(keyList,valueList,store)
    }
    catch{
      case e:Exception => {
	logger.trace("Failed to insert/update object for : " + keyList.mkString(","))
	throw new UpdateStoreFailedException("Failed to insert/update object for : " + keyList.mkString(","))
      }
    }
  }

  // The following batch function is useful when we store data in multiple tables
  // If we use Storage component library, note that each table is associated with a different
  // database connection( which itself can be mean different things depending on the type
  // of datastore, such as cassandra, hbase, etc..)
  def SaveObjectList(objList: Array[BaseElemDef]){
    logger.trace("Save " + objList.length + " objects in a single transaction ")
    val tranId = GetNewTranId
    var keyList = new Array[String](objList.length)
    var valueList = new Array[Array[Byte]](objList.length)
    var tableList = new Array[String](objList.length)
    try{
      var i = 0;
      objList.foreach(obj => {
	obj.tranId = tranId
	val key = (getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
	var value = serializer.SerializeObjectToByteArray(obj)
	keyList(i) = key
	valueList(i) = value
	tableList(i) = getTable(obj)
	i = i + 1
      })
      i = 0
      var storeObjects = new Array[IStorage](keyList.length)
      keyList.foreach( key => {
	var value = valueList(i)
	object obj extends IStorage{
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
	storeObjects(i) = obj
	i = i + 1
      })
      val storeType = GetMetadataAPIConfig.getProperty("DATABASE")
      storeType match {
	case "cassandra" => {
	  val store = tableStoreMap(tableList(0))
	  val t = store.beginTx
	  KeyValueBatchTransaction.putCassandraBatch(tableList,store,storeObjects)
	  store.commitTx(t)
	}
	case _ => {
	  var i = 0
	  tableList.foreach(table => {
	    val store = tableStoreMap(table)
	    val t = store.beginTx
	    store.put(storeObjects(i))
	    store.commitTx(t)
	    i = i + 1
	  })
	}
      }
    }
    catch{
      case e:Exception => {
	logger.trace("Failed to insert/update object for : " + keyList.mkString(","))
	throw new UpdateStoreFailedException("Failed to insert/update object for : " + keyList.mkString(","))
      }
    }
  }

  def SaveObject(key: String, value: String, store: DataStore){
    var ba = serializer.SerializeObjectToByteArray(value)
    SaveObject(key,ba,store)
  }

  def UpdateObject(key: String, value: Array[Byte], store: DataStore){
    SaveObject(key,value,store)
  }

  def ZooKeeperMessage(objList: Array[BaseElemDef],operations:Array[String]) : Array[Byte] = {
    try{
      val notification = JsonSerializer.zkSerializeObjectListToJson("Notifications",objList,operations)
      notification.getBytes
    }catch{
	case e:Exception => {
	  throw new InternalErrorException("Failed to generate a zookeeper message from the objList " + e.getMessage())
	}
    }
  }
	  
  def NotifyEngine(objList: Array[BaseElemDef],operations:Array[String]){
    try{
      val notifyEngine = GetMetadataAPIConfig.getProperty("NOTIFY_ENGINE")
      if( notifyEngine != "YES"){
	logger.warn("Not Notifying the engine about this operation because The property NOTIFY_ENGINE is not set to YES")
	PutTranId(objList(0).tranId)
	return
      }
      val data = ZooKeeperMessage(objList,operations)
      InitZooKeeper
      val znodePath = GetMetadataAPIConfig.getProperty("ZNODE_PATH")
      logger.trace("Set the data on the zookeeper node " + znodePath)
      zkc.setData().forPath(znodePath,data)
      PutTranId(objList(0).tranId)
    }catch{
      case e:Exception => {
	  throw new InternalErrorException("Failed to notify a zookeeper message from the objectList " + e.getMessage())
      }
    }
  }

  def GetNewTranId : Long = {
    try{
      val key = "transaction_id"
      val obj = GetObject(key,transStore)
      val idStr = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[String]
      idStr.toLong + 1
    }catch {
      case e:ObjectNotFoundException => {
	  // first time
	  1
      }
      case e:Exception =>{
	throw new TranIdNotFoundException("Unable to retrieve the transaction id " + e.toString)
      }
    }
  }

  def GetTranId : Long = {
    try{
      val key = "transaction_id"
      val obj = GetObject(key,transStore)
      val idStr = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[String]
      idStr.toLong
    }catch {
      case e:ObjectNotFoundException => {
	  // first time
	  0
      }
      case e:Exception =>{
	throw new TranIdNotFoundException("Unable to retrieve the transaction id " + e.toString)
      }
    }
  }

  def PutTranId(tId:Long) = {
    try{
      val key = "transaction_id"
      val value = tId.toString
      SaveObject(key,value,transStore)
    }catch {
      case e:Exception =>{
	throw new UpdateStoreFailedException("Unable to Save the transaction id " + tId + ":" + e.getMessage())
      }
    }
  }

  def SaveObject(obj: BaseElemDef,mdMgr: MdMgr){
    try{
      val key = (getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
      obj.tranId = GetNewTranId
      //val value = JsonSerializer.SerializeObjectToJson(obj)
      logger.trace("Serialize the object: name of the object => " + key)
      var value = serializer.SerializeObjectToByteArray(obj)
      obj match{
	case o:ModelDef => {
	  logger.trace("Adding the model to the cache: name of the object =>  " + key)
	  SaveObject(key,value,modelStore)
	  mdMgr.AddModelDef(o)
	}
	case o:MessageDef => {
	  logger.trace("Adding the message to the cache: name of the object =>  " + key)
	  SaveObject(key,value,messageStore)
	  mdMgr.AddMsg(o)
	}
	case o:ContainerDef => {
	  logger.trace("Adding the container to the cache: name of the object =>  " + key)
	  SaveObject(key,value,containerStore)
	  mdMgr.AddContainer(o)
	}
	case o:FunctionDef => {
          val funcKey = (obj.getClass().getName().split("\\.").last + "." + o.typeString).toLowerCase
	  logger.trace("Adding the function to the cache: name of the object =>  " + funcKey)
	  SaveObject(funcKey,value,functionStore)
	  mdMgr.AddFunc(o)
	}
	case o:AttributeDef => {
	  logger.trace("Adding the attribute to the cache: name of the object =>  " + key)
	  SaveObject(key,value,conceptStore)
	  mdMgr.AddAttribute(o)
	}
	case o:ScalarTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  mdMgr.AddScalar(o)
	}
	case o:ArrayTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  mdMgr.AddArray(o)
	}
	case o:ArrayBufTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  mdMgr.AddArrayBuffer(o)
	}
	case o:ListTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  mdMgr.AddList(o)
	}
	case o:QueueTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  mdMgr.AddQueue(o)
	}
	case o:SetTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  mdMgr.AddSet(o)
	}
	case o:TreeSetTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  mdMgr.AddTreeSet(o)
	}
	case o:SortedSetTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  mdMgr.AddSortedSet(o)
	}
	case o:MapTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  mdMgr.AddMap(o)
	}
	case o:ImmutableMapTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  mdMgr.AddImmutableMap(o)
	}
	case o:HashMapTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  mdMgr.AddHashMap(o)
	}
	case o:TupleTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  mdMgr.AddTupleType(o)
	}
	case o:ContainerTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  mdMgr.AddContainerType(o)
	}
	case _ => {
	  logger.error("SaveObject is not implemented for objects of type " + obj.getClass.getName)
	}
      }
    }catch{
      case e:AlreadyExistsException => {
	logger.error("Failed to Save the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
      case e:Exception => {
	logger.error("Failed to Save the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
    }
  }

  def UpdateObjectInDB(obj: BaseElemDef){
    try{
      val key = (getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase

      logger.trace("Serialize the object: name of the object => " + key)
      var value = serializer.SerializeObjectToByteArray(obj)
      obj match{
	case o:ModelDef => {
	  logger.trace("Updating the model in the DB: name of the object =>  " + key)
	  UpdateObject(key,value,modelStore)
	}
	case o:MessageDef => {
	  logger.trace("Updating the message in the DB: name of the object =>  " + key)
	  UpdateObject(key,value,messageStore)
	}
	case o:ContainerDef => {
	  logger.trace("Updating the container in the DB: name of the object =>  " + key)
	  UpdateObject(key,value,containerStore)
	}
	case o:FunctionDef => {
          val funcKey = (obj.getClass().getName().split("\\.").last + "." + o.typeString).toLowerCase
	  logger.trace("Updating the function in the DB: name of the object =>  " + funcKey)
	  UpdateObject(funcKey,value,functionStore)
	}
	case o:AttributeDef => {
	  logger.trace("Updating the attribute in the DB: name of the object =>  " + key)
	  UpdateObject(key,value,conceptStore)
	}
	case o:ScalarTypeDef => {
	  logger.trace("Updating the Type in the DB: name of the object =>  " + key)
	  UpdateObject(key,value,typeStore)
	}
	case o:ArrayTypeDef => {
	  logger.trace("Updating the Type in the DB: name of the object =>  " + key)
	  UpdateObject(key,value,typeStore)
	}
	case o:ArrayBufTypeDef => {
	  logger.trace("Updating the Type in the DB: name of the object =>  " + key)
	  UpdateObject(key,value,typeStore)
	}
	case o:ListTypeDef => {
	  logger.trace("Updating the Type in the DB: name of the object =>  " + key)
	  UpdateObject(key,value,typeStore)
	}
	case o:QueueTypeDef => {
	  logger.trace("Updating the Type in the DB: name of the object =>  " + key)
	  UpdateObject(key,value,typeStore)
	}
	case o:SetTypeDef => {
	  logger.trace("Updating the Type in the DB: name of the object =>  " + key)
	  UpdateObject(key,value,typeStore)
	}
	case o:TreeSetTypeDef => {
	  logger.trace("Updating the Type in the DB: name of the object =>  " + key)
	  UpdateObject(key,value,typeStore)
	}
	case o:SortedSetTypeDef => {
	  logger.trace("Updating the Type in the DB: name of the object =>  " + key)
	  UpdateObject(key,value,typeStore)
	}
	case o:MapTypeDef => {
	  logger.trace("Updating the Type in the DB: name of the object =>  " + key)
	  UpdateObject(key,value,typeStore)
	}
	case o:ImmutableMapTypeDef => {
	  logger.trace("Updating the Type in the DB: name of the object =>  " + key)
	  UpdateObject(key,value,typeStore)
	}
	case o:HashMapTypeDef => {
	  logger.trace("Updating the Type in the DB: name of the object =>  " + key)
	  UpdateObject(key,value,typeStore)
	}
	case o:TupleTypeDef => {
	  logger.trace("Updating the Type in the DB: name of the object =>  " + key)
	  UpdateObject(key,value,typeStore)
	}
	case o:ContainerTypeDef => {
	  logger.trace("Updating the Type in the DB: name of the object =>  " + key)
	  UpdateObject(key,value,typeStore)
	}
	case _ => {
	  logger.error("UpdateObject is not implemented for objects of type " + obj.getClass.getName)
	}
      }
    }catch{
      case e:AlreadyExistsException => {
	logger.error("Failed to Update the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
      case e:Exception => {
	logger.error("Failed to Update the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
    }
  }

  def UpdateObjectInCache(obj: BaseElemDef,operation:String,mdMgr:MdMgr): BaseElemDef = {
    var updatedObject:BaseElemDef = null
    try{
      obj match{
	case o:FunctionDef => {
	  updatedObject = mdMgr.ModifyFunction(o.nameSpace,o.name,o.ver,operation)
	}
	case o:ModelDef => {
	  updatedObject = mdMgr.ModifyModel(o.nameSpace,o.name,o.ver,operation)
	}
	case o:MessageDef => {
	  updatedObject = mdMgr.ModifyMessage(o.nameSpace,o.name,o.ver,operation)
	}
	case o:ContainerDef => {
	  updatedObject = mdMgr.ModifyContainer(o.nameSpace,o.name,o.ver,operation)
	}
	case o:AttributeDef => {
	  updatedObject = mdMgr.ModifyAttribute(o.nameSpace,o.name,o.ver,operation)
	}
	case o:ScalarTypeDef => {
	  updatedObject = mdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:ArrayTypeDef => {
	  updatedObject = mdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:ArrayBufTypeDef => {
	  updatedObject = mdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:ListTypeDef => {
	  updatedObject = mdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:QueueTypeDef => {
	  updatedObject = mdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:SetTypeDef => {
	  updatedObject = mdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:TreeSetTypeDef => {
	  updatedObject = mdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:SortedSetTypeDef => {
	  updatedObject = mdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:MapTypeDef => {
	  updatedObject = mdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:ImmutableMapTypeDef => {
	  updatedObject = mdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:HashMapTypeDef => {
	  updatedObject = mdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:TupleTypeDef => {
	  updatedObject = mdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:ContainerTypeDef => {
	  updatedObject = mdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case _ => {
	  throw new InternalErrorException("UpdateObjectInCache is not implemented for objects of type " + obj.getClass.getName)
	}
      }
      updatedObject
    }catch{
      case e:ObjectNolongerExistsException => {
	throw new ObjectNolongerExistsException("The object " + obj.FullNameWithVer + " nolonger exists in metadata : It may have been removed already")
      }
      case e:Exception => {
	throw new Exception("Unexpected error in UpdateObjectInCache: " + e.getMessage())
      }
    }
  }


  def AddObjectToCache(o: Object,mdMgr: MdMgr){
    // If the object's Delete flag is set, this is a noop.
    val obj = o.asInstanceOf[BaseElemDef]
    if (obj.IsDeleted )
      return
    try{
      val key = obj.FullNameWithVer.toLowerCase
      obj match{
	case o:ModelDef => {
	  logger.trace("Adding the model to the cache: name of the object =>  " + key)
	  mdMgr.AddModelDef(o)
	}
	case o:MessageDef => {
	  logger.trace("Adding the message to the cache: name of the object =>  " + key)
	  mdMgr.AddMsg(o)
	}
	case o:ContainerDef => {
	  logger.trace("Adding the container to the cache: name of the object =>  " + key)
	  mdMgr.AddContainer(o)
	}
	case o:FunctionDef => {
          val funcKey = o.typeString.toLowerCase
	  logger.trace("Adding the function to the cache: name of the object =>  " + funcKey)
	  mdMgr.AddFunc(o)
	}
	case o:AttributeDef => {
	  logger.trace("Adding the attribute to the cache: name of the object =>  " + key)
	  mdMgr.AddAttribute(o)
	}
	case o:ScalarTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  mdMgr.AddScalar(o)
	}
	case o:ArrayTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  mdMgr.AddArray(o)
	}
	case o:ArrayBufTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  mdMgr.AddArrayBuffer(o)
	}
	case o:ListTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  mdMgr.AddList(o)
	}
	case o:QueueTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  mdMgr.AddQueue(o)
	}
	case o:SetTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  mdMgr.AddSet(o)
	}
	case o:TreeSetTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  mdMgr.AddTreeSet(o)
	}
	case o:SortedSetTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  mdMgr.AddSortedSet(o)
	}
	case o:MapTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  mdMgr.AddMap(o)
	}
	case o:ImmutableMapTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  mdMgr.AddImmutableMap(o)
	}
	case o:HashMapTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  mdMgr.AddHashMap(o)
	}
	case o:TupleTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  mdMgr.AddTupleType(o)
	}
	case o:ContainerTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  mdMgr.AddContainerType(o)
	}
	case _ => {
	  logger.error("SaveObject is not implemented for objects of type " + obj.getClass.getName)
	}
      }
    }catch{
      case e:AlreadyExistsException => {
	logger.error("Failed to Save the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
      case e:Exception => {
	logger.error("Failed to Cache the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
    }
  }

  def ModifyObject(obj: BaseElemDef, operation: String){
    try{
      val o1 = UpdateObjectInCache(obj,operation,MdMgr.GetMdMgr)
      UpdateObjectInDB(o1)
    }catch{
      case e:ObjectNolongerExistsException => {
	logger.error("The object " + obj.FullNameWithVer + " nolonger exists in metadata : It may have been removed already")
      }
      case e:Exception => {
	throw new Exception("Unexpected error in ModifyObject: " + e.getMessage())
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
    try{
      ModifyObject(obj,"Remove")
    }catch{
      case e:ObjectNolongerExistsException => {
	logger.error("The object " + obj.FullNameWithVer + " nolonger exists in metadata : It may have been removed already")
      }
      case e:Exception => {
	throw new Exception("Unexpected error in DeleteObject: " + e.getMessage())
      }
    }
  }


  def ActivateObject(obj: BaseElemDef){
    try{
      ModifyObject(obj,"Activate")
    }catch{
      case e:ObjectNolongerExistsException => {
	logger.error("The object " + obj.FullNameWithVer + " nolonger exists in metadata : It may have been removed already")
      }
      case e:Exception => {
	throw new Exception("Unexpected error in ActivateObject: " + e.getMessage())
      }
    }
  }


  def DeactivateObject(obj: BaseElemDef){
    try{
      ModifyObject(obj,"Deactivate")
    }catch{
      case e:ObjectNolongerExistsException => {
	logger.error("The object " + obj.FullNameWithVer + " nolonger exists in metadata : It may have been removed already")
      }
      case e:Exception => {
	throw new Exception("Unexpected error in DeactivateObject: " + e.getMessage())
      }
    }
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
	  var databaseLocation = GetMetadataAPIConfig.getProperty("DATABASE_LOCATION")
	  connectinfo+= ("path" -> databaseLocation)
	  connectinfo+= ("schema" -> storeName)
	  connectinfo+= ("inmemory" -> "false")
	  connectinfo+= ("withtransaction" -> "true")
	}
	case "treemap" => {
	  var databaseLocation = GetMetadataAPIConfig.getProperty("DATABASE_LOCATION")
	  connectinfo+= ("path" -> databaseLocation)
	  connectinfo+= ("schema" -> storeName)
	  connectinfo+= ("inmemory" -> "false")
	  connectinfo+= ("withtransaction" -> "true")
	}
	case "cassandra" => {
	  var databaseHost = GetMetadataAPIConfig.getProperty("DATABASE_HOST")
	  var databaseSchema = GetMetadataAPIConfig.getProperty("DATABASE_SCHEMA")
	  if( databaseHost == null ){
	    databaseHost = "localhost"
	  }
	  connectinfo+= ("hostlist" -> databaseHost) 
	  connectinfo+= ("schema" -> databaseSchema)
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
      metadataStore     = GetDataStoreHandle(storeType,"metadata_store","metadata_objects")
      transStore        = GetDataStoreHandle(storeType,"metadata_trans","transaction_id")
      modelStore        = metadataStore
      messageStore      = metadataStore
      containerStore    = metadataStore
      functionStore     = metadataStore
      conceptStore      = metadataStore
      typeStore         = metadataStore
      otherStore        = metadataStore
      tableStoreMap = Map("models"     -> modelStore,
		       "messages"   -> messageStore,
		       "containers" -> containerStore,
		       "functions"  -> functionStore,
		       "concepts"   -> conceptStore,
		       "types"      -> typeStore,
		       "others"     -> otherStore,
		       "transaction_id" -> transStore)
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
      metadataStore.Shutdown()
      transStore.Shutdown()
    }catch{
      case e:Exception => {
	throw e;
      }
    }
  }

  def AddType(typeText:String, format:String): String = {
    try{
      logger.trace("Parsing type object given as Json String..")
      val typ = JsonSerializer.parseType(typeText,"JSON")
      SaveObject(typ,MdMgr.GetMdMgr)
      var apiResult = new ApiResult(0,"Type was Added",typeText)
      apiResult.toString()
    }catch {
      case e:AlreadyExistsException => {
	logger.trace("Failed to add the type, json => " + typeText  + "\nError => " + e.getMessage())
	var apiResult = new ApiResult(-1,"Failed to add a Type:",e.toString)
	apiResult.toString()
      }
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
      SaveObject(typeDef,MdMgr.GetMdMgr)
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
	    SaveObject(typ,MdMgr.GetMdMgr) 
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
    val key = typeNameSpace + "." + typeName + "." + version
    try{
      val typ = MdMgr.GetMdMgr.Type(typeNameSpace, typeName, version,true)
      typ match{
	case None => None
	  logger.trace("Type " + key + " is not found in the cache ")
	  var apiResult = new ApiResult(-1,"Failed to find type",key)
	  apiResult.toString()
	case Some(ts) => 
	  DeleteObject(ts.asInstanceOf[BaseElemDef])
	  var apiResult = new ApiResult(0,"Type was Deleted",key)
	  apiResult.toString()
      }
    }catch {
      case e:ObjectNolongerExistsException =>{
	var apiResult = new ApiResult(-1,"Failed to delete the Type(" + key + "):",e.toString)
	apiResult.toString()
      }
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to delete the Type(" + key + "):",e.toString)
	apiResult.toString()
      }
    }
  }

  def UpdateType(typeJson:String, format:String): String = {
    var apiResult = new ApiResult(-1,"UpdateType failed with an internal error",typeJson)
    try{
      implicit val jsonFormats: Formats = DefaultFormats
      val typeDef = JsonSerializer.parseType(typeJson,"JSON")
      val key = typeDef.nameSpace + "." + typeDef.name + "." + typeDef.ver
      val fName = MdMgr.MkFullName(typeDef.nameSpace,typeDef.name)
      val latestType = MdMgr.GetMdMgr.Types(typeDef.nameSpace,typeDef.name, true,true)
      latestType match{
	case None => None
	  logger.trace("No types with the name " + fName + " are found ")
	  apiResult = new ApiResult(-1,"Failed to Update type object,There are no existing Types Available",key)
	case Some(ts) => 
	  val tsa = ts.toArray
	  logger.trace("Found " + tsa.length + " types ")
	  if ( tsa.length > 1 ){
	    apiResult = new ApiResult(-1,"Failed to Update type object,There is more than one latest Type Available",fName)
	  }
	  else{
	    val latestVersion = tsa(0)
	    if( latestVersion.ver > typeDef.ver ){
	      RemoveType(latestVersion.nameSpace,latestVersion.name,latestVersion.ver)
	      AddType(typeDef)
	      apiResult = new ApiResult(0,"Successfully Updated Type",key)
	    }
	    else{
	      apiResult = new ApiResult(-1,"Failed to Update type object,New version must be greater than latest Available version",key)
	    }
	  }
      }
      apiResult.toString()	
    } catch {
      case e:MappingException =>{
	logger.trace("Failed to parse the type, json => " + typeJson  + ",Error => " + e.getMessage())
	apiResult = new ApiResult(-1,"Parsing Error: " + e.getMessage(),typeJson)
	apiResult.toString()
      }
      case e:AlreadyExistsException => {
	logger.trace("Failed to update the type, json => " + typeJson  + ",Error => " + e.getMessage())
	apiResult = new ApiResult(-1,"Error: " + e.getMessage(),typeJson)
	apiResult.toString()
      }
      case e:Exception => {
	logger.trace("Failed to up the type, json => " + typeJson  + ",Error => " + e.getMessage())
	apiResult = new ApiResult(-1,"Error: " + e.getMessage(),typeJson)
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
      SaveObject(attributeDef,MdMgr.GetMdMgr)
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
      SaveObject(functionDef,MdMgr.GetMdMgr)
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
	  SaveObject(func,MdMgr.GetMdMgr) 
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
	SaveObject(func,MdMgr.GetMdMgr) 
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

  def AddDerivedConcept(conceptsText:String, format:String): String = {
    try{
      if( format != "JSON" ){
	var apiResult = new ApiResult(0,"Not Implemented Yet","No Result")
	apiResult.toString()
      }
      else{
	  var concept = JsonSerializer.parseDerivedConcept(conceptsText,format)
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

  def AddConcepts(conceptsText:String, format:String): String = {
    try{
      if( format != "JSON" ){
	var apiResult = new ApiResult(0,"Not Implemented Yet","No Result")
	apiResult.toString()
      }
      else{
	  var conceptList = JsonSerializer.parseConceptList(conceptsText,format)
	  conceptList.foreach(concept => {
	    //logger.trace("Save concept object " + JsonSerializer.SerializeObjectToJson(concept))
	    SaveObject(concept,MdMgr.GetMdMgr) 
	  })
	  var apiResult = new ApiResult(0,"Concepts Are Added",conceptsText)
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to add concepts",e.getMessage())
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
      SaveObject(contDef,MdMgr.GetMdMgr)
      var apiResult = new ApiResult(0,"Container was Added",key)
      apiResult.toString()
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to add the Container:",e.toString)
	apiResult.toString()
      }
    }
  }


  def AddMessageDef(msgDef: MessageDef): String = {
    try{
      var key = msgDef.FullNameWithVer
      SaveObject(msgDef,MdMgr.GetMdMgr)
      var apiResult = new ApiResult(0,"MsgDef was Added",key)
      apiResult.toString()
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to add the msgDef:",e.toString)
	apiResult.toString()
      }
    }
  }

  // As per Rich's requirement, Add array/arraybuf/sortedset types for this messageDef
  // along with the messageDef.  
  def AddMessageTypes(msgDef:BaseElemDef,mdMgr: MdMgr): Array[BaseElemDef] = {
    logger.trace("The class name => " + msgDef.getClass().getName())
    try{
      var types = new Array[BaseElemDef](0)
      val msgType = getObjectType(msgDef)
      val depJars = if (msgDef.DependencyJarNames != null) 
		       (msgDef.DependencyJarNames :+ msgDef.JarName) else Array(msgDef.JarName)
      msgType match{
	case "MessageDef" | "ContainerDef" => {
	  // ArrayOf<TypeName>
	  var obj:BaseElemDef = mdMgr.MakeArray(msgDef.nameSpace,"arrayof"+msgDef.name,msgDef.nameSpace, msgDef.name,1,msgDef.ver)
	  obj.dependencyJarNames = depJars 
	  AddObjectToCache(obj,mdMgr)
	  types = types :+ obj
	  // ArrayBufferOf<TypeName>
	  obj = mdMgr.MakeArrayBuffer(msgDef.nameSpace,"arraybufferof"+msgDef.name, msgDef.nameSpace,msgDef.name,1,msgDef.ver)
	  obj.dependencyJarNames = depJars 
	  AddObjectToCache(obj,mdMgr)
	  types = types :+ obj
	  // SortedSetOf<TypeName>
	  obj = mdMgr.MakeSortedSet(msgDef.nameSpace,"sortedsetof"+msgDef.name, msgDef.nameSpace,msgDef.name,msgDef.ver)
	  obj.dependencyJarNames = depJars 
	  AddObjectToCache(obj,mdMgr)
	  types = types :+ obj
	  // ImmutableMapOfIntArrayOf<TypeName>
	  obj = mdMgr.MakeImmutableMap(msgDef.nameSpace, "immutablemapofintarrayof"+msgDef.name, ("System","Int"), (msgDef.nameSpace,"arrayof"+msgDef.name), msgDef.ver)
	  obj.dependencyJarNames = depJars 
	  AddObjectToCache(obj,mdMgr)
	  types = types :+ obj
	  // ImmutableMapOfString<TypeName>
	  obj = mdMgr.MakeImmutableMap(msgDef.nameSpace, "immutablemapofstringarrayof"+msgDef.name, ("System","String"), (msgDef.nameSpace,"arrayof"+msgDef.name), msgDef.ver)
	  obj.dependencyJarNames = depJars 
	  AddObjectToCache(obj,mdMgr)
	  types = types :+ obj
	  // ArrayOfArrayOf<TypeName>
	  obj = mdMgr.MakeArray(msgDef.nameSpace, "arrayofarrayof"+msgDef.name, msgDef.nameSpace, "arrayof"+msgDef.name, 1, msgDef.ver)
	  obj.dependencyJarNames = depJars 
	  AddObjectToCache(obj,mdMgr)
	  types = types :+ obj
	  // MapOfStringArrayOf<TypeName>
	  obj = mdMgr.MakeMap(msgDef.nameSpace, "mapofstringarrayof"+msgDef.name, ("System", "String"), ("System", "arrayof"+msgDef.name), msgDef.ver)
	  obj.dependencyJarNames = depJars 
	  AddObjectToCache(obj,mdMgr)
	  types = types :+ obj
	  // MapOfIntArrayOf<TypeName>
	  obj = mdMgr.MakeMap(msgDef.nameSpace, "mapofintarrayof"+msgDef.name, ("System", "Int"), ("System", "arrayof"+msgDef.name), msgDef.ver)
	  obj.dependencyJarNames = depJars 
	  AddObjectToCache(obj,mdMgr)
	  types = types :+ obj
	  // SetOf<TypeName>
	  obj = mdMgr.MakeSet(msgDef.nameSpace,"setof"+msgDef.name,msgDef.nameSpace, msgDef.name,msgDef.ver)
	  obj.dependencyJarNames = depJars 
	  AddObjectToCache(obj,mdMgr)
	  types = types :+ obj
	  // TreeSetOf<TypeName>
	  obj = mdMgr.MakeTreeSet(msgDef.nameSpace,"treesetof"+msgDef.name,msgDef.nameSpace, msgDef.name,msgDef.ver)
	  obj.dependencyJarNames = depJars 
	  AddObjectToCache(obj,mdMgr)
	  types = types :+ obj
	  types
	}
	case _ => {
	  throw new InternalErrorException("Unknown class in AddMessageTypes")
	}
      }
    }
    catch {
      case e:Exception =>{
	throw new Exception(e.getMessage())
      }
    }
  }


  def AddMessage(messageText:String, format:String): String = {
    try{
      var compProxy = new CompilerProxy
      compProxy.setLoggerLevel(Level.TRACE)
      val(classStr,msgDef) = compProxy.compileMessageDef(messageText)
      logger.trace("Message Compiler returned an object of type " + msgDef.getClass().getName())
      msgDef match{
	case msg:MessageDef =>{
	  AddObjectToCache(msg,MdMgr.GetMdMgr)
	  var objectsAdded = AddMessageTypes(msg,MdMgr.GetMdMgr)
	  objectsAdded = objectsAdded :+ msg
	  SaveObjectList(objectsAdded,metadataStore)
	  val operations = for (op <- objectsAdded) yield "Add"
	  NotifyEngine(objectsAdded,operations)
	  var apiResult = new ApiResult(0,"Added the message successfully:",msg.FullNameWithVer)
	  apiResult.toString()
	}
	case cont:ContainerDef =>{
	  AddObjectToCache(cont,MdMgr.GetMdMgr)
	  var objectsAdded = AddMessageTypes(cont,MdMgr.GetMdMgr)
	  objectsAdded = objectsAdded :+ cont
	  SaveObjectList(objectsAdded,metadataStore)
	  val operations = for (op <- objectsAdded) yield "Add"
	  NotifyEngine(objectsAdded,operations)
	  var apiResult = new ApiResult(0,"Added the container successfully:",cont.FullNameWithVer)
	  apiResult.toString()
	}
      }
    }
    catch {
      case e:MsgCompilationFailedException =>{
	var apiResult = new ApiResult(-1,"Failed to compile the msgDef:",e.toString)
	apiResult.toString()
      }
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
      logger.trace("Message Compiler returned an object of type " + msgDef.getClass().getName())
      msgDef match{
	case msg:MessageDef =>{
	  AddObjectToCache(msg,MdMgr.GetMdMgr)
	  var objectsAdded = AddMessageTypes(msg,MdMgr.GetMdMgr)
	  objectsAdded = objectsAdded :+ msg
	  SaveObjectList(objectsAdded,metadataStore)
	  val operations = for (op <- objectsAdded) yield "Add"
	  NotifyEngine(objectsAdded,operations)
	  var apiResult = new ApiResult(0,"Added the message successfully:",msg.FullNameWithVer)
	  apiResult.toString()
	}
	case cont:ContainerDef =>{
	  AddObjectToCache(cont,MdMgr.GetMdMgr)
	  var objectsAdded = AddMessageTypes(cont,MdMgr.GetMdMgr)
	  objectsAdded = objectsAdded :+ cont
	  SaveObjectList(objectsAdded,metadataStore)
	  val operations = for (op <- objectsAdded) yield "Add"
	  NotifyEngine(objectsAdded,operations)
	  var apiResult = new ApiResult(0,"Added the container successfully:",cont.FullNameWithVer)
	  apiResult.toString()
	}
      }
    }
    catch {
      case e:MsgCompilationFailedException =>{
	var apiResult = new ApiResult(-1,"Failed to compile the msgDef:",e.toString)
	apiResult.toString()
      }
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
	  val latestVersion = GetLatestMessage(msg)
	  var isValid = true
	  if( latestVersion != None ){
	    isValid = IsValidVersion(latestVersion.get,msg)
	  }
	  if ( isValid ){
	    RemoveMessage(latestVersion.get.nameSpace,latestVersion.get.name,latestVersion.get.ver)
	    AddMessageDef(msg)
	  }
	  else{
	    var apiResult = new ApiResult(-1,"Failed to update the message:" + key,"Invalid Version")
	    apiResult.toString()
	  }
	}
	case msg:ContainerDef =>{
	  val latestVersion = GetLatestContainer(msg)
	  var isValid = true
	  if( latestVersion != None ){
	    isValid = IsValidVersion(latestVersion.get,msg)
	  }
	  if ( isValid ){
	    RemoveContainer(latestVersion.get.nameSpace,latestVersion.get.name,latestVersion.get.ver)
	    AddContainerDef(msg)
	  }
	  else{
	    var apiResult = new ApiResult(-1,"Failed to update the container:" + key,"Invalid Version")
	    apiResult.toString()
	  }
	}
      }  	
    }
    catch {
      case e:MsgCompilationFailedException =>{
	var apiResult = new ApiResult(-1,"Failed to compile the msgDef:",e.toString)
	apiResult.toString()
      }
      case e:ObjectNotFoundException =>{
	var apiResult = new ApiResult(-1,"Failed to update the message:",e.toString)
	apiResult.toString()
      }  
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to update the message:",e.toString)
	apiResult.toString()
      }
    }
  }

  // Remove container with Container Name and Version Number
  def RemoveContainer(nameSpace:String,name:String, version:Int): String = {
    try{
      var key = nameSpace + "." + name + "." + version
      val o = MdMgr.GetMdMgr.Container(nameSpace.toLowerCase,name.toLowerCase,version,true)
      o match{
	case None => None
	  logger.trace("container not found => " + key)
	  var apiResult = new ApiResult(-1,"Failed to Fetch the container",key)
	  apiResult.toString()
	case Some(m) => 
	  logger.trace("container found => " + m.asInstanceOf[ContainerDef].FullNameWithVer)
	  val contDef = m.asInstanceOf[ContainerDef]
	  var objectsToBeRemoved = GetAdditionalTypesAdded(contDef,MdMgr.GetMdMgr)
	  // Also remove a type with same name as messageDef
	  var typeName = name 
	  var typeDef = GetType(nameSpace,typeName,version.toString,"JSON")
	  if( typeDef != None ){
	    objectsToBeRemoved = objectsToBeRemoved :+ typeDef.get
	  }
	  objectsToBeRemoved.foreach(typ => {
	    RemoveType(typ.nameSpace,typ.name,typ.ver)
	  })
	  // ContainerDef itself
	  DeleteObject(contDef)
	  objectsToBeRemoved :+ contDef

	  val operations = for (op <- objectsToBeRemoved) yield "Remove"
	  NotifyEngine(objectsToBeRemoved,operations)
	  var apiResult = new ApiResult(0,"Container Definition was Deleted",key)
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to delete the ContainerDef:",e.toString)
	apiResult.toString()
      }
    }
  }

  // Remove message with Message Name and Version Number
  def RemoveMessage(nameSpace:String,name:String, version:Int): String = {
    try{
      var key = nameSpace + "." + name + "." + version
      val o = MdMgr.GetMdMgr.Message(nameSpace.toLowerCase,name.toLowerCase,version,true)
      o match{
	case None => None
	  logger.trace("Message not found => " + key)
	  var apiResult = new ApiResult(-1,"Failed to Fetch the message",key)
	  apiResult.toString()
	case Some(m) => 
	  val msgDef = m.asInstanceOf[MessageDef]
	  logger.trace("message found => " + msgDef.FullNameWithVer)
	  var objectsToBeRemoved = GetAdditionalTypesAdded(msgDef,MdMgr.GetMdMgr)
	  // Also remove a type with same name as messageDef
	  var typeName = name 
	  var typeDef = GetType(nameSpace,typeName,version.toString,"JSON")
	  if( typeDef != None ){
	    objectsToBeRemoved = objectsToBeRemoved :+ typeDef.get
	  }
	  objectsToBeRemoved.foreach(typ => {
	    RemoveType(typ.nameSpace,typ.name,typ.ver)
	  })
	  // MessageDef itself
	  DeleteObject(msgDef)
	  objectsToBeRemoved :+ msgDef

	  val operations = for (op <- objectsToBeRemoved) yield "Remove"
	  NotifyEngine(objectsToBeRemoved,operations)
	  var apiResult = new ApiResult(0,"Message Definition was Deleted",key)
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to delete the MessageDef:",e.toString)
	apiResult.toString()
      }
    }
  }

  def GetAdditionalTypesAdded(msgDef:BaseElemDef,mdMgr:MdMgr): Array[BaseElemDef] = {
    var types = new Array[BaseElemDef](0)
    logger.trace("The class name => " + msgDef.getClass().getName())
    try{
      val msgType = getObjectType(msgDef)
      msgType match{
	case "MessageDef" | "ContainerDef" => {
	  // ArrayOf<TypeName>
	  var typeName = "arrayof"+msgDef.name 
	  var typeDef = GetType(msgDef.nameSpace,typeName,msgDef.ver.toString,"JSON")
	  if( typeDef != None ){
	    types = types :+ typeDef.get
	  }
	  // ArrayBufferOf<TypeName>
	  typeName = "arraybufferof"+msgDef.name 
	  typeDef = GetType(msgDef.nameSpace,typeName,msgDef.ver.toString,"JSON")
	  if( typeDef != None ){
	    types = types :+ typeDef.get
	  }
	  // SortedSetOf<TypeName>
	  typeName = "sortedsetof"+msgDef.name 
	  typeDef = GetType(msgDef.nameSpace,typeName,msgDef.ver.toString,"JSON")
	  if( typeDef != None ){
	    types = types :+ typeDef.get
	  }
	  // ImmutableMapOfIntArrayOf<TypeName>
	  typeName = "immutablemapofintarrayof"+msgDef.name 
	  typeDef = GetType(msgDef.nameSpace,typeName,msgDef.ver.toString,"JSON")
	  if( typeDef != None ){
	    types = types :+ typeDef.get
	  }
	  // ImmutableMapOfString<TypeName>
	  typeName = "immutablemapofstringarrayof"+msgDef.name 
	  typeDef = GetType(msgDef.nameSpace,typeName,msgDef.ver.toString,"JSON")
	  if( typeDef != None ){
	    types = types :+ typeDef.get
	  }
	  // ArrayOfArrayOf<TypeName>
	  typeName = "arrayofarrayof"+msgDef.name 
	  typeDef = GetType(msgDef.nameSpace,typeName,msgDef.ver.toString,"JSON")
	  if( typeDef != None ){
	    types = types :+ typeDef.get
	  }
	  // MapOfStringArrayOf<TypeName>
	  typeName = "mapofstringarrayof"+msgDef.name 
	  typeDef = GetType(msgDef.nameSpace,typeName,msgDef.ver.toString,"JSON")
	  if( typeDef != None ){
	    types = types :+ typeDef.get
	  }
	  // MapOfIntArrayOf<TypeName>
	  typeName = "mapofintarrayof"+msgDef.name 
	  typeDef = GetType(msgDef.nameSpace,typeName,msgDef.ver.toString,"JSON")
	  if( typeDef != None ){
	    types = types :+ typeDef.get
	  }
	  // SetOf<TypeName>
	  typeName = "setof"+msgDef.name 
	  typeDef = GetType(msgDef.nameSpace,typeName,msgDef.ver.toString,"JSON")
	  if( typeDef != None ){
	    types = types :+ typeDef.get
	  }
	  // TreeSetOf<TypeName>
	  typeName = "treesetof"+msgDef.name 
	  typeDef = GetType(msgDef.nameSpace,typeName,msgDef.ver.toString,"JSON")
	  if( typeDef != None ){
	    types = types :+ typeDef.get
	  }
	  logger.trace("Type objects to be removed = " + types.length)
	  types
	}
	case _ => {
	  throw new InternalErrorException("Unknown class in AddMessageTypes")
	}
      }
    }
    catch {
      case e:Exception =>{
	throw new Exception(e.getMessage())
      }
    }
  }

  // Remove message with Message Name and Version Number
  def RemoveMessageFromCache(zkMessage: ZooKeeperNotification) = {
    try{
      var key = zkMessage.NameSpace + "." + zkMessage.Name + "." + zkMessage.Version
      val o = MdMgr.GetMdMgr.Message(zkMessage.NameSpace,zkMessage.Name,zkMessage.Version.toInt,true)
      o match{
	case None => None
	  logger.trace("Message not found, Already Removed? => " + key)
	case Some(m) => 
	  val msgDef = m.asInstanceOf[MessageDef]
	  logger.trace("message found => " + msgDef.FullNameWithVer)
	  val types = GetAdditionalTypesAdded(msgDef,MdMgr.GetMdMgr)
	  
	  var typeName = zkMessage.Name 
	  MdMgr.GetMdMgr.RemoveType(zkMessage.NameSpace,typeName,zkMessage.Version.toInt)
	  typeName = "arrayof" + zkMessage.Name 
	  MdMgr.GetMdMgr.RemoveType(zkMessage.NameSpace,typeName,zkMessage.Version.toInt)
	  typeName = "sortedsetof" + zkMessage.Name 
	  MdMgr.GetMdMgr.RemoveType(zkMessage.NameSpace,typeName,zkMessage.Version.toInt)
	  typeName = "arraybufferof" + zkMessage.Name 
	  MdMgr.GetMdMgr.RemoveType(zkMessage.NameSpace,typeName,zkMessage.Version.toInt)
	  MdMgr.GetMdMgr.RemoveMessage(zkMessage.NameSpace,zkMessage.Name,zkMessage.Version.toInt)
      }
    }catch {
      case e:Exception =>{
	logger.error("Failed to delete the Message from cache:" + e.toString)
      }
    }
  }

  def RemoveContainerFromCache(zkMessage: ZooKeeperNotification) = {
    try{
      var key = zkMessage.NameSpace + "." + zkMessage.Name + "." + zkMessage.Version
      val o = MdMgr.GetMdMgr.Container(zkMessage.NameSpace,zkMessage.Name,zkMessage.Version.toInt,true)
      o match{
	case None => None
	  logger.trace("Message not found, Already Removed? => " + key)
	case Some(m) => 
	  val msgDef = m.asInstanceOf[MessageDef]
	  logger.trace("message found => " + msgDef.FullNameWithVer)
	  var typeName = zkMessage.Name 
	  MdMgr.GetMdMgr.RemoveType(zkMessage.NameSpace,typeName,zkMessage.Version.toInt)
	  typeName = "arrayof" + zkMessage.Name 
	  MdMgr.GetMdMgr.RemoveType(zkMessage.NameSpace,typeName,zkMessage.Version.toInt)
	  typeName = "sortedsetof" + zkMessage.Name 
	  MdMgr.GetMdMgr.RemoveType(zkMessage.NameSpace,typeName,zkMessage.Version.toInt)
	  typeName = "arraybufferof" + zkMessage.Name 
	  MdMgr.GetMdMgr.RemoveType(zkMessage.NameSpace,typeName,zkMessage.Version.toInt)
	  MdMgr.GetMdMgr.RemoveContainer(zkMessage.NameSpace,zkMessage.Name,zkMessage.Version.toInt)
      }
    }catch {
      case e:Exception =>{
	logger.error("Failed to delete the Message from cache:" + e.toString)
      }
    }
  }

  // Remove message with Message Name and Version Number
  def RemoveMessage(messageName:String, version:Int): String = {
    RemoveMessage(sysNS,messageName,version)
  }


  // Remove model with Model Name and Version Number
  def DeactivateModel(nameSpace:String, name:String, version:Int): String = {
    try{
      var key = nameSpace + "." + name + "." + version
      val o = MdMgr.GetMdMgr.Model(nameSpace.toLowerCase,name.toLowerCase,version,true)
      o match{
	case None => None
	  logger.trace("No active model found => " + key)
	  var apiResult = new ApiResult(-1,"Failed to Fetch the model",key)
	  apiResult.toString()
	case Some(m) => 
	  logger.trace("model found => " + m.asInstanceOf[ModelDef].FullNameWithVer)
	  DeactivateObject(m.asInstanceOf[ModelDef])
	  var objectsUpdated = new Array[BaseElemDef](0)
	  objectsUpdated = objectsUpdated :+ m.asInstanceOf[ModelDef]
	  val operations = for (op <- objectsUpdated) yield "Deactivate"
	  NotifyEngine(objectsUpdated,operations)
	  var apiResult = new ApiResult(0,"Model Definition was Deleted",key)
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to delete the ModelDef:",e.toString)
	apiResult.toString()
      }
    }
  }

  def ActivateModel(nameSpace:String, name:String, version:Int): String = {
    try{
      var key = nameSpace + "." + name + "." + version
      val o = MdMgr.GetMdMgr.Model(nameSpace.toLowerCase,name.toLowerCase,version,false)
      o match{
	case None => None
	  logger.trace("No active model found => " + key)
	  var apiResult = new ApiResult(-1,"Failed to Fetch the model",key)
	  apiResult.toString()
	case Some(m) => 
	  logger.trace("model found => " + m.asInstanceOf[ModelDef].FullNameWithVer)
	  ActivateObject(m.asInstanceOf[ModelDef])
	  var objectsUpdated = new Array[BaseElemDef](0)
	  objectsUpdated = objectsUpdated :+ m.asInstanceOf[ModelDef]
	  val operations = for (op <- objectsUpdated) yield "Activate"
	  NotifyEngine(objectsUpdated,operations)
	  var apiResult = new ApiResult(0,"Model Definition was Deleted",key)
	  apiResult.toString()
      }
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to delete the ModelDef:",e.toString)
	apiResult.toString()
      }
    }
  }

  // Remove model with Model Name and Version Number
  def RemoveModel(nameSpace:String, name:String, version:Int): String = {
    try{
      var key = nameSpace + "." + name + "." + version
      val o = MdMgr.GetMdMgr.Model(nameSpace.toLowerCase,name.toLowerCase,version,true)
      o match{
	case None => None
	  logger.trace("model not found => " + key)
	  var apiResult = new ApiResult(-1,"Failed to Fetch the model",key)
	  apiResult.toString()
	case Some(m) => 
	  logger.trace("model found => " + m.asInstanceOf[ModelDef].FullNameWithVer)
	  DeleteObject(m.asInstanceOf[ModelDef])
	  var objectsUpdated = new Array[BaseElemDef](0)
	  objectsUpdated = objectsUpdated :+ m.asInstanceOf[ModelDef]
	  var operations = for (op <- objectsUpdated) yield "Remove"
	  NotifyEngine(objectsUpdated,operations)
	  var apiResult = new ApiResult(0,"Model Definition was Deleted",key)
	  apiResult.toString()
      }
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
      SaveObject(model,MdMgr.GetMdMgr)
      var apiResult = new ApiResult(0,"Model was Added",key)
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

      // Make sure the version of the model is greater than any of previous models with same FullName
      var latestVersion = GetLatestModel(modDef)
      var isValid = true
      if( latestVersion != None ){
	isValid = IsValidVersion(latestVersion.get,modDef)
      }
      if ( isValid ){
	val apiResult = AddModel(modDef)
	logger.trace("Model is added..")
	var objectsAdded = new Array[BaseElemDef](0)
	objectsAdded = objectsAdded :+ modDef
	val operations = for (op <- objectsAdded) yield "Add"
	logger.trace("Notify engine via zookeeper")
	NotifyEngine(objectsAdded,operations)
	apiResult
      }
      else{
	var apiResult = new ApiResult(-1,"Failed to add the model:Version(" + modDef.ver + ") must be greater than any previous version including removed models ",modDef.FullNameWithVer)
	apiResult.toString()
      }
    }
    catch {
      case e:ModelCompilationFailedException =>{
	var apiResult = new ApiResult(-1,"Failed to add the model:","Error in producing scala file or Jar file..")
	apiResult.toString()
      } 
      case e:AlreadyExistsException =>{
	var apiResult = new ApiResult(-1,"Failed to add the model:",e.getMessage())
	apiResult.toString()
      } 
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
      val key = MdMgr.MkFullNameWithVersion(modDef.nameSpace,modDef.name,modDef.ver)
      val latestVersion = GetLatestModel(modDef)
      var isValid = true
      if( latestVersion != None ){
	isValid = IsValidVersion(latestVersion.get,modDef)
      }
      if ( isValid ){
	RemoveModel(latestVersion.get.nameSpace,latestVersion.get.name,latestVersion.get.ver)
	val result = AddModel(modDef)
	var objectsUpdated = new Array[BaseElemDef](0)
	var operations = new Array[String](0)
	objectsUpdated = objectsUpdated :+ latestVersion.get
	operations = operations :+ "Remove"
	objectsUpdated = objectsUpdated :+ modDef
	operations = operations :+ "Add"
	NotifyEngine(objectsUpdated,operations)
	result
      }
      else{
	var apiResult = new ApiResult(-1,"Failed to update the model:" + key,"Invalid Version")
	apiResult.toString()
      }  	
    }
    catch {
      case e:ObjectNotFoundException =>{
	var apiResult = new ApiResult(-1,"Failed to update the model:",e.toString)
	apiResult.toString()
      }  
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


  def GetAllModelsFromCache(active: Boolean) : Array[String] = {
    var modelList: Array[String] = new Array[String](0)
    try{
      val modDefs = MdMgr.GetMdMgr.Models(active,true)
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


  // Get the latest model for a given FullName
  def GetLatestModel(modDef: ModelDef) : Option[ModelDef] = {
    try{
      var key = modDef.nameSpace + "." + modDef.name + "." + modDef.ver
      val o = MdMgr.GetMdMgr.Models(modDef.nameSpace.toLowerCase,
				    modDef.name.toLowerCase,
				    false,
				    true)
      o match{
	case None => None
	  logger.trace("model not in the cache => " + key)
	  None
	case Some(m) => 
	  logger.trace("model found => " + m.asInstanceOf[ModelDef].FullNameWithVer)
	  Some(m.asInstanceOf[ModelDef])
      }
    }catch {
      case e:Exception =>{
	e.printStackTrace()
	throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }

  // Get the latest message for a given FullName
  def GetLatestMessage(msgDef: MessageDef) : Option[MessageDef] = {
    try{
      var key = msgDef.nameSpace + "." + msgDef.name + "." + msgDef.ver
      val o = MdMgr.GetMdMgr.Messages(msgDef.nameSpace.toLowerCase,
				    msgDef.name.toLowerCase,
				    false,
				    true)
      o match{
	case None => None
	  logger.trace("message not in the cache => " + key)
	  None
	case Some(m) => 
	  logger.trace("message found => " + m.asInstanceOf[MessageDef].FullNameWithVer)
	  Some(m.asInstanceOf[MessageDef])
      }
    }catch {
      case e:Exception =>{
	e.printStackTrace()
	throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }


  // Get the latest container for a given FullName
  def GetLatestContainer(contDef: ContainerDef) : Option[ContainerDef] = {
    try{
      var key = contDef.nameSpace + "." + contDef.name + "." + contDef.ver
      val o = MdMgr.GetMdMgr.Containers(contDef.nameSpace.toLowerCase,
				    contDef.name.toLowerCase,
				    false,
				    true)
      o match{
	case None => None
	  logger.trace("container not in the cache => " + key)
	  None
	case Some(m) => 
	  logger.trace("container found => " + m.asInstanceOf[ContainerDef].FullNameWithVer)
	  Some(m.asInstanceOf[ContainerDef])
      }
    }catch {
      case e:Exception =>{
	e.printStackTrace()
	throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }

  def IsValidVersion(oldObj:BaseElemDef, newObj:BaseElemDef) : Boolean = {
    if( newObj.ver > oldObj.ver ){
      return true
    }
    else{
      return false
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
      var key = "ModelDef" + "." + nameSpace + '.' + objectName + "." + version
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

  private def IsTypeObject(typeName: String) : Boolean = {
      typeName match{
	  case "scalartypedef" | "arraytypedef" | "arraybuftypedef" | "listtypedef" | "settypedef" | "treesettypedef" | "queuetypedef" | "maptypedef" | "immutablemaptypedef" | "hashmaptypedef" | "tupletypedef" | "structtypedef" | "sortedsettypedef" => { 
	    return true }
	  case _ => {
	    return false
	  }
      }
  }

  def GetAllKeys(objectType: String) : Array[String] = {
    try{
      var keys = scala.collection.mutable.Set[String]()
      typeStore.getAllKeys( {(key : Key) => { 
	val strKey = KeyAsStr(key)
	val i = strKey.indexOf(".")
	val objType = strKey.substring(0,i)
	val typeName = strKey.substring(i+1)
	objectType match{
	  case "TypeDef" => {
	    if (IsTypeObject(objType)){
	      keys.add(typeName)
	    }
	  }
	  case "FunctionDef" => {
	    if(objType == "functiondef" ){
	      keys.add(typeName)
	    }
	  }
	  case "MessageDef" => {
	    if(objType == "messagedef" ){
	      keys.add(typeName)
	    }
	  }
	  case "ContainerDef" => {
	    if(objType == "containerdef" ){
	      keys.add(typeName)
	    }
	  }
	  case "Concept" => {
	    if(objType == "attributedef" ){
	      keys.add(typeName)
	    }
	  }
	  case "ModelDef" => {
	    if(objType == "modeldef" ){
	      keys.add(typeName)
	    }
	  }
	  case _ => {
	    logger.error("Unknown object type " + objectType + " in GetAllKeys function")
	    throw InternalErrorException("Unknown object type " + objectType + " in GetAllKeys function")
	  }
	}
      }})
      keys.toArray
    }catch {
      case e: Exception => {
	e.printStackTrace()
	throw InternalErrorException("Failed to get keys from persistent store")
      }
    }
  }

  def LoadAllObjectsIntoCache{
    try{
      var objectsChanged = new Array[BaseElemDef](0)
      var operations = new Array[String](0)
      val maxTranId = GetTranId
      logger.trace("Max Transaction Id => " + maxTranId)
      var keys = scala.collection.mutable.Set[com.ligadata.keyvaluestore.Key]()
      metadataStore.getAllKeys( {(key : Key) => keys.add(key) } )
      val keyArray = keys.toArray
      if( keyArray.length == 0 ){
	logger.trace("No objects available in the Database")
	return
      }
      keyArray.foreach(key => { 
	val obj = GetObject(key,metadataStore)
	val mObj =  serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[BaseElemDef]
	if( mObj != null ){
	  if( mObj.tranId <= maxTranId ){
	    AddObjectToCache(mObj,MdMgr.GetMdMgr)
	  }
	  else{
	    logger.trace("The transaction id of the object => " + mObj.tranId)
	    AddObjectToCache(mObj,MdMgr.GetMdMgr)
	    logger.error("Transaction is incomplete with the object " + KeyAsStr(key) + ",we may not have notified engine, attempt to do it now...")
	    objectsChanged = objectsChanged :+ mObj
	    if( mObj.IsActive ){
	      operations = for (op <- objectsChanged) yield "Add"
	    }
	    else{
	      operations = for (op <- objectsChanged) yield "Remove"
	    }
	  }
	}
	else{
	  throw InternalErrorException("serializer.Deserialize returned a null object")
	}
      })
      if(objectsChanged.length > 0 ){
	NotifyEngine(objectsChanged,operations)
      }
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def LoadAllTypesIntoCache{
    try{
      val typeKeys = GetAllKeys("TypeDef")
      if( typeKeys.length == 0 ){
	logger.trace("No types available in the Database")
	return
      }
      typeKeys.foreach(key => { 
	val obj = GetObject(key.toLowerCase,typeStore)
	val typ =  serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
	if( typ != null ){
	  AddObjectToCache(typ,MdMgr.GetMdMgr)
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
	logger.trace("No concepts available in the Database")
	return
      }
      conceptKeys.foreach(key => { 
	val obj = GetObject(key.toLowerCase,conceptStore)
	val concept =  serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
	AddObjectToCache(concept.asInstanceOf[AttributeDef],MdMgr.GetMdMgr)
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
	logger.trace("No functions available in the Database")
	return
      }
      functionKeys.foreach(key => { 
	val obj = GetObject(key.toLowerCase,functionStore)
	val function =  serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
	AddObjectToCache(function.asInstanceOf[FunctionDef],MdMgr.GetMdMgr)
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
	logger.trace("No messages available in the Database")
	return
      }
      msgKeys.foreach(key => { 
	val obj = GetObject(key.toLowerCase,messageStore)
	val msg = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
	AddObjectToCache(msg.asInstanceOf[MessageDef],MdMgr.GetMdMgr)
      })
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def LoadMessageIntoCache(key: String){
    try{
        logger.trace("Fetch the object " + key + " from database ")
	val obj = GetObject(key.toLowerCase,messageStore)
        logger.trace("Deserialize the object " + key)
	val msg = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
        logger.trace("Add the object " + key + " to the cache ")
	AddObjectToCache(msg.asInstanceOf[MessageDef],MdMgr.GetMdMgr)
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def LoadTypeIntoCache(key: String){
    try{
        logger.trace("Fetch the object " + key + " from database ")
	val obj = GetObject(key.toLowerCase,typeStore)
        logger.trace("Deserialize the object " + key)
	val typ = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
	if( typ != null ){
          logger.trace("Add the object " + key + " to the cache ")
	  AddObjectToCache(typ,MdMgr.GetMdMgr)
	}
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def LoadModelIntoCache(key: String){
    try{
        logger.trace("Fetch the object " + key + " from database ")
	val obj = GetObject(key.toLowerCase,modelStore)
        logger.trace("Deserialize the object " + key)
	val model = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
        logger.trace("Add the object " + key + " to the cache ")
	AddObjectToCache(model.asInstanceOf[ModelDef],MdMgr.GetMdMgr)
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def LoadContainerIntoCache(key: String){
    try{
	val obj = GetObject(key.toLowerCase,containerStore)
	val cont = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
	AddObjectToCache(cont.asInstanceOf[ContainerDef],MdMgr.GetMdMgr)
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def LoadFunctionIntoCache(key: String){
    try{
	val obj = GetObject(key.toLowerCase,functionStore)
	val cont = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
	AddObjectToCache(cont.asInstanceOf[FunctionDef],MdMgr.GetMdMgr)
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }


  def LoadAttributeIntoCache(key: String){
    try{
	val obj = GetObject(key.toLowerCase,conceptStore)
	val cont = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
	AddObjectToCache(cont.asInstanceOf[AttributeDef],MdMgr.GetMdMgr)
    }catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def UpdateMdMgr(zkTransaction: ZooKeeperTransaction) = {
    var key:String = null
    try{
      zkTransaction.Notifications.foreach( zkMessage => {
	key = (zkMessage.ObjectType + "." + zkMessage.NameSpace + "." + zkMessage.Name + "." + zkMessage.Version).toLowerCase
	zkMessage.ObjectType match {
	  case "ModelDef" => {
	    zkMessage.Operation match{
	      case "Add" => {
		LoadModelIntoCache(key)
	      }
	      case "Remove" | "Activate" | "Deactivate" => {
		try{
		  MdMgr.GetMdMgr.ModifyModel(zkMessage.NameSpace,zkMessage.Name,zkMessage.Version.toInt,zkMessage.Operation)
		}catch {
		  case e:ObjectNolongerExistsException => {
		    logger.error("The object " + key + " nolonger exists in metadata : It may have been removed already")
		  }
		}
	      }
	      case _ => {
		logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")
	      }
	    }
	  }
	  case "MessageDef" => {
	    zkMessage.Operation match{
	      case "Add" => {
		LoadMessageIntoCache(key)
	      }
	      case "Remove" | "Activate" | "Deactivate" => {
		try{
		  MdMgr.GetMdMgr.ModifyMessage(zkMessage.NameSpace,zkMessage.Name,zkMessage.Version.toInt,zkMessage.Operation)
		}catch {
		  case e:ObjectNolongerExistsException => {
		    logger.error("The object " + key + " nolonger exists in metadata : It may have been removed already")
		  }
		}
	      }
	      case _ => {
		logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")
	      }
	    }
	  }
	  case "ContainerDef" => {
	    zkMessage.Operation match{
	      case "Add" => {
		LoadContainerIntoCache(key)
	      }
	      case "Remove" | "Activate" | "Deactivate" => {
		try{
		  MdMgr.GetMdMgr.ModifyContainer(zkMessage.NameSpace,zkMessage.Name,zkMessage.Version.toInt,zkMessage.Operation)
		}catch {
		  case e:ObjectNolongerExistsException => {
		    logger.error("The object " + key + " nolonger exists in metadata : It may have been removed already")
		  }
		}
	      }
	      case _ => {
		logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")
	      }
	    }
	  }
	  case "FunctionDef" => {
	    zkMessage.Operation match{
	      case "Add" => {
		LoadFunctionIntoCache(key)
	      }
	      case "Remove" | "Activate" | "Deactivate" => {
		try{
		  MdMgr.GetMdMgr.ModifyFunction(zkMessage.NameSpace,zkMessage.Name,zkMessage.Version.toInt,zkMessage.Operation)
		}catch {
		  case e:ObjectNolongerExistsException => {
		    logger.error("The object " + key + " nolonger exists in metadata : It may have been removed already")
		  }
		}
	      }
	      case _ => {
		logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")
	      }
	    }
	  }
	  case "AttributeDef" => {
	    zkMessage.Operation match{
	      case "Add" => {
		LoadAttributeIntoCache(key)
	      }
	      case "Remove" | "Activate" | "Deactivate" => {
		try{
		  MdMgr.GetMdMgr.ModifyAttribute(zkMessage.NameSpace,zkMessage.Name,zkMessage.Version.toInt,zkMessage.Operation)
		}catch {
		  case e:ObjectNolongerExistsException => {
		    logger.error("The object " + key + " nolonger exists in metadata : It may have been removed already")
		  }
		}
	      }
	      case _ => {
		logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")
	      }
	    }
	  }
	  case "ScalarTypeDef" | "ArrayTypeDef" | "ArrayBufTypeDef" | "ListTypeDef" | "SetTypeDef" | "TreeSetTypeDef" | "QueueTypeDef" | "MapTypeDef" | "ImmutableMapTypeDef" | "HashMapTypeDef" | "TupleTypeDef" | "StructTypeDef" | "SortedSetTypeDef" => {
	    zkMessage.Operation match{
	      case "Add" => {
		LoadTypeIntoCache(key)
	      }
	      case "Remove" | "Activate" | "Deactivate" => {
		try{
		  logger.trace("Remove the type " + key + " from cache ")
		  MdMgr.GetMdMgr.ModifyType(zkMessage.NameSpace,zkMessage.Name,zkMessage.Version.toInt,zkMessage.Operation)
		}catch {
		  case e:ObjectNolongerExistsException => {
		    logger.error("The object " + key + " nolonger exists in metadata : It may have been removed already")
		  }
		}
	      }
	      case _ => {
		logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")
	      }
	    }
	  }
	  case _ => {
	    logger.error("Unknown objectType " + zkMessage.ObjectType + " in zookeeper notification, notification is not processed ..")
	  }
	}
      })
    }catch {
      case e:AlreadyExistsException => {
	logger.error("Failed to load the object(" + key + ") into cache: " + e.getMessage())
      }
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def LoadAllContainersIntoCache{
    try{
      val contKeys = GetAllKeys("ContainerDef")
      if( contKeys.length == 0 ){
	logger.trace("No containers available in the Database")
	return
      }
      contKeys.foreach(key => { 
	val obj = GetObject(key.toLowerCase,containerStore)
	val contDef = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
	AddObjectToCache(contDef.asInstanceOf[ContainerDef],MdMgr.GetMdMgr)
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
	logger.trace("No models available in the Database")
	return
      }
      modKeys.foreach(key => { 
	val obj = GetObject(key.toLowerCase,modelStore)
	val modDef = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
	AddObjectToCache(modDef.asInstanceOf[ModelDef],MdMgr.GetMdMgr)
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
	  val csa = cs.toArray.filter(t => {t.getClass.getName.contains("DerivedAttributeDef")})
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
	  val csa = cs.toArray.filter(t => {t.getClass.getName.contains("DerivedAttributeDef")})
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
  def readMetadataAPIConfigFromPropertiesFile(configFile: String): Unit = {
    try{
      if( propertiesAlreadyLoaded ){
	return;
      }

      val input = new FileInputStream(configFile)
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

      var database_host = prop.getProperty("DATABASE_HOST")
      if (database_host == null ){
	database_host = "localhost"
      }
      logger.trace("database_host => " + database_host)


      var database_schema = "metadata"
      val database_schema1 = prop.getProperty("DATABASE_SCHEMA")
      if (database_schema1 != null){
	database_schema = database_schema1
      }
      logger.trace("DATABASE_SCHEMA(applicable to cassandra only) => " + database_schema)

      var database_location = "/tmp"
      val database_location1 = prop.getProperty("DATABASE_LOCATION")
      if (database_location1 != null ){
	database_location = database_location1
      }
      logger.trace("DATABASE_LOCATION(applicable to treemap or hashmap databases only) => " + database_location1)


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

      var notifyEngine = prop.getProperty("NOTIFY_ENGINE")
      if (notifyEngine == null ){
	logger.warn("The property NOTIFY_ENGINE is not defined in the config file " + configFile + ". It is set to \"NO\"");
	notifyEngine = "NO"
      }
      logger.trace("NOTIFY_ENGINE => " + notifyEngine)

      var znodePath = "/ligadata/metadata"
      var znodePathProp = prop.getProperty("ZNODE_PATH")
      if (znodePathProp == null ){
	logger.warn("The property ZNODE_PATH is not defined in the config file " + configFile + ". It is set to " + znodePath);
      }
      else{
	znodePath = znodePathProp
      }
      logger.trace("ZNODE_PATH => " + znodePath)
      
      var zkConnString = "localhost:2181"
      if( notifyEngine == "YES"){
	val zkStr = prop.getProperty("ZOOKEEPER_CONNECT_STRING")
	if (zkStr != null ){
	  zkConnString = zkStr
	}
	else{
	  logger.warn("The property ZOOKEEPER_CONNECT_STRING must be defined in the config file " + configFile + ". It is set to " + zkConnString)
	}
      }
      logger.trace("ZOOKEEPER_CONNECT_STRING => " + zkConnString)

      var nodeid = zkConnString
      var nodeid1 = prop.getProperty("NODE_ID")
      if (nodeid1 == null ){
	logger.trace("The property NODE_ID is not defined in the properties file:" + configFile)
	logger.trace("The property NODE_ID defaults to " + nodeid)
      }
      else{
	nodeid = nodeid1
      }
      logger.trace("NODE_ID => " + nodeid)

      var servicehost = "localhost"
      var servicehost1 = prop.getProperty("SERVICE_HOST")
      if (servicehost1 == null ){
	logger.trace("The property SERVICE_HOST is not defined in the properties file:" + configFile)
	logger.trace("The property SERVICE_HOST defaults to " + servicehost)
      }
      else{
	servicehost = servicehost1
      }
      logger.trace("SERVICE_HOST => " + servicehost)


      var serviceport = "8080"
      var serviceport1 = prop.getProperty("SERVICE_PORT")
      if (serviceport1 == null ){
	logger.trace("The property SERVICE_PORT is not defined in the properties file:" + configFile)
	logger.trace("The property SERVICE_PORT defaults to " + serviceport)
      }
      else{
	serviceport = serviceport1
      }
      logger.trace("SERVICE_PORT => " + serviceport)



      var apiLeaderSelectionZkNode = "/ligadata/apileader"
      val apiLeaderSelectionZkNode1 = prop.getProperty("API_LEADER_SELECTION_ZK_NODE")
      if (apiLeaderSelectionZkNode1 == null ){
	logger.warn("The property ApiLeaderSelectionZkNode is not defined in the config file " + configFile)
	logger.warn("The property ApiLeaderSelectionZkNode defaults to " + apiLeaderSelectionZkNode)
      }
      else{
	apiLeaderSelectionZkNode = apiLeaderSelectionZkNode1
      }
      logger.trace("API_LEADER_SELECTION_ZK_NODE => " + apiLeaderSelectionZkNode)

      var zkSessionTimeoutMs = "4000"
      val zkSessionTimeoutMs1 = prop.getProperty("ZK_SESSION_TIMEOUT_MS")
      if (zkSessionTimeoutMs1 == null ){
	logger.warn("The property zkSessionTimeoutMs defaults to " + zkSessionTimeoutMs)
      }
      else{
	zkSessionTimeoutMs = zkSessionTimeoutMs1
      }
      logger.trace("ZK_SESSION_TIMEOUT_MS => " + zkSessionTimeoutMs)

      var zkConnectionTimeoutMs = "30000"
      val zkConnectionTimeoutMs1 =  prop.getProperty("ZK_CONNECTION_TIMEOUT_MS")
      if (zkConnectionTimeoutMs1 == null ){
	logger.warn("The property zkConnectionTimeoutMs defaults to " + zkConnectionTimeoutMs)
      }
      else{
	zkConnectionTimeoutMs = zkConnectionTimeoutMs1
      }
      logger.trace("ZK_CONNECTION_TIMEOUT_MS => " + zkConnectionTimeoutMs)

      var MODEL_FILES_DIR = ""
      val MODEL_FILES_DIR1 = prop.getProperty("MODEL_FILES_DIR")
      if (MODEL_FILES_DIR1 == null ){
    	  MODEL_FILES_DIR = git_root + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Models"
      }
      else
    	  MODEL_FILES_DIR = MODEL_FILES_DIR1
      logger.trace("MODEL_FILES_DIR => " + MODEL_FILES_DIR)

      var TYPE_FILES_DIR = ""
      val TYPE_FILES_DIR1 = prop.getProperty("TYPE_FILES_DIR")
      if (TYPE_FILES_DIR1 == null ){
    	  TYPE_FILES_DIR = git_root + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Types"
      }
      else
    	  TYPE_FILES_DIR = TYPE_FILES_DIR1
      logger.trace("TYPE_FILES_DIR => " + TYPE_FILES_DIR)

      var FUNCTION_FILES_DIR = ""
      val FUNCTION_FILES_DIR1 = prop.getProperty("FUNCTION_FILES_DIR")
      if (FUNCTION_FILES_DIR1 == null ){
    	  FUNCTION_FILES_DIR = git_root + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Functions"
      }
      else
    	  FUNCTION_FILES_DIR = FUNCTION_FILES_DIR1
      logger.trace("FUNCTION_FILES_DIR => " + FUNCTION_FILES_DIR)

      var CONCEPT_FILES_DIR = ""
      val CONCEPT_FILES_DIR1 = prop.getProperty("CONCEPT_FILES_DIR")
      if (CONCEPT_FILES_DIR1 == null ){
    	  CONCEPT_FILES_DIR = git_root + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Concepts"
      }
      else
    	  CONCEPT_FILES_DIR = CONCEPT_FILES_DIR1
      logger.trace("CONCEPT_FILES_DIR => " + CONCEPT_FILES_DIR)

      var MESSAGE_FILES_DIR = ""
      val MESSAGE_FILES_DIR1 = prop.getProperty("MESSAGE_FILES_DIR")
      if (MESSAGE_FILES_DIR1 == null ){
    	  MESSAGE_FILES_DIR = git_root + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Messages"
      }
      else
    	  MESSAGE_FILES_DIR = MESSAGE_FILES_DIR1
      logger.trace("MESSAGE_FILES_DIR => " + MESSAGE_FILES_DIR)

      var CONTAINER_FILES_DIR = ""
      val CONTAINER_FILES_DIR1 = prop.getProperty("CONTAINER_FILES_DIR")
      if (CONTAINER_FILES_DIR1 == null ){
    	  CONTAINER_FILES_DIR = git_root + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Containers"
      }
      else
    	  CONTAINER_FILES_DIR = CONTAINER_FILES_DIR1
        
      logger.trace("CONTAINER_FILES_DIR => " + CONTAINER_FILES_DIR)

      var COMPILER_WORK_DIR = ""
      val COMPILER_WORK_DIR1 = prop.getProperty("COMPILER_WORK_DIR")
      if (COMPILER_WORK_DIR1 == null ){
    	  COMPILER_WORK_DIR = "/tmp"
      }
      else
    	  COMPILER_WORK_DIR = COMPILER_WORK_DIR1
        
      logger.trace("COMPILER_WORK_DIR => " + COMPILER_WORK_DIR)


      metadataAPIConfig.setProperty("ROOT_DIR",root_dir)
      metadataAPIConfig.setProperty("DATABASE",database)
      metadataAPIConfig.setProperty("DATABASE_HOST",database_host)
      metadataAPIConfig.setProperty("DATABASE_SCHEMA",database_schema)
      metadataAPIConfig.setProperty("DATABASE_LOCATION",database_location)
      metadataAPIConfig.setProperty("GIT_ROOT",git_root)
      metadataAPIConfig.setProperty("JAR_TARGET_DIR",jar_target_dir)
      metadataAPIConfig.setProperty("SCALA_HOME",scala_home)
      metadataAPIConfig.setProperty("JAVA_HOME",java_home)
      metadataAPIConfig.setProperty("MANIFEST_PATH",manifest_path)
      metadataAPIConfig.setProperty("CLASSPATH",classpath)
      metadataAPIConfig.setProperty("NOTIFY_ENGINE",notifyEngine)
      metadataAPIConfig.setProperty("ZNODE_PATH",znodePath)
      metadataAPIConfig.setProperty("ZOOKEEPER_CONNECT_STRING",zkConnString)
      metadataAPIConfig.setProperty("NODE_ID",nodeid)
      metadataAPIConfig.setProperty("SERVICE_HOST",servicehost)
      metadataAPIConfig.setProperty("SERVICE_PORT",serviceport)
      metadataAPIConfig.setProperty("API_LEADER_SELECTION_ZK_NODE",apiLeaderSelectionZkNode)
      metadataAPIConfig.setProperty("ZK_SESSION_TIMEOUT_MS",zkSessionTimeoutMs)
      metadataAPIConfig.setProperty("ZK_CONNECTION_TIMEOUT_MS",zkConnectionTimeoutMs)
      metadataAPIConfig.setProperty("MODEL_FILES_DIR",MODEL_FILES_DIR)
      metadataAPIConfig.setProperty("TYPE_FILES_DIR",TYPE_FILES_DIR)
      metadataAPIConfig.setProperty("FUNCTION_FILES_DIR",FUNCTION_FILES_DIR)
      metadataAPIConfig.setProperty("CONCEPT_FILES_DIR",CONCEPT_FILES_DIR)
      metadataAPIConfig.setProperty("MESSAGE_FILES_DIR",MESSAGE_FILES_DIR)
      metadataAPIConfig.setProperty("CONTAINER_FILES_DIR",CONTAINER_FILES_DIR)
      metadataAPIConfig.setProperty("COMPILER_WORK_DIR",COMPILER_WORK_DIR)

      propertiesAlreadyLoaded = true;
      
    } catch { 
      case e: Exception => 
	logger.error("Failed to load configuration: " + e.getMessage)
	sys.exit(1)
    }
  }


  @throws(classOf[MissingPropertyException])
  @throws(classOf[LoadAPIConfigException])
  def readMetadataAPIConfigFromJsonFile(cfgFile: String): Unit = {
    try{
      if( propertiesAlreadyLoaded ){
	return;
      }
      var configFile = "MetadataAPIConfig.json"
      if( cfgFile != null ){
	configFile = cfgFile
      }

      val configJson = Source.fromFile(configFile).mkString
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(configJson)

      logger.trace("Parsed the json : " + configJson)
      val configMap = json.extract[MetadataAPIConfig]

      var rootDir = configMap.APIConfigParameters.RootDir
      if (rootDir == null ){
	rootDir = System.getenv("HOME")
      }
      logger.trace("RootDir => " + rootDir)

      var gitRootDir = configMap.APIConfigParameters.GitRootDir
      if (gitRootDir == null ){
	gitRootDir = rootDir + "git_hub"
      }
      logger.trace("GitRootDir => " + gitRootDir)

      var database = configMap.APIConfigParameters.Database
      if (database == null ){
	database = "hashmap"
      }
      logger.trace("Database => " + database)

      var databaseHost = configMap.APIConfigParameters.DatabaseHost
      if (databaseHost == null ){
	databaseHost = "hashmap"
      }
      logger.trace("DatabaseHost => " + databaseHost)


      var databaseSchema = "metadata"
      val databaseSchemaOpt = configMap.APIConfigParameters.DatabaseSchema
      if (databaseSchemaOpt != None ){
	databaseSchema = databaseSchemaOpt.get
      }
      logger.trace("DatabaseSchema(applicable to cassandra only) => " + databaseSchema)

      var databaseLocation = "/tmp"
      val databaseLocationOpt = configMap.APIConfigParameters.DatabaseLocation
      if (databaseLocationOpt != None ){
	databaseLocation = databaseLocationOpt.get
      }
      logger.trace("DatabaseLocation(applicable to treemap or hashmap databases only) => " + databaseLocation)


      var jarTargetDir = configMap.APIConfigParameters.JarTargetDir
      if (jarTargetDir == null ){
	throw new MissingPropertyException("The property JarTargetDir must be defined in the config file " + configFile)
      }
      logger.trace("JarTargetDir => " + jarTargetDir)

      var scalaHome = configMap.APIConfigParameters.ScalaHome
      if (scalaHome == null ){
	throw new MissingPropertyException("The property ScalaHome must be defined in the config file " + configFile)
      }
      logger.trace("ScalaHome => " + scalaHome)

      var javaHome = configMap.APIConfigParameters.JavaHome
      if (javaHome == null ){
	throw new MissingPropertyException("The property JavaHome must be defined in the config file " + configFile)
      }
      logger.trace("JavaHome => " + javaHome)

      var manifestPath = configMap.APIConfigParameters.ManifestPath
      if (manifestPath == null ){
	throw new MissingPropertyException("The property ManifestPath must be defined in the config file " + configFile)
      }
      logger.trace("ManifestPath => " + manifestPath)

      var classPath = configMap.APIConfigParameters.ClassPath
      if (classPath == null ){
	throw new MissingPropertyException("The property ClassPath must be defined in the config file " + configFile)
      }
      logger.trace("ClassPath => " + classPath)

      var notifyEngine = configMap.APIConfigParameters.NotifyEngine
      if (notifyEngine == null ){
	throw new MissingPropertyException("The property NotifyEngine must be defined in the config file " + configFile)
      }
      logger.trace("NotifyEngine => " + notifyEngine)

      var znodePath = configMap.APIConfigParameters.ZnodePath
      if (znodePath == null ){
	throw new MissingPropertyException("The property ZnodePath must be defined in the config file " + configFile)
      }
      logger.trace("ZNodePath => " + znodePath)

      var zooKeeperConnectString = configMap.APIConfigParameters.ZooKeeperConnectString
      if (zooKeeperConnectString == null ){
	throw new MissingPropertyException("The property ZooKeeperConnectString must be defined in the config file " + configFile)
      }
      logger.trace("ZooKeeperConnectString => " + zooKeeperConnectString)

      var MODEL_FILES_DIR = ""
      val MODEL_FILES_DIR1 = configMap.APIConfigParameters.MODEL_FILES_DIR
      if (MODEL_FILES_DIR1 == None ){
    	  MODEL_FILES_DIR = gitRootDir + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Models"
      }
      else
    	  MODEL_FILES_DIR = MODEL_FILES_DIR1.get
      logger.trace("MODEL_FILES_DIR => " + MODEL_FILES_DIR)

      var TYPE_FILES_DIR = ""
      val TYPE_FILES_DIR1 = configMap.APIConfigParameters.TYPE_FILES_DIR
      if (TYPE_FILES_DIR1 == None ){
    	  TYPE_FILES_DIR = gitRootDir + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Types"
      }
      else
    	  TYPE_FILES_DIR = TYPE_FILES_DIR1.get
      logger.trace("TYPE_FILES_DIR => " + TYPE_FILES_DIR)

      var FUNCTION_FILES_DIR = ""
      val FUNCTION_FILES_DIR1 = configMap.APIConfigParameters.FUNCTION_FILES_DIR
      if (FUNCTION_FILES_DIR1 == None ){
    	  FUNCTION_FILES_DIR = gitRootDir + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Functions"
      }
      else
    	  FUNCTION_FILES_DIR = FUNCTION_FILES_DIR1.get
      logger.trace("FUNCTION_FILES_DIR => " + FUNCTION_FILES_DIR)

      var CONCEPT_FILES_DIR = ""
      val CONCEPT_FILES_DIR1 = configMap.APIConfigParameters.CONCEPT_FILES_DIR
      if (CONCEPT_FILES_DIR1 == None ){
    	  CONCEPT_FILES_DIR = gitRootDir + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Concepts"
      }
      else
    	  CONCEPT_FILES_DIR = CONCEPT_FILES_DIR1.get
      logger.trace("CONCEPT_FILES_DIR => " + CONCEPT_FILES_DIR)

      var MESSAGE_FILES_DIR = ""
      val MESSAGE_FILES_DIR1 = configMap.APIConfigParameters.MESSAGE_FILES_DIR
      if (MESSAGE_FILES_DIR1 == None ){
    	  MESSAGE_FILES_DIR = gitRootDir + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Messages"
      }
      else
    	  MESSAGE_FILES_DIR = MESSAGE_FILES_DIR1.get
      logger.trace("MESSAGE_FILES_DIR => " + MESSAGE_FILES_DIR)

      var CONTAINER_FILES_DIR = ""
      val CONTAINER_FILES_DIR1 = configMap.APIConfigParameters.CONTAINER_FILES_DIR
      if (CONTAINER_FILES_DIR1 == None ){
    	  CONTAINER_FILES_DIR = gitRootDir + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Containers"
      }
      else
    	  CONTAINER_FILES_DIR = CONTAINER_FILES_DIR1.get
        
      logger.trace("CONTAINER_FILES_DIR => " + CONTAINER_FILES_DIR)

      var COMPILER_WORK_DIR = ""
      val COMPILER_WORK_DIR1 = configMap.APIConfigParameters.COMPILER_WORK_DIR
      if (COMPILER_WORK_DIR1 == None ){
    	  COMPILER_WORK_DIR = "/tmp"
      }
      else
    	  COMPILER_WORK_DIR = COMPILER_WORK_DIR1.get
        
      logger.trace("COMPILER_WORK_DIR => " + COMPILER_WORK_DIR)


      metadataAPIConfig.setProperty("ROOT_DIR",rootDir)
      metadataAPIConfig.setProperty("GIT_ROOT",gitRootDir)
      metadataAPIConfig.setProperty("DATABASE",database)
      metadataAPIConfig.setProperty("DATABASE_HOST",databaseHost)
      metadataAPIConfig.setProperty("DATABASE_SCHEMA",databaseSchema)
      metadataAPIConfig.setProperty("DATABASE_LOCATION",databaseLocation)
      metadataAPIConfig.setProperty("JAR_TARGET_DIR",jarTargetDir)
      metadataAPIConfig.setProperty("SCALA_HOME",scalaHome)
      metadataAPIConfig.setProperty("JAVA_HOME",javaHome)
      metadataAPIConfig.setProperty("MANIFEST_PATH",manifestPath)
      metadataAPIConfig.setProperty("CLASSPATH",classPath)
      metadataAPIConfig.setProperty("NOTIFY_ENGINE",notifyEngine)
      metadataAPIConfig.setProperty("ZNODE_PATH",znodePath)
      metadataAPIConfig.setProperty("ZOOKEEPER_CONNECT_STRING",zooKeeperConnectString)
      metadataAPIConfig.setProperty("MODEL_FILES_DIR",MODEL_FILES_DIR)
      metadataAPIConfig.setProperty("TYPE_FILES_DIR",TYPE_FILES_DIR)
      metadataAPIConfig.setProperty("FUNCTION_FILES_DIR",FUNCTION_FILES_DIR)
      metadataAPIConfig.setProperty("CONCEPT_FILES_DIR",CONCEPT_FILES_DIR)
      metadataAPIConfig.setProperty("MESSAGE_FILES_DIR",MESSAGE_FILES_DIR)
      metadataAPIConfig.setProperty("CONTAINER_FILES_DIR",CONTAINER_FILES_DIR)
      metadataAPIConfig.setProperty("COMPILER_WORK_DIR",COMPILER_WORK_DIR)
      
      propertiesAlreadyLoaded = true;

    } catch { 
      case e:MappingException =>{
	throw Json4sParsingException(e.getMessage())
      }
      case e: Exception => {
	throw LoadAPIConfigException("Failed to load configuration: " + e.getMessage())
      }
    }
  }

  def InitMdMgr(configFile: String){
    MdMgr.GetMdMgr.truncate
    val mdLoader = new MetadataLoad (MdMgr.mdMgr, "","","","")
    mdLoader.initialize
    if( configFile.endsWith(".json") ){
      MetadataAPIImpl.readMetadataAPIConfigFromJsonFile(configFile)
    }
    else if( configFile.endsWith(".properties") ){
          MetadataAPIImpl.readMetadataAPIConfigFromPropertiesFile(configFile)
    }
    else{
      throw LoadAPIConfigException("Invalid name for config file, it should have a suffix  .json or .properties: " + configFile)
    }
    MetadataAPIImpl.OpenDbStore(GetMetadataAPIConfig.getProperty("DATABASE"))
    MetadataAPIImpl.LoadAllObjectsIntoCache
    MetadataAPIImpl.CloseDbStore
  }

  def InitMdMgrFromBootStrap(configFile: String){
    MdMgr.GetMdMgr.truncate
    val mdLoader = new MetadataLoad (MdMgr.mdMgr,"","","","")
    mdLoader.initialize
    if( configFile.endsWith(".json") ){
      MetadataAPIImpl.readMetadataAPIConfigFromJsonFile(configFile)
    }
    else if( configFile.endsWith(".properties") ){
          MetadataAPIImpl.readMetadataAPIConfigFromPropertiesFile(configFile)
    }
    else{
      throw LoadAPIConfigException("Invalid name for config file, it should have a suffix  .json or .properties: " + configFile)
    }
    MetadataAPIImpl.OpenDbStore(GetMetadataAPIConfig.getProperty("DATABASE"))
    MetadataAPIImpl.LoadAllObjectsIntoCache
  }

  def InitMdMgr(mgr:MdMgr, database:String, databaseHost:String, databaseSchema:String, databaseLocation:String){
    val mdLoader = new MetadataLoad (mgr,"","","","")
    mdLoader.initialize

    metadataAPIConfig.setProperty("DATABASE",database)
    metadataAPIConfig.setProperty("DATABASE_HOST",databaseHost)
    metadataAPIConfig.setProperty("DATABASE_SCHEMA",databaseSchema)
    metadataAPIConfig.setProperty("DATABASE_LOCATION",databaseLocation)

    MetadataAPIImpl.OpenDbStore(GetMetadataAPIConfig.getProperty("DATABASE"))
    MetadataAPIImpl.LoadAllObjectsIntoCache
  }
}
