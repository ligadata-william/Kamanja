package com.ligadata.MetadataAPI


import java.util.Properties
import java.io._
import scala.Enumeration
import scala.io._

import com.ligadata.olep.metadata.ObjType._
import com.ligadata.olep.metadata._
import com.ligadata.olep.metadata.MdMgr._

//import com.ligadata.olep.metadataload.MetadataLoad
import com.ligadata.edifecs.MetadataLoad

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

import com.ligadata.ZooKeeper._
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

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

case class ParameterMap(RootDir:String, GitRootDir: String, Database: String,DatabaseHost: String, JarTargetDir: String, ScalaHome: String, JavaHome: String, ManifestPath: String, ClassPath: String, NotifyEngine: String, ZnodePath:String, ZooKeeperConnectString: String)
case class MetadataAPIConfig(APIConfigParameters: ParameterMap)

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

case class LoadAPIConfigException(e: String) extends Throwable(e)
case class MissingPropertyException(e: String) extends Throwable(e)
case class InvalidPropertyException(e: String) extends Throwable(e)
case class KryoSerializationException(e: String) extends Throwable(e)
case class InternalErrorException(e: String) extends Throwable(e)


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
    
  lazy val sysNS = "System"		// system name space
  lazy val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  lazy val serializer = SerializerManager.GetSerializer("kryo")
  lazy val metadataAPIConfig = new Properties()
  var zkc:CuratorFramework = null
  val configFile = System.getenv("HOME") + "/MetadataAPIConfig.json"
  var propertiesAlreadyLoaded = false

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
      zkc = CreateClient.createSimple(zkcConnectString)
      if(zkc.checkExists().forPath(znodePath) == null ){
	zkc.create().withMode(CreateMode.PERSISTENT).forPath(znodePath,null);
      }
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

  def SaveObject(key: String, value: Array[Byte], store: DataStore){
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
    try{
      var obj = GetObject(key.toLowerCase,store)
      logger.trace("Found an existing deleted object for : " + key)
      UpdateObject(key,value,store)
    }
    catch{
      case e:ObjectNotFoundException => {
	logger.trace("Insert new object for : " + key)
	val t = store.beginTx
	store.add(i)
	store.commitTx(t)
      }
    }
  }

  def SaveObject(key: String, value: String, store: DataStore){
    var ba = serializer.SerializeObjectToByteArray(value)
    try{
      var obj = GetObject(key.toLowerCase,store)
      logger.trace("Found an existing deleted object for : " + key)
      UpdateObject(key,ba,store)
    }
    catch{
      case e:ObjectNotFoundException => {
	SaveObject(key,ba,store)
      }
    }
  }

  def UpdateObject(key: String, value: Array[Byte], store: DataStore){
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
	return
      }
      val data = ZooKeeperMessage(objList,operations)
      InitZooKeeper
      val znodePath = GetMetadataAPIConfig.getProperty("ZNODE_PATH")
      logger.trace("Set the data on the zookeeper node " + znodePath)
      zkc.setData().forPath(znodePath,data)
    }catch{
      case e:Exception => {
	  throw new InternalErrorException("Failed to notify a zookeeper message from the objectList " + e.getMessage())
      }
    }
  }

  def SaveObject(obj: BaseElemDef){
    try{
      val key = obj.FullNameWithVer.toLowerCase
      //val value = JsonSerializer.SerializeObjectToJson(obj)
      logger.trace("Serialize the object: name of the object => " + obj.FullNameWithVer)
      var value = serializer.SerializeObjectToByteArray(obj)
      obj match{
	case o:ModelDef => {
	  logger.trace("Adding the model to the cache: name of the object =>  " + key)
	  SaveObject(key,value,modelStore)
	  MdMgr.GetMdMgr.AddModelDef(o)
	}
	case o:MessageDef => {
	  logger.trace("Adding the message to the cache: name of the object =>  " + key)
	  SaveObject(key,value,messageStore)
	  MdMgr.GetMdMgr.AddMsg(o)
	}
	case o:ContainerDef => {
	  logger.trace("Adding the container to the cache: name of the object =>  " + key)
	  SaveObject(key,value,containerStore)
	  MdMgr.GetMdMgr.AddContainer(o)
	}
	case o:FunctionDef => {
          val funcKey = o.typeString.toLowerCase
	  logger.trace("Adding the function to the cache: name of the object =>  " + funcKey)
	  SaveObject(funcKey,value,functionStore)
	  MdMgr.GetMdMgr.AddFunc(o)
	}
	case o:AttributeDef => {
	  logger.trace("Adding the attribute to the cache: name of the object =>  " + key)
	  SaveObject(key,value,conceptStore)
	  MdMgr.GetMdMgr.AddAttribute(o)
	}
	case o:ScalarTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  MdMgr.GetMdMgr.AddScalar(o)
	}
	case o:ArrayTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  MdMgr.GetMdMgr.AddArray(o)
	}
	case o:ArrayBufTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  MdMgr.GetMdMgr.AddArrayBuffer(o)
	}
	case o:ListTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  MdMgr.GetMdMgr.AddList(o)
	}
	case o:QueueTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  MdMgr.GetMdMgr.AddQueue(o)
	}
	case o:SetTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  MdMgr.GetMdMgr.AddSet(o)
	}
	case o:TreeSetTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  MdMgr.GetMdMgr.AddTreeSet(o)
	}
	case o:SortedSetTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  MdMgr.GetMdMgr.AddSortedSet(o)
	}
	case o:MapTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  MdMgr.GetMdMgr.AddMap(o)
	}
	case o:HashMapTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  MdMgr.GetMdMgr.AddHashMap(o)
	}
	case o:TupleTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  MdMgr.GetMdMgr.AddTupleType(o)
	}
	case o:ContainerTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  SaveObject(key,value,typeStore)
	  MdMgr.GetMdMgr.AddContainerType(o)
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
      val key = obj.FullNameWithVer.toLowerCase
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
          val funcKey = o.typeString.toLowerCase
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
	  MdMgr.GetMdMgr.AddSet(o)
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

  def UpdateObjectInCache(obj: BaseElemDef,operation:String): BaseElemDef = {
    var updatedObject:BaseElemDef = null
    try{
      obj match{
	case o:FunctionDef => {
	  updatedObject = MdMgr.GetMdMgr.ModifyFunction(o.nameSpace,o.name,o.ver,operation)
	}
	case o:ModelDef => {
	  updatedObject = MdMgr.GetMdMgr.ModifyModel(o.nameSpace,o.name,o.ver,operation)
	}
	case o:MessageDef => {
	  updatedObject = MdMgr.GetMdMgr.ModifyMessage(o.nameSpace,o.name,o.ver,operation)
	}
	case o:ContainerDef => {
	  updatedObject = MdMgr.GetMdMgr.ModifyContainer(o.nameSpace,o.name,o.ver,operation)
	}
	case o:AttributeDef => {
	  updatedObject = MdMgr.GetMdMgr.ModifyAttribute(o.nameSpace,o.name,o.ver,operation)
	}
	case o:ScalarTypeDef => {
	  updatedObject = MdMgr.GetMdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:ArrayTypeDef => {
	  updatedObject = MdMgr.GetMdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:ArrayBufTypeDef => {
	  updatedObject = MdMgr.GetMdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:ListTypeDef => {
	  updatedObject = MdMgr.GetMdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:QueueTypeDef => {
	  updatedObject = MdMgr.GetMdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:SetTypeDef => {
	  updatedObject = MdMgr.GetMdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:TreeSetTypeDef => {
	  updatedObject = MdMgr.GetMdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:SortedSetTypeDef => {
	  updatedObject = MdMgr.GetMdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:MapTypeDef => {
	  updatedObject = MdMgr.GetMdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:HashMapTypeDef => {
	  updatedObject = MdMgr.GetMdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:TupleTypeDef => {
	  updatedObject = MdMgr.GetMdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
	}
	case o:ContainerTypeDef => {
	  updatedObject = MdMgr.GetMdMgr.ModifyType(o.nameSpace,o.name,o.ver,operation)
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



  def AddObjectToCache(obj: BaseElemDef){
    // If the object's Delete flag is set, this is a noop.
    if (obj.IsDeleted )
      return
    try{
      val key = obj.FullNameWithVer.toLowerCase
      obj match{
	case o:ModelDef => {
	  logger.trace("Adding the model to the cache: name of the object =>  " + key)
	  MdMgr.GetMdMgr.AddModelDef(o)
	}
	case o:MessageDef => {
	  logger.trace("Adding the message to the cache: name of the object =>  " + key)
	  MdMgr.GetMdMgr.AddMsg(o)
	}
	case o:ContainerDef => {
	  logger.trace("Adding the container to the cache: name of the object =>  " + key)
	  MdMgr.GetMdMgr.AddContainer(o)
	}
	case o:FunctionDef => {
          val funcKey = o.typeString.toLowerCase
	  logger.trace("Adding the function to the cache: name of the object =>  " + funcKey)
	  MdMgr.GetMdMgr.AddFunc(o)
	}
	case o:AttributeDef => {
	  logger.trace("Adding the attribute to the cache: name of the object =>  " + key)
	  MdMgr.GetMdMgr.AddAttribute(o)
	}
	case o:ScalarTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  MdMgr.GetMdMgr.AddScalar(o)
	}
	case o:ArrayTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  MdMgr.GetMdMgr.AddArray(o)
	}
	case o:ArrayBufTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  MdMgr.GetMdMgr.AddArrayBuffer(o)
	}
	case o:ListTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  MdMgr.GetMdMgr.AddList(o)
	}
	case o:QueueTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  MdMgr.GetMdMgr.AddQueue(o)
	}
	case o:SetTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  MdMgr.GetMdMgr.AddSet(o)
	}
	case o:TreeSetTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  MdMgr.GetMdMgr.AddTreeSet(o)
	}
	case o:SortedSetTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  MdMgr.GetMdMgr.AddSortedSet(o)
	}
	case o:MapTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  MdMgr.GetMdMgr.AddMap(o)
	}
	case o:HashMapTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  MdMgr.GetMdMgr.AddHashMap(o)
	}
	case o:TupleTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  MdMgr.GetMdMgr.AddTupleType(o)
	}
	case o:ContainerTypeDef => {
	  logger.trace("Adding the Type to the cache: name of the object =>  " + key)
	  MdMgr.GetMdMgr.AddContainerType(o)
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

  def DecodeObjectToMetadataType(obj: Object): BaseElemDef = {
    try{
      obj match{
	case o:ModelDef => {
	  logger.trace("Adding the model to the cache: name of the object =>  " + o.FullNameWithVer)
	  MdMgr.GetMdMgr.AddModelDef(o)
	  o
	}
	case o:MessageDef => {
	  logger.trace("Adding the message to the cache: name of the object =>  " + o.FullNameWithVer)
	  MdMgr.GetMdMgr.AddMsg(o)
	  o
	}
	case o:ContainerDef => {
	  logger.trace("Adding the container to the cache: name of the object =>  " + o.FullNameWithVer)
	  MdMgr.GetMdMgr.AddContainer(o)
	  o
	}
	case o:FunctionDef => {
	  logger.trace("Adding the function to the cache: name of the object =>  " + o.typeString)
	  MdMgr.GetMdMgr.AddFunc(o)
	  o
	}
	case o:AttributeDef => {
	  logger.trace("Adding the attribute to the cache: name of the object =>  " + o.FullNameWithVer)
	  MdMgr.GetMdMgr.AddAttribute(o)
	  o
	}
	case o:ScalarTypeDef => {
	  logger.trace("Adding the type to the cache: name of the object =>  " + o.FullNameWithVer)
	  MdMgr.GetMdMgr.AddScalar(o)
	  o
	}
	case o:ArrayTypeDef => {
	  logger.trace("Adding the type to the cache: name of the object =>  " + o.FullNameWithVer)
	  MdMgr.GetMdMgr.AddArray(o)
	  o
	}
	case o:ArrayBufTypeDef => {
	  logger.trace("Adding the type to the cache: name of the object =>  " + o.FullNameWithVer)
	  MdMgr.GetMdMgr.AddArrayBuffer(o)
	  o
	}
	case o:ListTypeDef => {
	  logger.trace("Adding the type to the cache: name of the object =>  " + o.FullNameWithVer)
	  MdMgr.GetMdMgr.AddList(o)
	  o
	}
	case o:QueueTypeDef => {
	  logger.trace("Adding the type to the cache: name of the object =>  " + o.FullNameWithVer)
	  MdMgr.GetMdMgr.AddQueue(o)
	  o
	}
	case o:SetTypeDef => {
	  logger.trace("Adding the type to the cache: name of the object =>  " + o.FullNameWithVer)
	  MdMgr.GetMdMgr.AddSet(o)
	  o
	}
	case o:TreeSetTypeDef => {
	  logger.trace("Adding the type to the cache: name of the object =>  " + o.FullNameWithVer)
	  MdMgr.GetMdMgr.AddTreeSet(o)
	  o
	}
	case o:SortedSetTypeDef => {
	  logger.trace("Adding the type to the cache: name of the object =>  " + o.FullNameWithVer)
	  MdMgr.GetMdMgr.AddSortedSet(o)
	  o
	}
	case o:MapTypeDef => {
	  logger.trace("Adding the type to the cache: name of the object =>  " + o.FullNameWithVer)
	  MdMgr.GetMdMgr.AddMap(o)
	  o
	}
	case o:HashMapTypeDef => {
	  logger.trace("Adding the type to the cache: name of the object =>  " + o.FullNameWithVer)
	  MdMgr.GetMdMgr.AddHashMap(o)
	  o
	}
	case o:TupleTypeDef => {
	  logger.trace("Adding the type to the cache: name of the object =>  " + o.FullNameWithVer)
	  MdMgr.GetMdMgr.AddTupleType(o)
	  o
	}
	case o:ContainerTypeDef => {
	  logger.trace("Adding the type to the cache: name of the object =>  " + o.FullNameWithVer)
	  MdMgr.GetMdMgr.AddContainerType(o)
	  o
	}
	case _ => {
	  throw new InternalErrorException("Internal Error, Deserialize returned unknown object of type " + obj.getClass.getName)
	}
      }
    }catch{
      case e:AlreadyExistsException => {
	throw new AlreadyExistsException("Failed to Save the object : It already exists: " + e.getMessage())
      }
      case e:Exception => {
	throw new Exception("Unexpected error in DecodeToMetadataType: " + e.getMessage())
      }
    }
  }

  def ModifyObject(obj: BaseElemDef, operation: String){
    try{
      val o1 = UpdateObjectInCache(obj,operation)
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
	  var databaseHost = GetMetadataAPIConfig.getProperty("DATABASE_HOST")
	  if( databaseHost == null ){
	    databaseHost = "localhost"
	  }
	  connectinfo+= ("hostlist" -> databaseHost) 
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
      logger.trace("Parsing type object given as Json String..")
      val typ = JsonSerializer.parseType(typeText,"JSON")
      SaveObject(typ)
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
	    SaveObject(concept) 
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
      SaveObject(contDef)
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
      SaveObject(msgDef)
      var apiResult = new ApiResult(0,"MsgDef was Added",key)
      apiResult.toString()
    }catch {
      case e:Exception =>{
	var apiResult = new ApiResult(-1,"Failed to add the msgDef:",e.toString)
	apiResult.toString()
      }
    }
  }


  def AddMessageTypes(msgDef:MessageDef): Array[BaseElemDef] = {
    val objectsAdded = new Array[BaseElemDef](3)
    try{
      // As per Rich's requirement, Add array/arraybuf/sortedset types for this messageDef
      // along with the messageDef.
      val arrayType = MdMgr.GetMdMgr.MakeArray(msgDef.nameSpace,"arrayof"+msgDef.name,msgDef.nameSpace,
					       msgDef.name,1,msgDef.ver)
      SaveObject(arrayType)
      objectsAdded(0) = arrayType
      val arrayBufType = MdMgr.GetMdMgr.MakeArrayBuffer(msgDef.nameSpace,"arraybufferof"+msgDef.name,
						    msgDef.nameSpace,msgDef.name,1,msgDef.ver)
      SaveObject(arrayBufType)
      objectsAdded(1) = arrayBufType
      val sortedSetType = MdMgr.GetMdMgr.MakeSortedSet(msgDef.nameSpace,"sortedsetof"+msgDef.name,
							   msgDef.nameSpace,msgDef.name,msgDef.ver)
      SaveObject(sortedSetType)
      objectsAdded(2) = sortedSetType
      objectsAdded
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
	  val result = AddMessageDef(msg)
	  var objectsAdded = AddMessageTypes(msg)
	  objectsAdded = objectsAdded :+ msg
	  val operations = for (op <- objectsAdded) yield "Add"
	  NotifyEngine(objectsAdded,operations)
	  result
	}
	case cont:ContainerDef =>{
	  AddContainerDef(cont)
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
	  AddMessageDef(msg)
	}
	case cont:ContainerDef =>{
	  AddContainerDef(cont)
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
	  val isValid = IsValidVersion(latestVersion,msg)
	  if ( isValid ){
	    RemoveMessage(latestVersion.nameSpace,latestVersion.name,latestVersion.ver)
	    AddMessageDef(msg)
	  }
	  else{
	    var apiResult = new ApiResult(-1,"Failed to update the message:" + key,"Invalid Version")
	    apiResult.toString()
	  }
	}
	case msg:ContainerDef =>{
	  val latestVersion = GetLatestContainer(msg)
	  val isValid = IsValidVersion(latestVersion,msg)
	  if ( isValid ){
	    RemoveContainer(latestVersion.nameSpace,latestVersion.name,latestVersion.ver)
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
	  DeleteObject(m.asInstanceOf[ContainerDef])
	  val objectsUpdated = new Array[BaseElemDef](1)
	  objectsUpdated(0) = m.asInstanceOf[ContainerDef]
	  val operations = for (op <- objectsUpdated) yield "Remove"
	  NotifyEngine(objectsUpdated,operations)
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

	  // remove a type with same name as messageDef
	  var typeName = name 
	  var objectsRemoved = new Array[BaseElemDef](5)
	  var typeDef = MdMgr.GetMdMgr.Type(nameSpace,typeName,version,true)
	  var i = 0
	  if( typeDef != None ){
	    objectsRemoved(i) = typeDef.get
	    i = i+1
	  }
	  RemoveType(nameSpace,typeName,version)

	  // arrayof messageDef
	  typeName = "arrayof" + name 
	  typeDef = MdMgr.GetMdMgr.Type(nameSpace,typeName,version,true)
	  if( typeDef != None ){
	    objectsRemoved(i) = typeDef.get
	    i = i+1
	  }
	  RemoveType(nameSpace,typeName,version)

	  // sortedset of MessgeDef
	  typeName = "sortedsetof" + name 
	  typeDef = MdMgr.GetMdMgr.Type(nameSpace,typeName,version,true)
	  if( typeDef != None ){
	    objectsRemoved(i) = typeDef.get
	    i = i+1
	  }
	  RemoveType(nameSpace,typeName,version)

	  // arraybuffer of MessageDef
	  typeName = "arraybufferof" + name 
	  typeDef = MdMgr.GetMdMgr.Type(nameSpace,typeName,version,true)
	  if( typeDef != None ){
	    objectsRemoved(i) = typeDef.get
	    i = i+1
	  }
	  RemoveType(nameSpace,typeName,version)

	  // MessageDef itself
	  DeleteObject(msgDef)
	  objectsRemoved(i) = msgDef

	  val operations = for (op <- objectsRemoved) yield "Remove"
	  NotifyEngine(objectsRemoved,operations)
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
	  val objectsUpdated = new Array[BaseElemDef](1)
	  objectsUpdated(0) = m.asInstanceOf[ModelDef]
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
	  val objectsUpdated = new Array[BaseElemDef](1)
	  objectsUpdated(0) = m.asInstanceOf[ModelDef]
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
	  val objectsUpdated = new Array[BaseElemDef](1)
	  objectsUpdated(0) = m.asInstanceOf[ModelDef]
	  val operations = for (op <- objectsUpdated) yield "Remove"
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
      //var value = JsonSerializer.SerializeObjectToJson(model)

      //logger.trace("key => " + key + ",value =>" + value);

      SaveObject(model)

      //Notify(model)
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
      val apiResult = AddModel(modDef)
      logger.trace("Model is added..")
      val objectsAdded = new Array[BaseElemDef](1)
      objectsAdded(0) = modDef
      val operations = for (op <- objectsAdded) yield "Add"
      logger.trace("Notify engine via zookeeper")
      NotifyEngine(objectsAdded,operations)
      apiResult
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
      val key = MdMgr.MkFullNameWithVersion(modDef.nameSpace,modDef.name,modDef.ver)
      val latestVersion = GetLatestModel(modDef)
      val isValid = IsValidVersion(latestVersion,modDef)
      if ( isValid ){
	RemoveModel(latestVersion.nameSpace,latestVersion.name,latestVersion.ver)
	val result = AddModel(modDef)
	val objectsUpdated = new Array[BaseElemDef](2)
	val operations = new Array[String](2)
	objectsUpdated(0) = latestVersion
	operations(0) = "Remove"
	objectsUpdated(1) = modDef
	operations(1) = "Add"
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
  def GetLatestModel(modDef: ModelDef) : ModelDef = {
    try{
      var key = modDef.nameSpace + "." + modDef.name + "." + modDef.ver
      val o = MdMgr.GetMdMgr.Models(modDef.nameSpace.toLowerCase,
				    modDef.name.toLowerCase,
				    true,
				    true)
      o match{
	case None => None
	  logger.trace("model not in the cache => " + key)
	  throw new ObjectNotFoundException("Unable to find the object " +  key)
	case Some(m) => 
	  logger.trace("model found => " + m.asInstanceOf[ModelDef].FullNameWithVer)
	  m.asInstanceOf[ModelDef]
      }
    }catch {
      case e:Exception =>{
	e.printStackTrace()
	throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }

  // Get the latest message for a given FullName
  def GetLatestMessage(msgDef: MessageDef) : MessageDef = {
    try{
      var key = msgDef.nameSpace + "." + msgDef.name + "." + msgDef.ver
      val o = MdMgr.GetMdMgr.Messages(msgDef.nameSpace.toLowerCase,
				    msgDef.name.toLowerCase,
				    true,
				    true)
      o match{
	case None => None
	  logger.trace("message not in the cache => " + key)
	  throw new ObjectNotFoundException("Unable to find the object " +  key)
	case Some(m) => 
	  logger.trace("message found => " + m.asInstanceOf[MessageDef].FullNameWithVer)
	  m.asInstanceOf[MessageDef]
      }
    }catch {
      case e:Exception =>{
	e.printStackTrace()
	throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }


  // Get the latest container for a given FullName
  def GetLatestContainer(contDef: ContainerDef) : ContainerDef = {
    try{
      var key = contDef.nameSpace + "." + contDef.name + "." + contDef.ver
      val o = MdMgr.GetMdMgr.Containers(contDef.nameSpace.toLowerCase,
				    contDef.name.toLowerCase,
				    true,
				    true)
      o match{
	case None => None
	  logger.trace("container not in the cache => " + key)
	  throw new ObjectNotFoundException("Unable to find the object " +  key)
	case Some(m) => 
	  logger.trace("container found => " + m.asInstanceOf[ContainerDef].FullNameWithVer)
	  m.asInstanceOf[ContainerDef]
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
	logger.trace("No types available in the Database")
	return
      }
      typeKeys.foreach(key => { 
	val typeKey = KeyAsStr(key)
	val obj = GetObject(typeKey.toLowerCase,typeStore)
	val typ =  serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
	if( typ != null ){
	  DecodeObjectToMetadataType(typ)
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
	val conceptKey = KeyAsStr(key)
	val obj = GetObject(conceptKey.toLowerCase,conceptStore)
	val concept =  serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
	AddObjectToCache(concept.asInstanceOf[AttributeDef])
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
	val functionKey = KeyAsStr(key)
	val obj = GetObject(functionKey.toLowerCase,functionStore)
	val function =  serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
	AddObjectToCache(function.asInstanceOf[FunctionDef])
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
	val msgKey = KeyAsStr(key)
	val obj = GetObject(msgKey.toLowerCase,messageStore)
	val msg = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
	AddObjectToCache(msg.asInstanceOf[MessageDef])
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
	AddObjectToCache(msg.asInstanceOf[MessageDef])
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
	  DecodeObjectToMetadataType(typ)
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
	AddObjectToCache(model.asInstanceOf[ModelDef])
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
	AddObjectToCache(cont.asInstanceOf[ContainerDef])
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
	AddObjectToCache(cont.asInstanceOf[FunctionDef])
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
	AddObjectToCache(cont.asInstanceOf[AttributeDef])
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
	key = zkMessage.NameSpace + "." + zkMessage.Name + "." + zkMessage.Version
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
	  case "ScalarTypeDef" | "ArrayTypeDef" | "ArrayBufTypeDef" | "ListTypeDef" | "SetTypeDef" | "TreeSetTypeDef" | "QueueTypeDef" | "HashMapTypeDef" | "TupleTypeDef" | "StructTypeDef" | "SortedSetTypeDef" => {
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
	val contKey = KeyAsStr(key)
	val obj = GetObject(contKey.toLowerCase,containerStore)
	val contDef = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
	AddObjectToCache(contDef.asInstanceOf[ContainerDef])
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
	val modKey = KeyAsStr(key)
	val obj = GetObject(modKey.toLowerCase,modelStore)
	val modDef = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
	AddObjectToCache(modDef.asInstanceOf[ModelDef])
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
	println("No models available in the Metadata")
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
  def readMetadataAPIConfigFromPropertiesFile(configFile: String): Unit = {
    try{
      if( propertiesAlreadyLoaded ){
	return;
      }
      //val input = MetadataAPIImpl.getClass.getClassLoader().getResourceAsStream(configFile)
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

      metadataAPIConfig.setProperty("ROOT_DIR",root_dir)
      metadataAPIConfig.setProperty("DATABASE",database)
      metadataAPIConfig.setProperty("DATABASE_HOST",database_host)
      metadataAPIConfig.setProperty("GIT_ROOT",git_root)
      metadataAPIConfig.setProperty("JAR_TARGET_DIR",jar_target_dir)
      metadataAPIConfig.setProperty("SCALA_HOME",scala_home)
      metadataAPIConfig.setProperty("JAVA_HOME",java_home)
      metadataAPIConfig.setProperty("MANIFEST_PATH",manifest_path)
      metadataAPIConfig.setProperty("CLASSPATH",classpath)
      metadataAPIConfig.setProperty("NOTIFY_ENGINE",notifyEngine)
      metadataAPIConfig.setProperty("ZNODE_PATH",znodePath)
      metadataAPIConfig.setProperty("ZOOKEEPER_CONNECT_STRING",zkConnString)

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
      logger.trace("NotifyEngine => " + znodePath)

      var zooKeeperConnectString = configMap.APIConfigParameters.ZooKeeperConnectString
      if (zooKeeperConnectString == null ){
	throw new MissingPropertyException("The property NotifyEngine must be defined in the config file " + configFile)
      }
      logger.trace("NotifyEngine => " + zooKeeperConnectString)


      metadataAPIConfig.setProperty("ROOT_DIR",rootDir)
      metadataAPIConfig.setProperty("GIT_ROOT",gitRootDir)
      metadataAPIConfig.setProperty("DATABASE",database)
      metadataAPIConfig.setProperty("DATABASE_HOST",databaseHost)
      metadataAPIConfig.setProperty("JAR_TARGET_DIR",jarTargetDir)
      metadataAPIConfig.setProperty("SCALA_HOME",scalaHome)
      metadataAPIConfig.setProperty("JAVA_HOME",javaHome)
      metadataAPIConfig.setProperty("MANIFEST_PATH",manifestPath)
      metadataAPIConfig.setProperty("CLASSPATH",classPath)
      metadataAPIConfig.setProperty("NOTIFY_ENGINE",notifyEngine)
      metadataAPIConfig.setProperty("ZNODE_PATH",znodePath)
      metadataAPIConfig.setProperty("ZOOKEEPER_CONNECT_STRING",zooKeeperConnectString)

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

  def InitMdMgr {
    MdMgr.GetMdMgr.truncate
    val mdLoader = new MetadataLoad (MdMgr.mdMgr, "","","","")
    mdLoader.initialize
    MetadataAPIImpl.readMetadataAPIConfigFromJsonFile(configFile)
    //MetadataAPIImpl.readMetadataAPIConfigFromPropertiesFile(configFile)
    MetadataAPIImpl.OpenDbStore(GetMetadataAPIConfig.getProperty("DATABASE"))
    MetadataAPIImpl.LoadObjectsIntoCache
    MetadataAPIImpl.CloseDbStore
  }

  def InitMdMgrFromBootStrap{
    MdMgr.GetMdMgr.truncate
    val mdLoader = new MetadataLoad (MdMgr.mdMgr,"","","","")
    mdLoader.initialize
    MetadataAPIImpl.readMetadataAPIConfigFromJsonFile(configFile)
    //MetadataAPIImpl.readMetadataAPIConfigFromPropertiesFile(configFile)
    MetadataAPIImpl.OpenDbStore(GetMetadataAPIConfig.getProperty("DATABASE"))
    MetadataAPIImpl.LoadObjectsIntoCache
  }
}
