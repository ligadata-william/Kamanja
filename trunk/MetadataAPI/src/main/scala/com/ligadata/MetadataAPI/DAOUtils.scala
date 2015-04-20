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
import scala.util.parsing.json.{ JSONObject, JSONArray }
import scala.collection.immutable.Map
import scala.collection.immutable.HashMap
import scala.collection.mutable.HashMap

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
import util.control.Breaks._

import java.util.Date

// The implementation class
object DAOUtils {

  lazy val sysNS = "System" // system name space
  lazy val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  lazy val serializer = SerializerManager.GetSerializer("kryo")

  private def GetMetadataAPIConfig: Properties = {
    MetadataAPIImpl.metadataAPIConfig
  }

  def SetLoggerLevel(level: Level) {
    logger.setLevel(level);
  }

  def KeyAsStr(k: com.ligadata.keyvaluestore.Key): String = {
    val k1 = k.toArray[Byte]
    new String(k1)
  }

  def ValueAsStr(v: com.ligadata.keyvaluestore.Value): String = {
    val v1 = v.toArray[Byte]
    new String(v1)
  }

  def GetObject(key: com.ligadata.keyvaluestore.Key, store: DataStore): IStorage = {
    try {
      object o extends IStorage {
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
      logger.debug("Found the object in the store, key => " + KeyAsStr(k))
      store.get(k, o)
      o
    } catch {
      case e: KeyNotFoundException => {
        logger.debug("The object " + KeyAsStr(key) + " is not found in the database. Error => " + e.getMessage())
        throw new ObjectNotFoundException(e.getMessage())
      }
      case e: Exception => {
        e.printStackTrace()
        logger.debug("The object " + KeyAsStr(key) + " is not found in the database, Generic Exception: Error => " + e.getMessage())
        throw new ObjectNotFoundException(e.getMessage())
      }
    }
  }

  def GetObject(key: String, store: DataStore): IStorage = {
    try {
      var k = new com.ligadata.keyvaluestore.Key
      for (c <- key) {
        k += c.toByte
      }
      GetObject(k, store)
    }
  }

  def SaveObject(key: String, value: Array[Byte], store: DataStore) {
    val t = store.beginTx
    object obj extends IStorage {
      var k = new com.ligadata.keyvaluestore.Key
      var v = new com.ligadata.keyvaluestore.Value
      for (c <- key) {
        k += c.toByte
      }
      for (c <- value) {
        v += c
      }
      def Key = k
      def Value = v
      def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
    }
    try {
      store.put(obj)
      store.commitTx(t)
    } catch {
      case e: Exception => {
        logger.debug("Failed to insert/update object for : " + key)
        store.endTx(t)
        throw new UpdateStoreFailedException("Failed to insert/update object for : " + key)
      }
    }
  }

  def SaveObjectList(keyList: Array[String], valueList: Array[Array[Byte]], store: DataStore) {
    var i = 0
    val t = store.beginTx
    var storeObjects = new Array[IStorage](keyList.length)
    keyList.foreach(key => {
      var value = valueList(i)
      object obj extends IStorage {
        var k = new com.ligadata.keyvaluestore.Key
        var v = new com.ligadata.keyvaluestore.Value
        for (c <- key) {
          k += c.toByte
        }
        for (c <- value) {
          v += c
        }
        def Key = k
        def Value = v
        def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
      }
      storeObjects(i) = obj
      i = i + 1
    })
    try {
      store.putBatch(storeObjects)
      store.commitTx(t)
    } catch {
      case e: Exception => {
        logger.debug("Failed to insert/update object for : " + keyList.mkString(","))
        store.endTx(t)
        throw new UpdateStoreFailedException("Failed to insert/update object for : " + keyList.mkString(","))
      }
    }
  }


  def RemoveObjectList(keyList: Array[String], store: DataStore) {
    var i = 0
    val t = store.beginTx
    val KeyList = new Array[Key](keyList.length)
    keyList.foreach(key => {
      var k = new com.ligadata.keyvaluestore.Key
      for (c <- key) {
        k += c.toByte
      }
      KeyList(i) = k
      i = i + 1
    })
    try {
      store.delBatch(KeyList)
      store.commitTx(t)
    } catch {
      case e: Exception => {
        logger.debug("Failed to delete object batch for : " + keyList.mkString(","))
        store.endTx(t)
        throw new UpdateStoreFailedException("Failed to delete object batch for : " + keyList.mkString(","))
      }
    }
  }

  // 
  // The following batch function is useful when we store data in single table
  // If we use Storage component library, note that table itself is associated with a single
  // database connection( which itself can be mean different things depending on the type
  // of datastore, such as cassandra, hbase, etc..)
  // 
  def SaveObjectList(objList: Array[BaseElemDef], store: DataStore) {
    logger.debug("Save " + objList.length + " objects in a single transaction ")
    val tranId = GetNewTranId
    var keyList = new Array[String](objList.length)
    var valueList = new Array[Array[Byte]](objList.length)
    try {
      var i = 0;
      objList.foreach(obj => {
        obj.tranId = tranId
        val key = (Utils.getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
        var value = serializer.SerializeObjectToByteArray(obj)
        keyList(i) = key
        valueList(i) = value
        i = i + 1
      })
      SaveObjectList(keyList, valueList, store)
    } catch {
      case e: Exception => {
        logger.debug("Failed to insert/update object for : " + keyList.mkString(","))
        throw new UpdateStoreFailedException("Failed to insert/update object for : " + keyList.mkString(","))
      }
    }
  }

  // If tables are different for each object, an internal utility function
  private def getTable(obj: BaseElemDef): String = {
    obj match {
      case o: ModelDef => {
        "models"
      }
      case o: MessageDef => {
        "messages"
      }
      case o: ContainerDef => {
        "containers"
      }
      case o: FunctionDef => {
        "functions"
      }
      case o: AttributeDef => {
        "concepts"
      }
      case o: BaseTypeDef => {
        "types"
      }
      case _ => {
        logger.error("getTable is not implemented for objects of type " + obj.getClass.getName)
        throw new InternalErrorException("getTable is not implemented for objects of type " + obj.getClass.getName)
      }
    }
  }

  // The following batch function is useful when we store data in multiple tables
  // If we use Storage component library, note that each table is associated with a different
  // database connection( which itself can be mean different things depending on the type
  // of datastore, such as cassandra, hbase, etc..)
  def SaveObjectList(objList: Array[BaseElemDef]) {
    logger.debug("Save " + objList.length + " objects in a single transaction ")
    val tranId = GetNewTranId
    var keyList = new Array[String](objList.length)
    var valueList = new Array[Array[Byte]](objList.length)
    var tableList = new Array[String](objList.length)
    try {
      var i = 0;
      objList.foreach(obj => {
        obj.tranId = tranId
        val key = (Utils.getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
        var value = serializer.SerializeObjectToByteArray(obj)
        keyList(i) = key
        valueList(i) = value
        tableList(i) = getTable(obj)
        i = i + 1
      })
      i = 0
      var storeObjects = new Array[IStorage](keyList.length)
      keyList.foreach(key => {
        var value = valueList(i)
        object obj extends IStorage {
          var k = new com.ligadata.keyvaluestore.Key
          var v = new com.ligadata.keyvaluestore.Value
          for (c <- key) {
            k += c.toByte
          }
          for (c <- value) {
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
          val store = MetadataAPIImpl.tableStoreMap(tableList(0))
          val t = store.beginTx
          KeyValueBatchTransaction.putCassandraBatch(tableList, store, storeObjects)
          store.commitTx(t)
        }
        case _ => {
          var i = 0
          tableList.foreach(table => {
            val store = MetadataAPIImpl.tableStoreMap(table)
            val t = store.beginTx
            store.put(storeObjects(i))
            store.commitTx(t)
            i = i + 1
          })
        }
      }
    } catch {
      case e: Exception => {
        logger.debug("Failed to insert/update object for : " + keyList.mkString(","))
        throw new UpdateStoreFailedException("Failed to insert/update object for : " + keyList.mkString(","))
      }
    }
  }

  def SaveObject(key: String, value: String, store: DataStore) {
    var ba = serializer.SerializeObjectToByteArray(value)
    SaveObject(key, ba, store)
  }

  def UpdateObject(key: String, value: Array[Byte], store: DataStore) {
    SaveObject(key, value, store)
  }

  private def GetNewTranId: Long = {
    try {
      val key = "transaction_id"
      val obj = GetObject(key, MetadataAPIImpl.transStore)
      val idStr = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[String]
      idStr.toLong + 1
    } catch {
      case e: ObjectNotFoundException => {
        // first time
        1
      }
      case e: Exception => {
        throw new TranIdNotFoundException("Unable to retrieve the transaction id " + e.toString)
      }
    }
  }

  def GetTranId: Long = {
    try {
      val key = "transaction_id"
      val obj = GetObject(key, MetadataAPIImpl.transStore)
      val idStr = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[String]
      idStr.toLong
    } catch {
      case e: ObjectNotFoundException => {
        // first time
        0
      }
      case e: Exception => {
        throw new TranIdNotFoundException("Unable to retrieve the transaction id " + e.toString)
      }
    }
  }

  def PutTranId(tId: Long) = {
    try {
      val key = "transaction_id"
      val value = tId.toString
      SaveObject(key, value, MetadataAPIImpl.transStore)
    } catch {
      case e: Exception => {
        throw new UpdateStoreFailedException("Unable to Save the transaction id " + tId + ":" + e.getMessage())
      }
    }
  }

  def SaveObject(obj: BaseElemDef, mdMgr: MdMgr) {
    try {
      val key = (Utils.getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
      obj.tranId = GetNewTranId
      //val value = JsonSerializer.SerializeObjectToJson(obj)
      logger.debug("Serialize the object: name of the object => " + key)
      var value = serializer.SerializeObjectToByteArray(obj)
      obj match {
        case o: ModelDef => {
          logger.debug("Adding the model to the cache: name of the object =>  " + key)
          SaveObject(key, value, MetadataAPIImpl.modelStore)
          mdMgr.AddModelDef(o)
        }
        case o: MessageDef => {
          logger.debug("Adding the message to the cache: name of the object =>  " + key)
          SaveObject(key, value, MetadataAPIImpl.messageStore)
          mdMgr.AddMsg(o)
        }
        case o: ContainerDef => {
          logger.debug("Adding the container to the cache: name of the object =>  " + key)
          SaveObject(key, value, MetadataAPIImpl.containerStore)
          mdMgr.AddContainer(o)
        }
        case o: FunctionDef => {
          val funcKey = (obj.getClass().getName().split("\\.").last + "." + o.typeString).toLowerCase
          logger.debug("Adding the function to the cache: name of the object =>  " + funcKey)
          SaveObject(funcKey, value, MetadataAPIImpl.functionStore)
          mdMgr.AddFunc(o)
        }
        case o: AttributeDef => {
          logger.debug("Adding the attribute to the cache: name of the object =>  " + key)
          SaveObject(key, value, MetadataAPIImpl.conceptStore)
          mdMgr.AddAttribute(o)
        }
        case o: ScalarTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, MetadataAPIImpl.typeStore)
          mdMgr.AddScalar(o)
        }
        case o: ArrayTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, MetadataAPIImpl.typeStore)
          mdMgr.AddArray(o)
        }
        case o: ArrayBufTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, MetadataAPIImpl.typeStore)
          mdMgr.AddArrayBuffer(o)
        }
        case o: ListTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, MetadataAPIImpl.typeStore)
          mdMgr.AddList(o)
        }
        case o: QueueTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, MetadataAPIImpl.typeStore)
          mdMgr.AddQueue(o)
        }
        case o: SetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, MetadataAPIImpl.typeStore)
          mdMgr.AddSet(o)
        }
        case o: TreeSetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, MetadataAPIImpl.typeStore)
          mdMgr.AddTreeSet(o)
        }
        case o: SortedSetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, MetadataAPIImpl.typeStore)
          mdMgr.AddSortedSet(o)
        }
        case o: MapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, MetadataAPIImpl.typeStore)
          mdMgr.AddMap(o)
        }
        case o: ImmutableMapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, MetadataAPIImpl.typeStore)
          mdMgr.AddImmutableMap(o)
        }
        case o: HashMapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, MetadataAPIImpl.typeStore)
          mdMgr.AddHashMap(o)
        }
        case o: TupleTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, MetadataAPIImpl.typeStore)
          mdMgr.AddTupleType(o)
        }
        case o: ContainerTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, MetadataAPIImpl.typeStore)
          mdMgr.AddContainerType(o)
        }
        case _ => {
          logger.error("SaveObject is not implemented for objects of type " + obj.getClass.getName)
        }
      }
    } catch {
      case e: AlreadyExistsException => {
        logger.warn("Failed to Save the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
      case e: Exception => {
        logger.warn("Failed to Save the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
    }
  }

  def UpdateObjectInDB(obj: BaseElemDef) {
    try {
      val key = (Utils.getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase

      logger.debug("Serialize the object: name of the object => " + key)
      var value = serializer.SerializeObjectToByteArray(obj)
      obj match {
        case o: ModelDef => {
          logger.debug("Updating the model in the DB: name of the object =>  " + key)
          UpdateObject(key, value, MetadataAPIImpl.modelStore)
        }
        case o: MessageDef => {
          logger.debug("Updating the message in the DB: name of the object =>  " + key)
          UpdateObject(key, value, MetadataAPIImpl.messageStore)
        }
        case o: ContainerDef => {
          logger.debug("Updating the container in the DB: name of the object =>  " + key)
          UpdateObject(key, value, MetadataAPIImpl.containerStore)
        }
        case o: FunctionDef => {
          val funcKey = (obj.getClass().getName().split("\\.").last + "." + o.typeString).toLowerCase
          logger.debug("Updating the function in the DB: name of the object =>  " + funcKey)
          UpdateObject(funcKey, value, MetadataAPIImpl.functionStore)
        }
        case o: AttributeDef => {
          logger.debug("Updating the attribute in the DB: name of the object =>  " + key)
          UpdateObject(key, value, MetadataAPIImpl.conceptStore)
        }
        case o: ScalarTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, MetadataAPIImpl.typeStore)
        }
        case o: ArrayTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, MetadataAPIImpl.typeStore)
        }
        case o: ArrayBufTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, MetadataAPIImpl.typeStore)
        }
        case o: ListTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, MetadataAPIImpl.typeStore)
        }
        case o: QueueTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, MetadataAPIImpl.typeStore)
        }
        case o: SetTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, MetadataAPIImpl.typeStore)
        }
        case o: TreeSetTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, MetadataAPIImpl.typeStore)
        }
        case o: SortedSetTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, MetadataAPIImpl.typeStore)
        }
        case o: MapTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, MetadataAPIImpl.typeStore)
        }
        case o: ImmutableMapTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, MetadataAPIImpl.typeStore)
        }
        case o: HashMapTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, MetadataAPIImpl.typeStore)
        }
        case o: TupleTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, MetadataAPIImpl.typeStore)
        }
        case o: ContainerTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, MetadataAPIImpl.typeStore)
        }
        case _ => {
          logger.error("UpdateObject is not implemented for objects of type " + obj.getClass.getName)
        }
      }
    } catch {
      case e: AlreadyExistsException => {
        logger.error("Failed to Update the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
      case e: Exception => {
        logger.error("Failed to Update the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
    }
  }

  def UploadJarsToDB(obj: BaseElemDef) {
    try {
      var keyList = new Array[String](0)
      var valueList = new Array[Array[Byte]](0)

      val jarPaths = GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet

      logger.debug("jarpaths => " + jarPaths)

      keyList = keyList :+ obj.jarName
      var jarName = GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR") + "/" + obj.jarName
      var value = Utils.GetJarAsArrayOfBytes(jarName)
      logger.debug("Update the jarfile (size => " + value.length + ") of the object: " + obj.jarName)
      valueList = valueList :+ value

      if (obj.DependencyJarNames != null) {
        obj.DependencyJarNames.foreach(j => {
          // do not upload if it already exist, minor optimization
          var loadObject = false
          if (j.endsWith(".jar")) {
            jarName = JarPathsUtils.GetValidJarFile(jarPaths, j)            
            value = Utils.GetJarAsArrayOfBytes(jarName)
            var mObj: IStorage = null
            try {
              mObj = GetObject(j, MetadataAPIImpl.jarStore)
            } catch {
              case e: ObjectNotFoundException => {
                loadObject = true
              }
            }

            if (loadObject == false) {
              val ba = mObj.Value.toArray[Byte]
              val fs = ba.length
              if (fs != value.length) {
                logger.debug("A jar file already exists, but it's size (" + fs + ") doesn't match with the size of the Jar (" +
                  jarName + "," + value.length + ") of the object(" + obj.FullNameWithVer + ")")
                loadObject = true
              }
            }

            if (loadObject) {
              keyList = keyList :+ j
              logger.debug("Update the jarfile (size => " + value.length + ") of the object: " + j)
              valueList = valueList :+ value
            } else {
              logger.debug("The jarfile " + j + " already exists in DB.")
            }
          }
        })
      }
      if (keyList.length > 0) {
        SaveObjectList(keyList, valueList, MetadataAPIImpl.jarStore)
      }
    } catch {
      case e: Exception => {
        throw new InternalErrorException("Failed to Update the Jar of the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
    }
  }

  def UploadJarToDB(jarName: String) {
    try {
      val f = new File(jarName)
      if (f.exists()) {
        var key = f.getName()
        var value = Utils.GetJarAsArrayOfBytes(jarName)
        logger.debug("Update the jarfile (size => " + value.length + ") of the object: " + jarName)
        SaveObject(key, value, MetadataAPIImpl.jarStore)
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "UploadJarToDB", null, ErrorCodeConstants.Upload_Jar_Successful + ":" + jarName)
        apiResult.toString()

      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UploadJarToDB", null, "Error : " + e.toString() + ErrorCodeConstants.Upload_Jar_Failed + ":" + jarName)
        apiResult.toString()
      }
    }
  }


  /**
   *  UploadJarToDB - Interface to the SERIVCES
   */
  def UploadJarToDB(jarName:String,byteArray: Array[Byte]): String = {
    try {
        var key = jarName
        var value = byteArray
        logger.debug("Update the jarfile (size => " + value.length + ") of the object: " + jarName)
        SaveObject(key, value, MetadataAPIImpl.jarStore)
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "UploadJarToDB", null, ErrorCodeConstants.Upload_Jar_Successful + ":" + jarName)
        apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UploadJarToDB", null, "Error : " + e.toString() + ErrorCodeConstants.Upload_Jar_Failed + ":" + jarName)
        apiResult.toString()
      }
    }
  }


  def DownloadJarFromDB(obj: BaseElemDef) {
    try {
      //val key:String = (Utils.getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
      if (obj.jarName == null) {
        logger.debug("The object " + obj.FullNameWithVer + " has no jar associated with it. Nothing to download..")
        return
      }
      var allJars = Utils.GetDependantJars(obj)
      logger.debug("Found " + allJars.length + " dependant jars ")
      if (allJars.length > 0) {
        val jarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet
        jarPaths.foreach(jardir => {
          val dir = new File(jardir)
          if (!dir.exists()) {
            // attempt to create the missing directory
            dir.mkdir();
          }
        })
        
        val dirPath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR")
        val dir = new File(dirPath)
        if (!dir.exists()) {
          // attempt to create the missing directory
          dir.mkdir();
        }
        
        allJars.foreach(jar => {
          // download only if it doesn't already exists
          val b = Utils.IsDownloadNeeded(jar, obj)
          if (b == true) {
            val key = jar
            val mObj = GetObject(key, MetadataAPIImpl.jarStore)
            val ba = mObj.Value.toArray[Byte]
            val jarName = dirPath + "/" + jar
            Utils.PutArrayOfBytesToJar(ba, jarName)
          }
        })
      }
    } catch {
      case e: Exception => {
        logger.error("Failed to download the Jar of the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
    }
  }

  def ModifyObject(obj: BaseElemDef, operation: String) {
    try {
      val o1 = Utils.UpdateObjectInCache(obj, operation, MdMgr.GetMdMgr)
      UpdateObjectInDB(o1)
    } catch {
      case e: ObjectNolongerExistsException => {
        logger.error("The object " + obj.FullNameWithVer + " nolonger exists in metadata : It may have been removed already")
      }
      case e: Exception => {
        throw new Exception("Unexpected error in ModifyObject: " + e.getMessage())
      }
    }
  }

  def DeleteObject(key: String, store: DataStore) {
    var k = new com.ligadata.keyvaluestore.Key
    for (c <- key) {
      k += c.toByte
    }
    val t = store.beginTx
    store.del(k)
    store.commitTx(t)
  }

  def DeleteObject(obj: BaseElemDef) {
    try {
      ModifyObject(obj, "Remove")
    } catch {
      case e: ObjectNolongerExistsException => {
        logger.error("The object " + obj.FullNameWithVer + " nolonger exists in metadata : It may have been removed already")
      }
      case e: Exception => {
        throw new Exception("Unexpected error in DeleteObject: " + e.getMessage())
      }
    }
  }

  def ActivateObject(obj: BaseElemDef) {
    try {
      ModifyObject(obj, "Activate")
    } catch {
      case e: ObjectNolongerExistsException => {
        logger.error("The object " + obj.FullNameWithVer + " nolonger exists in metadata : It may have been removed already")
      }
      case e: Exception => {
        throw new Exception("Unexpected error in ActivateObject: " + e.getMessage())
      }
    }
  }

  def DeactivateObject(obj: BaseElemDef) {
    try {
      ModifyObject(obj, "Deactivate")
    } catch {
      case e: ObjectNolongerExistsException => {
        logger.error("The object " + obj.FullNameWithVer + " nolonger exists in metadata : It may have been removed already")
      }
      case e: Exception => {
        throw new Exception("Unexpected error in DeactivateObject: " + e.getMessage())
      }
    }
  }

  def GetDataStoreProperties(storeType: String, storeName: String, tableName: String): PropertyMap = {
    var connectinfo = new PropertyMap
    try {
      connectinfo += ("connectiontype" -> storeType)
      connectinfo += ("table" -> tableName)
      storeType match {
        case "hbase" => {
          var databaseHost = GetMetadataAPIConfig.getProperty("DATABASE_HOST")
          connectinfo += ("hostlist" -> databaseHost)
          connectinfo += ("schema" -> storeName)
        }
        case "hashmap" => {
          var databaseLocation = GetMetadataAPIConfig.getProperty("DATABASE_LOCATION")
          connectinfo += ("path" -> databaseLocation)
          connectinfo += ("schema" -> storeName)
          connectinfo += ("inmemory" -> "false")
          connectinfo += ("withtransaction" -> "true")
        }
        case "treemap" => {
          var databaseLocation = GetMetadataAPIConfig.getProperty("DATABASE_LOCATION")
          connectinfo += ("path" -> databaseLocation)
          connectinfo += ("schema" -> storeName)
          connectinfo += ("inmemory" -> "false")
          connectinfo += ("withtransaction" -> "true")
        }
        case "cassandra" => {
          var databaseHost = GetMetadataAPIConfig.getProperty("DATABASE_HOST")
          var databaseSchema = GetMetadataAPIConfig.getProperty("DATABASE_SCHEMA")
          connectinfo += ("hostlist" -> databaseHost)
          connectinfo += ("schema" -> databaseSchema)
          connectinfo += ("ConsistencyLevelRead" -> "ONE")
        }
        case _ => {
          throw new CreateStoreFailedException("The database type " + storeType + " is not supported yet ")
        }
      }
      connectinfo
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new CreateStoreFailedException(e.getMessage())
      }
    }
  }


  @throws(classOf[CreateStoreFailedException])
  def GetDataStoreHandle(storeType: String, storeName: String, tableName: String): DataStore = {
    try {
      var connectinfo = GetDataStoreProperties(storeType,storeName,tableName)
      KeyValueManager.Get(connectinfo)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new CreateStoreFailedException(e.getMessage())
      }
    }
  }

  def LoadAllConfigObjectsIntoCache: Boolean = {
    try {
      var keys = scala.collection.mutable.Set[com.ligadata.keyvaluestore.Key]()
      MetadataAPIImpl.configStore.getAllKeys({ (key: Key) => keys.add(key) })
      val keyArray = keys.toArray
      if (keyArray.length == 0) {
        logger.debug("No config objects available in the Database")
        return false
      }
      keyArray.foreach(key => {
	//logger.debug("key => " + KeyAsStr(key))
        val obj = GetObject(key, MetadataAPIImpl.configStore)
        val strKey = KeyAsStr(key)
        val i = strKey.indexOf(".")
        val objType = strKey.substring(0, i)
        val typeName = strKey.substring(i + 1)
	objType match {
	  case "nodeinfo" => {
            val ni = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[NodeInfo]
	    MdMgr.GetMdMgr.AddNode(ni)
	  }
	  case "adapterinfo" => {
            val ai = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[AdapterInfo]
	    MdMgr.GetMdMgr.AddAdapter(ai)
	  }
	  case "clusterinfo" => {
            val ci = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[ClusterInfo]
	    MdMgr.GetMdMgr.AddCluster(ci)
	  }
	  case "clustercfginfo" => {
            val ci = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[ClusterCfgInfo]
	    MdMgr.GetMdMgr.AddClusterCfg(ci)
	  }
	  case _ => {
            throw InternalErrorException("LoadAllConfigObjectsIntoCache: Unknown objectType " + objType)
	  }
	}
      })
      return true
    } catch {
      case e: Exception => {
        e.printStackTrace()
	return false
      }
    }
  }
}
