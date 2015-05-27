package com.ligadata.MetadataAPI

import org.apache.log4j._
import com.ligadata.Utils._
import com.ligadata.fatafat.metadata.ObjType._
import com.ligadata.fatafat.metadata._
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.Serialize._
import com.ligadata.keyvaluestore._
import com.ligadata.keyvaluestore.mapdb._
import com.ligadata.keyvaluestore.cassandra._

object DaoImpl {
   lazy val loggerName = this.getClass.getName
   lazy val logger = Logger.getLogger(loggerName)
   lazy val serializer = SerializerManager.GetSerializer("kryo") 
   
   
  def ActivateObject(obj: BaseElemDef) {
    try {
      ModifyObject(obj, "Activate")
    } catch {
      case e: ObjectNolongerExistsException => {
        logger.error("The object " + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + " nolonger exists in metadata : It may have been removed already")
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
        logger.error("The object " + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + " nolonger exists in metadata : It may have been removed already")
      }
      case e: Exception => {
        throw new Exception("Unexpected error in DeactivateObject: " + e.getMessage())
      }
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
  
  // 
  // The following batch function is useful when we store data in single table
  // If we use Storage component library, note that table itself is associated with a single
  // database connection( which itself can be mean different things depending on the type
  // of datastore, such as cassandra, hbase, etc..)
  // 
  def SaveObjectList(objList: Array[BaseElemDef], store: DataStore) {
    logger.debug("Save " + objList.length + " objects in a single transaction ")
    val tranId = MetadataUtils.GetNewTranId
    var keyList = new Array[String](objList.length)
    var valueList = new Array[Array[Byte]](objList.length)
    try {
      var i = 0;
      objList.foreach(obj => {
        obj.tranId = tranId
        val key = (MetadataUtils.getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
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

  // The following batch function is useful when we store data in multiple tables
  // If we use Storage component library, note that each table is associated with a different
  // database connection( which itself can be mean different things depending on the type
  // of datastore, such as cassandra, hbase, etc..)
  def SaveObjectList(objList: Array[BaseElemDef]) {
    logger.debug("Save " + objList.length + " objects in a single transaction ")
    val tranId = MetadataUtils.GetNewTranId
    var keyList = new Array[String](objList.length)
    var valueList = new Array[Array[Byte]](objList.length)
    var tableList = new Array[String](objList.length)
    try {
      var i = 0;
      objList.foreach(obj => {
        obj.tranId = tranId
        val key = (MetadataUtils.getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
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
      val storeType = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DATABASE")
      storeType match {
        case "cassandra" => {
          val store = MetadataAPIImpl.getTableStoreMap(tableList(0))
          val t = store.beginTx
          KeyValueBatchTransaction.putCassandraBatch(tableList, store, storeObjects)
          store.commitTx(t)
        }
        case _ => {
          var i = 0
          tableList.foreach(table => {
            val store = MetadataAPIImpl.getTableStoreMap(table)
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
  
 def SaveObject(obj: BaseElemDef, mdMgr: MdMgr) {
    try {
      val key = (MetadataUtils.getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
      val dispkey = (MetadataUtils.getObjectType(obj) + "." + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version)).toLowerCase
      obj.tranId = MetadataUtils.GetNewTranId
      //val value = JsonSerializer.SerializeObjectToJson(obj)
      logger.debug("Serialize the object: name of the object => " + dispkey)
      var value = serializer.SerializeObjectToByteArray(obj)
      obj match {
        case o: ModelDef => {
          logger.debug("Adding the model to the cache: name of the object =>  " + dispkey)
          SaveObject(key, value, MetadataAPIImpl.getModelStore)
          mdMgr.AddModelDef(o)
        }
        case o: MessageDef => {
          logger.debug("Adding the message to the cache: name of the object =>  " + dispkey)
          SaveObject(key, value, MetadataAPIImpl.getMessageStore)
          mdMgr.AddMsg(o)
        }
        case o: ContainerDef => {
          logger.debug("Adding the container to the cache: name of the object =>  " + dispkey)
          SaveObject(key, value, MetadataAPIImpl.getContainerStore)
          mdMgr.AddContainer(o)
        }
        case o: FunctionDef => {
          val funcKey = (obj.getClass().getName().split("\\.").last + "." + o.typeString).toLowerCase
          logger.debug("Adding the function to the cache: name of the object =>  " + funcKey)
          SaveObject(funcKey, value, MetadataAPIImpl.getFunctionStore)
          mdMgr.AddFunc(o)
        }
        case o: AttributeDef => {
          logger.debug("Adding the attribute to the cache: name of the object =>  " + dispkey)
          SaveObject(key, value, MetadataAPIImpl.getConceptStore)
          mdMgr.AddAttribute(o)
        }
        case o: ScalarTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          SaveObject(key, value, MetadataAPIImpl.getTypeStore)
          mdMgr.AddScalar(o)
        }
        case o: ArrayTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          SaveObject(key, value, MetadataAPIImpl.getTypeStore)
          mdMgr.AddArray(o)
        }
        case o: ArrayBufTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          SaveObject(key, value, MetadataAPIImpl.getTypeStore)
          mdMgr.AddArrayBuffer(o)
        }
        case o: ListTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          SaveObject(key, value, MetadataAPIImpl.getTypeStore)
          mdMgr.AddList(o)
        }
        case o: QueueTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          SaveObject(key, value, MetadataAPIImpl.getTypeStore)
          mdMgr.AddQueue(o)
        }
        case o: SetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          SaveObject(key, value, MetadataAPIImpl.getTypeStore)
          mdMgr.AddSet(o)
        }
        case o: TreeSetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          SaveObject(key, value, MetadataAPIImpl.getTypeStore)
          mdMgr.AddTreeSet(o)
        }
        case o: SortedSetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          SaveObject(key, value, MetadataAPIImpl.getTypeStore)
          mdMgr.AddSortedSet(o)
        }
        case o: MapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          SaveObject(key, value, MetadataAPIImpl.getTypeStore)
          mdMgr.AddMap(o)
        }
        case o: ImmutableMapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          SaveObject(key, value, MetadataAPIImpl.getTypeStore)
          mdMgr.AddImmutableMap(o)
        }
        case o: HashMapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          SaveObject(key, value, MetadataAPIImpl.getTypeStore)
          mdMgr.AddHashMap(o)
        }
        case o: TupleTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          SaveObject(key, value, MetadataAPIImpl.getTypeStore)
          mdMgr.AddTupleType(o)
        }
        case o: ContainerTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          SaveObject(key, value, MetadataAPIImpl.getTypeStore)
          mdMgr.AddContainerType(o)
        }
        case _ => {
          logger.error("SaveObject is not implemented for objects of type " + obj.getClass.getName)
        }
      }
    } catch {
      case e: AlreadyExistsException => {
        logger.error("Failed to Save the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + "): " + e.getMessage())
     }
      case e: Exception => {
        logger.error("Failed to Save the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + "): " + e.getMessage())
      }
    }
  }
 
 

  def UpdateObjectInDB(obj: BaseElemDef) {
    try {
      val key = (MetadataUtils.getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
      val dispkey = (MetadataUtils.getObjectType(obj) + "." + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version)).toLowerCase

      logger.debug("Serialize the object: name of the object => " + dispkey)
      var value = serializer.SerializeObjectToByteArray(obj)
      obj match {
        case o: ModelDef => {
          logger.debug("Updating the model in the DB: name of the object =>  " + dispkey)
          UpdateObject(key, value, MetadataAPIImpl.getModelStore)
        }
        case o: MessageDef => {
          logger.debug("Updating the message in the DB: name of the object =>  " + dispkey)
          UpdateObject(key, value, MetadataAPIImpl.getMessageStore)
        }
        case o: ContainerDef => {
          logger.debug("Updating the container in the DB: name of the object =>  " + dispkey)
          UpdateObject(key, value, MetadataAPIImpl.getContainerStore)
        }
        case o: FunctionDef => {
          val funcKey = (obj.getClass().getName().split("\\.").last + "." + o.typeString).toLowerCase
          logger.debug("Updating the function in the DB: name of the object =>  " + funcKey)
          UpdateObject(funcKey, value, MetadataAPIImpl.getFunctionStore)
        }
        case o: AttributeDef => {
          logger.debug("Updating the attribute in the DB: name of the object =>  " + dispkey)
          UpdateObject(key, value, MetadataAPIImpl.getConceptStore)
        }
        case o: ScalarTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          UpdateObject(key, value, MetadataAPIImpl.getTypeStore)
        }
        case o: ArrayTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          UpdateObject(key, value, MetadataAPIImpl.getTypeStore)
        }
        case o: ArrayBufTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          UpdateObject(key, value, MetadataAPIImpl.getTypeStore)
        }
        case o: ListTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          UpdateObject(key, value, MetadataAPIImpl.getTypeStore)
        }
        case o: QueueTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          UpdateObject(key, value, MetadataAPIImpl.getTypeStore)
        }
        case o: SetTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          UpdateObject(key, value, MetadataAPIImpl.getTypeStore)
        }
        case o: TreeSetTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          UpdateObject(key, value, MetadataAPIImpl.getTypeStore)
        }
        case o: SortedSetTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          UpdateObject(key, value, MetadataAPIImpl.getTypeStore)
        }
        case o: MapTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          UpdateObject(key, value, MetadataAPIImpl.getTypeStore)
        }
        case o: ImmutableMapTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          UpdateObject(key, value, MetadataAPIImpl.getTypeStore)
        }
        case o: HashMapTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          UpdateObject(key, value, MetadataAPIImpl.getTypeStore)
        }
        case o: TupleTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          UpdateObject(key, value, MetadataAPIImpl.getTypeStore)
        }
        case o: ContainerTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          UpdateObject(key, value, MetadataAPIImpl.getTypeStore)
        }
        case _ => {
          logger.error("UpdateObject is not implemented for objects of type " + obj.getClass.getName)
        }
      }
    } catch {
      case e: AlreadyExistsException => {
        logger.error("Failed to Update the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + "): " + e.getMessage())
      }
      case e: Exception => {
        logger.error("Failed to Update the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + "): " + e.getMessage())
      }
    }
  }
  
  def UpdateObject(key: String, value: Array[Byte], store: DataStore) {
    SaveObject(key, value, store)
  }

  def ZooKeeperMessage(objList: Array[BaseElemDef], operations: Array[String]): Array[Byte] = {
    try {
      val notification = JsonSerializer.zkSerializeObjectListToJson("Notifications", objList, operations)
      notification.getBytes
    } catch {
      case e: Exception => {
        throw new InternalErrorException("Failed to generate a zookeeper message from the objList " + e.getMessage())
      }
    }
  }
  
  
   
  def DeleteObject(obj: BaseElemDef) {
    try {
      ModifyObject(obj, "Remove")
    } catch {
      case e: ObjectNolongerExistsException => {
        logger.error("The object " + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + " nolonger exists in metadata : It may have been removed already")
      }
      case e: Exception => {
        throw new Exception("Unexpected error in DeleteObject: " + e.getMessage())
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
   

  def AddObjectToCache(o: Object, mdMgr: MdMgr) {
    // If the object's Delete flag is set, this is a noop.
    val obj = o.asInstanceOf[BaseElemDef]
    if (obj.IsDeleted)
      return
    try {
      val key = obj.FullNameWithVer.toLowerCase
      val dispkey = obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version)
      obj match {
        case o: ModelDef => {
          logger.debug("Adding the model to the cache: name of the object =>  " + dispkey)
          mdMgr.AddModelDef(o)
        }
        case o: MessageDef => {
          logger.debug("Adding the message to the cache: name of the object =>  " + dispkey)
          mdMgr.AddMsg(o)
        }
        case o: ContainerDef => {
          logger.debug("Adding the container to the cache: name of the object =>  " + dispkey)
          mdMgr.AddContainer(o)
        }
        case o: FunctionDef => {
          val funcKey = o.typeString.toLowerCase
          logger.debug("Adding the function to the cache: name of the object =>  " + funcKey)
          mdMgr.AddFunc(o)
        }
        case o: AttributeDef => {
          logger.debug("Adding the attribute to the cache: name of the object =>  " + dispkey)
          mdMgr.AddAttribute(o)
        }
        case o: ScalarTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddScalar(o)
        }
        case o: ArrayTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddArray(o)
        }
        case o: ArrayBufTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddArrayBuffer(o)
        }
        case o: ListTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddList(o)
        }
        case o: QueueTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddQueue(o)
        }
        case o: SetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddSet(o)
        }
        case o: TreeSetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddTreeSet(o)
        }
        case o: SortedSetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddSortedSet(o)
        }
        case o: MapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddMap(o)
        }
        case o: ImmutableMapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddImmutableMap(o)
        }
        case o: HashMapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddHashMap(o)
        }
        case o: TupleTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddTupleType(o)
        }
        case o: ContainerTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddContainerType(o)
        }
        case _ => {
          logger.error("SaveObject is not implemented for objects of type " + obj.getClass.getName)
        }
      }
    } catch {
      case e: AlreadyExistsException => {
        logger.error("Failed to Save the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + "): " + e.getMessage())
      }
      case e: Exception => {
        logger.error("Failed to Cache the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + "): " + e.getMessage())
      }
    }
  }


  def UpdateObjectInCache(obj: BaseElemDef, operation: String, mdMgr: MdMgr): BaseElemDef = {
    var updatedObject: BaseElemDef = null
    try {
      obj match {
        case o: FunctionDef => {
          updatedObject = mdMgr.ModifyFunction(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ModelDef => {
          updatedObject = mdMgr.ModifyModel(o.nameSpace, o.name, o.ver, operation)
        }
        case o: MessageDef => {
          updatedObject = mdMgr.ModifyMessage(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ContainerDef => {
          updatedObject = mdMgr.ModifyContainer(o.nameSpace, o.name, o.ver, operation)
        }
        case o: AttributeDef => {
          updatedObject = mdMgr.ModifyAttribute(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ScalarTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ArrayTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ArrayBufTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ListTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: QueueTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: SetTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: TreeSetTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: SortedSetTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: MapTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ImmutableMapTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: HashMapTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: TupleTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ContainerTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case _ => {
          throw new InternalErrorException("UpdateObjectInCache is not implemented for objects of type " + obj.getClass.getName)
        }
      }
      updatedObject
    } catch {
      case e: ObjectNolongerExistsException => {
        throw new ObjectNolongerExistsException("The object " + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + " nolonger exists in metadata : It may have been removed already")
      }
      case e: Exception => {
        throw new Exception("Unexpected error in UpdateObjectInCache: " + e.getMessage())
      }
    }
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
      logger.debug("Get the object from store, key => " + MetadataUtils.KeyAsStr(k))
      store.get(k, o)
      o
    } catch {
      case e: KeyNotFoundException => {
        logger.debug("KeyNotFound Exception: Error => " + e.getMessage())
        throw new ObjectNotFoundException(e.getMessage())
      }
      case e: Exception => {
        e.printStackTrace()
        logger.debug("General Exception: Error => " + e.getMessage())
        throw new ObjectNotFoundException(e.getMessage())
      }
    }
  }

  def GetObject(key: String, store: DataStore): IStorage = {
    var k = new com.ligadata.keyvaluestore.Key
    for (c <- key) {
      k += c.toByte
    }
    GetObject(k, store)
  }  
  
  /**
   * 
   */
  def LoadObjectsIntoCache {
    ModelImpl.LoadAllModelsIntoCache
    MessageImpl.LoadAllMessagesIntoCache
    ContainerImpl.LoadAllContainersIntoCache
    FunctionImpl.LoadAllFunctionsIntoCache
    ConceptImpl.LoadAllConceptsIntoCache
    TypeImpl.LoadAllTypesIntoCache
  }
  
  
  def LoadAllObjectsIntoCache {
    try {
      val configAvailable = ConfigImpl.LoadAllConfigObjectsIntoCache
      if( configAvailable ){
        MetadataUtils.RefreshApiConfigForGivenNode(MetadataAPIImpl.metadataAPIConfig.getProperty("NODE_ID"))
      }
      else{
        logger.debug("Assuming bootstrap... No config objects in persistent store")
      }
      MetadataAPIImpl.startup = true
      var objectsChanged = new Array[BaseElemDef](0)
      var operations = new Array[String](0)
      val maxTranId = MetadataUtils.GetTranId
      logger.debug("Max Transaction Id => " + maxTranId)
      var keys = scala.collection.mutable.Set[com.ligadata.keyvaluestore.Key]()
      MetadataAPIImpl.getMetadataStore.getAllKeys({ (key: Key) => keys.add(key) })
      val keyArray = keys.toArray
      if (keyArray.length == 0) {
        logger.debug("No objects available in the Database")
        return
      }
      keyArray.foreach(key => {
        val obj = DaoImpl.GetObject(key, MetadataAPIImpl.getMetadataStore)
        val mObj = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[BaseElemDef]
        if (mObj != null) {
          if (mObj.tranId <= maxTranId) {
            AddObjectToCache(mObj, MdMgr.GetMdMgr)
            JarImpl.DownloadJarFromDB(mObj)
          } else {
            logger.debug("The transaction id of the object => " + mObj.tranId)
            AddObjectToCache(mObj, MdMgr.GetMdMgr)
            JarImpl.DownloadJarFromDB(mObj)
            logger.error("Transaction is incomplete with the object " + MetadataUtils.KeyAsStr(key) + ",we may not have notified engine, attempt to do it now...")
            objectsChanged = objectsChanged :+ mObj
            if (mObj.IsActive) {
              operations = for (op <- objectsChanged) yield "Add"
            } else {
              operations = for (op <- objectsChanged) yield "Remove"
            }
          }
        } else {
          throw InternalErrorException("serializer.Deserialize returned a null object")
        }
      })
      if (objectsChanged.length > 0) {
        MetadataSynchronizer.NotifyEngine(objectsChanged, operations)
      }
      MetadataAPIImpl.startup = false
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }
   
  private def ModifyObject(obj: BaseElemDef, operation: String) {
    try {
      val o1 = UpdateObjectInCache(obj, operation, MdMgr.GetMdMgr)
      UpdateObjectInDB(o1)
    } catch {
      case e: ObjectNolongerExistsException => {
        logger.error("The object " + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + " nolonger exists in metadata : It may have been removed already")
      }
      case e: Exception => {
        throw new Exception("Unexpected error in ModifyObject: " + e.getMessage())
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
  
  // If tables are different, an internal utility function
  def getTable(obj: BaseElemDef): String = {
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

}