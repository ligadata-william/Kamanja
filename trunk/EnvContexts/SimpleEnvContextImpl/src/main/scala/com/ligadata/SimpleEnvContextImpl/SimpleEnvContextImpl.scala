
package com.ligadata.SimpleEnvContextImpl

import scala.collection.immutable.Map
import scala.collection.mutable._
import scala.util.control.Breaks._
import scala.reflect.runtime.{ universe => ru }
import org.apache.log4j.Logger
import com.ligadata.keyvaluestore._
import com.ligadata.keyvaluestore.mapdb._
import com.ligadata.OnLEPBase._
import com.ligadata.OnLEPBase.{ EnvContext, MessageContainerBase }
import com.ligadata.olep.metadata._
import java.net.URLClassLoader
import com.ligadata.Serialize._

trait LogTrait {
  val loggerName = this.getClass.getName()
  val logger = Logger.getLogger(loggerName)
}

/**
 *  The SimpleEnvContextImpl supports kv stores that are based upon MapDb hash tables.
 */
object SimpleEnvContextImpl extends EnvContext with LogTrait {

  class MsgContainerInfo {
    var data: scala.collection.mutable.Map[String, MessageContainerBase] = scala.collection.mutable.Map[String, MessageContainerBase]()
    var dataStore: DataStore = null
    var containerType: BaseTypeDef = null
    var loadedAll: Boolean = false
    var tableName: String = ""
  }

  private[this] val _lock = new Object()

  private[this] val _messagesOrContainers = scala.collection.mutable.Map[String, MsgContainerInfo]()

  /** Add this one too for caching the arrays that are returned from the loaded map's values */
  private[this] var _filterArrays: scala.collection.mutable.Map[String, Array[MessageContainerBase]] = scala.collection.mutable.Map[String, Array[MessageContainerBase]]()

  private[this] var _serInfoBufBytes = 32

  private[this] var _kryoSer: Serializer = null
  private[this] var classLoader: java.lang.ClassLoader = null

  override def SetClassLoader(cl: java.lang.ClassLoader): Unit = {
    classLoader = cl
  }

  // Adding new messages or Containers
  override def AddNewMessageOrContainers(mgr: MdMgr, storeType: String, dataLocation: String, schemaName: String, containerNames: Array[String], loadAllData: Boolean): Unit = _lock.synchronized {
    containerNames.foreach(c1 => {
      val c = c1.toLowerCase
      val names: Array[String] = c.split('.')
      val namespace: String = names.head
      val name: String = names.last
      var containerType = mgr.ActiveType(namespace, name)
      if (containerType != null) {

        val tableName: String = containerType.FullName.toLowerCase

        val fnd = _messagesOrContainers.getOrElse(tableName, null)

        if (fnd != null) {
          // We already have this
        } else {
          val newMsgOrContainer = new MsgContainerInfo
          newMsgOrContainer.dataStore = GetDataStoreHandle(storeType, schemaName, tableName, dataLocation)
          newMsgOrContainer.containerType = containerType

          newMsgOrContainer.tableName = tableName

          /** create a map to cache the entries to be resurrected from the mapdb */
          _messagesOrContainers(tableName) = newMsgOrContainer

          if (loadAllData) {
            val keys: ArrayBuffer[String] = ArrayBuffer[String]()
            val keyCollector = (key: Key) => { collectKey(key, keys) }
            newMsgOrContainer.dataStore.getAllKeys(keyCollector)
            if (keys.size > 0) {
              loadMap(containerType, keys, newMsgOrContainer)
            }
            newMsgOrContainer.loadedAll = true
          }

        }
      } else {
        logger.error("Message/Container %s not found".format(c))
      }
    })
  }

  /**
   *  For the current container, load the values for each key, coercing it to the appropriate MessageContainerBase, and storing
   *  each in the supplied map.
   *
   *  @param containerType - a ContainerDef that describes the current container.  Its typeName is used to create an instance
   *  	of the MessageContainerBase derivative.
   *  @param keys : the keys to use on the dstore to extract a given element.
   *  @param dstore : the mapdb handle
   *  @param map : the map to be updated with key/MessageContainerBase pairs.
   */
  def loadMap(containerType: BaseTypeDef, keys: ArrayBuffer[String], msgCntrInfo: MsgContainerInfo): Unit = {

    var objs: Array[MessageContainerBase] = new Array[MessageContainerBase](1)
    keys.foreach(key => {
      try {
        val buildOne = (tupleBytes: Value) => { buildObject(tupleBytes, objs, containerType) }
        msgCntrInfo.dataStore.get(makeKey(key), buildOne)
        msgCntrInfo.data(key.toLowerCase) = objs(0)
      } catch {
        case e: ClassNotFoundException => {
          logger.error(s"unable to create a container named ${containerType.typeString}... is it defined in the metadata?")
          e.printStackTrace()
          throw e
        }
        case ooh: Throwable => {
          logger.error(s"unknown error encountered while processing ${containerType.typeString}.. stack trace = ${ooh.printStackTrace}")
          throw ooh
        }
      }
    })
    logger.trace("Loaded %d objects for %s".format(msgCntrInfo.data.size, containerType.FullName))
  }

  private def getSerializeInfo(tupleBytes: Value): String = {
    if (tupleBytes.size < _serInfoBufBytes) return ""
    val serInfoBytes = new Array[Byte](_serInfoBufBytes)
    tupleBytes.copyToArray(serInfoBytes, 0, _serInfoBufBytes)
    return (new String(serInfoBytes)).trim
  }

  private def getValueInfo(tupleBytes: Value): Array[Byte] = {
    if (tupleBytes.size < _serInfoBufBytes) return null
    val valInfoBytes = new Array[Byte](tupleBytes.size - _serInfoBufBytes)
    Array.copy(tupleBytes.toArray, _serInfoBufBytes, valInfoBytes, 0, tupleBytes.size - _serInfoBufBytes)
    valInfoBytes
  }

  private def buildObject(tupleBytes: Value, objs: Array[MessageContainerBase], containerType: BaseTypeDef) {
    // Get first _serInfoBufBytes bytes
    if (tupleBytes.size < _serInfoBufBytes) {
      val errMsg = s"Invalid input. This has only ${tupleBytes.size} bytes data. But we are expecting serializer buffer bytes as of size ${_serInfoBufBytes}"
      logger.error(errMsg)
      throw new Exception(errMsg)
    }

    val serInfo = getSerializeInfo(tupleBytes)

    serInfo.toLowerCase match {
      case "csv" => {
        val valInfo = getValueInfo(tupleBytes)
        objs(0) = Class.forName(containerType.typeString).newInstance().asInstanceOf[MessageContainerBase]
        val inputData = new DelimitedData(new String(valInfo), ",")
        inputData.tokens = inputData.dataInput.split(inputData.dataDelim, -1)
        inputData.curPos = 0
        objs(0).populate(inputData)
      }
      case "kryo" => {
        val valInfo = getValueInfo(tupleBytes)
        if (_kryoSer == null) {
          _kryoSer = SerializerManager.GetSerializer("kryo")
          if (_kryoSer != null && classLoader != null) {
            _kryoSer.SetClassLoader(classLoader)
          }
        }
        if (_kryoSer != null) {
          objs(0) = _kryoSer.DeserializeObjectFromByteArray(valInfo).asInstanceOf[MessageContainerBase]
        }
      }
      case _ => {
        throw new Exception("Found un-handled Serializer Info: " + serInfo)
      }
    }
  }

  def collectKey(key: Key, keys: ArrayBuffer[String]): Unit = {
    val buffer: StringBuilder = new StringBuilder
    key.foreach(c => buffer.append(c.toChar))
    val containerKey: String = buffer.toString
    keys += containerKey
  }

  def GetDataStoreHandle(storeType: String, storeName: String, tableName: String, dataLocation: String): DataStore = {
    try {
      var connectinfo = new PropertyMap
      connectinfo += ("connectiontype" -> storeType)
      connectinfo += ("table" -> tableName)
      storeType match {
        case "hashmap" => {
          connectinfo += ("path" -> dataLocation)
          connectinfo += ("schema" -> tableName) // Using tableName instead of storeName here to save into different tables
          connectinfo += ("inmemory" -> "false")
          connectinfo += ("withtransaction" -> "false")
        }
        case "treemap" => {
          connectinfo += ("path" -> dataLocation)
          connectinfo += ("schema" -> tableName) // Using tableName instead of storeName here to save into different tables
          connectinfo += ("inmemory" -> "false")
          connectinfo += ("withtransaction" -> "false")
        }
        case "cassandra" => {
          connectinfo += ("hostlist" -> dataLocation)
          connectinfo += ("schema" -> storeName)
          connectinfo += ("ConsistencyLevelRead" -> "ONE")
        }
        case _ => {
          throw new Exception("The database type " + storeType + " is not supported yet ")
        }
      }
      KeyValueManager.Get(connectinfo)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new Exception(e.getMessage())
      }
    }
  }

  //BUGBUG:: For now we are expecting Container for this function call. May be we need to handle anything.
  override def getObjects(containerName: String, key: String): Array[MessageContainerBase] = _lock.synchronized {
    // bugbug: implement partial match
    //Array(getObject(containerName, key))

    /**
     * Check the "FilterArrays" map for the array with the name "key.toLowerCase" to be returned.  If not present,
     * access the _messagesOrContainers map with the key. Project the keys of the map tuples to an array.
     * Add it to the FilterArrays map and return the array as the function result.
     */
    val lKey: String = key.toLowerCase()
    val filterSetValues: Array[MessageContainerBase] = if (_filterArrays.contains(lKey)) {
      _filterArrays.apply(lKey)
    } else {
      val fnd = _messagesOrContainers.getOrElse(lKey, null)
      val setVals: Array[MessageContainerBase] = if (fnd != null) {
        if (fnd.loadedAll) {
          val map: scala.collection.mutable.Map[String, MessageContainerBase] = fnd.data
          val filterVals: Array[MessageContainerBase] = map.values.toArray
          _filterArrays(lKey) = filterVals
          /** cache it for subsequent calls */
          filterVals
        } else {
          throw new Exception("Object %s is not loaded all at once. So, we can not get all objects here".format(fnd.tableName))
          Array[MessageContainerBase]()
        }
      } else {
        Array[MessageContainerBase]()
      }
      setVals
    }
    filterSetValues
  }

  private def makeKey(key: String): com.ligadata.keyvaluestore.Key = {
    var k = new com.ligadata.keyvaluestore.Key
    k ++= key.toLowerCase.getBytes("UTF8")

    k
  }

  private def makeValue(value: String, serializerInfo: String): com.ligadata.keyvaluestore.Value = {
    var v = new com.ligadata.keyvaluestore.Value
    v ++= serializerInfo.getBytes("UTF8")

    // Making sure we write first 32 bytes as serializerInfo. Pad it if it is less than 32 bytes
    if (v.size < 32) {
      val spacebyte = ' '.toByte
      for (c <- v.size to 32)
        v += spacebyte
    }

    // Trim if it is more than 32 bytes
    if (v.size > 32) {
      v.reduceToSize(32)
    }

    // Saving Value
    v ++= value.getBytes("UTF8")

    v
  }

  private def makeValue(value: Array[Byte], serializerInfo: String): com.ligadata.keyvaluestore.Value = {
    var v = new com.ligadata.keyvaluestore.Value
    v ++= serializerInfo.getBytes("UTF8")

    // Making sure we write first 32 bytes as serializerInfo. Pad it if it is less than 32 bytes
    if (v.size < 32) {
      val spacebyte = ' '.toByte
      for (c <- v.size to 32)
        v += spacebyte
    }

    // Trim if it is more than 32 bytes
    if (v.size > 32) {
      v.reduceToSize(32)
    }

    // Saving Value
    v ++= value

    v
  }

  override def getObject(containerName: String, key: String): MessageContainerBase = _lock.synchronized {
    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase(), null)
    if (container != null) {
      val v = container.data.getOrElse(key.toLowerCase(), null)
      if (v != null) return v
      var objs: Array[MessageContainerBase] = new Array[MessageContainerBase](1)
      val buildOne = (tupleBytes: Value) => { buildObject(tupleBytes, objs, container.containerType) }
      try {
        container.dataStore.get(makeKey(key), buildOne)
      } catch {
        case e: Exception => {
          logger.trace("Data not found for key:" + key)
        }
      }
      if (objs(0) != null)
        container.data(key.toLowerCase) = objs(0)
      return objs(0)
    } else null
  }

  override def setObject(containerName: String, key: String, value: MessageContainerBase): Unit = _lock.synchronized {
    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase(), null)
    if (container != null) {
      val k = key.toLowerCase
      container.data(k) = value
      if (_kryoSer == null) {
        _kryoSer = SerializerManager.GetSerializer("kryo")
        if (_kryoSer != null && classLoader != null) {
          _kryoSer.SetClassLoader(classLoader)
        }
      }
      try {
        val v = _kryoSer.SerializeObjectToByteArray(value)
        writeThru(k, v, container.dataStore, "kryo")
      } catch {
        case e: Exception => {
          logger.error("Failed to serialize/write data.")
          e.printStackTrace
        }
      }
    }
    // bugbug: throw exception
  }

  override def setObject(containerName: String, elementkey: Any, value: MessageContainerBase): Unit = _lock.synchronized {
    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase(), null)
    if (container != null) {
      val key: String = elementkey.toString.toLowerCase
      container.data(key) = value
      logger.info(s"Replacing container '$containerName' entry for key '$key' ... value = \n${value.toString}")
      if (_kryoSer == null) {
        _kryoSer = SerializerManager.GetSerializer("kryo")
        if (_kryoSer != null && classLoader != null) {
          _kryoSer.SetClassLoader(classLoader)
        }
      }
      try {
        val v = _kryoSer.SerializeObjectToByteArray(value)
        writeThru(key, v, container.dataStore, "kryo")
      } catch {
        case e: Exception => {
          logger.error("Failed to serialize/write data.")
          e.printStackTrace
        }
      }
    }
    // bugbug: throw exception

  }

  private def writeThru(key: String, value: Array[Byte], store: DataStore, serializerInfo: String) {
    object i extends IStorage {
      val k = makeKey(key)
      val v = makeValue(value, "kryo")

      def Key = k
      def Value = v
      def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
    }
    store.put(i)
  }

  /**
   *   Does the supplied key exist in a container with the supplied name?
   */
  override def contains(containerName: String, key: String): Boolean = {
    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase(), null)
    val isPresent = if (container != null) {
      val lkey: String = key.toString.toLowerCase()
      container.data.contains(lkey)
    } else {
      false
    }
    isPresent
  }

  /**
   *   Does at least one of the supplied keys exist in a container with the supplied name?
   */
  override def containsAny(containerName: String, keys: Array[String]): Boolean = {
    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase(), null)
    val isPresent = if (container != null) {
      val matches: Int = keys.filter(key => container.data.contains(key.toLowerCase())).size
      (matches > 0)
    } else {
      false
    }
    isPresent
  }

  /**
   *   Do all of the supplied keys exist in a container with the supplied name?
   */
  override def containsAll(containerName: String, keys: Array[String]): Boolean = {
    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase(), null)
    val isPresent = if (container != null) {
      val matches: Int = keys.filter(key => container.data.contains(key.toLowerCase())).size
      (matches == keys.size)
    } else {
      false
    }
    isPresent
  }

}


