package com.ligadata.MetadataAPI

import org.apache.log4j._
import com.ligadata.Serialize._
import com.ligadata.ZooKeeper._
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import com.ligadata.fatafat.metadata._


object MetadataSynchronizer {
  
  lazy val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName) 
  lazy val serializer = SerializerManager.GetSerializer("kryo")  
  var zkc: CuratorFramework = null
  private val lock = new Object
  private var zkListener: ZooKeeperListener = _
  private var cacheOfOwnChanges: scala.collection.mutable.Set[String] = scala.collection.mutable.Set[String]()
   
   
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
  
  /**
   * Create a listener to monitor Meatadata Cache
   */
  def initZkListener: Unit = {
     // Set up a zk listener for metadata invalidation   metadataAPIConfig.getProperty("AUDIT_IMPL_CLASS").trim
    var znodePath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZNODE_PATH")
    if (znodePath != null) znodePath = znodePath.trim + "/metadataupdate" else return
    var zkConnectString = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZOOKEEPER_CONNECT_STRING")
    if (zkConnectString != null) zkConnectString = zkConnectString.trim else return

    if (zkConnectString != null && zkConnectString.isEmpty() == false && znodePath != null && znodePath.isEmpty() == false) {
      try {
        CreateClient.CreateNodeIfNotExists(zkConnectString, znodePath)
        zkListener = new ZooKeeperListener
        zkListener.CreateListener(zkConnectString, znodePath, UpdateMetadata, 3000, 3000)
      } catch {
        case e: Exception => {
          logger.error("Failed to initialize ZooKeeper Connection. Reason:%s Message:%s".format(e.getCause, e.getMessage))
          throw e
        }
      }
    }   
  }
     

  /**
   * InitZooKeeper - Establish a connection to zookeeper
   */  
  def InitZooKeeper: Unit = {
    logger.debug("Connect to zookeeper..")
    if (zkc != null) {
      // Zookeeper is already connected
      return
    }

    val zkcConnectString = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZOOKEEPER_CONNECT_STRING")
    val znodePath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZNODE_PATH") + "/metadataupdate"
    logger.debug("Connect To ZooKeeper using " + zkcConnectString)
    try {
      CreateClient.CreateNodeIfNotExists(zkcConnectString, znodePath)
      zkc = CreateClient.createSimple(zkcConnectString)
    } catch {
      case e: Exception => {
        throw new InternalErrorException("Failed to start a zookeeper session with(" + zkcConnectString + "): " + e.getMessage())
      }
    }
  }
  
   
  /**
   * shutdownZkListener - should be called by application using MetadataAPIImpl directly to disable synching of Metadata cache.
   */
  def shutdownZkListener: Unit = {
    try {
      CloseZKSession
      if (zkListener != null) {
        zkListener.Shutdown
      }
    } catch {
        case e: Exception => {
          logger.error("Error trying to shutdown zookeeper listener.  ")
          throw e
        }
      }
  }
  
  def NotifyEngine(objList: Array[BaseElemDef], operations: Array[String]) {
    try {
      val notifyEngine = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("NOTIFY_ENGINE")
      if (notifyEngine != "YES") {
        logger.warn("Not Notifying the engine about this operation because The property NOTIFY_ENGINE is not set to YES")
        MetadataUtils.PutTranId(objList(0).tranId)
        return
      }
      
      // Set up the cache of CACHES!!!! Since we are listening for changes to Metadata, we will be notified by Zookeeper
      // of this change that we are making.  This cache of Caches will tell us to ignore this.
      var corrId: Int = 0
      objList.foreach ( elem => {
        cacheOfOwnChanges.add((operations(corrId)+"."+elem.NameSpace+"."+elem.Name+"."+elem.Version).toLowerCase)
        corrId = corrId + 1
      }) 
      
      
      val data = ZooKeeperMessage(objList, operations)
      InitZooKeeper
      val znodePath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZNODE_PATH") + "/metadataupdate"
      logger.debug("Set the data on the zookeeper node " + znodePath)
      zkc.setData().forPath(znodePath, data)
      MetadataUtils.PutTranId(objList(0).tranId)
    } catch {
      case e: Exception => {
        throw new InternalErrorException("Failed to notify a zookeeper message from the objectList " + e.getMessage())
      }
    }
  }
  
  def UpdateMdMgr(zkTransaction: ZooKeeperTransaction) = {
    var key: String = null
    var dispkey: String = null
    try {
      zkTransaction.Notifications.foreach(zkMessage => {
        key = (zkMessage.Operation + "." + zkMessage.NameSpace + "." + zkMessage.Name + "." + zkMessage.Version).toLowerCase
        dispkey = (zkMessage.Operation + "." + zkMessage.NameSpace + "." + zkMessage.Name + "." + MdMgr.Pad0s2Version(zkMessage.Version.toLong)).toLowerCase
        if (!cacheOfOwnChanges.contains(key)) {
          // Proceed with update.
          updateThisKey(zkMessage)
        } else {
          // Ignore the update, remove the element from set.
          cacheOfOwnChanges.remove(key)
        }
      })
    } catch {
      case e: AlreadyExistsException => {
        logger.warn("Failed to load the object(" + dispkey + ") into cache: " + e.getMessage())
      }
      case e: Exception => {
        logger.warn("Failed to load the object(" + dispkey + ") into cache: " + e.getMessage())
      }
    }
  }
  
 /**
  * UpdateMetadata - This is a callback function for the Zookeeper Listener.  It will get called when we detect Metadata being updated from
  *                  a different metadataImpl service.
  */
  def UpdateMetadata(receivedJsonStr: String): Unit = {
    logger.debug("Process ZooKeeper notification " + receivedJsonStr)

    if (receivedJsonStr == null || receivedJsonStr.size == 0 || !MetadataAPIImpl.isInitilized) {
      // nothing to do
      logger.info("Metadata synching is not available.")
      return
    }

    val zkTransaction = JsonSerializer.parseZkTransaction(receivedJsonStr, "JSON")
    UpdateMdMgr(zkTransaction)        
  }
  
   
  /**
   * 
   */
  private def updateThisKey(zkMessage: ZooKeeperNotification) {
    
    var key: String = (zkMessage.ObjectType + "." + zkMessage.NameSpace + "." + zkMessage.Name + "." + zkMessage.Version.toLong).toLowerCase
    val dispkey = (zkMessage.ObjectType + "." + zkMessage.NameSpace + "." + zkMessage.Name + "." + MdMgr.Pad0s2Version(zkMessage.Version.toLong)).toLowerCase

    zkMessage.ObjectType match {
      case "ModelDef" => {
        zkMessage.Operation match {
          case "Add" => {
            ModelImpl.LoadModelIntoCache(key)
          }
          case "Remove" | "Activate" | "Deactivate" => {
            try {
              MdMgr.GetMdMgr.ModifyModel(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, zkMessage.Operation)
            } catch {
              case e: ObjectNolongerExistsException => {
                logger.error("The object " + dispkey + " nolonger exists in metadata : It may have been removed already")
              }
            }
          }
          case _ => {logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")}
        }
      }
      case "MessageDef" => {
        zkMessage.Operation match {
          case "Add" => {
            MessageImpl.LoadMessageIntoCache(key)
          }
          case "Remove" => {
            try {
              MessageImpl.RemoveMessage(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong,false)
            } catch {
              case e: ObjectNolongerExistsException => {
                logger.error("The object " + dispkey + " nolonger exists in metadata : It may have been removed already")
              }
            }
          }
          case "Activate" | "Deactivate" => {
            try {
              MdMgr.GetMdMgr.ModifyMessage(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, zkMessage.Operation)
            } catch {
              case e: ObjectNolongerExistsException => {
                logger.error("The object " + dispkey + " nolonger exists in metadata : It may have been removed already")
              }
            }
          }
          case _ => {logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")}
        }
      }
      case "ContainerDef" => {
        zkMessage.Operation match {
          case "Add" => {
            ContainerImpl.LoadContainerIntoCache(key)
          }
          case "Remove" => {
            try {
              ContainerImpl.RemoveContainer(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong,false)
            } catch {
              case e: ObjectNolongerExistsException => {
                logger.error("The object " + dispkey + " nolonger exists in metadata : It may have been removed already")
              }
            }
          }
          case "Activate" | "Deactivate" => {
            try {
              MdMgr.GetMdMgr.ModifyContainer(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, zkMessage.Operation)
            } catch {
              case e: ObjectNolongerExistsException => {
                logger.error("The object " + dispkey + " nolonger exists in metadata : It may have been removed already")
              }
            }
          }
          case _ => {logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")}
        }
      }
      case "FunctionDef" => {
        zkMessage.Operation match {
          case "Add" => {
            FunctionImpl.LoadFunctionIntoCache(key)
          }
          case "Remove" | "Activate" | "Deactivate" => {
            try {
              MdMgr.GetMdMgr.ModifyFunction(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, zkMessage.Operation)
            } catch {
              case e: ObjectNolongerExistsException => {
                logger.error("The object " + dispkey + " nolonger exists in metadata : It may have been removed already")
              }
            }
          }
          case _ => {logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")}
        }
      }
      case "AttributeDef" => {
        zkMessage.Operation match {
          case "Add" => {
            ConceptImpl.LoadAttributeIntoCache(key)
          }
          case "Remove" | "Activate" | "Deactivate" => {
            try {
              MdMgr.GetMdMgr.ModifyAttribute(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, zkMessage.Operation)
            } catch {
              case e: ObjectNolongerExistsException => {
                logger.error("The object " + dispkey + " nolonger exists in metadata : It may have been removed already")
              }
            }
          }
          case _ => {logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")}
        }
      }
      case "JarDef" => {
        zkMessage.Operation match {
          case "Add" => {
            JarImpl.DownloadJarFromDB(MdMgr.GetMdMgr.MakeJarDef(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version))
          }
          case _ => {logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")}
        }
      }
      case "ScalarTypeDef" | "ArrayTypeDef" | "ArrayBufTypeDef" | "ListTypeDef" | "MappedMsgTypeDef" | "SetTypeDef" | "TreeSetTypeDef" | "QueueTypeDef" | "MapTypeDef" | "ImmutableMapTypeDef" | "HashMapTypeDef" | "TupleTypeDef" | "StructTypeDef" | "SortedSetTypeDef" => {
        zkMessage.Operation match {
          case "Add" => {
            TypeImpl.LoadTypeIntoCache(key)
          }
          case "Remove" | "Activate" | "Deactivate" => {
            try {
              logger.debug("Remove the type " + dispkey + " from cache ")
              MdMgr.GetMdMgr.ModifyType(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, zkMessage.Operation)
            } catch {
              case e: ObjectNolongerExistsException => {
                logger.error("The object " + dispkey + " nolonger exists in metadata : It may have been removed already")
              }
            }
          }
          case _ => { logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")}
        }
      }
      case _ => { logger.error("Unknown objectType " + zkMessage.ObjectType + " in zookeeper notification, notification is not processed ..") }
    }  
  }
  
  def CloseZKSession: Unit = lock.synchronized {
    if (zkc != null) {
      logger.debug("Closing zookeeper session ..")
      try {
        zkc.close()
        logger.debug("Closed zookeeper session ..")
        zkc = null
      } catch {
        case e: Exception => {
          logger.error("Unexpected Error while closing zookeeper session: " + e.getMessage())
        }
      }
    }
  }

}