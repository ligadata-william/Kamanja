
package com.ligadata.SimpleEnvContextImpl

import com.ligadata.OnLEPBase.{ EnvContext, BaseContainer, BaseMsg }
import scala.collection.immutable.Map
import com.ligadata.olep.metadata._

// For now we are holding stuff in memory.
object SimpleEnvContextImpl extends EnvContext {
  private[this] val _lock = new Object()
  private[this] var _containers = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, BaseContainer]]()
  private[this] var _messages = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, BaseMsg]]()
  private[this] var _bInitialized: Boolean = false
  private[this] var _bInitializedMessages: Boolean = false

  def initContainers(mgr: MdMgr, dataPath: String, containerNames: Array[String]): Unit = _lock.synchronized {
    if (_bInitialized) {
      throw new Exception("Already Initialized")
    }
    containerNames.foreach(c => {
      _containers(c.toLowerCase()) = scala.collection.mutable.Map[String, BaseContainer]()
    })
    _bInitialized = true
  }

  /**
   *  Intitialize the messages cache
   */
  def initMessages(mgr: MdMgr, dataPath: String, msgNames: Array[String]): Unit = _lock.synchronized {
    if (_bInitializedMessages) {
      throw new RuntimeException("Already Initialized")
    }
    msgNames.foreach(c => {
      val names: Array[String] = c.split('.')
      val namespace: String = names.head
      val name: String = names.last
      val msgType: MessageDef = mgr.ActiveMessage(namespace, name)
      if (msgType != null) {
        _messages(msgType.FullName.toLowerCase) = scala.collection.mutable.Map[String, BaseMsg]()
      }
    })

    _bInitializedMessages = true
  }

  override def getObjects(containerName: String, key: String): Array[BaseContainer] = _lock.synchronized {
    // bugbug: implement partial match
    Array(getObject(containerName, key))
  }

  override def getObject(containerName: String, key: String): BaseContainer = _lock.synchronized {
    val container = _containers.getOrElse(containerName.toLowerCase(), null)
    if (container != null) {
      val v = container.getOrElse(key.toLowerCase(), null)
      // LOG.info("Found Container:" + containerName + ". Value:" + v)
      v
    } else null
  }

  override def setObject(containerName: String, key: String, value: BaseContainer): Unit = _lock.synchronized {
    val container = _containers.getOrElse(containerName.toLowerCase(), null)
    if (container != null) container(key.toLowerCase()) = value
    // bugbug: throw exception
  }

  override def setObject(containerName: String, elementkey: Any, value: BaseContainer): Unit = _lock.synchronized {
    val container = _containers.getOrElse(containerName.toLowerCase(), null)
    if (container != null) container(elementkey.toString.toLowerCase()) = value
    // bugbug: throw exception
  }

  override def getMsgObject(msgName: String, key: String): BaseMsg = {
    val msg = _messages.getOrElse(msgName.toLowerCase, null)
    if (msg != null) {
      val v = msg.getOrElse(key.toLowerCase(), null)
      // LOG.info("Found Message:" + msgName + ". Value:" + v)
      v
    } else null
  }

  override def setMsgObject(msgName: String, key: String, value: BaseMsg): Unit = {
    val msg = _messages.getOrElse(msgName.toLowerCase, null)
    if (msg != null) msg(key.toLowerCase) = value
    // bugbug: throw exception
  }

}

