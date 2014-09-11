
package com.ligadata.SimpleEnvContextImpl

import com.ligadata.OnLEPBase.{ EnvContext, BaseContainer }
import scala.collection.immutable.Map
import com.ligadata.olep.metadata._

// For now we are holding stuff in memory.
object SimpleEnvContextImpl extends EnvContext {
  private[this] val _lock = new Object()
  private[this] var _containers = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, BaseContainer]]()
  private[this] var _bInitialized: Boolean = false

  def initContainers(mgr : MdMgr, dataPath : String, containerNames: Array[String]): Unit = _lock.synchronized {
    if (_bInitialized) {
      throw new Exception("Already Initialized")
    }
    containerNames.foreach(c => {
      _containers(c.toLowerCase()) = scala.collection.mutable.Map[String, BaseContainer]()
    })
    _bInitialized = true
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
}

