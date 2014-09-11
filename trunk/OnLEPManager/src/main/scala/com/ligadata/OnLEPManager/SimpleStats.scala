package com.ligadata.OnLEPManager

import com.ligadata.OnLEPBase.CountersAdapter

object SimpleStats extends CountersAdapter {

  private[this] val _lock = new Object()
  private[this] val _statusInfo = scala.collection.mutable.Map[String, Long]()

  override def addCntr(key: String, cnt: Long): Long = _lock.synchronized {
    val v = _statusInfo.getOrElse(key, 0L)
    _statusInfo(key) = v + cnt
    v + cnt
  }

  override def addCntr(cntrs: scala.collection.immutable.Map[String, Long]): Unit = _lock.synchronized {
    // Adding each key
    cntrs.foreach(kv => {
      val v = _statusInfo.getOrElse(kv._1, 0L)
      _statusInfo(kv._1) = v + kv._2
    })
  }

  override def getCntr(key: String): Long = _lock.synchronized {
    _statusInfo.getOrElse(key, 0L)
  }

  override def getDispString(delim: String): String = _lock.synchronized {
    _statusInfo.mkString(delim)
  }

  override def copyMap: scala.collection.immutable.Map[String, Long] = _lock.synchronized {
    _statusInfo.toMap
  }
}
