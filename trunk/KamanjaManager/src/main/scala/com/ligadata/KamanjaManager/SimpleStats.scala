/*
 * Copyright 2015 ligaDATA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ligadata.KamanjaManager

import com.ligadata.InputOutputAdapterInfo.CountersAdapter

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
