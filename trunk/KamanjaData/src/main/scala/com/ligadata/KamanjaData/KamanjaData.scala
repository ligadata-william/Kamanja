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

package com.ligadata.KamanjaData

import java.io.{ ByteArrayInputStream, DataInputStream, DataOutputStream, ByteArrayOutputStream }
import scala.collection.mutable.ArrayBuffer;
import scala.collection.JavaConversions._
import scala.util.control.Breaks._
import com.ligadata.KamanjaBase._
import com.ligadata.Exceptions.StackTrace
import org.apache.log4j._
import java.util.Date

object KamanjaData {
  def Version = 1 // Current Version
  val defaultTime = new Date(0)
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
}

class KamanjaData {
  private var typName: String = "" // Type name of Message/container
  private var bucketKey = ArrayBuffer[String]() // Partition Key/Bucket Key
  private var time: Date = KamanjaData.defaultTime // Start Time Range. Default is used if nothing is set
  private var data = ArrayBuffer[MessageContainerBase]() // Messages/Containers for this key & with in this date range.
  private var transactionId = 0L
  private val logger = KamanjaData.logger

  // Getting Key information
  def GetBucketKey = bucketKey.toArray

  // Setting Key information
  def SetBucketKey(partitionkey: Array[String]): Unit = {
    bucketKey.clear
    if (partitionkey != null)
      bucketKey ++= partitionkey
  }

  // Getting Type Name
  def GetTypeName = typName

  // Setting Type Name
  def SetTypeName(tmpTypName: String): Unit = {
    if (tmpTypName != null)
      typName = tmpTypName
    else
      typName = ""
  }

  // Getting Date Range
  def GetTime = time

  // Setting Date Range
  def SetTime(tm: Date): Unit = {
    time = tm
  }

  def SetTransactionId(txnId: Long): Unit = {
    transactionId = txnId
  }

  def GetTransactionId = transactionId

  // Getting All Data
  def GetAllData: Array[MessageContainerBase] = data.toArray

  // Getting All Data
  def DataSize: Int = data.size

  // Adding New Message or Container
  def AddMessageContainerBase(baseCntMsg: MessageContainerBase, checkForSameObj: Boolean, moveToEnd: Boolean): Unit = {
    if (checkForSameObj) {
      val primaryKey = baseCntMsg.PrimaryKeyData
      if (primaryKey.size > 0) {

        breakable {
          for (i <- 0 until data.size) {
            val pkd = data(i).PrimaryKeyData
            if (pkd.sameElements(primaryKey)) {
              if (moveToEnd) {
                data.remove(i) // Remove here and add it later
                break
              } else {
                data(i) = baseCntMsg // Do In-place Replace and return 
                return
              }
            }
          }
        }
      }
      data += baseCntMsg
    } else {
      data += baseCntMsg
    }
    return
  }

  // Getting Message or Container from existing list
  def GetMessageContainerBase(primaryKey: Array[String], newCopyIfFound: Boolean): MessageContainerBase = {
    if (primaryKey != null && primaryKey.size > 0) {
      for (i <- 0 until data.size) {
        val pkd = data(i).PrimaryKeyData
        if (pkd.sameElements(primaryKey)) {
          if (newCopyIfFound)
            return data(i) //BUGBUG:: Need to create Duplicate copy here for now we don't have method to duplicate yet.
          else
            return data(i)
        }
      }
    }
    return null
  }

  // BUGBUG:: Order of containers/messages are not guaranteed here. Because we don't know which one comes first in this. For now expecting this is old and appending collection is latest.
  def appendWithCheck(collection: KamanjaData): Unit = {
    if (bucketKey.sameElements(collection.bucketKey)) {
      try {
        collection.data.foreach(typ => {
          var replaced = false
          breakable {
            val primaryKey = typ.PrimaryKeyData
            if (primaryKey.size > 0) {
              for (i <- 0 until data.size) {
                val pkd = data(i).PrimaryKeyData
                if (pkd.sameElements(primaryKey)) {
                  data(i) = typ
                  replaced = true
                  break
                }
              }
            }
          }
          if (replaced == false)
            data += typ
        })
      } catch {
        case e: Exception => {
          logger.debug("StackTrace:" + StackTrace.ThrowableTraceString(e))
          throw e
        }
      }
    } else {
      throw new Exception("We can merge PartitionKeyMessages with same key only. %s != %s".format(bucketKey.mkString(","), collection.bucketKey.mkString(",")))
    }
  }

  // Here are not checking for duplicates existance. Thinking that collection always has new messages/containers.
  def appendNoDupCheck(collection: KamanjaData): Unit = {
    if (bucketKey.sameElements(collection.bucketKey)) {
      try {
        data ++= collection.data
      } catch {
        case e: Exception => {
          StackTrace.ThrowableTraceString(e)
          logger.debug("StackTrace:" + StackTrace.ThrowableTraceString(e))
          throw e
        }
      }
    } else {
      throw new Exception("We can append PartitionKeyMessages with same key only. %s != %s".format(bucketKey.mkString(","), collection.bucketKey.mkString(",")))
    }
  }

  def SerializeData: Array[Byte] = {
    val bos: ByteArrayOutputStream = new ByteArrayOutputStream(1024 * 1024)
    val dos = new DataOutputStream(bos)

    try {
      // Serializing Type Name
      dos.writeUTF(typName)

      // Serializing Partition Key
      dos.writeInt(bucketKey.size)
      for (i <- 0 until bucketKey.size) {
        dos.writeUTF(bucketKey(i))
      }

      // Serializing Time
      dos.writeLong(time.getTime())

      // Serializing types (all messages & containers with this key)
      dos.writeInt(data.size)
      data.foreach(d => {
        dos.writeUTF(d.FullName)
        dos.writeUTF(d.Version)
        dos.writeUTF(d.getClass.getName)
        d.Serialize(dos)
      })
      val arr = bos.toByteArray
      dos.close
      bos.close
      return arr

    } catch {
      case e: Exception => {
        logger.debug("StackTrace:" + StackTrace.ThrowableTraceString(e))
        dos.close
        bos.close
        throw e
      }
    }
    return null
  }

  // Instantiate this class and de-serialize
  def DeserializeData(bytearray: Array[Byte], mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader): Unit = {
    var dis = new DataInputStream(new ByteArrayInputStream(bytearray));

    // Clear key & types before de-serialize.
    // BUGBUG:: In case of exception while de-serializing, it would loose the previous state at this moment. Do we need to make a copy to persist the state???
    bucketKey.clear
    data.clear

    try {
      // DeSerializing Type Name
      typName = dis.readUTF

      // DeSerializing Partition Key
      val ksz = dis.readInt
      for (i <- 0 until ksz) {
        bucketKey += dis.readUTF
      }

      // DeSerializing Time
      time = new Date(dis.readLong)

      // DeSerializing types (all messages & containers with this key)
      val typeVals = dis.readInt
      for (i <- 0 until typeVals) {
        val fullName = dis.readUTF
        val version = dis.readUTF
        val classname = dis.readUTF

        // Expecting type name
        // get class instance for this type
        val typ = mdResolver.getMessgeOrContainerInstance(fullName)
        if (typ == null) {
          throw new Exception("Message/Container %s not found to deserialize".format(typName))
        }
        typ.Deserialize(dis, mdResolver, loader, version.toString)
        data += typ
      }
      dis.close
    } catch {
      case e: Exception => {
        logger.debug("StackTrace:" + StackTrace.ThrowableTraceString(e))
        dis.close
        throw e
      }
    }
  }

}

