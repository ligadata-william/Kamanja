package com.ligadata.KamanjaData

import java.io.{ ByteArrayInputStream, DataInputStream, DataOutputStream, ByteArrayOutputStream }
import scala.collection.mutable.ArrayBuffer;
import scala.collection.JavaConversions._
import scala.util.control.Breaks._
import com.ligadata.KamanjaBase._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import com.ligadata.Exceptions.StackTrace
import org.apache.log4j._

case class KamanjaDataKey(T: String, K: List[String], D: List[Int], V: Int)

object KamanjaData {
  def Version = 1 // Current Version
  def PrepareKey(typName: String, partitionkey: List[String], StartDateRange: Int, EndDateRange: Int): String = {
    /*
    return "T:%s|K:%s|D:%d,%d|V:%d".format(typName.toLowerCase, partitionkey.toList.map(k => k.toLowerCase).mkString(","), StartDateRange, EndDateRange, Version).getBytes("UTF8")
    val a1 = typName.toLowerCase
    val a2 =  partitionkey.toList.map(k => k.toLowerCase).mkString(",") // "abc12345671234567890" // partitionkey.toList.map(k => k.toLowerCase).mkString(",")
    return s"T:${a1}|K:{a2}|D:${StartDateRange},${EndDateRange}|V:${Version}".getBytes("UTF8")
    return x
*/

    val key = KamanjaDataKey(typName.toLowerCase, partitionkey.map(k => k.toLowerCase), List(StartDateRange, EndDateRange), Version)
    val json =
      ("T" -> key.T) ~
        ("K" -> key.K) ~
        ("D" -> key.D) ~
        ("V" -> key.V)
    return compact(render(json))
  }
}

class KamanjaData {
  private val ver = KamanjaData.Version // Version
  private var typName: String = "" // Type name (Message, container)
  private var key = ArrayBuffer[String]() // Partition Key
  private var StartDateRange: Int = 0 // Start Date Range
  private var EndDateRange: Int = 0 // End Date Range
  private var data = ArrayBuffer[MessageContainerBase]() // Messages/Containers for this key & with in this date range. 
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
  
  def Version = ver // Current Version

  // Getting Key information
  def GetKey = key.toArray

  // Setting Key information
  def SetKey(partitionkey: Array[String]): Unit = {
    key.clear
    if (partitionkey != null)
      key ++= partitionkey
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
  def GetDateRange: (Int, Int) = (StartDateRange, EndDateRange)

  // Setting Date Range
  def SetDateRange(stDateRange: Int, edDateRange: Int): Unit = {
    StartDateRange = stDateRange
    EndDateRange = edDateRange
  }

  // Getting All Data
  def GetAllData: Array[MessageContainerBase] = data.toArray

  // Getting All Data
  def DataSize: Int = data.size

  // Adding New Message or Container
  def AddMessageContainerBase(baseCntMsg: MessageContainerBase, checkForSameObj: Boolean, moveToEnd: Boolean): Unit = {
    if (checkForSameObj) {
      if (moveToEnd) {
        data -= baseCntMsg // Remove here and add it later
      } else {
        data.foreach(d => {
          if (d == baseCntMsg)
            return
        })
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
    if (key.sameElements(collection.key)) {
      try {
        collection.data.foreach(typ => {
          var replaced = false
          breakable {
            val primaryKey = typ.PrimaryKeyData
            for (i <- 0 until data.size) {
              val pkd = data(i).PrimaryKeyData
              if (pkd.sameElements(primaryKey)) {
                data(i) = typ
                replaced = true
                break
              }
            }
          }
          if (replaced == false)
            data += typ
        })
      } catch {
        case e: Exception => {
          logger.debug("StackTrace:"+StackTrace.ThrowableTraceString(e))
          throw e
        }
      }
    } else {
      throw new Exception("We can merge PartitionKeyMessages with same key only. %s != %s".format(key.mkString(","), collection.key.mkString(",")))
    }
  }

  // Here are not checking for duplicates existance. Thinking that collection always has new messages/containers.
  def appendNoDupCheck(collection: KamanjaData): Unit = {
    if (key.sameElements(collection.key)) {
      try {
        data ++= collection.data
      } catch {
        case e: Exception => {
          StackTrace.ThrowableTraceString(e)
          logger.debug("StackTrace:"+StackTrace.ThrowableTraceString(e))
          throw e
        }
      }
    } else {
      throw new Exception("We can append PartitionKeyMessages with same key only. %s != %s".format(key.mkString(","), collection.key.mkString(",")))
    }
  }

  def SerializeKey: String = return KamanjaData.PrepareKey(typName, key.toList, StartDateRange, EndDateRange)

  def SerializeData: Array[Byte] = {
    val bos: ByteArrayOutputStream = new ByteArrayOutputStream(1024 * 1024)
    val dos = new DataOutputStream(bos)

    try {
      // Serializing Version
      dos.writeInt(ver)

      // Serializing Type Name
      dos.writeUTF(typName)

      // Serializing Partition Key
      dos.writeInt(key.size)
      for (i <- 0 until key.size) {
        dos.writeUTF(key(i))
      }

      // Serializing Date Range
      dos.writeInt(StartDateRange)
      dos.writeInt(EndDateRange)

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
        logger.debug("StackTrace:"+StackTrace.ThrowableTraceString(e))
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
    key.clear
    data.clear

    try {
      // DeSerializing Version
      val tmpVer = dis.readInt

      // DeSerializing Type Name
      typName = dis.readUTF

      // DeSerializing Partition Key
      val ksz = dis.readInt
      for (i <- 0 until ksz) {
        key += dis.readUTF
      }

      // DeSerializing Date Range
      StartDateRange = dis.readInt
      EndDateRange = dis.readInt

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
        logger.debug("StackTrace:"+StackTrace.ThrowableTraceString(e))
        dis.close
        throw e
      }
    }
  }

}

