
package com.ligadata.AdaptersConfiguration

import com.ligadata.OnLEPBase.{ AdapterConfiguration, PartitionUniqueRecordKey, PartitionUniqueRecordValue }
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._

class FileAdapterConfiguration extends AdapterConfiguration {
  var Files: Array[String] = _ // Array of files and each execute one by one
  var CompressionString: String = _ // If it is null or empty we treat it as TEXT file
  var MessagePrefix: String = _ // This is the first String in the message
  var IgnoreLines: Int = _ // number of lines to ignore in each file
  var AddTS2MsgFlag: Boolean = false // Add TS after the Prefix Msg
}

case class FileKeyData(Version: Int, Type: String, Name: Option[String]) // Using most of the values as optional values. Just thinking about future changes. Don't know the performance issues.

class FilePartitionUniqueRecordKey extends PartitionUniqueRecordKey {
  val Version: Int = 1
  val Type: String = "File"
  var Name: String = _

  override def Serialize: String = { // Making String from key
    val json =
      ("Version" -> Version) ~
        ("Type" -> Type) ~
        ("Name" -> Name)
    compact(render(json))
  }

  override def Deserialize(key: String): Unit = { // Making Key from Serialized String
    implicit val jsonFormats: Formats = DefaultFormats
    val keyData = parse(key).extract[FileKeyData]
    if (keyData.Version == Version && keyData.Type.compareTo(Type) == 0) {
      Name = keyData.Name.get
    }
    // else { } // Not yet handling other versions
  }
}

case class FileRecData(Version: Int, FileFullPath: Option[String], Offset: Option[Long]) // Using most of the values as optional values. Just thinking about future changes. Don't know the performance issues.

class FilePartitionUniqueRecordValue extends PartitionUniqueRecordValue {
  val Version: Int = 1
  var FileFullPath: String = _
  var Offset: Long = _ // Current Record File Offset

  override def Serialize: String = { // Making String from Value
    val json =
      ("Version" -> Version) ~
        ("FileFullPath" -> FileFullPath) ~
        ("Offset" -> Offset)
    compact(render(json))
  }

  override def Deserialize(key: String): Unit = { // Making Value from Serialized String
    implicit val jsonFormats: Formats = DefaultFormats
    val recData = parse(key).extract[FileRecData]
    if (recData.Version == Version) {
      FileFullPath = recData.FileFullPath.get
      Offset = recData.Offset.get
    }
    // else { } // Not yet handling other versions
  }
}

