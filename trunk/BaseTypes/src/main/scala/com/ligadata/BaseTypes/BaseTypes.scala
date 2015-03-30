package com.ligadata.BaseTypes

import java.util._
import java.nio.ByteBuffer
import java.nio.charset.Charset
import com.ligadata.olep.metadata.TypeImplementation
import java.io.{ DataOutputStream, DataInputStream }
 
// helper class that implement TypeImplementation 
object IntImpl extends TypeImplementation[Int] {
  def Input(value: String): Int = { // Converts String to Type T
    value match {
      case null => 0
      case x: String if x.length() == 0 => 0
      case x: String => x.toInt
    }
  }
  def Serialize(value: Int): Array[Byte] = { // Convert Type T to Array[Byte]
    ByteBuffer.allocate(4).putInt(value).array()
  }
  def Deserialize(value: Array[Byte]): Int = { // Convert Array[Byte] to Type T
    ByteBuffer.wrap(value).getInt
  }
  def SerializeIntoDataOutputStream(dos: DataOutputStream, value: Int): Unit = { // Add Type T to DataOutputStream
    dos.writeInt(value)
  }
  def DeserializeFromDataInputStream(dis: DataInputStream): Int = { // get Type T from DataInputStream 
    dis.readInt
  }
  def toString(value: Int): String = { // Convert Type T to String
    value.toString
  }
  def toJsonString(value: Int): String = { // Convert Type T to Json String
    "\"" + value.toString + "\""
  }
}

object LongImpl extends TypeImplementation[Long] {
  def Input(value: String): Long = { // Converts String to Type T
    value match {
      case null => 0
      case x: String if x.length() == 0 => 0
      case x: String => x.toLong
    }
  }
  def Serialize(value: Long): Array[Byte] = { // Convert Type T to Array[Byte]
    ByteBuffer.allocate(8).putLong(value).array()
  }
  def Deserialize(value: Array[Byte]): Long = { // Convert Array[Byte] to Type T
    ByteBuffer.wrap(value).getLong
  }
  def SerializeIntoDataOutputStream(dos: DataOutputStream, value: Long): Unit = { // Add Type T to DataOutputStream
    dos.writeLong(value)
  }
  def DeserializeFromDataInputStream(dis: DataInputStream): Long = { // get Type T from DataInputStream 
    dis.readLong
  }
  def toString(value: Long): String = { // Convert Type T to String
    value.toString
  }
  def toJsonString(value: Long): String = { // Convert Type T to Json String
    "\"" + value.toString + "\""
  }
}

object FloatImpl extends TypeImplementation[Float] {
  def Input(value: String): Float = { // Converts String to Type T
    value match {
      case null => 0
      case x: String if x.length() == 0 => 0
      case x: String => x.toFloat
    }
  }
  def Serialize(value: Float): Array[Byte] = { // Convert Type T to Array[Byte]
    ByteBuffer.allocate(4).putFloat(value).array()
  }
  def Deserialize(value: Array[Byte]): Float = { // Convert Array[Byte] to Type T
    ByteBuffer.wrap(value).getFloat
  }
  def SerializeIntoDataOutputStream(dos: DataOutputStream, value: Float): Unit = { // Add Type T to DataOutputStream
    dos.writeFloat(value)
  }
  def DeserializeFromDataInputStream(dis: DataInputStream): Float = { // get Type T from DataInputStream 
    dis.readFloat
  }
  def toString(value: Float): String = { // Convert Type T to String
    value.toString
  }
  def toJsonString(value: Float): String = { // Convert Type T to Json String
    "\"" + value.toString + "\""
  }
}

object DoubleImpl extends TypeImplementation[Double] {
  def Input(value: String): Double = { // Converts String to Type T
    value match {
      case null => 0
      case x: String if x.length() == 0 => 0
      case x: String => x.toDouble
    }
  }
  def Serialize(value: Double): Array[Byte] = { // Convert Type T to Array[Byte]
    ByteBuffer.allocate(8).putDouble(value).array()
  }
  def Deserialize(value: Array[Byte]): Double = { // Convert Array[Byte] to Type T
    ByteBuffer.wrap(value).getDouble
  }
  def SerializeIntoDataOutputStream(dos: DataOutputStream, value: Double): Unit = { // Add Type T to DataOutputStream
    dos.writeDouble(value)
  }
  def DeserializeFromDataInputStream(dis: DataInputStream): Double = { // get Type T from DataInputStream 
    dis.readDouble
  }
  def toString(value: Double): String = { // Convert Type T to String
    value.toString
  }
  def toJsonString(value: Double): String = { // Convert Type T to Json String
    "\"" + value.toString + "\""
  }
}

// currently only changes boolean to 1 or 0.

object BoolImpl extends TypeImplementation[Boolean] {
  def Input(value: String): Boolean = { // Converts String to Type T
    val i = value.toInt
    (i != 0)
  }
  def Serialize(value: Boolean): Array[Byte] = { // Convert Type T to Array[Byte]
    ByteBuffer.allocate(1).putInt(if (value) 1 else 0).array()
  }
  def Deserialize(value: Array[Byte]): Boolean = { // Convert Array[Byte] to Type T
    ByteBuffer.wrap(value).getInt != 0
  }
  def SerializeIntoDataOutputStream(dos: DataOutputStream, value: Boolean): Unit = { // Add Type T to DataOutputStream
    dos.writeBoolean(value)
  }
  def DeserializeFromDataInputStream(dis: DataInputStream): Boolean = { // get Type T from DataInputStream 
    dis.readBoolean
  }
  def toString(value: Boolean): String = { // Convert Type T to String
    if (value) 1.toString else 0.toString
  }
  def toJsonString(value: Boolean): String = { // Convert Type T to Json String
    "\"" + toString(value) + "\""
  }
}

object StringImpl extends TypeImplementation[String] {
  def Input(value: String): String = { // Converts String to Type T
    value 
  }
  def Serialize(value: String): Array[Byte] = { // Convert Type T to Array[Byte]
 if(value != null) return value.getBytes("UTF-8")
    else return "".getBytes("UTF-8")  
}
  def Deserialize(value: Array[Byte]): String = { // Convert Array[Byte] to Type T
    new String(value, "UTF-8")
  }
  def SerializeIntoDataOutputStream(dos: DataOutputStream, value: String): Unit = { // Add Type T to DataOutputStream
 if(value != null) dos.writeUTF(value)
    else dos.writeUTF("")  
}
  def DeserializeFromDataInputStream(dis: DataInputStream): String = { // get Type T from DataInputStream 
    val value = dis.readUTF
    if(value != null) return value
    else return ""
  }
  def toString(value: String): String = { // Convert Type T to String
    value
  }
  def toJsonString(value: String): String = { // Convert Type T to Json String
    "\"" + value + "\""
  }
}

object CharImpl extends TypeImplementation[Char] {
  def Input(value: String): Char = { // Converts String to Type T
    value match {
      case null => ' '
      case x: String if x.length() == 0 => ' '
      case x: String => x.charAt(0)
    }
  }
  def Serialize(value: Char): Array[Byte] = { // Convert Type T to Array[Byte]
    ByteBuffer.allocate(1).putChar(value).array()
  }
  def Deserialize(value: Array[Byte]): Char = { // Convert Array[Byte] to Type T
    ByteBuffer.wrap(value).getChar
  }
  def SerializeIntoDataOutputStream(dos: DataOutputStream, value: Char): Unit = { // Add Type T to DataOutputStream
    dos.writeChar(value)
  }
  def DeserializeFromDataInputStream(dis: DataInputStream): Char = { // get Type T from DataInputStream 
    dis.readChar
  }
  def toString(value: Char): String = { // Convert Type T to String
    value.toString
  }
  def toJsonString(value: Char): String = { // Convert Type T to Json String
    "\"" + value.toString + "\""
  }
}


