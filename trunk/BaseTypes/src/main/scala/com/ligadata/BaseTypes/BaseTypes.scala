package com.ligadata.BaseTypes

import java.util._
import java.nio.ByteBuffer
import java.nio.charset.Charset
import com.ligadata.olep.metadata.TypeImplementation

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
    value.getBytes("UTF-8")
  }
  def Deserialize(value: Array[Byte]): String = { // Convert Array[Byte] to Type T
    new String(value, "UTF-8")
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
  def toString(value: Char): String = { // Convert Type T to String
    value.toString
  }
  def toJsonString(value: Char): String = { // Convert Type T to Json String
    "\"" + value.toString + "\""
  }
}


