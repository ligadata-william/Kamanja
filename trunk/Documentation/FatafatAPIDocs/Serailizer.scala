package com.ligadata.Serialize

import java.io._
import com.ligadata.olep.metadata._

import java.io.ByteArrayOutputStream

import org.apache.log4j._

trait Serializer{
  def SetLoggerLevel(level: Level)
  def SetClassLoader(cl : java.lang.ClassLoader): Unit
  def SerializeObjectToByteArray(obj : Object) : Array[Byte]
  def DeserializeObjectFromByteArray(ba: Array[Byte]) : Object
  def DeserializeObjectFromByteArray(ba: Array[Byte],objectType: String) : Object
}
