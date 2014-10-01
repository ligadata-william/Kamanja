package com.ligadata.Serialize

import java.io._
import com.ligadata.olep.metadata._

import java.io.ByteArrayOutputStream

trait Serializer{
  def SerializeObjectToByteArray[T <: BaseElemDef](obj : T) : Array[Byte]
  def DeserializeObjectFromByteArray[T <: BaseElemDef](obj: Array[Byte], cls: T) : T
  //def SerializeObjectToJson[T <: BaseElemDef](obj : T) : String
  //def SerializeObjectListToJson[T <: BaseElemDef](objType:String, objList: Array[T]) : String
  //def SerializeObjectListToJson[T <: BaseElemDef](obj : T) : String
}
