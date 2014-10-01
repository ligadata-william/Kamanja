package com.ligadata.Serialize

import java.util.Properties
import java.io._
import scala.Enumeration
import scala.io.Source._
import org.apache.log4j._

import com.ligadata.olep.metadata._

import com.twitter.chill.ScalaKryoInstantiator
import com.esotericsoftware.kryo.io.{Input, Output}

case class KryoSerializationException(e: String) extends Throwable(e)

class KryoSerializer extends Serializer{

  lazy val kryoFactory = new ScalaKryoInstantiator

  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)

  def SetLoggerLevel(level: Level){
    logger.setLevel(level);
  }

  def SerializeObjectToByteArray[T <: BaseElemDef](obj : T) : Array[Byte] = {
    try{
      val kryo = kryoFactory.newKryo()
      val baos = new ByteArrayOutputStream
      val output = new Output(baos)
      kryo.writeObject(output,obj)
      output.close()
      baos.toByteArray()
    }catch{
      case e:Exception => {
	throw new KryoSerializationException("Failed to Serialize the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
    }
  }


  def DeserializeObjectFromByteArray[T <: BaseElemDef](obj: Array[Byte], cls: T) : T = {
    try{
      val kryo = kryoFactory.newKryo()
      val bais = new ByteArrayInputStream(obj);
      val inp = new Input(bais)
      val m = kryo.readObject(inp,cls.getClass)
      inp.close()
      m
    }catch{
      case e:Exception => {
	throw new KryoSerializationException("Failed to DeSerialize the object of class(" + cls.getClass().getName() + "): " + e.getMessage())
      }
    }
  }
}
