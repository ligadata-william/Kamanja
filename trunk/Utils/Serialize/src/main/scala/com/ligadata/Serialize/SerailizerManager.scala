package com.ligadata.Serialize

import com.ligadata.Exceptions._
import com.ligadata.Exceptions.StackTrace
import org.apache.log4j._

object SerializerManager {
   val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
  def GetSerializer(serializerType: String): Serializer = {
    try {
      serializerType.toLowerCase match {
        case "kryo" => return new KryoSerializer
        case "protobuf" => return new ProtoBufSerializer
        case _ => {
          throw new SerializerManagerException("Unknown Serializer Type : " + serializerType)
        }
      }
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        logger.debug("StackTrace:"+stackTrace)
        throw new SerializerManagerException("Failed to create Serializer for : " + serializerType)
      }
    }
  }
}
