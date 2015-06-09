package com.ligadata.Serialize

import com.ligadata.Exceptions._

object SerializerManager {
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
        e.printStackTrace()
        throw new SerializerManagerException("Failed to create Serializer for : " + serializerType)
      }
    }
  }
}
