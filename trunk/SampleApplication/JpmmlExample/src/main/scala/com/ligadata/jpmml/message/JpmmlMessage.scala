package com.ligadata.jpmml.message

import java.io.{DataOutputStream, DataInputStream}

import com.ligadata.KamanjaBase.{MdBaseResolveInfo, BaseMsg, InputData, MessageContainerBase}

/**
 * A basic message with only get value by key as a usable method. There is nothing specific to jpmml in here
 */
case class JpmmlMessage(attributeMap: Map[String, Any]) extends BaseMsg {


  // Message or Container Full Name
  override def NameSpace: String = ""

  override def Serialize(dos: DataOutputStream): Unit = throw new RuntimeException("Not implemented")

  override def Clone(): MessageContainerBase = this.copy()

  override def set(key: String, value: Any): Unit = throw new RuntimeException("Not allowed")

  override def get(key: String): Any = attributeMap.get(key).get

  override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = throw new RuntimeException("Not implemented")

  // Message or Container Name
  override def Deserialize(dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: ClassLoader, savedDataVersion: String): Unit = throw new RuntimeException("Not implemented")

  override def GetMessage(childPath: Array[(String, String)], primaryKey: Array[String]): BaseMsg = throw new RuntimeException("Not implemented")

  override def getOrElse(key: String, default: Any): Any = attributeMap.getOrElse(key, default)

  override def IsFixed: Boolean = false

  override def Version: String = "1.0.0"

  override def CanPersist: Boolean = false

  // Primary key data
  override def FullName: String = throw new RuntimeException("Not implemented")

  // Partition key data
  override def PrimaryKeyData: Array[String] = throw new RuntimeException("Not implemented")

  override def populate(inputdata: InputData): Unit = throw new RuntimeException("Not implemented")

  // Message or Container NameSpace
  override def Name: String = throw new RuntimeException("Not implemented")

  // Message or Container Version
  override def PartitionKeyData: Array[String] = throw new RuntimeException("Not implemented")

  override def Save(): Unit = throw new RuntimeException("Not implemented")

  override def IsKv: Boolean = throw new RuntimeException("Not implemented")

  override var transactionId: Long = _
}
