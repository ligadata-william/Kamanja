
package com.ligadata.OnLEPBase

trait MessageContainerBase {
  def isMessage: Boolean
  def isContainer: Boolean 
  def IsFixed: Boolean
  def IsKv: Boolean
  def populate(inputdata: InputData): Unit
  def set(key: String, value: Any): Unit
  def get(key: String): Any
  def getOrElse(key: String, default: Any): Any
  def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit
  def Version: String // Message or Container Version
  def PartitionKeyData: Array[String] // Partition key data
  def PrimaryKeyData: Array[String] // Primary key data
  def FullName: String // Message or Container Full Name
  def NameSpace: String // Message or Container NameSpace
  def Name: String // Message or Container Name
}

trait MessageContainerObjBase {
  def isMessage: Boolean
  def isContainer: Boolean 
  def IsFixed: Boolean
  def IsKv: Boolean
  def FullName: String // Message or Container FullName
  def NameSpace: String // Message or Container NameSpace
  def Name: String // Message or Container Name
  def Version: String // Message or Container Version
}

trait BaseContainer extends MessageContainerBase {
  override def isMessage: Boolean = false
  override def isContainer: Boolean = true
}

trait BaseContainerObj extends MessageContainerObjBase {
  override def isMessage: Boolean = false
  override def isContainer: Boolean = true
  def CreateNewContainer: BaseContainer 
}

trait BaseMsg extends MessageContainerBase {
  override def isMessage: Boolean = true
  override def isContainer: Boolean = false
}

trait BaseMsgObj extends MessageContainerObjBase {
  override def isMessage: Boolean = true
  override def isContainer: Boolean = false
  def NeedToTransformData: Boolean // Filter & Rearrange input attributes if needed
  def TransformDataAttributes: TransformMessage // Filter & Rearrange input columns if needed
  def CreateNewMessage: BaseMsg 
}

// BUGBUG:: for now handling only CSV input data.
// Assuming this is filled properly, we are not checking whether outputFields are subset of inputFields or not.
// Assuming the field names are all same case (lower or upper). Because we don't want to convert them every time.
class TransformMessage {
  var messageType: String = null // Type of the message (first field from incoming data)
  var inputFields: Array[String] = null // All input fields
  var outputFields: Array[String] = null // All output fields filters from input field. These are subset of input fields.
  var outputKeys: Array[String] = null // Output Key field names from input fields.
}

