
package com.ligadata.OnLEPBase

trait BaseContainer {
  def IsFixed: Boolean
  def IsKv: Boolean
  def populate(inputdata: InputData): Unit
  def get(key: String): Any
  def getOrElse(key: String, default: Any): Any
  def set(key: String, value: Any): Unit
  def getContainerName: String // Container Name
  def getVersion: String // Container Version
}

trait BaseContainerObj {
  def IsFixed: Boolean
  def IsKv: Boolean
  def getContainerName: String // Container Name
  def getVersion: String // Container Version
  def CreateNewContainer: BaseContainer 
}

trait BaseMsg {
  def IsFixed: Boolean
  def IsKv: Boolean
  def populate(inputdata: InputData): Unit
  def getMessageName: String // Message Name
  def getVersion: String // Message Version
  def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = { }
  def getKeyData: String = ""
}

trait BaseMsgObj {
  def IsFixed: Boolean
  def IsKv: Boolean
  def NeedToTransformData: Boolean // Filter & Rearrange input attributes if needed
  def TransformDataAttributes: TransformMessage // Filter & Rearrange input columns if needed
  def getMessageName: String // Message Name
  def getVersion: String // Message Version
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

