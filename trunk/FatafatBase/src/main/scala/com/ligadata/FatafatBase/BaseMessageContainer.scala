
package com.ligadata.FatafatBase
import java.net.URL
import java.net.URLClassLoader
import java.io.{ ByteArrayInputStream, DataInputStream, DataOutputStream, ByteArrayOutputStream }
import com.ligadata.Utils.Utils


trait MessageContainerBase {
  var transactionId: Long
  def isMessage: Boolean
  def isContainer: Boolean
  def IsFixed: Boolean
  def IsKv: Boolean
  def CanPersist: Boolean
  def populate(inputdata: InputData): Unit
  def set(key: String, value: Any): Unit
  def get(key: String): Any
  def getOrElse(key: String, default: Any): Any
  def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit
  def GetMessage(childPath: Array[(String, String)], primaryKey: Array[String]): BaseMsg
  def Version: String // Message or Container Version
  def PartitionKeyData: Array[String] // Partition key data
  def PrimaryKeyData: Array[String] // Primary key data
  def FullName: String // Message or Container Full Name
  def NameSpace: String // Message or Container NameSpace
  def Name: String // Message or Container Name
  def Deserialize(dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, savedDataVersion: String): Unit
  def Serialize(dos: DataOutputStream): Unit
  def Save(): Unit  
  final def TransactionId(transId: Long): Unit = { transactionId = transId }
  final def TransactionId(): Long = transactionId
}

trait MessageContainerObjBase {
  def isMessage: Boolean
  def isContainer: Boolean
  def IsFixed: Boolean
  def IsKv: Boolean
  def CanPersist: Boolean
  def FullName: String // Message or Container FullName
  def NameSpace: String // Message or Container NameSpace
  def Name: String // Message or Container Name
  def Version: String // Message or Container Version
  def PartitionKeyData(inputdata: InputData): Array[String] // Partition key data
  def PrimaryKeyData(inputdata: InputData): Array[String] // Primary key data
}

trait MdBaseResolveInfo {
  def getMessgeOrContainerInstance(typName: String): MessageContainerBase
}

object SerializeDeserialize {
  def Serialize(inst: MessageContainerBase): Array[Byte] = {
    val bos: ByteArrayOutputStream = new ByteArrayOutputStream(1024 * 1024)
    val dos = new DataOutputStream(bos)

    try {
      dos.writeUTF(inst.FullName)
      dos.writeUTF(inst.Version)
      dos.writeUTF(inst.getClass.getName)
      inst.Serialize(dos)
      val arr = bos.toByteArray
      dos.close
      bos.close
      return arr

    } catch {
      case e: Exception => {
        //LOG.error("Failed to get classname :" + clsName)
        Utils.ThrowableTraceString(e)
        dos.close
        bos.close
        throw e
      }
    }
    null
  }

  def Deserialize(bytearray: Array[Byte], mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, isTopObject:Boolean, desClassName: String): MessageContainerBase = {
    var dis = new DataInputStream(new ByteArrayInputStream(bytearray));

    val typName = dis.readUTF
    val version = dis.readUTF
    val classname = dis.readUTF
    try {
      // Expecting type name
      // get class instance for this type
      val typ =
        if (isTopObject) {
          mdResolver.getMessgeOrContainerInstance(typName)
        } else {
          var curClz = Class.forName(desClassName, true, loader)
          curClz.newInstance().asInstanceOf[MessageContainerBase]
        }
      if (typ == null) {
        throw new Exception("Message/Container %s not found to deserialize".format(typName))
      }
      typ.Deserialize(dis, mdResolver, loader, version.toString)
      dis.close
      return typ
    } catch {
      case e: Exception => {
        // LOG.error("Failed to get classname :" + clsName)
        Utils.ThrowableTraceString(e)
        dis.close
        throw e
      }
    }
    null
  }
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

