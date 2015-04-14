package com.ligadata.messagedef

import com.ligadata.olep.metadata._
import scala.collection._
import scala.collection.mutable.ArrayBuffer

object BaseTypesHandler {

  private val pad1 = "\t"
  private val pad2 = "\t\t"
  private val pad3 = "\t\t\t"
  private val pad4 = "\t\t\t\t"
  private val newline = "\n"

  def serializeMsgContainer(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element, mappedMsgBaseTypeIdx: Int): String = {
    serialize(typ, fixed, f, mappedMsgBaseTypeIdx)
  }

  def deSerializeMsgContainer(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element, mappedMsgBaseTypeIdx: Int): String = {
    deSerialize(typ, fixed, f, mappedMsgBaseTypeIdx)
  }

  def prevObjDeserializeMsgContainer(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element, childs: Map[String, Any], baseTypIdx: Int, prevVerMsgBaseTypesIdxArry: ArrayBuffer[String]): (String, String, String, String, String, ArrayBuffer[String]) = {
    prevObjDeserialize(typ, fixed, f, childs, baseTypIdx, prevVerMsgBaseTypesIdxArry)
  }

  private def serialize(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element, mappedMsgBaseTypeIdx: Int): String = {
    var serializedBuf = new StringBuilder(8 * 1024)
    try {
      if (typ.getOrElse("None").equals("None"))
        throw new Exception("Type not found in metadata for Name: " + f.Name + " , NameSpace: " + f.NameSpace + " , Type : " + f.Ttype)
      if (f.Name == null || f.Name.trim() == "")
        throw new Exception("Field name do not exists ")

      val serType = typ.get.implementationName + ".SerializeIntoDataOutputStream"
      if (serType != null && serType.trim() != "") {
        if (fixed.toLowerCase().equals("true")) {
          serializedBuf = serializedBuf.append("%s%s(%s,%s);%s".format(pad1, serType, "dos", f.Name, newline))
        } else if (fixed.toLowerCase().equals("false")) {
          if (mappedMsgBaseTypeIdx != -1)
            serializedBuf = serializedBuf.append("%s case %s => %s(dos, field._2._2.asInstanceOf[%s])  %s".format(pad1, mappedMsgBaseTypeIdx, serType, typ.get.physicalName, newline))
        }
      }
    } catch {
      case e: Exception => throw new Exception("Exception occured " + e.getCause())
    }

    serializedBuf.toString
  }

  private def deSerialize(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element, mappedMsgBaseTypeIdx: Int): String = {
    var deserializedBuf = new StringBuilder(8 * 1024)

    try {
      if (typ.getOrElse("None").equals("None"))
        throw new Exception("Type not found in metadata for Name: " + f.Name + " , NameSpace: " + f.NameSpace + " , Type : " + f.Ttype)
      if (f.Name == null || f.Name.trim() == "")
        throw new Exception("Field name do not exists")

      val implName = typ.get.implementationName
      if (implName == null || implName.trim() == "")
        throw new Exception("Implementation Name do not exist in Type")

      val deserType = implName + ".DeserializeFromDataInputStream"
      val dis = "dis"
      if (deserType != null && deserType.trim() != "") {
        if (fixed.toLowerCase().equals("true")) {
          deserializedBuf = deserializedBuf.append("%s%s = %s(%s);%s".format(pad1, f.Name, deserType, dis, newline))
        } else if (fixed.toLowerCase().equals("false")) {
          if (mappedMsgBaseTypeIdx != -1)
            deserializedBuf = deserializedBuf.append("%s case %s => fields(key) = (typIdx, %s(dis));%s".format(pad1, mappedMsgBaseTypeIdx, deserType, newline))

          //deserializedBuf = deserializedBuf.append("%sset(\"%s\" , %s(%s));%s".format(pad1, f.Name, deserType, dis, newline))
        }
      }

    } catch {
      case e: Exception => throw new Exception("Exception occured " + e.getCause())
    }

    deserializedBuf.toString
  }

  private def prevObjDeserialize(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element, childs: Map[String, Any], baseTypIdx: Int, prevVerMsgBaseTypesIdxArry: ArrayBuffer[String]): (String, String, String, String, String, ArrayBuffer[String]) = {
    var prevObjDeserializedBuf = new StringBuilder(8 * 1024)
    var convertOldObjtoNewObjBuf = new StringBuilder(8 * 1024)
    var mappedPrevVerMatchkeys = new StringBuilder(8 * 1024)
    var mappedPrevTypNotrMatchkeys = new StringBuilder(8 * 1024)
    var prevObjTypNotMatchDeserializedBuf = new StringBuilder(8 * 1024)

    try {
      if (typ.getOrElse("None").equals("None"))
        throw new Exception("Type not found in metadata for Name: " + f.Name + " , NameSpace: " + f.NameSpace + " , Type : " + f.Ttype)
      if (f.Name == null || f.Name.trim() == "")
        throw new Exception("Field name do not exists")
      if (typ.get.FullName == null || typ.get.FullName.trim() == "")
        throw new Exception("Full name of Type " + f.Ttype + " do not exists in metadata ")

      var memberExists: Boolean = false
      var membrMatchTypeNotMatch = false // for mapped messages to handle if prev ver obj and current version obj member types do not match...
      var childTypeImplName: String = ""
      var childtypeName: String = ""
      var childtypePhysicalName: String = ""

      var sameType: Boolean = false
      if (childs != null) {
        if (childs.contains(f.Name)) {
          var child = childs.getOrElse(f.Name, null)
          if (child != null) {
            val typefullname = child.asInstanceOf[AttributeDef].aType.FullName
            childtypeName = child.asInstanceOf[AttributeDef].aType.tTypeType.toString
            childtypePhysicalName = child.asInstanceOf[AttributeDef].aType.physicalName
            if (typefullname != null && typefullname.trim() != "" && typefullname.equals(typ.get.FullName)) {
              memberExists = true

            } else {
              membrMatchTypeNotMatch = true
              childTypeImplName = child.asInstanceOf[AttributeDef].aType.implementationName
            }
          }
        }
      }
      if (memberExists) {
        if (fixed.toLowerCase().equals("true")) {

          prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s%s = prevVerObj.%s;%s".format(pad1, f.Name, f.Name, newline))
          convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append("%s%s = oldObj.%s%s".format(pad2, f.Name, f.Name, newline))
        } else if (fixed.toLowerCase().equals("false")) {
          mappedPrevVerMatchkeys.append("\"" + f.Name + "\",")
          //if (baseTypIdx != -1)
          //prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s case %s => fields(key) = (typIdx, prevObjfield._2._2);)%s".format(pad2, baseTypIdx, newline))

          //prevObjDeserializedBuf = prevObjDeserializedBuf.append("%sset(\"%s\", prevVerObj.getOrElse(\"%s\", null))%s".format(pad1, f.Name, f.Name, newline))
          // convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append("%sset(\"%s\", oldObj.getOrElse(\"%s\", null))%s".format(pad2, f.Name, f.Name, newline))
        }
      }
      if (membrMatchTypeNotMatch) {
        if (fixed.toLowerCase().equals("false")) {

          if (childtypeName.toLowerCase().equals("tscalar")) {

            val implName = typ.get.implementationName + ".Input"

            //  => data = com.ligadata.BaseTypes.StringImpl.toString(prevObjfield._2._2.asInstanceOf[String];
            mappedPrevTypNotrMatchkeys = mappedPrevTypNotrMatchkeys.append("\"" + f.Name + "\",")
            if (!prevVerMsgBaseTypesIdxArry.contains(childTypeImplName)) {
              prevVerMsgBaseTypesIdxArry += childTypeImplName
              prevObjTypNotMatchDeserializedBuf = prevObjTypNotMatchDeserializedBuf.append("%s case \"%s\" => data = %s.toString(value.asInstanceOf[%s]); %s".format(pad2, childTypeImplName, childTypeImplName, childtypePhysicalName, newline))
            }
          }
        }

      }

    } catch {
      case e: Exception => throw new Exception("Exception occured " + e.getCause())
    }

    (prevObjDeserializedBuf.toString, convertOldObjtoNewObjBuf.toString, mappedPrevVerMatchkeys.toString, mappedPrevTypNotrMatchkeys.toString, prevObjTypNotMatchDeserializedBuf.toString, prevVerMsgBaseTypesIdxArry)
  }

}