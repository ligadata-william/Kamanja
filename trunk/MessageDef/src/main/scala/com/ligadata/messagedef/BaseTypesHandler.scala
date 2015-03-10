package com.ligadata.messagedef

import com.ligadata.olep.metadata._

object BaseTypesHandler {

  private val pad1 = "\t"
  private val pad2 = "\t\t"
  private val pad3 = "\t\t\t"
  private val pad4 = "\t\t\t\t"
  private val newline = "\n"

  def serializeMsgContainer(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element, dval: String, funcName: String): String = {
    serialize(typ, fixed, f, dval, funcName)
  }

  def deSerializeMsgContainer(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element): String = {
    deSerialize(typ, fixed, f)
  }

  def prevObjDeserializeMsgContainer(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element, childs: Map[String, Any]): (String, String) = {
    prevObjDeserialize(typ, fixed, f, childs)
  }

  private def serialize(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element, dval: String, funcName: String): String = {
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
          serializedBuf = serializedBuf.append("%s%s(%s,%s(getOrElse(\"%s\", %s).toString));%s".format(pad1, serType, "dos", funcName, f.Name,  dval, newline))
        }
      }
    } catch {
      case e: Exception => throw new Exception("Exception occured " + e.getCause())
    }

    serializedBuf.toString
  }

  private def deSerialize(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element): String = {
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
          deserializedBuf = deserializedBuf.append("%sset(\"%s\" , %s(%s));%s".format(pad1, f.Name, deserType, dis, newline))
        }
      }

    } catch {
      case e: Exception => throw new Exception("Exception occured " + e.getCause())
    }

    deserializedBuf.toString
  }

  private def prevObjDeserialize(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element, childs: Map[String, Any]): (String, String) = {
    var prevObjDeserializedBuf = new StringBuilder(8 * 1024)
    var convertOldObjtoNewObjBuf = new StringBuilder(8 * 1024)

    try {
      if (typ.getOrElse("None").equals("None"))
        throw new Exception("Type not found in metadata for Name: " + f.Name + " , NameSpace: " + f.NameSpace + " , Type : " + f.Ttype)
      if (f.Name == null || f.Name.trim() == "")
        throw new Exception("Field name do not exists")
      if (typ.get.FullName == null || typ.get.FullName.trim() == "")
        throw new Exception("Full name of Type " + f.Ttype + " do not exists in metadata ")

      var memberExists: Boolean = false

      var sameType: Boolean = false
      if (childs != null) {
        if (childs.contains(f.Name)) {
          var child = childs.getOrElse(f.Name, null)
          if (child != null) {
            val fullname = child.asInstanceOf[AttributeDef].aType.FullName
            if (fullname != null && fullname.trim() != "" && fullname.equals(typ.get.FullName)) {
              memberExists = true

            }
          }
        }
      }
      if (memberExists) {
        if (fixed.toLowerCase().equals("true")) {

          prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s%s = prevVerObj.%s;%s".format(pad1, f.Name, f.Name, newline))
          convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append("%s%s = oldObj.%s%s".format(pad2, f.Name, f.Name, newline))
        } else if (fixed.toLowerCase().equals("false")) {

          prevObjDeserializedBuf = prevObjDeserializedBuf.append("%sset(\"%s\", prevVerObj.getOrElse(\"%s\", null))%s".format(pad1, f.Name, f.Name, newline))
          convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append("%sset(\"%s\", oldObj.getOrElse(\"%s\", null))%s".format(pad2, f.Name, f.Name, newline))
        }
      }

    } catch {
      case e: Exception => throw new Exception("Exception occured " + e.getCause())
    }

    (prevObjDeserializedBuf.toString, convertOldObjtoNewObjBuf.toString)
  }

}