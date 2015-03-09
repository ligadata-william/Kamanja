package com.ligadata.messagedef

import com.ligadata.olep.metadata._

object ArrayTypeHandler {

  private val pad1 = "\t"
  private val pad2 = "\t\t"
  private val pad3 = "\t\t\t"
  private val pad4 = "\t\t\t\t"
  private val newline = "\n"

  def serializeMsgContainer(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element): String = {
    serialize(typ, fixed, f)
  }

  def deSerializeMsgContainer(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element): String = {
    deSerialize(typ, fixed, f)
  }

  def prevObjDeserializeMsgContainer(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element, childs: Map[String, Any]): (String, String) = {
    prevObjDeserialize(typ, fixed, f, childs)
  }

  //serialize String for array of messages or containers
  private def serialize(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element): String = {
    var serializedBuf = new StringBuilder(8 * 1024)
    try {
      if (f.Name == null || f.Name.trim() == "")
        throw new Exception("Field name do not exists ")
      if (fixed.toLowerCase().equals("true")) {

        serializedBuf.append("%sif ((%s==null) ||(%s.size == 0)) com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,0);%s".format(pad2, f.Name, f.Name, newline))
        serializedBuf.append("%selse {%s%scom.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,%s.size);%s".format(pad2, newline, pad2, f.Name, newline))
        serializedBuf.append("%s%s.foreach(obj => {%s".format(pad2, f.Name, newline))
        serializedBuf.append("%sval bytes = SerializeDeserialize.Serialize(obj)%s".format(pad2, newline))
        serializedBuf.append("%scom.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,bytes.length)%s".format(pad2, newline))
        serializedBuf.append("%sdos.write(bytes)})}%s".format(pad2, newline))

      } else if (fixed.toLowerCase().equals("false")) {

        serializedBuf.append("%s{%s%sval v = getOrElse(\"%s\", null)%s".format(pad2, newline, pad3, f.Name, newline))
        serializedBuf.append("%sif (v == null) com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,0)%s".format(pad3, newline))
        serializedBuf.append("%selse {  val o = v.asInstanceOf[%s]%s".format(pad3, typ.get.typeString, newline))
        serializedBuf.append("%scom.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,o.size)%s".format(pad3, newline))
        serializedBuf.append("%so.foreach(obj => { if(obj != null){ val bytes = SerializeDeserialize.Serialize(obj)%s".format(pad3, newline))
        serializedBuf.append("%scom.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,bytes.length)%s".format(pad3, newline))
        serializedBuf.append("%sdos.write(bytes)}})}}%s".format(pad3, newline))
      }
    } catch {
      case e: Exception => throw new Exception("Exception occured " + e.getCause())
    }

    return serializedBuf.toString()
  }

  //Deserialize String for array of messages or containers
  private def deSerialize(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element): String = {
    var deserializedBuf = new StringBuilder(8 * 1024)

    try {
      if (typ.getOrElse("None").equals("None"))
        throw new Exception("Type not found in metadata for Name: " + f.Name + " , NameSpace: " + f.NameSpace + " , Type : " + f.Ttype)

      if (f.Name == null || f.Name.trim() == "")
        throw new Exception("Field name do not exists ")

      val typeStr = typ.get.typeString.toString()

      if (typeStr == null || typeStr.trim() == "")
        throw new Exception("Implementation Name do not exist in Type")

      val childType = typeStr.split("\\[")(1).substring(0, typeStr.split("\\[")(1).length() - 1)

      if (fixed.toLowerCase().equals("true")) {

        deserializedBuf.append("%s{%s%s%svar arraySize = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);%s".format(pad1, newline, pad1, pad2, newline))
        deserializedBuf.append("%sval i:Int = 0;%s".format(pad2, newline))
        deserializedBuf.append("%s for (i <- 0 until arraySize) {%s".format(pad2, newline))
        deserializedBuf.append("%svar bytes = new Array[Byte](dis.readInt)%s".format(pad2, newline))
        deserializedBuf.append("%sdis.read(bytes);%s".format(pad2, newline))
        deserializedBuf.append("%sval inst = SerializeDeserialize.Deserialize(bytes, mdResolver, loader, false, \"%s\");%s".format(pad2, childType, newline))
        deserializedBuf.append("%s%s += inst.asInstanceOf[%s;%s%s}%s%s%s}%s".format(pad2, f.Name, typ.get.typeString.toString().split("\\[")(1), newline, pad2, newline, newline, pad2, newline))

      } else if (fixed.toLowerCase().equals("false")) {

        deserializedBuf.append("%s{var arraySize = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis)%s".format(pad2, newline))
        // deserializedBuf.append("%sval %s = new %s %s".format(pad2, f.Name, typ.get.typeString.toString(), newline))
        deserializedBuf.append("%sval i:Int = 0;%s".format(pad2, newline))
        deserializedBuf.append("%s for (i <- 0 until arraySize) {%s".format(pad2, newline))
        deserializedBuf.append("%svar bytes = new Array[Byte](dis.readInt)%s".format(pad2, newline))
        deserializedBuf.append("%sdis.read(bytes);%s".format(pad2, newline))
        deserializedBuf.append("%sval inst = SerializeDeserialize.Deserialize(bytes, mdResolver, loader, false, \"%s\");%s".format(pad2, childType, newline))
        deserializedBuf.append("%s%s += inst.asInstanceOf[%s%s}%s".format(pad2, f.Name, typ.get.typeString.toString().split("\\[")(1), newline, pad2, newline))
        deserializedBuf.append("%s%s set(\"%s\", %s)}%s".format(newline, pad2, f.Name, f.Name, newline))

      }
    } catch {
      case e: Exception => throw new Exception("Exception occured " + e.getCause())
    }
    return deserializedBuf.toString
  }

  //Previous object Deserialize String and Convert Old object to new Object for array of messages or containers
  private def prevObjDeserialize(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element, childs: Map[String, Any]): (String, String) = {
    var prevObjDeserializedBuf = new StringBuilder(8 * 1024)
    var convertOldObjtoNewObjBuf = new StringBuilder(8 * 1024)
    try {
      if (typ.getOrElse("None").equals("None"))
        throw new Exception("Type not found in metadata for Name: " + f.Name + " , NameSpace: " + f.NameSpace + " , Type : " + f.Ttype)

      if (f.Name == null || f.Name.trim() == "")
        throw new Exception("Field name do not exists ")

      val childtype = typ.get.typeString.toString()
      var childTypeStr: String = ""
      if (childtype != null && childtype.trim() != "") {
        childTypeStr = childtype.split("\\[")(1).substring(0, childtype.split("\\[")(1).length() - 1)
      }
      var memberExists: Boolean = false
      var sameType: Boolean = false
      if (childs != null) {
        if (childs.contains(f.Name)) {
          var child = childs.getOrElse(f.Name, null)
          if (child != null) {
            val fullname = child.asInstanceOf[AttributeDef].aType.FullName
            if (fullname != null && fullname.trim() != "" && fullname.equals(typ.get.FullName)) {
              memberExists = true
              val childPhysicalName = child.asInstanceOf[AttributeDef].aType.typeString
              if (childPhysicalName != null && childPhysicalName.trim() != "") {
                val childName = childPhysicalName.toString().split("\\[")(1).substring(0, childPhysicalName.toString().split("\\[")(1).length() - 1)
                if (childName.equals(childTypeStr))
                  sameType = true
              }
            }
          }
        }
      }
      if (fixed.toLowerCase().equals("true")) {
        if (memberExists) {
          prevObjDeserializedBuf = prevObjDeserializedBuf.append("%sprevVerObj.%s.foreach(child => {%s".format(pad2, f.Name, newline))
          if (sameType)
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s%s += child})%s".format(pad2, f.Name, newline))
          else {
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%sval curVerObj = new %s()%s".format(pad2, childTypeStr, newline))
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%scurVerObj.ConvertPrevToNewVerObj(child)%s".format(pad2, newline))
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s%s += curVerObj})%s".format(pad2, f.Name, newline))
          }
        }
      } else if (fixed.toLowerCase().equals("false")) {
        if (memberExists) {
          prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s%s{var obj =  prevVerObj.getOrElse(\"%s\", null)%s".format(newline, pad2, f.Name, newline))
          if (sameType)
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%sif(obj != null){set(\"%s\", obj)}}%s".format(pad2, f.Name, newline))
          else {
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%sif(obj != null){obj.foreach(child => {%s".format(pad2, newline))
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%sval curVerObj = new %s()%s".format(pad2, childTypeStr, newline))
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%scurVerObj.ConvertPrevToNewVerObj(child)%s".format(pad2, newline))
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s%s += curVerObj})}%s".format(pad2, f.Name, newline))
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%sset(\"%s\", %s)}%s".format(pad2, f.Name, f.Name, newline))
          }
        }
      }
    } catch {
      case e: Exception => throw new Exception("Exception occured " + e.getCause())
    }

    (prevObjDeserializedBuf.toString, convertOldObjtoNewObjBuf.toString)
  }

  //Serialize, deserailize array of primitives  
  def getSerDeserPrimitives(typeString: String, typFullName: String, fieldName: String, implName: String, childs: Map[String, Any], isArayBuf: Boolean, recompile: Boolean, fixed: String): (String, String, String, String) = {
    var serializedBuf = new StringBuilder(8 * 1024)
    var deserializedBuf = new StringBuilder(8 * 1024)
    var prevObjDeserializedBuf = new StringBuilder(8 * 1024)
    var convertOldObjtoNewObjBuf = new StringBuilder(8 * 1024)
    val pad1 = "\t"
    val pad2 = "\t\t"
    val pad3 = "\t\t\t"
    val pad4 = "\t\t\t\t"
    val newline = "\n"

    if (implName == null || implName.trim() == "")
      throw new Exception("Check Type Implementation Name")

    val serType = implName + ".SerializeIntoDataOutputStream"
    val deserType = implName + ".DeserializeFromDataInputStream"
    val dis = "dis"

    if (fixed.toLowerCase().equals("true")) {
      serializedBuf.append("%sif (%s==null || %s.size == 0) com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,0);%s".format(pad2, fieldName, fieldName, newline))
      serializedBuf.append("%selse {%s%scom.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,%s.size);%s".format(pad2, newline, pad2, fieldName, newline))
      serializedBuf.append("%s%s.foreach(v => {%s".format(pad2, fieldName, newline))
      serializedBuf.append("%s%s(%s, v);%s".format(pad1, serType, "dos", newline))
      serializedBuf.append("%s})}%s".format(pad2, newline))

      deserializedBuf.append("%s{%s%s%svar arraySize = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);%s".format(pad1, newline, pad1, pad2, newline))
      deserializedBuf.append("%sval i:Int = 0;%s".format(pad2, newline))
      deserializedBuf.append("%s if(arraySize > 0){ %s = new Array[%s](arraySize)%s%s for (i <- 0 until arraySize) {%s".format(pad2, fieldName, typeString, newline, pad2, newline))
      deserializedBuf.append("%sval inst = %s(%s);%s".format(pad1, deserType, dis, newline))
      if (isArayBuf)
        deserializedBuf.append("%s%s :+= inst;%s%s}%s%s%s}%s".format(pad2, fieldName, newline, pad2, newline, newline, pad2, newline))
      else {
        // deserializedBuf.append("%sif(inst != null)%s".format(pad2, newline))
        deserializedBuf.append("%s%s(i) = inst;%s%s}%s%s%s}}%s".format(pad2, fieldName, newline, pad2, newline, newline, pad2, newline))
      }

    } else if (fixed.toLowerCase().equals("false")) {

      serializedBuf.append("%s{val arr = getOrElse(\"%s\", null).asInstanceOf[List[String]]%s".format(pad2, fieldName, newline))
      serializedBuf.append("%sif (arr == null || arr.size == 0) com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, 0)%s".format(pad2, newline))
      serializedBuf.append("%selse { com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, arr.size)%s".format(pad2, newline))
      serializedBuf.append("%sarr.foreach(v => {com.ligadata.BaseTypes.StringImpl.SerializeIntoDataOutputStream(dos, v)})}}%s".format(pad2, newline))

      deserializedBuf.append("%s{var arraySize = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis)%s".format(pad2, newline))
      deserializedBuf.append("%sval i:Int = 0%s".format(pad2, newline))
      deserializedBuf.append("%sif(arraySize > 0){ var arrValue = new Array[String](arraySize)%s".format(pad2, newline))
      deserializedBuf.append("%sfor (i <- 0 until arraySize) { val inst = com.ligadata.BaseTypes.StringImpl.DeserializeFromDataInputStream(dis)%s".format(pad2, newline))
      deserializedBuf.append("%sarrValue(i) = inst }%s".format(pad2, newline))
      deserializedBuf.append("%sset(\"%s\", arrValue)}}%s".format(pad2, fieldName, newline))

    }
    var memberExists: Boolean = false
    var sameType: Boolean = false
    if (childs != null) {
      if (childs.contains(fieldName)) {
        var child = childs.getOrElse(fieldName, null)
        if (child != null) {
          val fullname = child.asInstanceOf[AttributeDef].aType.FullName
          if (fullname != null && fullname.trim() != "" && fullname.equals(typFullName)) {
            memberExists = true

          }
        }
      }
    }
    //prevVerObj.icd9_dgns_cds.foreach(v => icd9_dgns_cds :+= v);)
    // prevVerObj.inpaticonvertOldObjtoNewObjBufent_claims.foreach(ip => inpatient_claims += ip)
    //typ.get.typeString.toString().split("\\[")(1)
    if (memberExists) {

      if (fixed.toLowerCase().equals("true")) {
        //prevObjDeserializedBuf = prevObjDeserializedBuf.append("%sprevVerObj.%s.foreach(v => %s :+= v);%s".format(pad1, fieldName, fieldName, newline))
        prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s%s = prevVerObj.%s;%s".format(pad1, fieldName, fieldName, newline))
        convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append("%s%s = oldObj.%s%s".format(pad2, fieldName, fieldName, newline))
      } else if (fixed.toLowerCase().equals("false")) {

        prevObjDeserializedBuf = prevObjDeserializedBuf.append("%sset(\"%s\", prevVerObj.getOrElse(\"%s\", null))%s".format(pad1, fieldName, fieldName, newline))
        convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append("%sset(\"%s\", oldObj.getOrElse(\"%s\", null))%s".format(pad2, fieldName, fieldName, newline))

      }
    }

    (serializedBuf.toString, prevObjDeserializedBuf.toString, deserializedBuf.toString, convertOldObjtoNewObjBuf.toString)
  }

}