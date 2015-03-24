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

  def prevObjDeserializeMsgContainer(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element, childs: Map[String, Any]): (String, String, String, String) = {
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
        serializedBuf.append("%s if (v == null) com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,0)%s".format(pad3, newline))
        serializedBuf.append("%s else {  val o = v.asInstanceOf[%s]%s".format(pad3, typ.get.typeString, newline))
        serializedBuf.append("%s com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,o.size)%s".format(pad3, newline))
        serializedBuf.append("%s o.foreach(obj => { if(obj != null){ val bytes = SerializeDeserialize.Serialize(obj)%s".format(pad3, newline))
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
        deserializedBuf.append("%svar bytes = new Array[Byte](com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis))%s".format(pad2, newline))
        deserializedBuf.append("%sdis.read(bytes);%s".format(pad2, newline))
        deserializedBuf.append("%sval inst = SerializeDeserialize.Deserialize(bytes, mdResolver, loader, false, \"%s\");%s".format(pad2, childType, newline))
        deserializedBuf.append("%s%s += inst.asInstanceOf[%s;%s%s}%s%s%s}%s".format(pad2, f.Name, typ.get.typeString.toString().split("\\[")(1), newline, pad2, newline, newline, pad2, newline))

      } else if (fixed.toLowerCase().equals("false")) {

        deserializedBuf.append("%s{%s var %s = new %s %s".format(pad2, newline, f.Name, typ.get.typeString, newline))
        deserializedBuf.append("%svar arraySize = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis)%s".format(pad2, newline))
        // deserializedBuf.append("%sval %s = new %s %s".format(pad2, f.Name, typ.get.typeString.toString(), newline))
        deserializedBuf.append("%s val i:Int = 0;%s".format(pad2, newline))
        deserializedBuf.append("%s for (i <- 0 until arraySize) {%s".format(pad2, newline))
        deserializedBuf.append("%s  val byteslength = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis) %s".format(pad2, newline))
        deserializedBuf.append("%s var bytes = new Array[Byte](byteslength)%s".format(pad2, newline))
        deserializedBuf.append("%s dis.read(bytes);%s".format(pad2, newline))
        deserializedBuf.append("%s val inst = SerializeDeserialize.Deserialize(bytes, mdResolver, loader, false, \"%s\");%s".format(pad2, childType, newline))
        deserializedBuf.append("%s%s += inst.asInstanceOf[%s%s}%s".format(pad2, f.Name, typ.get.typeString.toString().split("\\[")(1), newline, pad2, newline))
        deserializedBuf.append("%s%s fields(\"%s\") = (-1, %s)}%s".format(newline, pad2, f.Name, f.Name, newline))

      }
    } catch {
      case e: Exception => throw new Exception("Exception occured " + e.getCause())
    }
    return deserializedBuf.toString
  }

  //Previous object Deserialize String and Convert Old object to new Object for array of messages or containers
  private def prevObjDeserialize(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], fixed: String, f: Element, childs: Map[String, Any]): (String, String, String, String) = {
    var prevObjDeserializedBuf = new StringBuilder(8 * 1024)
    var convertOldObjtoNewObjBuf = new StringBuilder(8 * 1024)
    var mappedPrevVerMatchkeys = new StringBuilder(8 * 1024)
    var mappedPrevTypNotrMatchkeys = new StringBuilder(8 * 1024)
    try {
      if (typ.getOrElse("None").equals("None"))
        throw new Exception("Type not found in metadata for Name: " + f.Name + " , NameSpace: " + f.NameSpace + " , Type : " + f.Ttype)

      if (f.Name == null || f.Name.trim() == "")
        throw new Exception("Field name do not exists ")

      val curObjtype = typ.get.typeString.toString()
      var curObjtypeStr: String = ""
      if (curObjtype != null && curObjtype.trim() != "") {
        curObjtypeStr = curObjtype.split("\\[")(1).substring(0, curObjtype.split("\\[")(1).length() - 1)
      }
      var memberExists: Boolean = false
      var membrMatchTypeNotMatch = false // for mapped messages to handle if prev ver obj and current version obj member types do not match...
      var childTypeImplName: String = ""

      var childName: String = ""
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
                childName = childPhysicalName.toString().split("\\[")(1).substring(0, childPhysicalName.toString().split("\\[")(1).length() - 1)
                if (childName.equals(curObjtypeStr))
                  sameType = true
              }
            } else {
              membrMatchTypeNotMatch = true
              childTypeImplName = child.asInstanceOf[AttributeDef].aType.implementationName
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
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%sval curVerObj = new %s()%s".format(pad2, curObjtypeStr, newline))
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%scurVerObj.ConvertPrevToNewVerObj(child)%s".format(pad2, newline))
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s%s += curVerObj})%s".format(pad2, f.Name, newline))
          }
        }
      } else if (fixed.toLowerCase().equals("false")) {
        if (memberExists) {
          mappedPrevVerMatchkeys.append("\"" + f.Name + "\",")

          // prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s%s{var obj =  prevVerObj.getOrElse(\"%s\", null)%s".format(newline, pad2, f.Name, newline))
          prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s case \"%s\" => { %s".format(pad2, f.Name, newline))

          if (sameType)
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%sif(prevObjfield._2._2 != null){ fields(\"%s\") = (-1, prevObjfield._2._2)}}%s".format(pad2, f.Name, newline))
          else {
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s type typ = scala.collection.mutable.ArrayBuffer[%s]%s".format(pad2, childName, newline))
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s var %s : %s = new %s;%s".format(pad2, f.Name, curObjtype, curObjtype, newline))

            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s if(prevObjfield._2._2 != null  && prevObjfield._2._2.isInstanceOf[typ]){%s%s prevObjfield._2._2.asInstanceOf[typ].foreach(child => {%s".format(pad2, newline, pad2, newline))
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s val curVerObj = new %s()%s".format(pad2, curObjtypeStr, newline))
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s curVerObj.ConvertPrevToNewVerObj(child)%s".format(pad2, newline))
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s %s += curVerObj})}%s".format(pad2, f.Name, newline))
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s fields(\"%s\") = (-1, %s)}%s".format(pad2, f.Name, f.Name, newline))

          }

        } else if (membrMatchTypeNotMatch) {
          mappedPrevTypNotrMatchkeys = mappedPrevTypNotrMatchkeys.append("\"" + f.Name + "\",")
        }
      }
    } catch {
      case e: Exception => throw new Exception("Exception occured " + e.getCause())
    }

    (prevObjDeserializedBuf.toString, convertOldObjtoNewObjBuf.toString, mappedPrevVerMatchkeys.toString, mappedPrevTypNotrMatchkeys.toString)
  }

  //Serialize, deserailize array of primitives  
  def getSerDeserPrimitives(typeString: String, typFullName: String, fieldName: String, implName: String, childs: Map[String, Any], isArayBuf: Boolean, recompile: Boolean, fixed: String): (String, String, String, String, String, String) = {
    var serializedBuf = new StringBuilder(8 * 1024)
    var deserializedBuf = new StringBuilder(8 * 1024)
    var prevObjDeserializedBuf = new StringBuilder(8 * 1024)
    var convertOldObjtoNewObjBuf = new StringBuilder(8 * 1024)
    var mappedPrevVerMatchkeys = new StringBuilder(8 * 1024)
    var mappedPrevVerTypNotMatchkeys = new StringBuilder(8 * 1024)
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
      deserializedBuf.append("%s if(arraySize > 0){ %s = new %s(arraySize)%s%s for (i <- 0 until arraySize) {%s".format(pad2, fieldName, typeString, newline, pad2, newline))
      deserializedBuf.append("%sval inst = %s(%s);%s".format(pad1, deserType, dis, newline))
      if (isArayBuf)
        deserializedBuf.append("%s%s :+= inst;%s%s}%s%s%s}%s".format(pad2, fieldName, newline, pad2, newline, newline, pad2, newline))
      else {
        deserializedBuf.append("%sif(inst != null)%s".format(pad2, newline))
        deserializedBuf.append("%s%s(i) = inst;%s%s}%s%s%s}}%s".format(pad2, fieldName, newline, pad2, newline, newline, pad2, newline))
      }

    } else if (fixed.toLowerCase().equals("false")) {

      serializedBuf.append("%s{val arr = getOrElse(\"%s\", null)%s".format(pad2, fieldName, newline))
      serializedBuf.append("%sif (arr == null ) com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, 0)%s".format(pad2, newline))
      serializedBuf.append("%s else { %s val a = arr.asInstanceOf[%s]%s".format(pad2, newline, typeString, newline))
      serializedBuf.append("%sif( a.size == 0) com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, 0);%s".format(pad2, newline))
      serializedBuf.append("%selse{ com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, a.size)%s".format(pad2, newline))
      serializedBuf.append("%s a.foreach(v => {%s %s(dos, %s.Input(v.toString))})}}}%s".format(pad2, newline, serType, implName, newline))

      deserializedBuf.append("%s{val arraySize = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis)%s".format(pad2, newline))
      deserializedBuf.append("%sval i:Int = 0%s".format(pad2, newline))
      deserializedBuf.append("%sif(arraySize > 0){ var arrValue = new %s(arraySize)%s".format(pad2, typeString, newline))
      deserializedBuf.append("%sfor (i <- 0 until arraySize) { val inst = %s(dis)%s".format(pad2, deserType, newline))
      deserializedBuf.append("%sarrValue(i) = inst }%s".format(pad2, newline))
      deserializedBuf.append("%s fields(\"%s\") = (-1, arrValue)}}%s".format(pad2, fieldName, newline))

    }
    var membrMatchTypeNotMatch = false // for mapped messages to handle if prev ver obj and current version obj member types do not match...
    var childTypeImplName: String = ""
    var memberExists: Boolean = false
    var sameType: Boolean = false
    if (childs != null) {
      if (childs.contains(fieldName)) {
        var child = childs.getOrElse(fieldName, null)
        if (child != null) {
          val fullname = child.asInstanceOf[AttributeDef].aType.FullName

          if (fullname != null && fullname.trim() != "" && fullname.equals(typFullName)) {
            memberExists = true
          } else {
            membrMatchTypeNotMatch = true
            childTypeImplName = child.asInstanceOf[AttributeDef].aType.implementationName
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

        mappedPrevVerMatchkeys.append("\"" + fieldName + "\",")
        //prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s{ val (prevFldIdx, prevFldVal) = prevVerObj.fields(\"%s\")%s".format(pad1, fieldName, newline))
        //prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s  fields(\"%s\") = (prevFldIdx, prevFldVal)%s } %s".format(pad1, fieldName, newline, newline))

        // prevObjDeserializedBuf = prevObjDeserializedBuf.append("%sfields(\"%s\") = (1, prevVerObj.getOrElse(\"%s\", null))%s".format(pad1, fieldName, fieldName, newline))
        //convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append("%s fields(\"%s\",) = (-1, oldObj.getOrElse(\"%s\", null))%s".format(pad2, fieldName, fieldName, newline))

      }
    }
    // particular for mapped messages...
    if (membrMatchTypeNotMatch) {
      if (fixed.toLowerCase().equals("false")) {
        mappedPrevVerTypNotMatchkeys.append("\"" + fieldName + "\",")
        // prevObjDeserializedBuf = prevObjDeserializedBuf.append("%sfields(\"%s\") = (1, prevVerObj.getOrElse(\"%s\", null))%s".format(pad1, fieldName, fieldName, newline))
        //convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append("%s fields(\"%s\",) = (-1, oldObj.getOrElse(\"%s\", null))%s".format(pad2, fieldName, fieldName, newline))

      }
    }

    (serializedBuf.toString, prevObjDeserializedBuf.toString, deserializedBuf.toString, convertOldObjtoNewObjBuf.toString, mappedPrevVerMatchkeys.toString, mappedPrevVerTypNotMatchkeys.toString)
  }

}