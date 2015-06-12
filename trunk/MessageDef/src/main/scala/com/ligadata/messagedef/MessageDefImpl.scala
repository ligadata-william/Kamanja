package com.ligadata.messagedef

import scala.util.parsing.json.JSON
import scala.reflect.runtime.universe
import scala.io.Source
import java.io.File
import java.io.PrintWriter
import java.util.Date
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.Formats
import org.apache.log4j.Logger
import com.ligadata.fatafat.metadata.MdMgr
import com.ligadata.fatafat.metadata.EntityType
import com.ligadata.fatafat.metadata.MessageDef
import com.ligadata.fatafat.metadata.BaseAttributeDef
import com.ligadata.fatafat.metadata.ContainerDef
import com.ligadata.fatafat.metadata.ArrayTypeDef
import com.ligadata.fatafat.metadata.ArrayBufTypeDef
import com.ligadata.fatafat.metadata._
import com.ligadata.Exceptions._

trait Attrib {
  var NameSpace: String
  var Name: String
  var Type: String
}

class Message(var msgtype: String, var NameSpace: String, var Name: String, var PhysicalName: String, var Version: String, var Description: String, var Fixed: String, var Persist: Boolean, var Elements: List[Element], var TDataExists: Boolean, var TrfrmData: TransformData, var jarset: Set[String], var pkg: String, var concepts: List[String], var Ctype: String, var CCollectiontype: String, var Containers: List[String], var PartitionKey: List[String], var PrimaryKeys: List[String], var ClsNbr: Long)
class TransformData(var input: Array[String], var output: Array[String], var keys: Array[String])
class Field(var NameSpace: String, var Name: String, var Ttype: String, var CollectionType: String, var Fieldtype: String, var FieldtypeVer: String)
class Element(var NameSpace: String, var Name: String, var Ttype: String, var CollectionType: String, var ElemType: String, var FieldtypeVer: String)

case class Concept(var NameSpace: Option[String], var Name: Option[String], var Type: Option[String])
class ConceptList(var Concepts: List[Concept])
case class DictionaryMessage(var NameSpace: Option[String], var Name: Option[String], var PhysicalName: Option[String], var Version: Option[String], var Description: Option[String], var Fixed: Option[String], var concepts: Option[List[String]])

// The class caster that can throw exceptions... 
class ClassCaster[T] {
  def unapply(a: Any): Option[T] = Some(a.asInstanceOf[T])
}

// concrete instances of the casters for pattern matching
object AsMap extends ClassCaster[Map[String, Any]]
object AsList extends ClassCaster[List[Any]]
object AsString extends ClassCaster[String]

class MessageDefImpl {

  val logger = this.getClass.getName
  lazy val log = Logger.getLogger(logger)
  val pkg: String = "com.ligadata.messagescontainers"
  var rddHandler = new RDDHandler

  private def error[T](prefix: String): Option[T] =
    throw MessageException("%s must be specified".format(prefix))

  //creates the class string

  private def createObj(msg: Message, partitionPos: Array[Int], primaryPos: Array[Int], clsname: String): (StringBuilder) = {
    var cobj: StringBuilder = new StringBuilder
    var tdataexists: String = ""
    var tattribs: String = ""
    val newline = "\n"
    val cbrace = "}"
    val isFixed = getIsFixed(msg)
    val canPersist = getCanPersist(msg)
    var primaryKeyDef: String = ""
    var partitionKeyDef: String = ""

    val pratitionKeys = partitionkeyStrObj(msg, partitionPos)
    val primaryKeys = primarykeyStrObj(msg, primaryPos)

    /*if (pratitionKeys != null && pratitionKeys.trim() != "") {
      partitionKeyDef = getPartitionKeyDef
    }
    if (primaryKeys != null && primaryKeys.trim() != "") {
      primaryKeyDef = getPrimaryKeyDef
    }
*/
    if (msg.msgtype.equals("Message")) {
      if (msg.TDataExists) {
        tdataexists = gettdataexists + msg.TDataExists.toString
        tattribs = tdataattribs(msg)
      } else {
        tdataexists = gettdataexists + msg.TDataExists.toString
        tattribs = notdataattribs
      }
      // cobj.append(tattribs + newline + tdataexists + newline + getMessageName(msg) + newline + getName(msg) + newline + getVersion(msg) + newline + createNewMessage(msg) + newline + isFixed + cbrace + newline)

      //cobj.append(tattribs + newline + tdataexists + newline + getName(msg) + newline + getVersion(msg) + newline + createNewMessage(msg) + newline + isFixed + pratitionKeys + primaryKeys + newline + primaryKeyDef + partitionKeyDef + cbrace + newline)
      cobj.append(tattribs + newline + tdataexists + newline + getName(msg) + newline + getVersion(msg) + newline + createNewMessage(msg, clsname) + newline + isFixed + canPersist + rddHandler.HandleRDD(msg.Name) + newline + pratitionKeys + primaryKeys + newline + cbrace + newline)

    } else if (msg.msgtype.equals("Container")) {
      // cobj.append(getMessageName(msg) + newline + getName(msg) + newline + getVersion(msg) + newline + createNewContainer(msg) + newline + isFixed + cbrace + newline)

      //  cobj.append(getName(msg) + newline + getVersion(msg) + newline + createNewContainer(msg) + newline + isFixed + pratitionKeys + primaryKeys + newline + primaryKeyDef + partitionKeyDef + cbrace + newline)
      cobj.append(getName(msg) + newline + getVersion(msg) + newline + createNewContainer(msg, clsname) + newline + isFixed + canPersist + rddHandler.HandleRDD(msg.Name) + newline + pratitionKeys + primaryKeys + newline + cbrace + newline)

    }
    cobj
  }

  private def partitionkeyStrObj(message: Message, partitionPos: Array[Int]): String = {
    val pad1 = "\t"
    var partitionStr = new StringBuilder
    partitionStr.append("Array(")
    for (p <- partitionPos) {
      partitionStr.append(p + ",")
    }
    var partitionString = partitionStr.substring(0, partitionStr.length - 1)
    partitionString = partitionString + ")"

    val partitionKeys = if (message.PartitionKey != null && message.PartitionKey.size > 0) ("Array(\"" + message.PartitionKey.map(p => p.toLowerCase).mkString("\", \"") + "\")") else ""

    if (partitionKeys != null && partitionKeys.trim() != "")
      "\n	val partitionKeys : Array[String] = " + partitionKeys + "\n    val partKeyPos = " + partitionString.toString + getPartitionKeyDef
    else
      "\n   override def PartitionKeyData(inputdata:InputData): Array[String] = Array[String]()"
  }

  private def primarykeyStrObj(message: Message, primaryPos: Array[Int]): String = {
    val prmryKeys = if (message.PrimaryKeys != null && message.PrimaryKeys.size > 0) ("Array(\"" + message.PrimaryKeys.map(p => p.toLowerCase).mkString("\", \"") + "\")") else ""
    val pad1 = "\t"
    var primaryStr = new StringBuilder
    log.debug("primaryPos " + primaryPos.length)
    primaryStr.append("Array(")
    for (p <- primaryPos) {
      primaryStr.append(p + ",")
    }
    var primaryString = primaryStr.substring(0, primaryStr.length - 1)
    primaryString = primaryString + ")"

    if (prmryKeys != null && prmryKeys.trim() != "")
      "\n	val primaryKeys : Array[String] = " + prmryKeys + "\n    val prmryKeyPos = " + primaryString + getPrimaryKeyDef
    else
      "\n    override def PrimaryKeyData(inputdata:InputData): Array[String] = Array[String]()"
  }

  private def getName(msg: Message) = {
    "\toverride def FullName: String = " + "\"" + msg.NameSpace + "." + msg.Name + "\"" + "\n" +
      "\toverride def NameSpace: String = " + "\"" + msg.NameSpace + "\"" + "\n" +
      "\toverride def Name: String = " + "\"" + msg.Name + "\""
  }

  private def getMessageName(msg: Message) = {
    if (msg.msgtype.equals("Message"))
      "\toverride def getMessageName: String = " + "\"" + msg.NameSpace + "." + msg.Name + "\""
    else if (msg.msgtype.equals("Container"))
      "\toverride def getContainerName: String = " + "\"" + msg.NameSpace + "." + msg.Name + "\""

  }

  private def getVersion(msg: Message) = {
    "\toverride def Version: String = " + "\"" + msg.Version + "\""

  }
  private def createNewMessage(msg: Message, classname: String) = {
    "\toverride def CreateNewMessage: BaseMsg  = new " + classname + "()"
  }

  private def createNewContainer(msg: Message, classname: String) = {
    "\toverride def CreateNewContainer: BaseContainer  = new " + classname + "()"
  }

  private def gettdataexists = {
    "\toverride def NeedToTransformData: Boolean = "
  }
  private def notdataattribs = {
    "\toverride def TransformDataAttributes: TransformMessage = null"
  }

  private def tdataattribs(msg: Message) = {
    """
	def TransformDataAttributes: TransformMessage = {
	    val rearr = new TransformMessage()
	    rearr.messageType = """ + "\"" + msg.Name + "\"" +
      """
	    rearr.inputFields = """ + gettdataattribs(msg.TrfrmData.input) +
      """
	    rearr.outputFields = """ + gettdataattribs(msg.TrfrmData.output) +
      """
	    rearr.outputKeys = """ + gettdataattribs(msg.TrfrmData.keys) +
      """
	    rearr
	}
    """
  }

  private def gettdataattribs(a: Array[String]): String = {
    var str: String = "Array ("
    var i: Int = 0
    for (s <- a) {
      if (i == a.size - 1) {
        str = str + "\"" + s + "\" "
      } else
        str = str + "\"" + s + "\", "
      i = i + 1
    }
    str + ")"
  }

  private def getBaseTrait(message: Message): (String, String, String) = {
    var btrait: String = ""
    var strait: String = ""
    var csetters: String = ""
    if (message.msgtype.equals("Message")) {
      btrait = "BaseMsgObj"
      strait = "BaseMsg"
      csetters = ""
    } else if (message.msgtype.equals("Container")) {
      btrait = "BaseContainerObj"
      strait = "BaseContainer"
      csetters = cSetter
    }
    (btrait, strait, csetters)
  }

  private def addMessage(addMsg: String, msg: Message): String = {
    var addMessageFunc: String = ""
    var msgType: String = ""

    try {

      if ((addMsg != null) && (addMsg.trim() != "")) {
        addMessageFunc = """
    override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = {
	    if (childPath == null || childPath.size == 0) { // Invalid case
		  return
	    }
	   val curVal = childPath(0)
	   if (childPath.size == 1) { // If this is last level
	 """ + addMsg + """
	  	} else { // Yet to handle it. Make sure we add the message to one of the value given
	  		throw new Exception("Not yet handled messages more than one level")
	 	}
	}
	 
     """
      } else {

        addMessageFunc = """
    override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = { }
     
     """
      }

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    addMessageFunc
  }

  private def getMessage(getMsg: String): String = {
    var getMessageFunc: String = ""

    if (getMsg != null && getMsg.trim() != "") {

      getMessageFunc = """
    override def GetMessage(childPath: Array[(String, String)], primaryKey:Array[String]): com.ligadata.FatafatBase.BaseMsg = {
	    if (childPath == null || childPath.size == 0 || primaryKey == null || primaryKey.size == 0) { // Invalid case
    		return null
	    }
	   val curVal = childPath(0)
       if (childPath.size == 1) { // If this is last level
	 """ + getMsg + """
	  	} else { // Yet to handle it. Make sure we add the message to one of the value given
	  		throw new Exception("Not yet handled messages more than one level")
	 	}
	 	return null
	}
	 
     """
    } else {

      getMessageFunc = """
    override def GetMessage(childPath: Array[(String, String)], primaryKey:Array[String]): com.ligadata.FatafatBase.BaseMsg = {
       return null
    } 
     
     """
    }
    getMessageFunc
  }
  private def assignJsonForArray(fname: String, typeImpl: String, msg: Message, typ: String): String = {
    var funcStr: String = ""

    val funcName = "fields(\"" + fname + "\")";
    if (msg.Fixed.toLowerCase().equals("true")) {
      funcStr = """
			if (map.contains("""" + fname + """")){
				val arr = map.getOrElse("""" + fname + """", null)
			if (arr != null) {
				val arrFld = CollectionAsArrString(arr)
				""" + fname + """  = arrFld.map(v => """ + typeImpl + """(v.toString)).toArray
			} else """ + fname + """  = new """ + typ + """(0)
	    }
	      """
    } else if (msg.Fixed.toLowerCase().equals("false")) {
      funcStr = """
			if (map.contains("""" + fname + """" )){
				val arr = map.getOrElse("""" + fname + """", null)
			if (arr != null) {
				val arrFld = CollectionAsArrString(arr)
				""" + funcName + """   = (-1, arrFld.map(v =>  {""" + typeImpl + """(v.toString) } ).toArray)
			}else 
				""" + funcName + """   = (-1, new """ + typ + """(0))
	    }
	      """
    }
    funcStr
  }

  private def assignJsonForPrimArrayBuffer(fname: String, typeImpl: String, msg: Message, typ: String): String = {
    var funcStr: String = ""

    val funcName = "fields(\"" + fname + "\")";
    if (msg.Fixed.toLowerCase().equals("true")) {
      funcStr = """
			if (map.contains("""" + fname + """")){
				val arr = map.getOrElse("""" + fname + """", null)
			if (arr != null) {
				val arrFld = CollectionAsArrString(arr)
				 arrFld.map(v => {""" + fname + """  :+=""" + typeImpl + """(v.toString)})
			}else """ + fname + """  = new """ + typ + """(0)
	    }
	      """
    } else if (msg.Fixed.toLowerCase().equals("false")) {
      funcStr = """
			if (map.contains("""" + fname + """" )){
				val arr = map.getOrElse("""" + fname + """", null)
			if (arr != null) {
                var """ + fname + """  = new """ + typ + """
				val arrFld = CollectionAsArrString(arr)
				""" + funcName + """   = (-1, arrFld.map(v => {""" + fname + """  :+=""" + typeImpl + """(v.toString)}))
			}else 
				""" + funcName + """   = (-1, new """ + typ + """(0))
	    }
	      """
    }
    funcStr
  }

  private def assignJsonForCntrArrayBuffer(fname: String, typeImpl: String) = {
    """
	 
		if (map.getOrElse("""" + fname + """", null).isInstanceOf[List[tMap]])
	    	list = map.getOrElse("""" + fname + """", null).asInstanceOf[List[Map[String, Any]]]
        if (list != null) {
        	""" + fname + """++= list.map(item => {
        	val inputData = new JsonData(json.dataInput)
        	inputData.root_json = json.root_json
        	inputData.cur_json = Option(item)
        	val elem = new """ + typeImpl + """()
        	elem.populate(inputData)
        	elem
        	})
	    }
	    """
  }
  private def assignJsonDataMessage(mName: String) = {
    """   
        val inputData = new JsonData(json.dataInput)
        inputData.root_json = json.root_json
        inputData.cur_json = Option(map.getOrElse("""" + mName + """", null))
	    """ + mName + """.populate(inputData)
	    """
  }

  private def getSerializedType(valType: String): String = {
    valType match {
      case "int" => "writeInt"
      case "string" => "writeUTF"
      case "long" => "writeLong"
      case "double" => "writeDouble"
      case "boolean" => "writeBoolean"
      case "char" => "writeChar"
      case "float" => "writeFloat"
      case _ => ""
    }
  }

  private def getDeserializedType(valType: String): String = {
    valType match {
      case "int" => "readInt"
      case "string" => "readUTF"
      case "long" => "readLong"
      case "double" => "readDouble"
      case "boolean" => "readBoolean"
      case "char" => "readChar"
      case "float" => "readFloat"
      case _ => ""
    }
  }

  private def handleBaseTypes(keysSet: Set[String], fixed: String, typ: Option[com.ligadata.fatafat.metadata.BaseTypeDef], f: Element, msgVersion: String, childs: Map[String, Any], prevVerMsgBaseTypesIdxArry: ArrayBuffer[String], recompile: Boolean, mappedTypesABuf: ArrayBuffer[String], firstTimeBaseType: Boolean): (String, String, String, String, List[(String, String)], List[(String, String, String, String, Boolean, String)], String, String, String, Set[String], String, String, String, String, ArrayBuffer[String], String, String, String, ArrayBuffer[String], String) = {
    var scalaclass = new StringBuilder(8 * 1024)
    var assignCsvdata = new StringBuilder(8 * 1024)
    var assignJsondata = new StringBuilder(8 * 1024)
    var assignXmldata = new StringBuilder(8 * 1024)
    var addMsg = new StringBuilder(8 * 1024)
    var list = List[(String, String)]()
    var argsList = List[(String, String, String, String, Boolean, String)]()
    var jarset: Set[String] = Set();
    val pad1 = "\t"
    val pad2 = "\t\t"
    val pad3 = "\t\t\t"
    val pad4 = "\t\t\t\t"
    val newline = "\n"
    var keysStr = new StringBuilder(8 * 1024)
    var mappedPrevVerMatchkeys = new StringBuilder(8 * 1024)
    var typeImpl = new StringBuilder(8 * 1024)
    var fname: String = ""
    var serializedBuf = new StringBuilder(8 * 1024)
    var deserializedBuf = new StringBuilder(8 * 1024)
    var prevObjDeserializedBuf = new StringBuilder(8 * 1024)
    var convertOldObjtoNewObjBuf = new StringBuilder(8 * 1024)
    var mappedPrevTypNotMatchkeys = new StringBuilder(8 * 1024)
    var prevObjTypNotMatchDeserializedBuf = new StringBuilder(8 * 1024)
    var prevVerMsgBaseTypesIdxArry1: ArrayBuffer[String] = new ArrayBuffer[String]

    var fixedMsgGetKeyStrBuf = new StringBuilder(8 * 1024)

    var mapBaseTypesSetRet: Set[Int] = Set()
    try {

      if (typ.get.implementationName.isEmpty())
        throw new Exception("Implementation Name not found in metadata for namespace %s" + f.Ttype)

      if (typ.getOrElse("None").equals("None"))
        throw new Exception("Type not found in metadata for Name: " + f.Name + " , NameSpace: " + f.NameSpace + " , Version: " + msgVersion + " , Type : " + f.Ttype)

      if (typ.get.typeString.isEmpty())
        throw new Exception("Type not found in metadata for namespace %s" + f.Ttype)

      argsList = (f.NameSpace, f.Name, typ.get.NameSpace, typ.get.Name, false, null) :: argsList

      fname = typ.get.implementationName + ".Input"

      if ((typ.get.dependencyJarNames != null) && (typ.get.JarName != null))
        jarset = jarset + typ.get.JarName ++ typ.get.dependencyJarNames
      else if (typ.get.JarName != null)
        jarset = jarset + typ.get.JarName
      else if (typ.get.dependencyJarNames != null)
        jarset = jarset ++ typ.get.dependencyJarNames

      val dval: String = getDefVal(typ.get.typeString.toLowerCase())
      list = (f.Name, f.Ttype) :: list

      var baseTypId = -1

      if (fixed.toLowerCase().equals("true")) {
        scalaclass = scalaclass.append("%svar %s:%s = _ ;%s".format(pad1, f.Name, typ.get.physicalName, newline))
        assignCsvdata.append("%s%s = %s(list(inputdata.curPos));\n%sinputdata.curPos = inputdata.curPos+1\n".format(pad2, f.Name, fname, pad2))
        assignJsondata.append("%s %s = %s(map.getOrElse(\"%s\", %s).toString)%s".format(pad2, f.Name, fname, f.Name, dval, newline))
        assignXmldata.append("%sval _%sval_  = (xml \\\\ \"%s\").text.toString %s%sif (_%sval_  != \"\")%s%s =  %s( _%sval_ ) else %s = %s%s".format(pad3, f.Name, f.Name, newline, pad3, f.Name, pad2, f.Name, fname, f.Name, f.Name, dval, newline))

      } else if (fixed.toLowerCase().equals("false")) {

        if (keysSet != null)
          keysSet.foreach { key =>
            if (f.Name.toLowerCase().equals(key.toLowerCase()))
              scalaclass = scalaclass.append("%svar %s:%s = _ ;%s".format(pad1, f.Name, typ.get.physicalName, newline))
          }
        var typstring = typ.get.implementationName
        if (mappedTypesABuf.contains(typstring)) {
          if (mappedTypesABuf.size == 1 && firstTimeBaseType)
            baseTypId = mappedTypesABuf.indexOf(typstring)
        } else {

          mappedTypesABuf += typstring
          baseTypId = mappedTypesABuf.indexOf(typstring)
        }

        keysStr.append("(\"" + f.Name + "\", " + mappedTypesABuf.indexOf(typstring) + "),")

      }
      serializedBuf = serializedBuf.append(BaseTypesHandler.serializeMsgContainer(typ, fixed, f, baseTypId))
      deserializedBuf = deserializedBuf.append(BaseTypesHandler.deSerializeMsgContainer(typ, fixed, f, baseTypId))
      val (prevObjDeserialized, convertOldObjtoNewObj, mappedPrevVerMatch, mappedPrevTypNotMatchkey, prevObjTypNotMatchDeserialized, prevVerMsgBaseTypesIdxArryBuf) = BaseTypesHandler.prevObjDeserializeMsgContainer(typ, fixed, f, childs, baseTypId, prevVerMsgBaseTypesIdxArry)
      prevObjDeserializedBuf = prevObjDeserializedBuf.append(prevObjDeserialized)
      convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append(convertOldObjtoNewObj)
      mappedPrevVerMatchkeys = mappedPrevVerMatchkeys.append(mappedPrevVerMatch)
      mappedPrevTypNotMatchkeys = mappedPrevTypNotMatchkeys.append(mappedPrevTypNotMatchkey)
      prevObjTypNotMatchDeserializedBuf = prevObjTypNotMatchDeserializedBuf.append(prevObjTypNotMatchDeserialized)
      prevVerMsgBaseTypesIdxArry1 = prevVerMsgBaseTypesIdxArryBuf
      fixedMsgGetKeyStrBuf.append("%s if(key.equals(\"%s\")) return %s; %s".format(pad1, f.Name, f.Name, newline))

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }

    (scalaclass.toString, assignCsvdata.toString, assignJsondata.toString, assignXmldata.toString, list, argsList, addMsg.toString, keysStr.toString, typeImpl.toString, jarset, serializedBuf.toString, deserializedBuf.toString, prevObjDeserializedBuf.toString, convertOldObjtoNewObjBuf.toString, mappedTypesABuf, mappedPrevVerMatchkeys.toString, mappedPrevTypNotMatchkeys.toString, prevObjTypNotMatchDeserializedBuf.toString, prevVerMsgBaseTypesIdxArry1, fixedMsgGetKeyStrBuf.toString)
  }

  ///serialize deserialize primative types

  private def handleArrayType(keysSet: Set[String], typ: Option[com.ligadata.fatafat.metadata.BaseTypeDef], f: Element, msg: Message, childs: Map[String, Any], prevVerMsgBaseTypesIdxArry: ArrayBuffer[String], recompile: Boolean): (String, String, String, String, List[(String, String)], List[(String, String, String, String, Boolean, String)], String, Set[String], String, String, String, String, String, String, String, String, String, String, String) = {
    var scalaclass = new StringBuilder(8 * 1024)
    var assignCsvdata = new StringBuilder(8 * 1024)
    var assignJsondata = new StringBuilder(8 * 1024)
    var assignXmldata = new StringBuilder(8 * 1024)
    var addMsg = new StringBuilder(8 * 1024)
    var getMsg = new StringBuilder(8 * 1024)
    var list = List[(String, String)]()
    var argsList = List[(String, String, String, String, Boolean, String)]()
    var jarset: Set[String] = Set();
    val pad1 = "\t"
    val pad2 = "\t\t"
    val pad3 = "\t\t\t"
    val pad4 = "\t\t\t\t"
    val newline = "\n"
    var fname: String = ""
    var keysStr = new StringBuilder(8 * 1024)
    var arrayType: ArrayTypeDef = null
    var serializedBuf = new StringBuilder(8 * 1024)
    var deserializedBuf = new StringBuilder(8 * 1024)
    var prevObjDeserializedBuf = new StringBuilder(8 * 1024)
    var convertOldObjtoNewObjBuf = new StringBuilder(8 * 1024)
    var collections = new StringBuilder(8 * 1024)
    var mappedPrevVerMatchkeys = new StringBuilder(8 * 1024)
    var mappedMsgFieldsArry = new StringBuilder(8 * 1024)
    var msgAndCntnrsStr = new StringBuilder(8 * 1024)
    var mappedPrevTypNotrMatchkeys = new StringBuilder(8 * 1024)
    var fixedMsgGetKeyStrBuf = new StringBuilder(8 * 1024)

    try {
      arrayType = typ.get.asInstanceOf[ArrayTypeDef]

      if (arrayType == null) throw new Exception("Array type " + f.Ttype + " do not exist")

      if ((arrayType.elemDef.physicalName.equals("String")) || (arrayType.elemDef.physicalName.equals("Int")) || (arrayType.elemDef.physicalName.equals("Float")) || (arrayType.elemDef.physicalName.equals("Double")) || (arrayType.elemDef.physicalName.equals("Char"))) {
        if (arrayType.elemDef.implementationName.isEmpty())
          throw new Exception("Implementation Name not found in metadata for namespace %s" + f.Ttype)
        else
          fname = arrayType.elemDef.implementationName + ".Input"

        if (msg.Fixed != null) {
          if (msg.Fixed.toLowerCase().equals("true")) {
            assignCsvdata.append("%s%s = list(inputdata.curPos).split(arrvaldelim, -1).map(v => %s(v));\n%sinputdata.curPos = inputdata.curPos+1\n".format(pad2, f.Name, fname, pad2))
            scalaclass = scalaclass.append("%svar %s: %s = _ ;%s".format(pad1, f.Name, typ.get.typeString, newline))

          } else if (msg.Fixed.toLowerCase().equals("false")) {

            //scalaclass = scalaclass.append("%svar %s: %s = _ ;%s".format(pad1, f.Name, typ.get.typeString, newline))
            collections.append("\"" + f.Name + "\",")

            mappedMsgFieldsArry = mappedMsgFieldsArry.append("%s fields(\"%s\") = (-1, new %s(0));%s".format(pad1, f.Name, typ.get.typeString, newline))

            assignCsvdata.append(" ")
          }
          val (serStr, prevDeserStr, deserStr, converToNewObj, mappedPrevVerMatch, mappedPrevVerTypNotMatchKys) = ArrayTypeHandler.getSerDeserPrimitives(typ.get.typeString, typ.get.FullName, f.Name, arrayType.elemDef.implementationName, childs, false, recompile, msg.Fixed.toLowerCase())
          // println("serStr   " + serStr)
          serializedBuf.append(serStr)
          deserializedBuf.append(deserStr)
          prevObjDeserializedBuf.append(prevDeserStr)
          convertOldObjtoNewObjBuf.append(converToNewObj)
          mappedPrevVerMatchkeys.append(mappedPrevVerMatch)
          mappedPrevTypNotrMatchkeys.append(mappedPrevVerTypNotMatchKys)
        }
        assignJsondata.append(assignJsonForArray(f.Name, fname, msg, typ.get.typeString))

      } else {
        if (arrayType.elemDef.tTypeType.toString().toLowerCase().equals("tcontainer")) {
          assignCsvdata.append(newline + getArrayStr(f.Name, arrayType.elemDef.physicalName) + newline + "\t\tinputdata.curPos = inputdata.curPos+1" + newline)
          assignJsondata.append(assignJsonForCntrArrayBuffer(f.Name, arrayType.elemDef.physicalName))
          keysStr.append("\"" + f.Name + "\",")
          if (msg.Fixed.toLowerCase().equals("false")) {
            assignJsondata.append("%s fields.put(\"%s\", (-1, %s)) %s".format(pad1, f.Name, f.Name, newline))
          }
          scalaclass = scalaclass.append("%svar %s: %s = %s();%s".format(pad1, f.Name, typ.get.typeString, typ.get.typeString, newline))

        } else if (arrayType.elemDef.tTypeType.toString().toLowerCase().equals("tmessage")) {
          throw new Exception("Adding Child Message/Messages are not allowed in Message/Container Definition")

          if (typ.get.typeString.toString().split("\\[").size == 2) {
            addMsg.append(pad2 + "if(curVal._2.compareToIgnoreCase(\"" + f.Name + "\") == 0) {" + newline + "\t")
            addMsg.append(pad3 + f.Name + " += msg.asInstanceOf[" + typ.get.typeString.toString().split("\\[")(1) + newline + pad3 + "} else ")
            if (msg.PrimaryKeys != null) {
              getMsg.append(pad2 + "if(curVal._2.compareToIgnoreCase(\"" + f.Name + "\") == 0) {" + newline + "\t")
              getMsg.append("%s%s.foreach(o => {%s%sif(o != null) { val pkd = o.PrimaryKeyData%s%sif(pkd.sameElements(primaryKey)) {%s%sreturn o%s%s}}%s%s)}%s%s}".format(pad3, f.Name, newline, pad3, newline, pad3, newline, pad3, newline, pad2, newline, pad2))
            }
          }
        }
        serializedBuf = serializedBuf.append(ArrayTypeHandler.serializeMsgContainer(typ, msg.Fixed.toLowerCase(), f))
        deserializedBuf = deserializedBuf.append(ArrayTypeHandler.deSerializeMsgContainer(typ, msg.Fixed.toLowerCase(), f, false))
        val (prevObjDeserialized, convertOldObjtoNewObj, mappedPrevVerMatch, mappedPrevTypNotMatchKys) = ArrayTypeHandler.prevObjDeserializeMsgContainer(typ, msg.Fixed.toLowerCase(), f, childs, false)
        prevObjDeserializedBuf = prevObjDeserializedBuf.append(prevObjDeserialized)
        convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append(convertOldObjtoNewObj)
        msgAndCntnrsStr.append("\"" + f.Name + "\",")
        mappedPrevVerMatchkeys.append(mappedPrevVerMatch)
        mappedPrevTypNotrMatchkeys.append(mappedPrevTypNotMatchKys)

        //assignCsvdata.append(newline + "//Array of " + arrayType.elemDef.physicalName + "not handled at this momemt" + newline)

      }

      fixedMsgGetKeyStrBuf.append("%s if(key.equals(\"%s\")) return %s; %s".format(pad1, f.Name, f.Name, newline))

      argsList = (f.NameSpace, f.Name, typ.get.NameSpace, typ.get.Name, false, null) :: argsList
      log.debug("typ.get.typeString " + typ.get.typeString)

      if ((arrayType.dependencyJarNames != null) && (arrayType.JarName != null))
        jarset = jarset + arrayType.JarName ++ arrayType.dependencyJarNames
      else if (arrayType.JarName != null)
        jarset = jarset + arrayType.JarName
      else if (arrayType.dependencyJarNames != null)
        jarset = jarset ++ arrayType.dependencyJarNames
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }

    (scalaclass.toString, assignCsvdata.toString, assignJsondata.toString, assignXmldata.toString, list, argsList, addMsg.toString, jarset, keysStr.toString, getMsg.toString, serializedBuf.toString, deserializedBuf.toString, prevObjDeserializedBuf.toString, convertOldObjtoNewObjBuf.toString, collections.toString, mappedPrevVerMatchkeys.toString, mappedMsgFieldsArry.toString, mappedPrevTypNotrMatchkeys.toString, fixedMsgGetKeyStrBuf.toString)
  }

  private def handleArrayBuffer(keysSet: Set[String], msg: Message, typ: Option[com.ligadata.fatafat.metadata.BaseTypeDef], f: Element, childs: Map[String, Any], prevVerMsgBaseTypesIdxArry: ArrayBuffer[String], recompile: Boolean): (String, String, String, String, List[(String, String)], List[(String, String, String, String, Boolean, String)], String, Set[String], String, String, String, String, String, String, String, String, String, String, String, String, String) = {
    var scalaclass = new StringBuilder(8 * 1024)
    var assignCsvdata = new StringBuilder(8 * 1024)
    var assignJsondata = new StringBuilder(8 * 1024)
    var assignXmldata = new StringBuilder(8 * 1024)
    var addMsg = new StringBuilder(8 * 1024)
    var getMsg = new StringBuilder(8 * 1024)
    var list = List[(String, String)]()
    var argsList = List[(String, String, String, String, Boolean, String)]()
    var jarset: Set[String] = Set();
    val pad1 = "\t"
    val pad2 = "\t\t"
    val pad3 = "\t\t\t"
    val pad4 = "\t\t\t\t"
    val newline = "\n"
    var fname: String = ""
    var arrayBufType: ArrayBufTypeDef = null
    var keysStr = new StringBuilder(8 * 1024)
    var msgAndCntnrsStr = new StringBuilder(8 * 1024)
    var msgNameSpace = ""
    var serializedBuf = new StringBuilder(8 * 1024)
    var deserializedBuf = new StringBuilder(8 * 1024)
    var prevObjDeserializedBuf = new StringBuilder(8 * 1024)
    var convertOldObjtoNewObjBuf = new StringBuilder(8 * 1024)
    var collections = new StringBuilder(8 * 1024)
    var mappedPrevVerMatchkeys = new StringBuilder(8 * 1024)
    var mappedMsgFieldsVar = new StringBuilder(8 * 1024)
    var mappedPrevTypNotrMatchkeys = new StringBuilder(8 * 1024)
    var mappedMsgFieldsArryBuffer = new StringBuilder(8 * 1024)
    var fixedMsgGetKeyStrBuf = new StringBuilder(8 * 1024)

    try {
      arrayBufType = typ.get.asInstanceOf[ArrayBufTypeDef]
      if (arrayBufType == null) throw new Exception("Array Byffer of " + f.Ttype + " do not exists throwing Null Pointer")

      if ((arrayBufType.elemDef.physicalName.equals("String")) || (arrayBufType.elemDef.physicalName.equals("Int")) || (arrayBufType.elemDef.physicalName.equals("Float")) || (arrayBufType.elemDef.physicalName.equals("Double")) || (arrayBufType.elemDef.physicalName.equals("Char"))) {
        if (arrayBufType.elemDef.implementationName.isEmpty())
          throw new Exception("Implementation Name not found in metadata for namespace %s" + f.Ttype)
        else
          fname = arrayBufType.elemDef.implementationName + ".Input"

        if (msg.Fixed != null) {
          if (msg.Fixed.toLowerCase().equals("true")) {
            //  list(inputdata.curPos).split(arrvaldelim, -1).map(v => { xyz :+= com.ligadata.BaseTypes.IntImpl.Input(v)});
            assignCsvdata.append("%slist(inputdata.curPos).split(arrvaldelim, -1).map(v => {%s :+= %s(v)});\n%sinputdata.curPos = inputdata.curPos+1\n".format(pad2, f.Name, fname, pad2))
            scalaclass = scalaclass.append("%svar %s: %s = new %s;%s".format(pad1, f.Name, typ.get.typeString, typ.get.typeString, newline))
          } else if (msg.Fixed.toLowerCase().equals("false")) {
            //scalaclass = scalaclass.append("%svar %s: %s = new %s;%s".format(pad1, f.Name, typ.get.typeString, typ.get.typeString, newline))

            assignCsvdata.append(" ")
            //Adding fields to keys Map
            // keysStr.append("(\"" + f.Name + "\"," + typ.get.implementationName + "),")

            collections.append("\"" + f.Name + "\",")
            mappedMsgFieldsArryBuffer = mappedMsgFieldsArryBuffer.append("%s fields(\"%s\") = (-1, new %s(0));%s".format(pad1, f.Name, typ.get.typeString, newline))

          }
          val (serStr, prevDeserStr, deserStr, converToNewObj, mappedPrevVerMatch, mappedPrevVerTypNotMatchKys) = ArrayTypeHandler.getSerDeserPrimitives(typ.get.typeString, typ.get.FullName, f.Name, arrayBufType.elemDef.implementationName, childs, true, recompile, msg.Fixed.toLowerCase())
          serializedBuf.append(serStr)
          deserializedBuf.append(deserStr)
          prevObjDeserializedBuf.append(prevDeserStr)
          convertOldObjtoNewObjBuf.append(converToNewObj)
          mappedPrevVerMatchkeys.append(mappedPrevVerMatch)
          mappedPrevTypNotrMatchkeys.append(mappedPrevVerTypNotMatchKys)

        }
        assignJsondata.append(assignJsonForPrimArrayBuffer(f.Name, fname, msg, typ.get.typeString))

      } else {
        if (msg.NameSpace != null)
          msgNameSpace = msg.NameSpace
        argsList = (msgNameSpace, f.Name, arrayBufType.NameSpace, arrayBufType.Name, false, null) :: argsList
        val msgtype = "scala.collection.mutable.ArrayBuffer[com.ligadata.FatafatBase.BaseMsg]"
        if (msg.Fixed.toLowerCase().equals("true")) //--- --- commented to declare the arraybuffer of messages in memeber variables section for both fixed adn mapped messages
          scalaclass = scalaclass.append("%svar %s: %s = new %s;%s".format(pad1, f.Name, typ.get.typeString, typ.get.typeString, newline))

        if (f.ElemType.toLowerCase().equals("container")) {
          assignCsvdata.append("%s//%s Implementation of Array Buffer of Container is not handled %s".format(pad2, f.Name, newline))

          if (typ.get.typeString.toString().split("\\[").size == 2) {
            assignJsondata.append(assignJsonForCntrArrayBuffer(f.Name, typ.get.typeString.toString().split("\\[")(1).split("\\]")(0)))
          }
          if (msg.Fixed.toLowerCase().equals("false")) {
            scalaclass = scalaclass.append("%svar %s: %s = new %s;%s".format(pad1, f.Name, typ.get.typeString, typ.get.typeString, newline))
            assignJsondata.append("%s fields.put(\"%s\", (-1, %s)) %s".format(pad1, f.Name, f.Name, newline))

            //    keysStr.append("(\"" + f.Name + "\"," + typ.get.typeString + "),")
          }
          // assignJsondata.append("%s%s.assignJsonData(map)%s".format(pad1, f.Name, newline))
          // assignCsvdata.append(newline + getArrayStr(f.Name, arrayBufType.elemDef.physicalName) + newline + "\t\tinputdata.curPos = inputdata.curPos+1" + newline)
        } else if (f.ElemType.toLowerCase().equals("message")) {
          throw new Exception("Adding Child Message/Messages is not allowed in Message/Container Definition")

          //  assignCsvdata.append("%s//%s Implementation of messages is not handled at this time%s".format(pad2, f.Name, newline))
          //  assignJsondata.append("%s//%s Implementation of messages is not handled %s".format(pad2, f.Name, newline))
          if (typ.get.typeString.toString().split("\\[").size == 2) {
            if (msg.Fixed.toLowerCase().equals("true")) {

              addMsg.append(newline + pad2 + "if(curVal._2.compareToIgnoreCase(\"" + f.Name + "\") == 0) {" + newline + "\t")
              addMsg.append(pad3 + f.Name + " += msg.asInstanceOf[" + typ.get.typeString.toString().split("\\[")(1) + newline + pad3 + "} else ")

            } else if (msg.Fixed.toLowerCase().equals("false")) {

              addMsg.append(pad2 + "if(curVal._2.compareToIgnoreCase(\"" + f.Name + "\") == 0) {" + newline + "\t")
              addMsg.append(newline + pad3 + "val x = getOrElse(\"" + f.Name + "\", null)" + newline + pad3)
              addMsg.append("var " + f.Name + ": " + typ.get.typeString + " = null " + newline) //--- commented to declare the arraybuffer of messages in memeber variables section
              addMsg.append(pad3 + "if (x == null) {" + newline + pad3 + f.Name + " = new " + typ.get.typeString + newline + pad3 + "} else {" + newline)
              addMsg.append(pad3 + f.Name + "= x.asInstanceOf[" + typ.get.typeString + "]" + newline + pad3 + "}" + newline)
              addMsg.append(pad3 + f.Name + " += msg.asInstanceOf[" + typ.get.typeString.toString().split("\\[")(1) + newline)
              addMsg.append(pad3 + "fields(\"" + f.Name + "\") = (-1, " + f.Name + ")" + newline + pad2 + newline + pad2 + "} else ")

              mappedMsgFieldsVar.append("%s fields(\"%s\") = (-1, new %s )%s".format(pad2, f.Name, typ.get.typeString, newline))

              //adding Array of Messages to Map
              // keysStr.append("(\"" + f.Name + "\"," + arrayBuf + "[" + msgType + "),")
            }
            if (msg.PrimaryKeys != null) {
              getMsg.append(pad2 + "if(curVal._2.compareToIgnoreCase(\"" + f.Name + "\") == 0) {" + newline + "\t")

              if (msg.Fixed.toLowerCase().equals("false")) {
                getMsg.append(newline + pad3 + "val x = getOrElse(\"" + f.Name + "\", null)" + newline)
                getMsg.append(pad3 + "if (x == null) return null" + newline)
                getMsg.append(pad3 + "val " + f.Name + "= x.asInstanceOf[" + typ.get.typeString + "]" + newline + pad3)
                getMsg.append("%s%s.foreach(o => {%s%s if(o != null) { val pkd = o.PrimaryKeyData%s%sif(pkd.sameElements(primaryKey)) {%s%sreturn o%s%s} }%s%s})%s%s}else ".format(pad2, f.Name, newline, pad3, newline, pad3, newline, pad3, newline, pad2, newline, pad2, newline, pad2))

              } else if (msg.Fixed.toLowerCase().equals("true")) {
                getMsg.append("%s%s.foreach(o => {%s%s if(o != null) { val pkd = o.PrimaryKeyData%s%sif(pkd.sameElements(primaryKey)) {%s%sreturn o%s%s}}%s%s})%s%s}else ".format(pad2, f.Name, newline, pad3, newline, pad3, newline, pad3, newline, pad2, newline, pad2, newline, pad2))
              }
            }
          }

        }
        serializedBuf = serializedBuf.append(ArrayTypeHandler.serializeMsgContainer(typ, msg.Fixed.toLowerCase(), f))
        deserializedBuf = deserializedBuf.append(ArrayTypeHandler.deSerializeMsgContainer(typ, msg.Fixed.toLowerCase(), f, true))
        val (prevObjDeserialized, convertOldObjtoNewObj, mappedPrevVerMatch, mappedPrevTypNotMatchKys) = ArrayTypeHandler.prevObjDeserializeMsgContainer(typ, msg.Fixed.toLowerCase(), f, childs, true)
        prevObjDeserializedBuf = prevObjDeserializedBuf.append(prevObjDeserialized)
        convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append(convertOldObjtoNewObj)
        msgAndCntnrsStr.append("\"" + f.Name + "\",")
        mappedPrevVerMatchkeys.append(mappedPrevVerMatch)
        mappedPrevTypNotrMatchkeys.append(mappedPrevTypNotMatchKys)
      }
      fixedMsgGetKeyStrBuf.append("%s if(key.equals(\"%s\")) return %s; %s".format(pad1, f.Name, f.Name, newline))

      if ((arrayBufType.dependencyJarNames != null) && (arrayBufType.JarName != null))
        jarset = jarset + arrayBufType.JarName ++ arrayBufType.dependencyJarNames
      else if (arrayBufType.JarName != null)
        jarset = jarset + arrayBufType.JarName
      else if (arrayBufType.dependencyJarNames != null)
        jarset = jarset ++ arrayBufType.dependencyJarNames

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (scalaclass.toString, assignCsvdata.toString, assignJsondata.toString, assignXmldata.toString, list, argsList, addMsg.toString, jarset, msgAndCntnrsStr.toString, keysStr.toString, getMsg.toString, serializedBuf.toString, deserializedBuf.toString, prevObjDeserializedBuf.toString, convertOldObjtoNewObjBuf.toString, collections.toString, mappedPrevVerMatchkeys.toString, mappedMsgFieldsVar.toString, mappedPrevTypNotrMatchkeys.toString, mappedMsgFieldsArryBuffer.toString, fixedMsgGetKeyStrBuf.toString)
  }

  private def handleContainer(msg: Message, mdMgr: MdMgr, ftypeVersion: Long, f: Element, recompile: Boolean, childs: Map[String, Any]): (String, String, String, String, List[(String, String)], List[(String, String, String, String, Boolean, String)], String, Set[String], String, String, String, String, String, String, String, String) = {
    var scalaclass = new StringBuilder(8 * 1024)
    var assignCsvdata = new StringBuilder(8 * 1024)
    var assignJsondata = new StringBuilder(8 * 1024)
    var assignXmldata = new StringBuilder(8 * 1024)
    var addMsg = new StringBuilder(8 * 1024)
    var list = List[(String, String)]()
    var argsList = List[(String, String, String, String, Boolean, String)]()
    var keysStr = new StringBuilder(8 * 1024)
    var jarset: Set[String] = Set();
    val pad1 = "\t"
    val pad2 = "\t\t"
    val pad3 = "\t\t\t"
    val pad4 = "\t\t\t\t"
    val newline = "\n"
    var fname: String = ""
    var serializedBuf = new StringBuilder(8 * 1024)
    var deserializedBuf = new StringBuilder(8 * 1024)
    var prevObjDeserializedBuf = new StringBuilder(8 * 1024)
    var convertOldObjtoNewObjBuf = new StringBuilder(8 * 1024)
    var mappedMsgFieldsVar = new StringBuilder(8 * 1024)
    var mappedPrevVerMatchkeys = new StringBuilder(8 * 1024)
    var mappedPrevTypNotrMatchkeys = new StringBuilder(8 * 1024)
    var fixedMsgGetKeyStrBuf = new StringBuilder(8 * 1024)

    try {
      var ctrDef: ContainerDef = mdMgr.Container(f.Ttype, ftypeVersion, true).getOrElse(null)
      if (ctrDef == null) throw new Exception("Container  " + f.Ttype + " do not exists throwing null pointer")

      scalaclass = scalaclass.append("%svar %s:%s = new %s();%s".format(pad1, f.Name, ctrDef.PhysicalName, ctrDef.PhysicalName, newline))
      assignCsvdata.append("%s%s.populate(inputdata);\n%sinputdata.curPos = inputdata.curPos+1\n".format(pad2, f.Name, pad2))
      assignJsondata.append(assignJsonDataMessage(f.Name))
      if (msg.Fixed.toLowerCase().equals("false")) {
        assignJsondata.append("%s fields.put(\"%s\", (-1, %s)) %s".format(pad1, f.Name, f.Name, newline))
      }
      assignXmldata.append("%s%s.populate(xmlData)%s".format(pad3, f.Name, newline))

      if ((ctrDef.dependencyJarNames != null) && (ctrDef.jarName != null)) {
        jarset = jarset + ctrDef.JarName ++ ctrDef.dependencyJarNames
      } else if ((ctrDef.jarName != null))
        jarset = jarset + ctrDef.JarName
      else if (ctrDef.dependencyJarNames != null)
        jarset = jarset ++ ctrDef.dependencyJarNames
      // val typ = MdMgr.GetMdMgr.Type(f.Ttype, ftypeVersion, true)
      argsList = (f.NameSpace, f.Name, ctrDef.NameSpace, ctrDef.Name, false, null) :: argsList
      //   keysStr.append("\"" + f.Name + "\",")
      // argsList = (f.NameSpace, f.Name, typ.get.NameSpace, typ.get.Name, false, null) :: argsList

      fixedMsgGetKeyStrBuf.append("%s if(key.equals(\"%s\")) return %s; %s".format(pad1, f.Name, f.Name, newline))

      if (msg.Fixed.toLowerCase().equals("true")) {
        serializedBuf.append("%s {if (%s == null)  com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,0);%s".format(pad2, f.Name, newline))

      } else if (msg.Fixed.toLowerCase().equals("false")) {
        serializedBuf.append("%s { val %s = getOrElse(\"%s\", null);%s".format(pad2, f.Name, f.Name, newline))
        serializedBuf.append("%s if (%s == null) com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, 0) %s".format(pad2, f.Name, newline))
      }
      serializedBuf.append("%s else { val bytes = SerializeDeserialize.Serialize(%s.asInstanceOf[%s])%s".format(pad2, f.Name, ctrDef.typeString, newline))
      serializedBuf.append("%s com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,bytes.length);%s".format(pad2, newline))
      serializedBuf.append("%s dos.write(bytes);%s} %s }%s".format(pad2, newline, newline, newline))

      val curObjtype = ctrDef.physicalName

      var memberExists: Boolean = false
      var membrMatchTypeNotMatch = false
      var sameType: Boolean = false
      if (childs != null) {
        if (childs.contains(f.Name)) {
          var child = childs.getOrElse(f.Name, null)
          if (child != null) {
            val fullname = child.asInstanceOf[AttributeDef].aType.FullName
            if (fullname != null && fullname.trim() != "" && fullname.equals(ctrDef.FullName)) {
              memberExists = true
              val childPhysicalName = child.asInstanceOf[AttributeDef].aType.typeString
              if (childPhysicalName != null && childPhysicalName.trim() != "") {
                if (childPhysicalName.equals(curObjtype))
                  sameType = true
              }
            } else {
              membrMatchTypeNotMatch = true
            }
          }
        }
      }

      /*  case "inpatient" => {
                  val curVerObj = new com.ligadata.messagescontainers.System_InpatientClaim_100_1427140791051()
                  curVerObj.ConvertPrevToNewVerObj(prevObjfield._2._2)
                  fields("inpatient") = (-1, curVerObj)
                }
                * 
                */

      if (memberExists) {
        // convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append("%s%s = oldObj.%s%s".format(pad2, f.Name, f.Name, newline))
        if (msg.Fixed.toLowerCase().equals("true")) {
          prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s{%s%sval curVerObj = new %s()%s".format(pad2, newline, pad2, ctrDef.typeString, newline))
          prevObjDeserializedBuf = prevObjDeserializedBuf.append("%scurVerObj.ConvertPrevToNewVerObj(prevVerObj.%s)%s".format(pad2, f.Name, newline))
          prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s%s = curVerObj}%s".format(pad2, f.Name, newline))
        }
        if (msg.Fixed.toLowerCase().equals("false")) {
          mappedPrevVerMatchkeys.append("\"" + f.Name + "\",")
          prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s case \"%s\" => { %s".format(pad2, f.Name, newline))
          prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s if(prevObjfield._2._2 != null){ fields(\"%s\") = (prevObjfield._2._1,prevObjfield._2._2) }}%s".format(pad2, f.Name, newline))
          if (sameType) {

          } else {
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s{%s%sval curVerObj = new %s()%s".format(pad2, newline, pad2, ctrDef.typeString, newline))
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%scurVerObj.ConvertPrevToNewVerObj(prevObjfield._2._2)%s".format(pad2, f.Name, newline))
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s fields(\"%s\") = (prevObjfield._2._1,curVerObj) }%s".format(pad2, f.Name, newline))
          }
        }
      }
      //  val childType = typ.get.typeString.toString().split("\\[")(1).substring(0, typ.get.typeString.toString().split("\\[")(1).length() - 1)

      deserializedBuf.append("%s{ %s val length = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis) %s".format(pad2, newline, newline))
      deserializedBuf.append("%s if (length > 0) { %s%svar bytes = new Array[Byte](length);%s".format(pad2, newline, pad2, newline))
      deserializedBuf.append("%sdis.read(bytes);%s".format(pad2, newline))
      deserializedBuf.append("%sval inst = SerializeDeserialize.Deserialize(bytes, mdResolver, loader, false, \"%s\");%s".format(pad2, ctrDef.PhysicalName, newline))

      if (msg.Fixed.toLowerCase().equals("true")) {
        deserializedBuf.append("%s%s = inst.asInstanceOf[%s];%s%s} }%s".format(pad2, f.Name, ctrDef.typeString, newline, pad2, newline))
      } else if (msg.Fixed.toLowerCase().equals("false")) {
        deserializedBuf.append("%s fields(\"%s\") = (-1, inst.asInstanceOf[%s]);%s%s}%s".format(pad2, f.Name, ctrDef.typeString, newline, pad2, newline))
        deserializedBuf.append("%s else fields(\"%s\") = (-1, new %s()) } %s".format(pad2, f.Name, ctrDef.typeString, newline, pad2, newline))
        if (membrMatchTypeNotMatch) {
          mappedPrevTypNotrMatchkeys.append("\"" + f.Name + "\",")

        }
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (scalaclass.toString, assignCsvdata.toString, assignJsondata.toString, assignXmldata.toString, list, argsList, addMsg.toString, jarset, keysStr.toString, serializedBuf.toString, deserializedBuf.toString, prevObjDeserializedBuf.toString, convertOldObjtoNewObjBuf.toString, mappedPrevVerMatchkeys.toString, mappedPrevTypNotrMatchkeys.toString, fixedMsgGetKeyStrBuf.toString)
  }

  private def handleMessage(mdMgr: MdMgr, ftypeVersion: Long, f: Element, msg: Message, childs: Map[String, Any], recompile: Boolean): (String, String, String, String, List[(String, String)], List[(String, String, String, String, Boolean, String)], String, Set[String], String, String, String, String, String, String, String, String) = {

    var scalaclass = new StringBuilder(8 * 1024)
    var assignCsvdata = new StringBuilder(8 * 1024)
    var assignJsondata = new StringBuilder(8 * 1024)
    var assignXmldata = new StringBuilder(8 * 1024)
    var addMsg = new StringBuilder(8 * 1024)
    var getMsg = new StringBuilder(8 * 1024)
    var list = List[(String, String)]()
    var argsList = List[(String, String, String, String, Boolean, String)]()
    var jarset: Set[String] = Set();
    val pad1 = "\t"
    val pad2 = "\t\t"
    val pad3 = "\t\t\t"
    val pad4 = "\t\t\t\t"
    val newline = "\n"
    var fname: String = ""
    var serializedBuf = new StringBuilder(8 * 1024)
    var deserializedBuf = new StringBuilder(8 * 1024)
    var prevObjDeserializedBuf = new StringBuilder(8 * 1024)
    var convertOldObjtoNewObjBuf = new StringBuilder(8 * 1024)
    var mappedMsgFieldsVar = new StringBuilder(8 * 1024)
    var mappedPrevVerMatchkeys = new StringBuilder(8 * 1024)
    var mappedPrevTypNotrMatchkeys = new StringBuilder(8 * 1024)
    var fixedMsgGetKeyStrBuf = new StringBuilder(8 * 1024)

    try {

      var msgDef: MessageDef = mdMgr.Message(f.Ttype, ftypeVersion, true).getOrElse(null)
      if (msgDef == null) throw new Exception(f.Ttype + " do not exists throwing null pointer")
      val msgDefobj = "com.ligadata.FatafatBase.BaseMsg"
      scalaclass = scalaclass.append("%svar %s:%s = _;%s".format(pad1, f.Name, msgDefobj, newline))
      assignCsvdata.append("%s//Not Handling CSV Populate for member type Message".format(pad2))
      assignJsondata.append("%s//Not Handling JSON Populate for member type Message".format(pad2))
      assignXmldata.append("%s//Not Handling XML Populate for member type Message".format(pad2))
      //  assignCsvdata.append("%s%s.populate(inputdata);\n%sinputdata.curPos = inputdata.curPos+1\n".format(pad2, f.Name, pad2))
      // assignJsondata.append(assignJsonDataMessage(f.Name))
      //  assignXmldata.append("%s%s.populate(xmlData)%s".format(pad3, f.Name, newline))

      if ((msgDef.dependencyJarNames != null) && (msgDef.jarName != null)) {
        jarset = jarset + msgDef.JarName ++ msgDef.dependencyJarNames
      } else if ((msgDef.jarName != null))
        jarset = jarset + msgDef.JarName
      else if (msgDef.dependencyJarNames != null)
        jarset = jarset ++ msgDef.dependencyJarNames

      argsList = (f.NameSpace, f.Name, msgDef.NameSpace, msgDef.Name, false, null) :: argsList
      addMsg.append(pad2 + "if(curVal._2.compareToIgnoreCase(\"" + f.Name + "\") == 0) {" + newline + "\t")
      addMsg.append(pad3 + f.Name + " = msg.asInstanceOf[" + msgDef.typeString + "]" + newline + pad3 + "} else ")
      if (msg.PrimaryKeys != null) {
        getMsg.append(pad2 + "if(curVal._2.compareToIgnoreCase(\"" + f.Name + "\") == 0) {" + newline + "\t")
        getMsg.append("%sval pkd = %s.PrimaryKeyData%s".format(pad3, f.Name, newline))
        getMsg.append("%sif(pkd.sameElements(primaryKey)) {%s".format(pad3, newline))
        getMsg.append("%sreturn %s %s%s}%s%s}else".format(pad3, f.Name, newline, pad3, newline, pad3))
      }

      if (msg.Fixed.toLowerCase().equals("false")) {
        mappedMsgFieldsVar.append("%s fields(\"%s\") = (-1, new %s )%s".format(pad2, f.Name, msgDef.typeString, newline))

      }

      if (msg.Fixed.toLowerCase().equals("true")) {
        serializedBuf.append("%s {if (%s == null)  com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,0);%s".format(pad2, f.Name, newline))

      } else if (msg.Fixed.toLowerCase().equals("false")) {
        serializedBuf.append("%s { val (idx, %s) = getOrElse(\"%s\", null);%s".format(pad2, f.Name, f.Name, newline))
        serializedBuf.append("%s if (%s == null) com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, 0) %s".format(pad2, f.Name, newline))
      }
      serializedBuf.append("%s else { val bytes = SerializeDeserialize.Serialize(%s.asInstanceOf[%s])%s".format(pad2, f.Name, msgDef.typeString, newline))
      serializedBuf.append("%s com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,bytes.length);%s".format(pad2, newline))
      serializedBuf.append("%s dos.write(bytes);%s} %s }%s".format(pad2, newline, newline, newline))

      val curObjtype = msgDef.physicalName

      var memberExists: Boolean = false
      var membrMatchTypeNotMatch = false
      var sameType: Boolean = false
      if (childs != null) {
        if (childs.contains(f.Name)) {
          var child = childs.getOrElse(f.Name, null)
          if (child != null) {
            val fullname = child.asInstanceOf[AttributeDef].aType.FullName
            if (fullname != null && fullname.trim() != "" && fullname.equals(msgDef.FullName)) {
              memberExists = true
              val childPhysicalName = child.asInstanceOf[AttributeDef].aType.typeString
              if (childPhysicalName != null && childPhysicalName.trim() != "") {
                if (childPhysicalName.equals(curObjtype))
                  sameType = true
              }
            } else {
              membrMatchTypeNotMatch = true
            }
          }
        }
      }

      if (memberExists) {
        // convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append("%s%s = oldObj.%s%s".format(pad2, f.Name, f.Name, newline))
        if (msg.Fixed.toLowerCase().equals("true")) {
          prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s{%s%sval curVerObj = new %s()%s".format(pad2, newline, pad2, msgDef.typeString, newline))
          prevObjDeserializedBuf = prevObjDeserializedBuf.append("%scurVerObj.ConvertPrevToNewVerObj(prevVerObj.%s)%s".format(pad2, f.Name, newline))
          prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s%s = curVerObj}%s".format(pad2, f.Name, newline))
        }
        if (msg.Fixed.toLowerCase().equals("false")) {
          mappedPrevVerMatchkeys.append("\"" + f.Name + "\",")
          prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s case \"%s\" => { %s".format(pad2, f.Name, newline))
          prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s if(prevObjfield._2._2 != null){ fields(\"%s\") = (prevObjfield._2._1,prevObjfield._2._2) }}%s".format(pad2, f.Name, newline))
          if (sameType) {

          } else {
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s{%s%sval curVerObj = new %s()%s".format(pad2, newline, pad2, msgDef.typeString, newline))
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%scurVerObj.ConvertPrevToNewVerObj(prevObjfield._2._2)%s".format(pad2, f.Name, newline))
            prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s fields(\"%s\") = (prevObjfield._2._1,curVerObj) }%s".format(pad2, f.Name, newline))
          }
        }
      }

      //  val childType = typ.get.typeString.toString().split("\\[")(1).substring(0, typ.get.typeString.toString().split("\\[")(1).length() - 1)

      deserializedBuf.append("%s{ %s val length = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis) %s".format(pad2, newline, newline))
      deserializedBuf.append("%s if (length > 0) { %s%svar bytes = new Array[Byte](length);%s".format(pad2, newline, pad2, newline))
      deserializedBuf.append("%sdis.read(bytes);%s".format(pad2, newline))
      deserializedBuf.append("%sval inst = SerializeDeserialize.Deserialize(bytes, mdResolver, loader, false, \"%s\");%s".format(pad2, msgDef.PhysicalName, newline))

      if (msg.Fixed.toLowerCase().equals("true")) {
        deserializedBuf.append("%s%s = inst.asInstanceOf[BaseMsg];%s%s} }%s".format(pad2, f.Name, newline, pad2, newline))
      } else if (msg.Fixed.toLowerCase().equals("false")) {
        deserializedBuf.append("%s fields(\"%s\") = (-1, inst.asInstanceOf[%s]);%s%s}%s".format(pad2, f.Name, msgDef.typeString, newline, pad2, newline))
        deserializedBuf.append("%s else fields(\"%s\") = (-1, new %s()) } %s".format(pad2, f.Name, msgDef.typeString, newline, pad2, newline))
        if (membrMatchTypeNotMatch) {
          mappedPrevTypNotrMatchkeys.append("\"" + f.Name + "\",")

        }
      }
      fixedMsgGetKeyStrBuf.append("%s if(key.equals(\"%s\")) return %s; %s".format(pad1, f.Name, f.Name, newline))
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (scalaclass.toString, assignCsvdata.toString, assignJsondata.toString, assignXmldata.toString, list, argsList, addMsg.toString, jarset, getMsg.toString, serializedBuf.toString, deserializedBuf.toString, prevObjDeserializedBuf.toString, convertOldObjtoNewObjBuf.toString, mappedPrevVerMatchkeys.toString, mappedPrevTypNotrMatchkeys.toString, fixedMsgGetKeyStrBuf.toString)
  }

  private def handleConcept(mdMgr: MdMgr, ftypeVersion: Long, f: Element): (String, String, String, String, List[(String, String)], List[(String, String, String, String, Boolean, String)], String, Set[String], String) = {

    var scalaclass = new StringBuilder(8 * 1024)
    var assignCsvdata = new StringBuilder(8 * 1024)
    var assignJsondata = new StringBuilder(8 * 1024)
    var assignXmldata = new StringBuilder(8 * 1024)
    var addMsg = new StringBuilder(8 * 1024)
    var list = List[(String, String)]()
    var argsList = List[(String, String, String, String, Boolean, String)]()
    var jarset: Set[String] = Set();
    val pad1 = "\t"
    val pad2 = "\t\t"
    val pad3 = "\t\t\t"
    val pad4 = "\t\t\t\t"
    val newline = "\n"
    var fname: String = ""
    var fixedMsgGetKeyStrBuf = new StringBuilder(8 * 1024)

    try {

      var attribute: BaseAttributeDef = mdMgr.Attribute(f.Ttype, ftypeVersion, true).getOrElse(null)

      if (attribute == null) throw new Exception("Attribute " + f.Ttype + " do not exists throwing Null pointer")

      if ((attribute.typeString.equals("String")) || (attribute.typeString.equals("Int")) || (attribute.typeString.equals("Float")) || (attribute.typeString.equals("Double")) || (attribute.typeString.equals("Char"))) {
        if (attribute.typeDef.implementationName.isEmpty())
          throw new Exception("Implementation Name not found in metadata for namespace %s" + f.Ttype)
        else
          fname = attribute.typeDef.implementationName + ".Input"

        if (f.Name.split("\\.").size == 2) {

          val dval: String = getDefVal("system." + attribute.typeString.toString().toLowerCase())

          scalaclass = scalaclass.append("%svar %s:%s = _ ;%s".format(pad1, f.Name.split("\\.")(1), attribute.typeString, newline))
          assignCsvdata.append("%s%s = %s(list(inputdata.curPos));\n%sinputdata.curPos = inputdata.curPos+1\n".format(pad2, f.Name.split("\\.")(1), fname, pad2))
          assignJsondata.append("%s %s = %s(map.getOrElse(\"%s\", %s).toString)%s".format(pad2, f.Name.split("\\.")(1), fname, f.Name.split("\\.")(1), dval, newline))
          assignXmldata.append("%sval _%sval_  = (xml \\\\ \"%s\").text.toString %s%sif (_%sval_  != \"\")%s%s =  %s( _%sval_ ) else %s = %s%s".format(pad3, f.Name.split("\\.")(1), f.Name.split("\\.")(1), newline, pad3, f.Name.split("\\.")(1), pad2, f.Name.split("\\.")(1), fname, f.Name.split("\\.")(1), f.Name.split("\\.")(1), dval, newline))

        }

        // assignCsvdata.append("%s%s = %s(list(inputdata.curPos));\n%sinputdata.curPos = inputdata.curPos+1\n".format(pad2, f.Name, fname, pad2))

      } else {
        scalaclass = scalaclass.append("%svar %s:%s = new %s();%s".format(pad1, f.Name, attribute.typeString, attribute.typeString, newline))
      }
      //assignCsvdata.append("%sinputdata.curPos = %s.assignCSV(list, inputdata.curPos);\n%sinputdata.curPos = inputdata.curPos+1\n".format(pad2, f.Name, pad2))

      if ((attribute.dependencyJarNames != null) && (attribute.jarName != null)) {
        jarset = jarset + attribute.JarName ++ attribute.dependencyJarNames
      } else if ((attribute.jarName != null))
        jarset = jarset + attribute.JarName
      else if (attribute.dependencyJarNames != null)
        jarset = jarset ++ attribute.dependencyJarNames

      argsList = (attribute.NameSpace, attribute.Name, attribute.typeDef.NameSpace, attribute.typeDef.Name, true, null) :: argsList
      fixedMsgGetKeyStrBuf.append("%s if(key.equals(\"%s\")) return %s; %s".format(pad1, f.Name, f.Name, newline))

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (scalaclass.toString, assignCsvdata.toString, assignJsondata.toString, assignXmldata.toString, list, argsList, addMsg.toString, jarset, fixedMsgGetKeyStrBuf.toString)
  }

  //get the childs from previous existing message or container

  //generates the variables string and assign string
  private def classStr(message: Message, mdMgr: MdMgr, recompile: Boolean): (String, String, String, String, Int, List[(String, String)], List[(String, String, String, String, Boolean, String)], String, String, Array[Int], Array[Int], String, String, String, String, String, String, String, String) = {
    var scalaclass = new StringBuilder(8 * 1024)
    var assignCsvdata = new StringBuilder(8 * 1024)
    var assignJsondata = new StringBuilder(8 * 1024)
    var assignXmldata = new StringBuilder(8 * 1024)
    var addMsg = new StringBuilder(8 * 1024)
    var getMsg = new StringBuilder(8 * 1024)
    var getmessage: String = ""
    var keysStr = new StringBuilder(8 * 1024)
    var msgAndCntnrsStr = new StringBuilder(8 * 1024)
    var typeImpl = new StringBuilder(8 * 1024)
    var keysVarStr: String = ""
    var MsgsAndCntrsVarStr: String = ""
    var typeImplStr: String = ""
    var serializedBuf = new StringBuilder(8 * 1024)
    var deserializedBuf = new StringBuilder(8 * 1024)
    var mappedSerBaseTypesBuf = new StringBuilder(8 * 1024)
    var mappedDeserBaseTypesBuf = new StringBuilder(8 * 1024)
    var mappedBaseTypesSet: Set[Int] = Set()

    var prevDeserializedBuf = new StringBuilder(8 * 1024)
    var convertOldObjtoNewObjBuf = new StringBuilder(8 * 1024)
    var collections = new StringBuilder(8 * 1024)
    var mappedPrevVerMatchkeys = new StringBuilder(8 * 1024)
    var mappedMsgFieldsVar = new StringBuilder(8 * 1024)
    var mappedMsgConstructor: String = ""
    var mappedMsgFieldsArry = new StringBuilder(8 * 1024)
    var mappedMsgFieldsArryBuffer = new StringBuilder(8 * 1024)
    var mappedPrevTypNotMatchkeys = new StringBuilder(8 * 1024)
    var prevObjTypNotMatchDeserializedBuf = new StringBuilder(8 * 1024)
    var fixedMsgGetKeyStrBuf = new StringBuilder(8 * 1024)

    var list = List[(String, String)]()
    var argsList = List[(String, String, String, String, Boolean, String)]()
    val pad1 = "\t"
    val pad2 = "\t\t"
    val pad3 = "\t\t\t"
    val pad4 = "\t\t\t\t"
    val newline = "\n"
    var jarset: Set[String] = Set();
    var arrayType: ArrayTypeDef = null
    var arrayBufType: ArrayBufTypeDef = null
    var fname: String = ""
    var count: Int = 0
    var concepts: String = "concepts"
    var keysSet: Set[String] = Set()
    var keys: Set[String] = Set()

    var ftypeVersion: Long = -1
    var addMessage: String = ""

    var partitionPos = Array[Int]()
    var primaryPos = Array[Int]()
    var prevVerMsgObjstr: String = ""
    var childs: Map[String, Any] = Map[String, Any]()
    var prevVerMsgBaseTypesIdxArry = new ArrayBuffer[String]
    try {

      //Mapped Messages - Add String as first element to Array Buffer for typs variable
      var mappedTypesABuf = new scala.collection.mutable.ArrayBuffer[String]
      val stringType = MdMgr.GetMdMgr.Type("System.String", -1, true)
      if (stringType.getOrElse("None").equals("None"))
        throw new Exception("Type not found in metadata for String ")
      mappedTypesABuf += stringType.get.implementationName
      var firstTimeBaseType: Boolean = true
      //Get the previous added Message or Container from Metadata and get all the childs from the Previous message/Container for Deserialize purpose

      val (ismsg, pMsgdef, pMsgObjstr) = getPrevVersionMsgContainer(message: Message, mdMgr: MdMgr)
      prevVerMsgObjstr = pMsgObjstr
      val isMsg = ismsg
      if (pMsgdef != null) {
        val (childAttrs, prevVerMsgBaseTypesIdxArray) = getPrevVerMsgChilds(pMsgdef, isMsg, message.Fixed)
        childs = childAttrs

        if ((pMsgdef.dependencyJarNames != null) && (pMsgdef.JarName != null))
          jarset = jarset + pMsgdef.JarName ++ pMsgdef.dependencyJarNames
        else if (pMsgdef.JarName != null)
          jarset = jarset + pMsgdef.JarName
        else if (pMsgdef.dependencyJarNames != null)
          jarset = jarset ++ pMsgdef.dependencyJarNames
      }

      scalaclass = scalaclass.append(getIsFixedCls(message) + newline + getCanPersistCls(message) + newline + getNameVerCls(message) + newline + newline)
      // for (e <- message.Elements) {
      //  var fields = e.Fields
      var paritionkeys: List[String] = null
      var primaryKeys: List[String] = null
      // if ((message.PartitionKey != null) && (message.PartitionKey(0) != null) && (message.PartitionKey(0).trim() != ""))
      //    paritionkey = message.PartitionKey(0).toLowerCase()
      if (message.PartitionKey != null)
        paritionkeys = message.PartitionKey

      if (message.PrimaryKeys != null)
        primaryKeys = message.PrimaryKeys

      if (paritionkeys != null && paritionkeys.size > 0)
        paritionkeys.foreach { partitionkey =>
          keys = keys + partitionkey.toLowerCase()
        }

      if (primaryKeys != null && primaryKeys.size > 0)
        primaryKeys.foreach { primarykey =>
          keys = keys + primarykey.toLowerCase()
        }

      if (message.Elements != null)
        for (f <- message.Elements) {

          // val typ = MdMgr.GetMdMgr.Type(key, ver, onlyActive)(f.Ttype)
          //val attr = MdMgr.GetMdMgr.Attribute(message.NameSpace, message.Name)
          //  val container = MdMgr.GetMdMgr.Containers(onlyActive, latestVersion)
          if (keysSet.contains(f.Name)) throw new Exception("Duplicate Key " + f.Name + " Exists, please check Message Definition JSON")
          keysSet = keysSet + f.Name

          if (f.FieldtypeVer != null)
            ftypeVersion = MdMgr.ConvertVersionToLong(message.Version)

          if ((f.ElemType.equals("Field")) || (f.ElemType.equals("Fields"))) {

            log.debug("message.Version " + MdMgr.ConvertVersionToLong(message.Version))

            val typ = MdMgr.GetMdMgr.Type(f.Ttype, ftypeVersion, true) // message.Version.toLong

            if (typ.getOrElse("None").equals("None"))
              throw new Exception("Type not found in metadata for Name: " + f.Name + " , NameSpace: " + f.NameSpace + " , Version: " + message.Version + " , Type : " + f.Ttype)

            if (typ.get.tType == null) throw new Exception("tType in Type do not exists")

            if (typ.get.tType.toString().equals("tArray")) {

              val (arr_1, arr_2, arr_3, arr_4, arr_5, arr_6, arr_7, arr_8, arr_9, arr_10, arr_11, arr_12, arr_13, arr_14, arr_15, arr_16, arr_17, arr_18, arr_19) = handleArrayType(keys, typ, f, message, childs, prevVerMsgBaseTypesIdxArry, recompile)

              scalaclass = scalaclass.append(arr_1)
              assignCsvdata = assignCsvdata.append(arr_2)
              assignJsondata = assignJsondata.append(arr_3)

              assignXmldata = assignXmldata.append(arr_4)
              list = arr_5
              argsList = argsList ::: arr_6
              addMsg = addMsg.append(arr_7)
              jarset = jarset ++ arr_8
              msgAndCntnrsStr = msgAndCntnrsStr.append(arr_9)
              getMsg = getMsg.append(arr_10)
              serializedBuf = serializedBuf.append(arr_11)
              deserializedBuf = deserializedBuf.append(arr_12)

              prevDeserializedBuf = prevDeserializedBuf.append(arr_13)
              convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append(arr_14)
              collections = collections.append(arr_15)
              mappedPrevVerMatchkeys = mappedPrevVerMatchkeys.append(arr_16)
              mappedMsgFieldsArry = mappedMsgFieldsArry.append(arr_17)
              mappedPrevTypNotMatchkeys = mappedPrevTypNotMatchkeys.append(arr_18)
              fixedMsgGetKeyStrBuf = fixedMsgGetKeyStrBuf.append(arr_19)

              //       =  assignCsvdata.toString, assignJsondata.toString, assignXmldata.toString,  list, argsList, addMsg.toString)  = 
            } else if (typ.get.tType.toString().equals("tArrayBuf")) {
              val (arrayBuf_1, arrayBuf_2, arrayBuf_3, arrayBuf_4, arrayBuf_5, arrayBuf_6, arrayBuf_7, arrayBuf_8, arrayBuf_9, arrayBuf_10, arrayBuf_11, arrayBuf_12, arrayBuf_13, arrayBuf_14, arrayBuf_15, arrayBuf_16, arrayBuf_17, arrayBuf_18, arrayBuf_19, arrayBuf_20, arrayBuf_21) = handleArrayBuffer(keys, message, typ, f, childs, prevVerMsgBaseTypesIdxArry, recompile)
              scalaclass = scalaclass.append(arrayBuf_1)
              assignCsvdata = assignCsvdata.append(arrayBuf_2)
              assignJsondata = assignJsondata.append(arrayBuf_3)
              assignXmldata = assignXmldata.append(arrayBuf_4)
              list = arrayBuf_5
              argsList = argsList ::: arrayBuf_6
              addMsg = addMsg.append(arrayBuf_7)
              jarset = jarset ++ arrayBuf_8
              msgAndCntnrsStr = msgAndCntnrsStr.append(arrayBuf_9)
              keysStr = keysStr.append(arrayBuf_10)
              getMsg = getMsg.append(arrayBuf_11)
              serializedBuf = serializedBuf.append(arrayBuf_12)
              deserializedBuf = deserializedBuf.append(arrayBuf_13)
              prevDeserializedBuf = prevDeserializedBuf.append(arrayBuf_14)
              convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append(arrayBuf_15)
              collections = collections.append(arrayBuf_16)
              mappedPrevVerMatchkeys = mappedPrevVerMatchkeys.append(arrayBuf_17)
              mappedMsgFieldsVar = mappedMsgFieldsVar.append(arrayBuf_18)
              mappedPrevTypNotMatchkeys = mappedPrevTypNotMatchkeys.append(arrayBuf_19)
              mappedMsgFieldsArryBuffer = mappedMsgFieldsArryBuffer.append(arrayBuf_20)
              fixedMsgGetKeyStrBuf = fixedMsgGetKeyStrBuf.append(arrayBuf_21)

            } else if (typ.get.tType.toString().equals("tHashMap")) {

              assignCsvdata.append(newline + "//HashMap not handled at this momemt" + newline)
              assignJsondata.append(newline + "//HashMap not handled at this momemt" + newline)
            } else if (typ.get.tType.toString().equals("tTreeSet")) {

              assignCsvdata.append(newline + "//Tree Set not handled at this momemt" + newline)
              assignJsondata.append(newline + "//Tree Set not handled at this momemt" + newline)
            } else {
              val (baseTyp_1, baseTyp_2, baseTyp_3, baseTyp_4, baseTyp_5, baseTyp_6, baseTyp_7, baseTyp_8, baseTyp_9, baseTyp_10, baseTyp_11, baseTyp_12, baseTyp_13, baseTyp_14, baseTyp_15, baseTyp_16, baseTyp_17, baseTyp_18, baseTyp_19, baseTyp_20) = handleBaseTypes(keys, message.Fixed, typ, f, message.Version, childs, prevVerMsgBaseTypesIdxArry, recompile, mappedTypesABuf, firstTimeBaseType)
              scalaclass = scalaclass.append(baseTyp_1)
              assignCsvdata = assignCsvdata.append(baseTyp_2)
              assignJsondata = assignJsondata.append(baseTyp_3)
              assignXmldata = assignXmldata.append(baseTyp_4)
              list = baseTyp_5
              argsList = argsList ::: baseTyp_6
              addMsg = addMsg.append(baseTyp_7)
              keysStr = keysStr.append(baseTyp_8)
              typeImpl = typeImpl.append(baseTyp_9)
              jarset = jarset ++ baseTyp_10
              if (message.Fixed.toLowerCase().equals("true")) {
                serializedBuf = serializedBuf.append(baseTyp_11)
                deserializedBuf = deserializedBuf.append(baseTyp_12)
              } else if (message.Fixed.toLowerCase().equals("false")) {
                mappedSerBaseTypesBuf = mappedSerBaseTypesBuf.append(baseTyp_11)
                mappedDeserBaseTypesBuf = mappedDeserBaseTypesBuf.append(baseTyp_12)
              }

              prevDeserializedBuf = prevDeserializedBuf.append(baseTyp_13)
              convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append(baseTyp_14)
              mappedTypesABuf = baseTyp_15
              mappedPrevVerMatchkeys = mappedPrevVerMatchkeys.append(baseTyp_16)
              mappedPrevTypNotMatchkeys = mappedPrevTypNotMatchkeys.append(baseTyp_17)
              prevObjTypNotMatchDeserializedBuf = prevObjTypNotMatchDeserializedBuf.append(baseTyp_18)
              prevVerMsgBaseTypesIdxArry = baseTyp_19
              fixedMsgGetKeyStrBuf = fixedMsgGetKeyStrBuf.append(baseTyp_20)

              if (paritionkeys != null && paritionkeys.size > 0) {
                if (paritionkeys.contains(f.Name)) {
                  partitionPos = partitionPos :+ count
                }
              }
              if (primaryKeys != null && primaryKeys.size > 0) {
                if (primaryKeys.contains(f.Name)) {
                  primaryPos = primaryPos :+ count
                }
              }
              firstTimeBaseType = false
            }
            count = count + 1

          } else if ((f.ElemType.equals("Container")) || (f.ElemType.equals("Message"))) {

            if (f.ElemType.equals("Message"))
              throw new Exception("Adding Child Messages are not allowed in Message/Container Definition")

            //val typ = MdMgr.GetMdMgr.Type(f.Ttype, ftypeVersion, true) // message.Version.toLong
            val typ = MdMgr.GetMdMgr.Type(f.Ttype, -1, true) // message.Version.toLong

            if (typ.getOrElse("None").equals("None"))
              throw new Exception("Type not found in metadata for Name: " + f.Name + " , NameSpace: " + f.NameSpace + " , Version: " + message.Version + " , Type : " + f.Ttype)

            if (typ.getOrElse(null) != null) {
              if (typ.get.tType.toString().equals("tArrayBuf")) {
                val (arrayBuf_1, arrayBuf_2, arrayBuf_3, arrayBuf_4, arrayBuf_5, arrayBuf_6, arrayBuf_7, arrayBuf_8, arrayBuf_9, arrayBuf_10, arrayBuf_11, arrayBuf_12, arrayBuf_13, arrayBuf_14, arrayBuf_15, arrayBuf_16, arrayBuf_17, arrayBuf_18, arrayBuf_19, arrayBuf_20, arrayBuf_21) = handleArrayBuffer(keys, message, typ, f, childs, prevVerMsgBaseTypesIdxArry, recompile)
                scalaclass = scalaclass.append(arrayBuf_1)
                assignCsvdata = assignCsvdata.append(arrayBuf_2)
                assignJsondata = assignJsondata.append(arrayBuf_3)
                assignXmldata = assignXmldata.append(arrayBuf_4)
                list = arrayBuf_5
                argsList = argsList ::: arrayBuf_6
                addMsg = addMsg.append(arrayBuf_7)
                jarset = jarset ++ arrayBuf_8
                msgAndCntnrsStr = msgAndCntnrsStr.append(arrayBuf_9)
                keysStr = keysStr.append(arrayBuf_10)
                getMsg = getMsg.append(arrayBuf_11)
                serializedBuf = serializedBuf.append(arrayBuf_12)
                deserializedBuf = deserializedBuf.append(arrayBuf_13)
                prevDeserializedBuf = prevDeserializedBuf.append(arrayBuf_14)
                convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append(arrayBuf_15)
                collections = collections.append(arrayBuf_16)
                mappedPrevVerMatchkeys = mappedPrevVerMatchkeys.append(arrayBuf_17)
                mappedMsgFieldsVar = mappedMsgFieldsVar.append(arrayBuf_18)
                mappedPrevTypNotMatchkeys = mappedPrevTypNotMatchkeys.append(arrayBuf_19)
                mappedMsgFieldsArryBuffer = mappedMsgFieldsArryBuffer.append(arrayBuf_20)
                fixedMsgGetKeyStrBuf = fixedMsgGetKeyStrBuf.append(arrayBuf_21)
                arrayBuf_19

              } else if (typ.get.tType.toString().equals("tArray")) {

                val (arr_1, arr_2, arr_3, arr_4, arr_5, arr_6, arr_7, arr_8, arr_9, arr_10, arr_11, arr_12, arr_13, arr_14, arr_15, arr_16, arr_17, arr_18, arr_19) = handleArrayType(keys, typ, f, message, childs, prevVerMsgBaseTypesIdxArry, recompile)

                scalaclass = scalaclass.append(arr_1)
                assignCsvdata = assignCsvdata.append(arr_2)
                assignJsondata = assignJsondata.append(arr_3)

                assignXmldata = assignXmldata.append(arr_4)
                list = arr_5
                argsList = argsList ::: arr_6
                addMsg = addMsg.append(arr_7)
                jarset = jarset ++ arr_8
                msgAndCntnrsStr = msgAndCntnrsStr.append(arr_9)
                getMsg = getMsg.append(arr_10)
                serializedBuf = serializedBuf.append(arr_11)
                deserializedBuf = deserializedBuf.append(arr_12)

                prevDeserializedBuf = prevDeserializedBuf.append(arr_13)
                convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append(arr_14)
                collections = collections.append(arr_15)
                mappedPrevVerMatchkeys = mappedPrevVerMatchkeys.append(arr_16)
                mappedMsgFieldsArry = mappedMsgFieldsArry.append(arr_17)
                mappedPrevTypNotMatchkeys = mappedPrevTypNotMatchkeys.append(arr_18)
                fixedMsgGetKeyStrBuf = fixedMsgGetKeyStrBuf.append(arr_19)

                //       =  assignCsvdata.toString, assignJsondata.toString, assignXmldata.toString,  list, argsList, addMsg.toString)  = 
              } else {

                if (f.ElemType.equals("Container")) {

                  val (cntnr_1, cntnr_2, cntnr_3, cntnr_4, cntnr_5, cntnr_6, cntnr_7, cntnr_8, cntnr_9, cntnr_10, cntnr_11, cntnr_12, cntnr_13, cntnr_14, cntnr_15, cntnr_16) = handleContainer(message, mdMgr, ftypeVersion, f, recompile, childs)
                  scalaclass = scalaclass.append(cntnr_1)
                  assignCsvdata = assignCsvdata.append(cntnr_2)
                  assignJsondata = assignJsondata.append(cntnr_3)
                  assignXmldata = assignXmldata.append(cntnr_4)
                  list = cntnr_5
                  argsList = argsList ::: cntnr_6
                  addMsg = addMsg.append(cntnr_7)
                  jarset = jarset ++ cntnr_8
                  msgAndCntnrsStr = msgAndCntnrsStr.append(cntnr_9)
                  serializedBuf = serializedBuf.append(cntnr_10)
                  deserializedBuf = deserializedBuf.append(cntnr_11)
                  prevDeserializedBuf = prevDeserializedBuf.append(cntnr_12)
                  convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append(cntnr_13)
                  mappedPrevVerMatchkeys = mappedPrevVerMatchkeys.append(cntnr_14)
                  mappedPrevTypNotMatchkeys = mappedPrevTypNotMatchkeys.append(cntnr_15)

                  fixedMsgGetKeyStrBuf = fixedMsgGetKeyStrBuf.append(cntnr_16)
                  //   mappedPrevVerMatchkeys

                }
                /**
                 * Commenting the below code since the child messages are not allowed in message def.
                 *
                 */
                /*else if (f.ElemType.equals("Message")) {
                  val (msg_1, msg_2, msg_3, msg_4, msg_5, msg_6, msg_7, msg_8, msg_9, msg_10, msg_11, msg_12, msg_13, msg_14, msg_15) = handleMessage(mdMgr, ftypeVersion, f, message, childs, recompile)
                  scalaclass = scalaclass.append(msg_1)
                  assignCsvdata = assignCsvdata.append(msg_2)
                  assignJsondata = assignJsondata.append(msg_3)
                  assignXmldata = assignXmldata.append(msg_4)
                  list = msg_5
                  argsList = argsList ::: msg_6
                  addMsg = addMsg.append(msg_7)
                  jarset = jarset ++ msg_8
                  getMsg = getMsg.append(msg_9)
                  serializedBuf = serializedBuf.append(msg_10)
                  deserializedBuf = deserializedBuf.append(msg_11)
                  prevDeserializedBuf = prevDeserializedBuf.append(msg_12)
                  convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append(msg_13)
                  mappedPrevVerMatchkeys = mappedPrevVerMatchkeys.append(msg_14)
                  mappedPrevTypNotMatchkeys = mappedPrevTypNotMatchkeys.append(msg_15)

                  // mappedPrevVerMatchkeys

                }
                * */

              }
            }
          } else if (f.ElemType.equals("Concept")) {
            val (ccpt_1, ccpt_2, ccpt_3, ccpt_4, ccpt_5, ccpt_6, ccpt_7, ccpt_8, ccpt_9) = handleConcept(mdMgr, ftypeVersion, f)
            scalaclass = scalaclass.append(ccpt_1)
            assignCsvdata = assignCsvdata.append(ccpt_2)
            assignJsondata = assignJsondata.append(ccpt_3)
            assignXmldata = assignXmldata.append(ccpt_4)
            list = ccpt_5
            argsList = argsList ::: ccpt_6
            addMsg = addMsg.append(ccpt_7)
            jarset = jarset ++ ccpt_8
            fixedMsgGetKeyStrBuf = fixedMsgGetKeyStrBuf.append(ccpt_9)
          }

        }
      if (message.concepts != null) {

      }

      var partitionKeys: String = ""
      var prmryKeys: String = ""
      if (message.Fixed.toLowerCase().equals("false")) {

        //if (keysStr != null && keysStr.toString.trim != "")
          keysVarStr = getKeysStr(keysStr.toString)
        MsgsAndCntrsVarStr = getMsgAndCntnrs(msgAndCntnrsStr.toString)
        //typeImplStr = getTypeImplStr(typeImpl.toString)
        // mappedMsgConstructor = getAddMappedMsgsInConstructor(mappedMsgFieldsVar.toString)

        scalaclass.append(keysVarStr + newline + pad1 + typeImplStr + newline + pad1 + MsgsAndCntrsVarStr + newline + pad1 + getMappedBaseTypesArray(mappedTypesABuf) + newline)
        scalaclass.append(getAddMappedMsgsInConstructor(mappedMsgFieldsVar.toString) + getAddMappedArraysInConstructor(mappedMsgFieldsArry.toString, mappedMsgFieldsArryBuffer.toString) + getMappedMsgPrevVerMatchKeys(mappedPrevVerMatchkeys.toString) + mappedToStringForKeys)
        scalaclass.append(getMappedMsgPrevVerTypeNotMatchKeys(mappedPrevTypNotMatchkeys.toString) + mappedPrevObjTypNotMatchDeserializedBuf(prevObjTypNotMatchDeserializedBuf.toString))

        partitionKeys = if (message.PartitionKey != null && message.PartitionKey.size > 0) ("Array(" + message.PartitionKey.map(p => "toStringForKey(\"" + p.toLowerCase + "\")").mkString(", ") + ")") else ""
        prmryKeys = if (message.PrimaryKeys != null && message.PrimaryKeys.size > 0) ("Array(" + message.PrimaryKeys.map(p => "toStringForKey(\"" + p.toLowerCase + "\")").mkString(", ") + ")") else ""

      } else if (message.Fixed.toLowerCase().equals("true")) {
        partitionKeys = if (message.PartitionKey != null && message.PartitionKey.size > 0) ("Array(" + message.PartitionKey.map(p => p.toLowerCase + ".toString").mkString(", ") + ")") else ""
        prmryKeys = if (message.PrimaryKeys != null && message.PrimaryKeys.size > 0) ("Array(" + message.PrimaryKeys.map(p => p.toLowerCase + ".toString").mkString(", ") + ")") else ""
      }
      /*
      var partitionKeyStr = new StringBuilder(8 * 1024)
      if (message.PartitionKey != null)
        message.PartitionKey.foreach { p =>
          partitionKeyStr = partitionKeyStr.append("\"" + p.toLowerCase() + "\",")
        }
      var partitionKeys: String = ""
      if (partitionKeyStr != null && partitionKeyStr.toString.trim() != "")
        partitionKeys = "scala.Array(" + partitionKeyStr.substring(0, partitionKeyStr.toString.length() - 1) + ")"
*/

      scalaclass = scalaclass.append(partitionkeyStr(partitionKeys) + newline + primarykeyStr(prmryKeys) + newline + getsetMethods(message, fixedMsgGetKeyStrBuf.toString))

      if (getMsg != null && getMsg.toString.trim() != "" && getMsg.size > 5)
        getmessage = getMsg.toString.substring(0, getMsg.length - 5)
      if (addMsg != null && addMsg.toString.trim() != "" && addMsg.size > 5)
        addMessage = addMsg.toString.substring(0, addMsg.length - 5)

      log.debug("final arglist " + argsList)
      if (jarset != null)
        message.jarset = jarset

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }

    (scalaclass.toString, assignCsvdata.toString, assignJsondata.toString, assignXmldata.toString, count, list, argsList, addMessage, getmessage, partitionPos, primaryPos, serializedBuf.toString, deserializedBuf.toString, prevDeserializedBuf.toString, prevVerMsgObjstr, convertOldObjtoNewObjBuf.toString, collections.toString, mappedSerBaseTypesBuf.toString, mappedDeserBaseTypesBuf.toString)

  }

  //Mapped Messages For primary key adn parition key - value to String to avoid null pointer exception 

  private def mappedToStringForKeys() = {

    """
    private def toStringForKey(key: String): String = {
	  val field = fields.getOrElse(key, (-1, null))
	  if (field._2 == null) return ""
	  field._2.toString
	}
    """
  }
  // Default Array Buffer of message values in Mapped Messages
  private def getAddMappedMsgsInConstructor(mappedMsgFieldsVar: String): String = {
    if (mappedMsgFieldsVar == null || mappedMsgFieldsVar.trim() == "") return ""
    else return """
      AddMsgsInConstructor

    private def AddMsgsInConstructor: Unit = {
      """ + mappedMsgFieldsVar + """
    }
      """

  }
  // Default Array Buffer of primitive values in Mapped Messages

  private def getAddMappedArraysInConstructor(mappedArrayFieldsVar: String, mappedMsgFieldsArryBuffer: String): String = {
    if (mappedArrayFieldsVar == null || mappedArrayFieldsVar.trim() == "") return ""
    else return """
    AddArraysInConstructor

    private def AddArraysInConstructor: Unit = {
      """ + mappedArrayFieldsVar + """
      """ + mappedMsgFieldsArryBuffer + """
    }
      """

  }

  //Get the actve Message or container from Metadata 
  private def getRecompiledMsgContainerVersion(messagetype: String, namespace: String, name: String, mdMgr: MdMgr): String = {
    var msgdef: Option[ContainerDef] = null

    if (namespace == null || namespace.trim() == "")
      throw new Exception("Proper Namespace do not exists in message/container definition")
    if (name == null || name.trim() == "")
      throw new Exception("Proper Name do not exists in message")
    if (messagetype == null || messagetype.trim() == "")
      throw new Exception("Proper Version do not exists in message/container definition")

    try {

      if (messagetype.toLowerCase().equals("message")) {
        msgdef = mdMgr.Message(namespace, name, -1, false)
        val isMsg = true
      } else if (messagetype.toLowerCase().equals("container")) {
        msgdef = mdMgr.Container(namespace, name, -1, false)
      }

    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    log.debug("version from metadata " + msgdef.get.Version)

    var newver = msgdef.get.Version + 1

    MdMgr.ConvertLongVersionToString(newver)
  }

  //Get the previous Existing Message/continer from metadata for deserialize function purpose

  private def getPrevVersionMsgContainer(message: Message, mdMgr: MdMgr): (Boolean, ContainerDef, String) = {
    var msgdef: ContainerDef = null
    var msgdefobj: Option[ContainerDef] = null
    var prevVerMsgObjstr: String = ""
    var childs: ArrayBuffer[(String, String)] = ArrayBuffer[(String, String)]()
    var isMsg: Boolean = false
    try {
      val messagetype = message.msgtype
      val namespace = message.NameSpace
      val name = message.Name
      val ver = message.Version

      if (namespace == null || namespace.trim() == "")
        throw new Exception("Proper Namespace do not exists in message/container definition")
      if (name == null || name.trim() == "")
        throw new Exception("Proper Name do not exists in message")
      if (ver == null || ver.trim() == "")
        throw new Exception("Proper Version do not exists in message/container definition")

      if (messagetype != null && messagetype.trim() != "") {

        if (messagetype.toLowerCase().equals("message")) {
          msgdefobj = mdMgr.Message(namespace, name, -1, false)
          val isMsg = true
        } else if (messagetype.toLowerCase().equals("container")) {
          msgdefobj = mdMgr.Container(namespace, name, -1, false)
        }

        msgdefobj match {
          case None =>
            msgdef = null
          case Some(m) =>
            if (isMsg)
              msgdef = m.asInstanceOf[MessageDef]
            else
              msgdef = m.asInstanceOf[ContainerDef]
            val fullname = msgdef.FullNameWithVer.replaceAll("[.]", "_")
            prevVerMsgObjstr = msgdef.PhysicalName
        }
      }

    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }

    (isMsg, msgdef, prevVerMsgObjstr)

  }

  private def getPrevVerMsgChilds(pMsgdef: ContainerDef, isMsg: Boolean, fixed: String): (Map[String, Any], ArrayBuffer[String]) = {
    var prevVerCtrdef: ContainerDef = new ContainerDef()
    var prevVerMsgdef: MessageDef = new MessageDef()
    var childs: Map[String, Any] = Map[String, Any]()
    var prevVerMsgBaseTypesIdxArry = new ArrayBuffer[String]

    if (pMsgdef != null) {

      if (isMsg) {
        prevVerCtrdef = pMsgdef.asInstanceOf[MessageDef]
      } else {
        prevVerCtrdef = pMsgdef.asInstanceOf[ContainerDef]
      }
      if (fixed.toLowerCase().equals("true")) {
        val memberDefs = prevVerCtrdef.containerType.asInstanceOf[StructTypeDef].memberDefs
        if (memberDefs != null) {
          childs ++= memberDefs.filter(a => (a.isInstanceOf[AttributeDef])).map(a => (a.Name, a))
        }
      } else if (fixed.toLowerCase().equals("false")) {
        val attrMap = prevVerCtrdef.containerType.asInstanceOf[MappedMsgTypeDef].attrMap
        if (attrMap != null) {

          attrMap.foreach(a => {
            val attributes = a._2.asInstanceOf[AttributeDef].aType
            if (attributes != null)
              if (attributes.implementationName != null && attributes.tTypeType.toString().toLowerCase().equals("tscalar") && !prevVerMsgBaseTypesIdxArry.contains(a._2.asInstanceOf[AttributeDef].aType.implementationName))
                prevVerMsgBaseTypesIdxArry += a._2.asInstanceOf[AttributeDef].aType.implementationName;
          })
          childs ++= attrMap.filter(a => (a._2.isInstanceOf[AttributeDef])).map(a => (a._2.Name, a._2))
        }
      }
    }
    (childs, prevVerMsgBaseTypesIdxArry)
  }

  /* private def getPrevVerMappedMsgChilds(pMsgdef: ContainerDef, isMsg: Boolean, fixed: String): Map[String, Any] = {
    var prevVerCtrdef: ContainerDef = new ContainerDef()
    var prevVerMsgdef: MessageDef = new MessageDef()
    var childs: Map[String, Any] = Map[String, Any]()

    if (pMsgdef != null) {

      if (isMsg) {
        prevVerCtrdef = pMsgdef.asInstanceOf[MessageDef]
      } else {
        prevVerCtrdef = pMsgdef.asInstanceOf[ContainerDef]
      }
      if (fixed.toLowerCase().equals("false")) {
        val memberDefs = prevVerCtrdef.containerType.asInstanceOf[MappedMsgTypeDef].attrMap
        children ++= attrMap.filter(a => (a._2.isInstanceOf[AttributeDef] && (a._2.asInstanceOf[AttributeDef].aType.isInstanceOf[MappedMsgTypeDef] || a._2.asInstanceOf[AttributeDef].aType.isInstanceOf[StructTypeDef]))).map(a => (a._2.Name, a._2.asInstanceOf[AttributeDef]))

        if (memberDefs != null) {
          //    childs ++= memberDefs.filter(a => (a.isInstanceOf[MappedMsgTypeDef])).map(a => (a.Name, a))
        }
      }
    }
    childs
  }
  * 
  */

  /**
   * For Mapped messages - converversion to Current obj
   *
   */

  private def getConvertOldVertoNewVer() = {
    """
     oldObj.fields.foreach(field => {
         if(field._2._1 >= 0)
       
    	   fields(field._1) = (field._2._1, field._2._2);
     })
    
    """

  }

  /*
   * function to convert the old version to new version in desrializarion of messages especially when ArrayBuffer/Array of child messages or Child Message occurs
   */
  private def getConvertOldVertoNewVer(convertStr: String, oldObj: String, newObj: Any): String = {
    var convertFuncStr: String = ""
    // if (prevObjExists) {
    if (oldObj != null && oldObj.toString.trim() != "") {
      if (convertStr != null && convertStr.trim() != "") {

        convertFuncStr = """
     def ConvertPrevToNewVerObj(oldObj : """ + oldObj + """) : Unit = {    
         if( oldObj != null){
           """ + convertStr + """
         }  
       }"""
        //    }
      }
    } else {
      convertFuncStr = """
   def ConvertPrevToNewVerObj(obj : Any) : Unit = { }
   """
    }

    convertFuncStr
  }

  //this method is particulraly for Mapped Messages - a variable in mapped message with key name match and types not match 
  private def getMappedMsgPrevVerTypeNotMatchKeys(typeNotMatchKeys: String): String = {

    if (typeNotMatchKeys != null && typeNotMatchKeys.trim != "")
      " val prevVerTypesNotMatch = Array(" + typeNotMatchKeys.toString.substring(0, typeNotMatchKeys.toString.length - 1) + ") \n "
    else
      " val prevVerTypesNotMatch = Array(\"\") \n "
  }

  //this method is particulraly for Mapped Messages - a variable in mapped message with key name and types also match 

  private def getMappedMsgPrevVerMatchKeys(matchKeys: String): String = {

    if (matchKeys != null && matchKeys.trim != "")
      " val prevVerMatchKeys = Array(" + matchKeys.toString.substring(0, matchKeys.toString.length - 1) + ") \n "
    else
      " val prevVerMatchKeys = Array(\"\") \n "
  }

  private def getKeysStr(keysStr: String) = {
    println("keysStrkeysStrkeysStrkeysStr    "+keysStr)
    if (keysStr != null && keysStr.trim() != "") {

      """ 
      var keys = Map(""" + keysStr.toString.substring(0, keysStr.toString.length - 1) + ") \n " +
        """
      var fields: scala.collection.mutable.Map[String, (Int, Any)] = new scala.collection.mutable.HashMap[String, (Int, Any)];
	"""
    } else {
      """ 
	    var keys = Map[String, Int]()
	    var fields: scala.collection.mutable.Map[String, (Int, Any)] = new scala.collection.mutable.HashMap[String, (Int, Any)];
	  
	  """
    }
  }
  //Messages and containers in Set for mapped messages to poplulate the data

  private def getMsgAndCntnrs(msgsAndCntnrs: String): String = {
    var str = ""
    if (msgsAndCntnrs != null && msgsAndCntnrs.toString.trim != "")
      str = "val messagesAndContainers = Set(" + msgsAndCntnrs.toString.substring(0, msgsAndCntnrs.toString.length - 1) + ") \n "
    else
      str = "val messagesAndContainers = Set(\"\") \n "

    str
  }

  //Messages and containers in Set for mapped messages to poplulate the data

  private def getCollectionsMapped(collections: String): String = {
    var str = ""
    if (collections != null && collections.toString.trim != "")
      str = "val collectionTypes = Set(" + collections.toString.substring(0, collections.toString.length - 1) + ") \n "
    else
      str = "val collectionTypes = Set(\"\") \n "

    str
  }

  private def getIsFixed(message: Message): String = {
    val pad1 = "\t"
    val pad2 = "\t\t"
    val pad3 = "\t\t\t"
    val newline = "\n"
    val isf: String = "false"
    val isKV: String = "true"
    var isfixed = new StringBuilder(8 * 1024)
    if (message.Fixed.equals("true"))
      isfixed = isfixed.append("%soverride def IsFixed:Boolean = %s;%s%soverride def IsKv:Boolean = %s;%s".format(pad1, message.Fixed, newline, pad1, isf, newline))
    else
      isfixed = isfixed.append("%soverride def IsFixed:Boolean = %s;%s%soverride def IsKv:Boolean = %s;%s".format(pad1, isf, newline, pad1, isKV, newline))

    isfixed.toString
  }

  private def getIsFixedCls(message: Message): String = {
    val pad1 = "\t"
    val newline = "\n"
    var isfixed = new StringBuilder(8 * 1024)
    isfixed = isfixed.append("%soverride def IsFixed : Boolean = %s.IsFixed;%s%soverride def IsKv : Boolean = %s.IsKv;%s".format(pad1, message.Name, newline, pad1, message.Name, newline))
    isfixed.toString
  }

  private def getCanPersist(message: Message): String = {
    val pad1 = "\t"
    val newline = "\n"

    var CanPersist = new StringBuilder(8 * 1024)
    if (message.Persist)
      CanPersist = CanPersist.append("%s override def CanPersist: Boolean = %s;%s".format(pad1, "true", newline))
    else
      CanPersist = CanPersist.append("%s override def CanPersist: Boolean = %s;%s".format(pad1, "false", newline))

    CanPersist.toString
  }
  private def getCanPersistCls(message: Message): String = {
    val pad1 = "\t"
    val newline = "\n"
    var CanPersist = new StringBuilder(8 * 1024)
    CanPersist = CanPersist.append("%s override def CanPersist: Boolean = %s.CanPersist;%s".format(pad1, message.Name, newline))
    CanPersist.toString
  }

  private def getNameVerCls(msg: Message): String = {

    "\toverride def FullName: String = " + msg.Name.trim() + ".FullName" + "\n" +
      "\toverride def NameSpace: String = " + msg.Name.trim() + ".NameSpace" + "\n" +
      "\toverride def Name: String = " + msg.Name.trim() + ".Name" + "\n" +
      "\toverride def Version: String = " + msg.Name.trim() + ".Version" + "\n" +
      "\tdef this(from: " + msg.Name.trim() + ") = { this}" + "\n"

  }

  private def getArrayStr(mbrVar: String, classname: String): String = {

    "\t\tfor (i <- 0 until " + mbrVar + ".length) {\n" +
      "\t\t\tvar ctrVar: " + classname + " = i.asInstanceOf[" + classname + "]\n\t\t\t" +
      """try {
          		if (ctrVar != null)
          			ctrVar.populate(inputdata)
            } catch {
            	case e: Exception => {
            	e.printStackTrace()
            	throw e
            	}
            }
        }
    """
  }

  private def importStmts(msg: Message): (String, String) = {
    var imprt: String = ""
    if (msg.msgtype.equals("Message"))
      imprt = "import com.ligadata.FatafatBase.{BaseMsg, BaseMsgObj, TransformMessage, BaseContainer, MdBaseResolveInfo, MessageContainerBase, RDDObject, RDD, TimeRange}"
    else if (msg.msgtype.equals("Container"))
      imprt = "import com.ligadata.FatafatBase.{BaseMsg, BaseContainer, BaseContainerObj, MdBaseResolveInfo, MessageContainerBase, RDDObject, RDD, TimeRange}"
    var nonVerPkg = "package " + msg.pkg + "." + msg.NameSpace +  "\n"
    var verPkg = "package " + msg.pkg + "." + msg.NameSpace + ".V" + MdMgr.ConvertVersionToLong(msg.Version).toString + "\n"
    var otherImprts = """
  
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.Formats
import scala.xml.XML
import scala.xml.Elem
import com.ligadata.FatafatBase.{InputData, DelimitedData, JsonData, XmlData}
import com.ligadata.BaseTypes._
import com.ligadata.FatafatBase.SerializeDeserialize
import java.io.{ DataInputStream, DataOutputStream , ByteArrayOutputStream}

"""
    val versionPkgImport = verPkg + otherImprts + imprt
    val nonVerPkgImport = nonVerPkg + otherImprts + imprt
    (versionPkgImport, nonVerPkgImport)
  }

  private def classname(msg: Message, recompile: Boolean): (StringBuilder, StringBuilder, String) = {
    var sname: String = ""
    var oname: String = ""
    var clssb: StringBuilder = new StringBuilder()
    var objsb: StringBuilder = new StringBuilder()
    val ver = MdMgr.ConvertVersionToLong(msg.Version).toString
    val xtends: String = "extends"
    val space = " "
    val uscore = "_"
    val cls = "class"
    val obj = "object"
    if (msg.msgtype.equals("Message")) {
      oname = "BaseMsgObj with RDDObject[" + msg.Name + "]{"
      sname = "BaseMsg {"
    } else if (msg.msgtype.equals("Container")) {
      oname = "BaseContainerObj with RDDObject[" + msg.Name + "]{"
      sname = "BaseContainer {"
    }
    //val clsname = msg.NameSpace + uscore + msg.Name + uscore + ver + uscore + msg.ClsNbr
    //val clsstr = cls + space + msg.NameSpace + uscore + msg.Name + uscore + ver + uscore + msg.ClsNbr + space + xtends + space + sname
    //val objstr = obj + space + msg.NameSpace + uscore + msg.Name + uscore + ver + uscore + msg.ClsNbr + space + xtends + space + oname
    val clsname = msg.Name
    val clsstr = cls + space + msg.Name + space + xtends + space + sname
    val objstr = obj + space + msg.Name + space + xtends + space + oname

    (clssb.append(clsstr), objsb.append(objstr), clsname)
  }

  //trait - BaseMsg	
  def traitBaseMsg = {
    """
trait BaseMsg {
	def Fixed:Boolean;
}
	  """
  }

  private def traitBaseContainer = {
    """
trait BaseContainer {
  def Fixed: Boolean
  def populate(inputdata: InputData): Unit
  def get(key: String): Any
  def getOrElse(key: String, default: Any): Any
  def set(key: String, value: Any): Unit
}
	  """
  }

  /* private def partitionkeyStrObj(message: Message): String = {

    val partitionKeys = if (message.PartitionKey != null) ("Array(\"" + message.PartitionKey.map(p => p.toLowerCase).mkString(", ") + "\")") else ""

    if (partitionKeys != null && partitionKeys.trim() != "")
      "\n	val partitionKeys : Array[String] = " + partitionKeys
    else
      ""
  }

  private def primarykeyStrObj(message: Message): String = {
    val prmryKeys = if (message.PrimaryKeys != null) ("Array(\"" + message.PrimaryKeys.map(p => p.toLowerCase).mkString(", ") + "\")") else ""

    if (prmryKeys != null && prmryKeys.trim() != "")
      "\n	val primaryKeys : Array[String] = " + prmryKeys
    else
      ""
  }
  * 
  * */

  private def partitionkeyStr(paritionKeys: String): String = {
    if (paritionKeys != null && paritionKeys.trim() != "")
      "\n	override def PartitionKeyData: Array[String] = " + paritionKeys
    else
      "\n	override def PartitionKeyData: Array[String] = Array[String]()"
  }

  private def primarykeyStr(primaryKeys: String): String = {
    if (primaryKeys != null && primaryKeys.trim() != "")
      "\n	override def PrimaryKeyData: Array[String] = " + primaryKeys
    else
      "\n	override def PrimaryKeyData: Array[String] = Array[String]()"
  }

  private def partitionkeyStrMapped(message: Message, partitionPos: Array[Int]): String = {
    val pad1 = "\t"
    var partitionStr = new StringBuilder
    partitionStr.append("Array(")
    for (p <- partitionPos) {
      partitionStr.append(p + ",")
    }
    var partitionString = partitionStr.substring(0, partitionStr.length - 1)
    partitionString = partitionString + ")"

    val partitionKeys = if (message.PartitionKey != null && message.PartitionKey.size > 0) ("Array(\"" + message.PartitionKey.map(p => p.toLowerCase).mkString("\", \"") + "\")") else ""

    if (partitionKeys != null && partitionKeys.trim() != "")
      "\n	val partitionKeys : Array[String] = " + partitionKeys + "\n    val partKeyPos = " + partitionString.toString + "\n " + pad1 + "override def PartitionKeyData: Array[String] =  partitionKeys.map(p => { fields.getOrElse(p, \"\").toString})  "
    else
      "\n   override def PartitionKeyData(inputdata:InputData): Array[String] = Array[String]()"

  }

  private def primarykeyStrMapped(message: Message, primaryPos: Array[Int]): String = {
    val prmryKeys = if (message.PrimaryKeys != null && message.PrimaryKeys.size > 0) ("Array(\"" + message.PrimaryKeys.map(p => p.toLowerCase).mkString("\", \"") + "\")") else ""
    val pad1 = "\t"
    var primaryStr = new StringBuilder
    log.debug("primaryPos " + primaryPos.length)
    primaryStr.append("Array(")
    for (p <- primaryPos) {
      primaryStr.append(p + ",")
    }
    var primaryString = primaryStr.substring(0, primaryStr.length - 1)
    primaryString = primaryString + ")"

    if (prmryKeys != null && prmryKeys.trim() != "")
      "\n	val primaryKeys : Array[String] = " + prmryKeys + "\n    val prmryKeyPos = " + primaryString + "\n" + pad1 + "\n	override def PrimaryKeyData: Array[String] =  primaryKeys.map(p => { fields.getOrElse(p, \"\").toString})  "
    else
      "\n    override def PrimaryKeyData(inputdata:InputData): Array[String] = Array[String]()"

  }

  private def inputData = {
    """ 
trait InputData {
	var dataInput: String
}

class DelimitedData(var dataInput: String, var dataDelim: String) extends InputData(){ }
class JsonData(var dataInput: String) extends InputData(){ }
class XmlData(var dataInput: String) extends InputData(){ }
	  """
  }

  private def getsetMethods(msg: Message, fixedMsgGetKeyStr: String): String = {

    val uscore = "_"
    val ver = MdMgr.ConvertVersionToLong(msg.Version).toString
    var getsetters = new StringBuilder(8 * 1024)
    val clsname = msg.NameSpace + uscore + msg.Name + uscore + ver + uscore + msg.ClsNbr
    if (msg.Fixed.toLowerCase().equals("true")) {
      getsetters = getsetters.append("""
    override def set(key: String, value: Any): Unit = { throw new Exception("set function is not yet implemented") }
   
    override def get(key: String): Any = {
    	try {
    		  // Try with reflection
    		  return getWithReflection(key)
    	} catch {
    		  case e: Exception => {
    		  // Call By Name
             return getByName(key)
    		  }
    	}
    }
    override def getOrElse(key: String, default: Any): Any = { throw new Exception("getOrElse function is not yet implemented") }
    
    private def getByName(key: String): Any = {
    	try {
	    """ + fixedMsgGetKeyStr +
        """
		     // if (key.equals("desynpuf_id")) return desynpuf_id;
		      //if (key.equals("clm_id")) return clm_id;
		      return null;
		    } catch {
		      case e: Exception => {
		        e.printStackTrace()
		        throw e
		      }
		    }
		  }

		private def getWithReflection(key: String): Any = {
		  val ru = scala.reflect.runtime.universe
		  val m = ru.runtimeMirror(getClass.getClassLoader)
		  val im = m.reflect(this)
		  val fieldX = ru.typeOf[""" + clsname + """].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
	  val fmX = im.reflectField(fieldX)
	  fmX.get
	}
    """)
    } else if (msg.Fixed.toLowerCase().equals("false")) {

      getsetters = getsetters.append("""
    override def set(key: String, value: Any): Unit = {
	  if (key == null) throw new Exception(" key should not be null in set method")
	  fields.put(key, (-1, value))
    }
    override def get(key: String): Any = {
      fields.get(key) match {
    	 case Some(f) => {
    		  //println("Key : "+ key + " Idx " + f._1 +" Value" + f._2 )
    		  return f._2
    	}
    	case None => {
    		  //println("Key - value null : "+ key )
    		  return null
    	  }
    	}
     // fields.get(key).get._2
    }

    override def getOrElse(key: String, default: Any): Any = {
      fields.getOrElse(key, (-1, default))._2
    }
    """)

    }
    getsetters.toString
  }

  /**
   * getPartition Key Data for Message Defitnition Object   *
   *
   */

  def getPartitionKeyDef(): String = {

    var getParitionData = new StringBuilder(8 * 1024)

    getParitionData = getParitionData.append("""
        
    override def PartitionKeyData(inputdata:InputData): Array[String] = {
    	if (partKeyPos.size == 0 || partitionKeys.size == 0)
    		return Array[String]()
    	if (inputdata.isInstanceOf[DelimitedData]) {
    		val csvData = inputdata.asInstanceOf[DelimitedData]
    			if (csvData.tokens == null) {
            		return partKeyPos.map(pos => "")
            	}
    		return partKeyPos.map(pos => csvData.tokens(pos))
    	} else if (inputdata.isInstanceOf[JsonData]) {
    		val jsonData = inputdata.asInstanceOf[JsonData]
    		val mapOriginal = jsonData.cur_json.get.asInstanceOf[Map[String, Any]]
    		if (mapOriginal == null) {
    			return partKeyPos.map(pos => "")
    		}
    		val map: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map[String, Any]()
    		mapOriginal.foreach(kv => { map(kv._1.toLowerCase()) = kv._2 })
    		return partitionKeys.map(key => map.getOrElse(key, "").toString)
    	} else if (inputdata.isInstanceOf[XmlData]) {
    		val xmlData = inputdata.asInstanceOf[XmlData]
                    // Fix this
    	} else throw new Exception("Invalid input data")
    	return Array[String]()
    }
    """)

    getParitionData.toString
  }

  def getPrimaryKeyDef(): String = {

    var getPrimaryKeyData = new StringBuilder(8 * 1024)

    getPrimaryKeyData = getPrimaryKeyData.append("""
	  
    override def PrimaryKeyData(inputdata:InputData): Array[String] = {
    	if (prmryKeyPos.size == 0 || primaryKeys.size == 0)
    		return Array[String]()
    	if (inputdata.isInstanceOf[DelimitedData]) {
    		val csvData = inputdata.asInstanceOf[DelimitedData]
    		if (csvData.tokens == null) {
            	return prmryKeyPos.map(pos => "")
            }
    		return prmryKeyPos.map(pos => csvData.tokens(pos))
    	} else if (inputdata.isInstanceOf[JsonData]) {
    		val jsonData = inputdata.asInstanceOf[JsonData]
    		val mapOriginal = jsonData.cur_json.get.asInstanceOf[Map[String, Any]]
    		if (mapOriginal == null) {
    			return prmryKeyPos.map(pos => "")
    		}
    		val map: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map[String, Any]()
    		mapOriginal.foreach(kv => { map(kv._1.toLowerCase()) = kv._2 })

    		return primaryKeys.map(key => map.getOrElse(key, "").toString)
    	} else if (inputdata.isInstanceOf[XmlData]) {
    		val xmlData = inputdata.asInstanceOf[XmlData]
                    // Fix this
    	} else throw new Exception("Invalid input data")
    	return Array[String]()
    }
    """)

    getPrimaryKeyData.toString
  }

  // For Mapped messages to track the type of field assign specific value for each type

  private def getBaseTypeID(valType: String): Int = {
    valType match {
      case "string" => 0
      case "int" => 1
      case "float" => 2
      case "boolean" => 3
      case "double" => 4
      case "long" => 5
      case "char" => 6
      case _ => -1

    }
  }

  private def getMappedBaseTypesArray(arrayBuf: ArrayBuffer[String]): String = {
    var string = arrayBuf.toString
    var mappedBaseTypes = new StringBuilder(8 * 1024)
    var mappedBaseTypes1 = new StringBuilder(8 * 1024)

    if (arrayBuf.size > 0) {
      arrayBuf.foreach(t => {
        mappedBaseTypes = mappedBaseTypes.append(t + ",")
        mappedBaseTypes1 = mappedBaseTypes1.append("\"" + t + "\",")
      })

      return "val typs = Array(" + mappedBaseTypes.toString.substring(0, mappedBaseTypes.toString.length() - 1) + "); \n\t val typsStr = Array(" + mappedBaseTypes1.toString.substring(0, mappedBaseTypes1.toString.length() - 1) + "); "

    } else {
      return ""
    }
  }

  //input function conversion
  private def getDefVal(valType: String): String = {
    valType match {
      case "int" => "0"
      case "float" => "0"
      case "boolean" => "false"
      case "double" => "0.0"
      case "long" => "0"
      case "char" => "\' \'"
      case "string" => "\"\""
      case _ => ""
    }
  }

  private def getMemberType(valType: String): String = {
    valType match {
      case "system.int" => "Int"
      case "system.float" => "Float"
      case "system.boolean" => "Boolean"
      case "system.double" => "Double"
      case "system.long" => "Long"
      case "system.char" => "OptChar"
      case "system.string" => "String"
      case _ => ""
    }
  }

  private def cSetter() = ""
  /*{
    """
    def get(key: String): Any ={
		null
	}
	def getOrElse(key: String, default: Any): Any ={
	    null
	}
	def set(key: String, value: Any): Unit = {
	    null
	}
    """
  }*/
  //populate method in msg-TransactionMsg class
  private def populate = {
    """
  def populate(inputdata:InputData) = {
	  if (inputdata.isInstanceOf[DelimitedData])	
		populateCSV(inputdata.asInstanceOf[DelimitedData])
	  else if (inputdata.isInstanceOf[JsonData])
			populateJson(inputdata.asInstanceOf[JsonData])
	  else if (inputdata.isInstanceOf[XmlData])
			populateXml(inputdata.asInstanceOf[XmlData])
	  else throw new Exception("Invalid input data")
    
    }
		"""
  }

  /**
   * //populateCSV fucntion in meg class
   * def populatecsv = {
   * """
   * private def populateCSV(inputdata:DelimitedData): Unit = {
   * inputdata.curPos = assignCsv(inputdata.tokens, inputdata.curPos)
   * }
   * """
   * }
   */
  private def populateMappedCSV(assignCsvdata: String, count: Int): String = {
    """
  private def populateCSV(inputdata:DelimitedData): Unit = {
	val list = inputdata.tokens
    val arrvaldelim = "~"
	try{
""" + "\t\tif(list.size < " + count + ") throw new Exception(\"Incorrect input data size\")" + """
  """ + assignCsvdata +
      """
 	}catch{
		case e:Exception =>{
			e.printStackTrace()
  			throw e
		}
	}
  }
	  """
  }

  ////populateCSV fucntion in msg class
  private def populatecsv(assignCsvdata: String, count: Int): String = {
    """
	  private def populateCSV(inputdata:DelimitedData): Unit = {
		val list = inputdata.tokens
	    val arrvaldelim = "~"
		try{
	""" + "\t\tif(list.size < " + count + ") throw new Exception(\"Incorrect input data size\")" + """
	  """ + assignCsvdata +
      """
	 	}catch{
			case e:Exception =>{
				e.printStackTrace()
	  			throw e
			}
		}
	  }
		  """
  }

  private def populateJson = {
    """
  private def populateJson(json:JsonData) : Unit = {
	try{
         if (json == null || json.cur_json == null || json.cur_json == None) throw new Exception("Invalid json data")
     	 assignJsonData(json)
	}catch{
	    case e:Exception =>{
   	    	e.printStackTrace()
   	  		throw e	    	
	  	}
	  }
	}
	  """
  }

  private def collectionsStr = {
    """
    def CollectionAsArrString(v: Any): Array[String] = {
	  if (v.isInstanceOf[Set[_]]) {
	  	return v.asInstanceOf[Set[String]].toArray
	  }
	  if (v.isInstanceOf[List[_]]) {
	  	return v.asInstanceOf[List[String]].toArray
	  }
	  if (v.isInstanceOf[Array[_]]) {
     	return v.asInstanceOf[Array[String]].toArray
	  }
	  throw new Exception("Unhandled Collection")
	 }
    """

  }
  private def assignJsonData(assignJsonData: String) = {
    collectionsStr +
      """
    private def assignJsonData(json: JsonData) : Unit =  {
	type tList = List[String]
	type tMap = Map[String, Any]
	var list : List[Map[String, Any]] = null 
	try{ 
	  	val mapOriginal = json.cur_json.get.asInstanceOf[Map[String, Any]]
       	if (mapOriginal == null)
        	throw new Exception("Invalid json data")
       
       	val map : scala.collection.mutable.Map[String, Any] =  scala.collection.mutable.Map[String, Any]()
       	mapOriginal.foreach(kv => {map(kv._1.toLowerCase()) = kv._2 } )      
    
	  """ + assignJsonData +
      """
	  }catch{
  			case e:Exception =>{
   				e.printStackTrace()
   			throw e	    	
	  	}
	}
  }
	"""
  }
  private def assignMappedJsonData(assignJsonData: String) = {
    collectionsStr +
      """
   	def ValueToString(v: Any): String = {
		if (v.isInstanceOf[Set[_]]) {
      		return v.asInstanceOf[Set[_]].mkString(",")
	  	}
	  	if (v.isInstanceOf[List[_]]) {
      		return v.asInstanceOf[List[_]].mkString(",")
	  	}
	  	if (v.isInstanceOf[Array[_]]) {
      		return v.asInstanceOf[Array[_]].mkString(",")
	  	}
	  	v.toString
	}
    
   private def assignJsonData(json: JsonData) : Unit =  {
	type tList = List[String]
	type tMap = Map[String, Any]
	var list : List[Map[String, Any]] = null 
    var keySet: Set[Any] = Set();
	try{
	   val mapOriginal = json.cur_json.get.asInstanceOf[Map[String, Any]]
       if (mapOriginal == null)
         throw new Exception("Invalid json data")
       
       val map : scala.collection.mutable.Map[String, Any] =  scala.collection.mutable.Map[String, Any]()
       mapOriginal.foreach(kv => {map(kv._1.toLowerCase()) = kv._2 } )      
    
	  	var msgsAndCntrs : scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map[String, Any]()
    
	  	// Traverse through whole map and make KEYS are lowercase and populate
	  	map.foreach(kv => {
        	val key = kv._1.toLowerCase
        	val typConvidx = keys.getOrElse(key, -1)
        	if (typConvidx > 0) {
          	// Cast to correct type
          	val v1 = typs(typConvidx).Input(kv._2.toString)
	  		// println("==========v1"+v1)
          	fields.put(key, (typConvidx, v1))
        } else { // Is this key is a message or container???
          if (messagesAndContainers(key))
            msgsAndCntrs.put(key, kv._2)
          else if (collectionTypes(key)) {
            // BUGBUG:: what to dfo?
          } else
            fields.put(key, (0, ValueToString(kv._2)))
        }
      })
    """ + assignJsonData +
      """
     // fields.foreach(field => println("Key : "+ field._1 + "Idx " + field._2._1 +"Value" + field._2._2 ))
   
	  } catch {
      	case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }
  """
  }

  private def addInputJsonLeftOverKeys = {
    """
        var dataKeySet: Set[Any] = Set();
        dataKeySet = dataKeySet ++ map.keySet
        dataKeySet = dataKeySet.diff(keySet)
        var lftoverkeys: Array[Any] = dataKeySet.toArray
        for (i <- 0 until lftoverkeys.length) {
        	val v = map.getOrElse(lftoverkeys(i).asInstanceOf[String], null)
         	if (v != null)
          	fields.put(lftoverkeys(i).asInstanceOf[String], v)
      	}
   """
  }
  private def populateXml = {
    """
  private def populateXml(xmlData:XmlData) : Unit = {	  
	try{
    	val xml = XML.loadString(xmlData.dataInput)
    	assignXml(xml)
	} catch{
		case e:Exception =>{
   			e.printStackTrace()
   	  		throw e	    	
    	}
	}
  }
	  """
  }

  private def assignXmlData(xmlData: String) = {
    """
  private def populateXml(xmlData:XmlData) : Unit = {
	try{
	  val xml = XML.loadString(xmlData.dataInput)
	  if(xml == null) throw new Exception("Invalid xml data")
""" + xmlData +
      """
	}catch{
	  case e:Exception =>{
	    e.printStackTrace()
		throw e	    	
	  }
   	}
  }
"""
  }

  private def assignMappedXmlData(xmlData: String) = {
    """
   private def populateXml(xmlData:XmlData) : Unit = {
	try{
	  //XML Population is not handled at this time      
     
	}catch{
	  case e:Exception =>{
	    e.printStackTrace()
		throw e	    	
	  }
   	}
  }
"""
  }

  //creates the message class file
  private def createScalaFile(scalaClass: String, version: String, className: String): Unit = {
    try {
      val writer = new PrintWriter(new File("/tmp/" + className + "_" + version + ".scala"))
      writer.write(scalaClass.toString)
      writer.close()
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }

  def processMsgDef(jsonstr: String, msgDfType: String, mdMgr: MdMgr, recompile: Boolean = false): (String, ContainerDef, String) = {
    var classname: String = null
    var ver: String = null
    var classstr_1: String = null
    var nonVerClassStrVal_1: String = null
    var containerDef: ContainerDef = null
    try {
      if (mdMgr == null)
        throw new Exception("MdMgr is not found")
      if (msgDfType.equals("JSON")) {
        var message: Message = null
        message = processJson(jsonstr, mdMgr, recompile).asInstanceOf[Message]

        if (message.Fixed.equals("true")) {
          val (classStrVal, containerDefVal, nonVerClassStrVal) = createFixedMsgClass(message, mdMgr, recompile)
          classstr_1 = classStrVal
          containerDef = containerDefVal
          nonVerClassStrVal_1 = nonVerClassStrVal
        } else if (message.Fixed.equals("false")) {
          val (classStrVal, containerDefVal, nonVerClassStrVal) = createMappedMsgClass(message, mdMgr, recompile)
          classstr_1 = classStrVal
          nonVerClassStrVal_1 = nonVerClassStrVal
          containerDef = containerDefVal
        }

      } else throw new Exception("MsgDef Type JSON is only supported")
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (classstr_1, containerDef, nonVerClassStrVal_1)
  }

  private def createFixedMsgClass(message: Message, mdMgr: MdMgr, recompile: Boolean = false): (String, ContainerDef, String) = {
    var containerDef: ContainerDef = null
    var classstr_1: String = null
    var nonVerScalaClassstr_1: String = null
    try {
      val (classname, ver, classstr, list, argsList, nonVerScalaClassstr) = createClassStr(message, mdMgr, recompile)
      classstr_1 = classstr
      nonVerScalaClassstr_1 = nonVerScalaClassstr
      //createScalaFile(classstr, ver, classname)
      val cname = message.pkg + "." + message.NameSpace + "_" + message.Name.toString() + "_" + MdMgr.ConvertVersionToLong(message.Version).toString

      if (message.msgtype.equals("Message"))
        containerDef = createFixedMsgDef(message, list, mdMgr, argsList, recompile)
      else if (message.msgtype.equals("Container"))
        containerDef = createFixedContainerDef(message, list, mdMgr, argsList, recompile)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (classstr_1, containerDef, nonVerScalaClassstr_1)
  }

  private def createMappedMsgClass(message: Message, mdMgr: MdMgr, recompile: Boolean = false): (String, ContainerDef, String) = {
    var containerDef: ContainerDef = null
    var classstr_1: String = null
    var nonVerScalaClassstr_1: String = null
    try {
      val (classname, ver, classstr, list, argsList, nonVerScalaClassstr) = createMappedClassStr(message, mdMgr, recompile)
      classstr_1 = classstr
      nonVerScalaClassstr_1 = nonVerScalaClassstr
      // createScalaFile(classstr, ver, classname)
      val cname = message.pkg + "." + message.NameSpace + "_" + message.Name.toString() + "_" + MdMgr.ConvertVersionToLong(message.Version).toString

      if (message.msgtype.equals("Message"))
        containerDef = createMappedMsgDef(message, list, mdMgr, argsList, recompile)
      else if (message.msgtype.equals("Container"))
        containerDef = createMappedContainerDef(message, list, mdMgr, argsList, recompile)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (classstr_1, containerDef, nonVerScalaClassstr_1)
  }

  private def createClassStr(message: Message, mdMgr: MdMgr, recompile: Boolean): (String, String, String, List[(String, String)], List[(String, String, String, String, Boolean, String)], String) = {
    var scalaclass = new StringBuilder(8 * 1024)
    var nonVerScalaCls = new StringBuilder(8 * 1024)
    val ver = MdMgr.ConvertVersionToLong(message.Version).toString
    val newline = "\n"
    var addMsgStr: String = ""
    var getMsgStr: String = ""
    var list_msg = List[(String, String)]()
    var argsList_msg = List[(String, String, String, String, Boolean, String)]()

    try {
      val (classstr, csvassignstr, jsonstr, xmlStr, count, list, argsList, addMsg, getMsg, partkeyPos, primarykeyPos, serializedBuf, deserializedBuf, prevDeserializedBuf, prevVerMsgObjstr, convertOldObjtoNewObjStr, collections, "", "") = classStr(message, mdMgr, recompile)
      val getSerializedFuncStr = getSerializedFunction(serializedBuf)

      val getDeserializedFuncStr = getDeserializedFunction(true, deserializedBuf, prevDeserializedBuf, prevVerMsgObjstr, recompile)
      val convertOldObjtoNewObj = getConvertOldVertoNewVer(convertOldObjtoNewObjStr, prevVerMsgObjstr, message.PhysicalName)
      list_msg = list
      argsList_msg = argsList
      addMsgStr = addMessage(addMsg, message)
      getMsgStr = getMessage(getMsg)
      val (btrait, striat, csetters) = getBaseTrait(message)
      val (clsstr, objstr, clasname) = classname(message, recompile)
      val cobj = createObj(message, partkeyPos, primarykeyPos, clasname)
      val isFixed = getIsFixed(message)
      val  (versionPkgImport, nonVerPkgImport) = importStmts(message) 
      scalaclass = scalaclass.append(versionPkgImport.toString() + newline + newline + objstr + newline + cobj.toString + newline + clsstr.toString + newline)
      scalaclass = scalaclass.append(classstr + csetters + addMsgStr + getMsgStr + populate + populatecsv(csvassignstr, count) + populateJson + assignJsonData(jsonstr) + assignXmlData(xmlStr) + getSerializedFuncStr + getDeserializedFuncStr + convertOldObjtoNewObj + " \n}")
      nonVerScalaCls = nonVerScalaCls.append(nonVerPkgImport.toString() + newline + newline + objstr + newline + cobj.toString + newline + clsstr.toString + newline)
      nonVerScalaCls = nonVerScalaCls.append(classstr + csetters + addMsgStr + getMsgStr + populate + populatecsv(csvassignstr, count) + populateJson + assignJsonData(jsonstr) + assignXmlData(xmlStr) + getSerializedFuncStr + getDeserializedFuncStr + convertOldObjtoNewObj + " \n}")

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (message.Name, ver.toString, scalaclass.toString, list_msg, argsList_msg, nonVerScalaCls.toString)
  }

  private def createMappedClassStr(message: Message, mdMgr: MdMgr, recompile: Boolean): (String, String, String, List[(String, String)], List[(String, String, String, String, Boolean, String)], String) = {
    var scalaclass = new StringBuilder(8 * 1024)
    val ver = MdMgr.ConvertVersionToLong(message.Version).toString
    val newline = "\n"
    var addMsgStr: String = ""
    var getMsgStr: String = ""
    var list_msg = List[(String, String)]()
    var argsList_msg = List[(String, String, String, String, Boolean, String)]()
    var nonVerScalaCls = new StringBuilder(8 * 1024)
    try {
      val (classstr, csvassignstr, jsonstr, xmlStr, count, list, argsList, addMsg, getMsg, partitionPos, primaryPos, serializedBuf, deserializedBuf, prevDeserializedBuf, prevVerMsgObjstr, convertOldObjtoNewObjStr, collections, mappedSerBaseTypesBuf, mappedDeserBaseTypesBuf) = classStr(message, mdMgr, recompile)
      list_msg = list
      argsList_msg = argsList
      addMsgStr = addMessage(addMsg, message)
      getMsgStr = getMessage(getMsg)

      val getSerializedFuncStr = getSerializedFunction(serializedBuf)
      //println(deserializedBuf)
      mappedMsgPrevVerDeser
      val mapBaseDeser = MappedMsgDeserBaseTypes(mappedDeserBaseTypesBuf)
      val getDeserializedFuncStr = getDeserializedFunction(true, mapBaseDeser + deserializedBuf, prevVerLessThanCurVerCheck(prevDeserializedBuf), prevVerMsgObjstr, recompile)
      val convertOldObjtoNewObj = getConvertOldVertoNewVer(getConvertOldVertoNewVer, prevVerMsgObjstr, message.PhysicalName)

      val (btrait, striat, csetters) = getBaseTrait(message)
      val (clsstr, objstr, clasname) = classname(message, recompile)
      val cobj = createObj(message, partitionPos, primaryPos, clasname)
      val isFixed = getIsFixed(message)
      val  (versionPkgImport, nonVerPkgImport) = importStmts(message) 
      scalaclass = scalaclass.append(versionPkgImport.toString()  + newline + newline + objstr + newline + cobj.toString + newline + clsstr.toString + newline)
      scalaclass = scalaclass.append(classstr + getCollectionsMapped(collections) + csetters + addMsgStr + getMsgStr + populate + populateMappedCSV(csvassignstr, count) + populateJson + assignMappedJsonData(jsonstr) + assignMappedXmlData(xmlStr) + MappedMsgSerialize + MappedMsgSerializeBaseTypes(mappedSerBaseTypesBuf) + MappedMsgSerializeArrays(serializedBuf) + "" + getDeserializedFuncStr + convertOldObjtoNewObj + " \n}")
      nonVerScalaCls = nonVerScalaCls.append(nonVerPkgImport.toString() + newline + newline + objstr + newline + cobj.toString + newline + clsstr.toString + newline)
      nonVerScalaCls = nonVerScalaCls.append(classstr + getCollectionsMapped(collections) + csetters + addMsgStr + getMsgStr + populate + populateMappedCSV(csvassignstr, count) + populateJson + assignMappedJsonData(jsonstr) + assignMappedXmlData(xmlStr) + MappedMsgSerialize + MappedMsgSerializeBaseTypes(mappedSerBaseTypesBuf) + MappedMsgSerializeArrays(serializedBuf) + "" + getDeserializedFuncStr + convertOldObjtoNewObj + " \n}")

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (message.Name, ver.toString, scalaclass.toString, list_msg, argsList_msg, nonVerScalaCls.toString)

  }

  private def SerDeserStr = {
    """
    override def Serialize(dos: DataOutputStream) : Unit = { }
	override def Deserialize(dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, savedDataVersion: String): Unit = { }
    """
  }

  /*private def getFields = {
    """
   var fields: Map[String, Any] = new HashMap[String, Any];
    """
  }*/

  private def createMappedContainerDef(msg: Message, list: List[(String, String)], mdMgr: MdMgr, argsList: List[(String, String, String, String, Boolean, String)], recompile: Boolean = false): ContainerDef = {
    var containerDef: ContainerDef = new ContainerDef()
    try {

      if (msg.Persist) {
        if (msg.PartitionKey == null || msg.PartitionKey.size == 0) {
          throw new Exception("Please provide parition keys in the MessageDefinition since the Message has to be Persisted")
        }
      }
      if (msg.PartitionKey != null)
        containerDef = mdMgr.MakeMappedContainer(msg.NameSpace, msg.Name, msg.PhysicalName, argsList, MdMgr.ConvertVersionToLong(msg.Version), null, msg.jarset.toArray, null, null, msg.PartitionKey.toArray, recompile, msg.Persist)
      else
        containerDef = mdMgr.MakeMappedContainer(msg.NameSpace, msg.Name, msg.PhysicalName, argsList, MdMgr.ConvertVersionToLong(msg.Version), null, msg.jarset.toArray, null, null, null, recompile, msg.Persist)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    containerDef
  }

  private def createMappedMsgDef(msg: Message, list: List[(String, String)], mdMgr: MdMgr, argsList: List[(String, String, String, String, Boolean, String)], recompile: Boolean = false): MessageDef = {
    var msgDef: MessageDef = new MessageDef()
    try {

      if (msg.Persist) {
        if (msg.PartitionKey == null || msg.PartitionKey.size == 0) {
          throw new Exception("Please provide parition keys in the MessageDefinition since the Message has to be Persisted")
        }
      }

      if (msg.PartitionKey != null)
        msgDef = mdMgr.MakeMappedMsg(msg.NameSpace, msg.Name, msg.PhysicalName, argsList, MdMgr.ConvertVersionToLong(msg.Version), null, msg.jarset.toArray, null, null, msg.PartitionKey.toArray, recompile, msg.Persist)
      else
        msgDef = mdMgr.MakeMappedMsg(msg.NameSpace, msg.Name, msg.PhysicalName, argsList, MdMgr.ConvertVersionToLong(msg.Version), null, msg.jarset.toArray, null, null, null, recompile, msg.Persist)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    msgDef
  }

  private def createFixedContainerDef(msg: Message, list: List[(String, String)], mdMgr: MdMgr, argsList: List[(String, String, String, String, Boolean, String)], recompile: Boolean = false): ContainerDef = {
    var containerDef: ContainerDef = new ContainerDef()
    try {
      if (msg.Persist) {
        if (msg.PartitionKey == null || msg.PartitionKey.size == 0) {
          throw new Exception("Please provide parition keys in the MessageDefinition since the Message has to be Persisted")
        }
      }

      if (msg.PartitionKey != null)
        containerDef = mdMgr.MakeFixedContainer(msg.NameSpace, msg.Name, msg.PhysicalName, argsList, MdMgr.ConvertVersionToLong(msg.Version), null, msg.jarset.toArray, null, null, msg.PartitionKey.toArray, recompile, msg.Persist)
      else
        containerDef = mdMgr.MakeFixedContainer(msg.NameSpace, msg.Name, msg.PhysicalName, argsList, MdMgr.ConvertVersionToLong(msg.Version), null, msg.jarset.toArray, null, null, null, recompile, msg.Persist)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    containerDef
  }

  private def createFixedMsgDef(msg: Message, list: List[(String, String)], mdMgr: MdMgr, argsList: List[(String, String, String, String, Boolean, String)], recompile: Boolean = false): MessageDef = {
    var msgDef: MessageDef = new MessageDef()

    try {
      var version = MdMgr.ConvertVersionToLong(msg.Version)
      if (msg.Persist) {
        if (msg.PartitionKey == null || msg.PartitionKey.size == 0) {
          throw new Exception("Please provide parition keys in the MessageDefinition since the Message will be Persisted based on Partition Keys")
        }
      }

      if (msg.PartitionKey != null)
        msgDef = mdMgr.MakeFixedMsg(msg.NameSpace, msg.Name, msg.PhysicalName, argsList, version, null, msg.jarset.toArray, null, null, msg.PartitionKey.toArray, recompile, msg.Persist)
      else
        msgDef = mdMgr.MakeFixedMsg(msg.NameSpace, msg.Name, msg.PhysicalName, argsList, version, null, msg.jarset.toArray, null, null, null, recompile, msg.Persist)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    msgDef
  }

  //create the serialized function in generated scala class 
  private def getSerializedFunction(serStr: String): String = {
    var getSerFunc: String = ""

    if (serStr != null && serStr.trim() != "") {
      getSerFunc = """
    override def Serialize(dos: DataOutputStream) : Unit = {
        try {
    	   """ + serStr + """
    	} catch {
    		case e: Exception => {
    	    e.printStackTrace()
    	  }
        }
     } 
     """
    } else {
      getSerFunc = """ 
    } 
     
     """
    }
    getSerFunc
  }

  //create the deserialized function in generated scala class 

  private def getPrevDeserStr(prevVerMsgObjstr: String, prevObjDeserStr: String, recompile: Boolean): String = {
    var preVerDeserStr: String = ""
    // if (recompile == false && prevVerMsgObjstr != null && prevVerMsgObjstr.trim() != "") {

    if (prevVerMsgObjstr != null && prevVerMsgObjstr.trim() != "") {
      val prevVerObjStr = "val prevVerObj = new %s()".format(prevVerMsgObjstr)
      preVerDeserStr = """
        if (prevVer < currentVer) {
                """ + prevVerObjStr + """ 
                prevVerObj.Deserialize(dis, mdResolver, loader, savedDataVersion)   
               """ + prevObjDeserStr + """ 
           
	     } else """
    }

    preVerDeserStr
  }

  private def getDeserStr(deserStr: String, fixed: Boolean): String = {
    var deSer: String = ""

    if (deserStr != null && deserStr.trim() != "") {
      deSer = """
         if(prevVer == currentVer){  
              """ + deserStr + """
        } else throw new Exception("Current Message/Container Version "+currentVer+" should be greater than Previous Message Version " +prevVer + "." )
     """
    }
    deSer
  }

  private def deSerializeStr(preVerDeserStr: String, deSer: String) = {

    """
    override def Deserialize(dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, savedDataVersion: String): Unit = {
	  try {
      	if (savedDataVersion == null || savedDataVersion.trim() == "")
        	throw new Exception("Please provide Data Version")
    
      	val prevVer = savedDataVersion.replaceAll("[.]", "").toLong
      	val currentVer = Version.replaceAll("[.]", "").toLong
      	""" + preVerDeserStr + """ 
      	""" + deSer + """ 
      	} catch {
      		case e: Exception => {
          		e.printStackTrace()
      		}
      	}
    } 
     """
  }

  private def getDeserializedFunction(fixed: Boolean, deserStr: String, prevObjDeserStr: String, prevVerMsgObjstr: String, recompile: Boolean): String = {

    var getDeserFunc: String = ""
    var preVerDeserStr: String = ""
    var deSer: String = ""
    preVerDeserStr = getPrevDeserStr(prevVerMsgObjstr, prevObjDeserStr, recompile)
    deSer = getDeserStr(deserStr, fixed)

    if (deserStr != null && deserStr.trim() != "")
      getDeserFunc = deSerializeStr(preVerDeserStr, deSer)

    getDeserFunc
  }
  /// DeSerialize Base Msg Types for mapped Mapped 

  private def MappedMsgDeserBaseTypes(baseTypesDeserialize: String) = {
    """
	  val desBaseTypes = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis)
	  //println("desBaseTypes "+desBaseTypes)
        for (i <- 0 until desBaseTypes) {
          val key = com.ligadata.BaseTypes.StringImpl.DeserializeFromDataInputStream(dis)
          val typIdx = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis)
          
	  	  typIdx match {
          	""" + baseTypesDeserialize + """
          	case _ => { throw new Exception("Bad TypeIndex found") } // should not come here
          }
         
       }

   """
  }

  /// Serialize Base Msg Types for mapped Mapped 

  private def MappedMsgSerializeBaseTypes(baseTypesSerialize: String) = {
    """
    private def SerializeBaseTypes(dos: DataOutputStream): Unit = {
  
    var cntCanSerialize: Int = 0
    fields.foreach(field => {
      if (field._2._1 >= 0)
        cntCanSerialize += 1
    })
    com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, cntCanSerialize)

    // Note:: fields has all base and other stuff
    fields.foreach(field => {
	  if (field._2._1 >= 0) {
      	val key = field._1.toLowerCase
      	com.ligadata.BaseTypes.StringImpl.SerializeIntoDataOutputStream(dos, key)
      	com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, field._2._1)
      	field._2._1 match {
      	""" + baseTypesSerialize + """
   
      		case _ => {} // could be -1
      	}
      }
    })
   }
  """
  }

  //Serialize function of mapped maeesge

  private def MappedMsgSerialize() = {
    """
    override def Serialize(dos: DataOutputStream): Unit = {
    try {
      // Base Stuff
      SerializeBaseTypes(dos)
       // Non Base Types
     SerializeNonBaseTypes(dos)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }
  """
  }

  // Mapped Messages Serialization for Array of primitives
  private def MappedMsgSerializeArrays(mappedMsgSerializeArray: String) = {
    """
    private def SerializeNonBaseTypes(dos: DataOutputStream): Unit = {
    """ + mappedMsgSerializeArray + """
    }
    """
  }

  private def mappedMsgPrevVerDeser(): String = {

    """ 
    prevVerObj.fields.foreach(field => {

          if (prevVerMatchKeys.contains(field)) {
            fields(field._1.toLowerCase) = (field._2._1, field._2._2)
          } else {
            fields(field._1.toLowerCase) =  (0, ValueToString(field._2._2))
          }

        })
      
    """
  }

  private def mappedPrevObjTypNotMatchDeserializedBuf(prevObjTypNotMatchDeserializedBuf: String) = {
    """
    private def getStringIdxFromPrevFldValue(oldTypName: Any, value: Any): (String, Int) = {
	  	var data: String = null    
	  	oldTypName match {
	  		""" + prevObjTypNotMatchDeserializedBuf + """
	  		case _ => { throw new Exception("Bad TypeIndex found") } // should not come here
	    }
	    return (data, typsStr.indexOf(oldTypName))
	  }
    """
  }

  //mapped messages - block of prevObj version < current Obj version check 

  private def prevVerLessThanCurVerCheck(caseStmsts: String) = {
    var caseStr: String = ""
    if (caseStmsts != null && caseStmsts.trim() != "") {
      caseStr = """prevObjfield._1 match{
      		""" + caseStmsts + """
  			case _ => {
  				fields(prevObjfield._1) = (prevObjfield._2._1, prevObjfield._2._2)
  			}
  		  }
      	"""
    } else caseStr = "fields(prevObjfield._1) = (prevObjfield._2._1, prevObjfield._2._2)"

    """
      val prevTyps = prevVerObj.typsStr
      prevVerObj.fields.foreach(prevObjfield => {

      //Name and Base Types Match

      if (prevVerMatchKeys.contains(prevObjfield._1)) {
      	if(prevObjfield._2._1 >= 0){
           fields(prevObjfield._1) = (typsStr.indexOf(prevTyps(prevObjfield._2._1)), prevObjfield._2._2);
	  	}else {
           """ + caseStr + """
      	}
        } else if (prevVerTypesNotMatch.contains(prevObjfield._1)) { //Name Match and Base Types Not Match
            if (prevObjfield._2._1 >= 0) {
              val oldTypName = prevTyps(prevObjfield._2._1)
              val (data, newTypeIdx) = getStringIdxFromPrevFldValue(oldTypName, prevObjfield._2._2)
              val v1 = typs(newTypeIdx).Input(data)
              fields.put(prevObjfield._1, (newTypeIdx, v1))
            }

        } else if (!(prevVerMatchKeys.contains(prevObjfield._1) && prevVerTypesNotMatch.contains(prevObjfield._1))) { //////Extra Fields in Prev Ver Obj
            if (prevObjfield._2._1 >= 0) {
              val oldTypName = prevTyps(prevObjfield._2._1)
              val (data, index) = getStringIdxFromPrevFldValue(oldTypName, prevObjfield._2._2)

              val v1 = typs(0).Input(data)
              fields.put(prevObjfield._1, (0, v1))
            }
            else {
               fields.put(prevObjfield._1, (0, typs(0).Input(prevObjfield._2._2.asInstanceOf[String])))
            }
         }
       })
	 """
  }

  /*
  def processConcept(conceptJson: String, mdMgr: MdMgr): Unit = {
    val list: List[(String, String, String)] = listConceptArgs(parseConceptJson(conceptJson))

  }

  def listConceptArgs(cpts: Concepts): List[(String, String, String)] = {
    var list = List[(String, String, String)]()
    for (c <- cpts.Concepts) {
      list ::= (c.NameSpace, c.Name, c.Type)
    }
    list
  }

  def parseConceptJson(conceptJson: String): Concepts = {
    try {
      val pConcpt = JSON.parseFull(conceptJson)
      if (pConcpt.isEmpty) throw MessageException("Unable to parse JSON.")

      val cptsDef = for {
        Some(AsMap(map)) <- List(pConcpt)
        AsList(concepts) <- map get "Concepts"
      } yield new Concepts(for {
        AsMap(concept) <- concepts
        AsString(name) <- concept get "Name" orElse error("Structure Name in Field does not exist in json")
        AsString(ctype) <- concept get "Type" orElse error("Structure Type in Field does not exist in json")
        AsString(namespace) <- concept get "NameSpace" orElse error("Structure Type in Field does not exist in json")

      } yield new Concept(namespace, name, ctype))
      cptsDef.head
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }
*/
  private def processJson(json: String, mdMgr: MdMgr, recompile: Boolean = false): Message = {
    var message: Message = null
    var jtype: String = null
    //val parsed = JSON.parseFull(json)
    val map = parse(json).values.asInstanceOf[Map[String, Any]]
    // val map = parsed.get.asInstanceOf[Map[String, Any]]
    //  val map = parsed
    var key: String = ""
    try {
      jtype = geJsonType(map)
      message = processJsonMap(jtype, map, mdMgr, recompile).asInstanceOf[Message]

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    message
  }

  private def geJsonType(map: Map[String, Any]): String = {
    var jtype: String = null
    try {
      if (map.contains("Message"))
        jtype = "Message"
      else if (map.contains("Container"))
        jtype = "Container"
      else if (map.contains("Concepts"))
        jtype = "Concepts"
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    jtype
  }

  private def processJsonMap(key: String, map: Map[String, Any], mdMgr: MdMgr, recompile: Boolean = false): Message = {
    var msg1: Message = null
    type messageMap = Map[String, Any]
    try {
      if (map.contains(key)) {
        if (map.get(key).get.isInstanceOf[messageMap]) {
          val message = map.get(key).get.asInstanceOf[Map[String, Any]]
          // if (message.get("Fixed").get.equals("true")) {
          msg1 = getMsgorCntrObj(message, key, mdMgr, recompile).asInstanceOf[Message]
          //  } else if (message.get("Fixed").get.equals("false")) {
          //    msg1 = geKVMsgorCntrObj(message, key).asInstanceOf[Message]
          //  } else throw new Exception("Message in json is not Map")

        }
      } else throw new Exception("Incorrect json")
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }

    msg1
  }

  /* private def geKVMsgorCntrObj(message: Map[String, Any], mtype: String): Message = {
    var pkg: String = "com.ligadata.messagescontainers"
    val physicalName: String = pkg + "." + message.get("NameSpace").get.toString + "_" + message.get("Name").get.toString() + "_" + MdMgr.ConvertVersionToInt(extractVersion(message)).toString
    var tdata: TransformData = null
    var tdataexists: Boolean = false
    var conceptList: List[String] = null
    try {
      //println("message" + message)
      if (message.contains("Concepts")) {
        // println("concepts")
        conceptList = message.get("Concepts").get.asInstanceOf[List[String]]
      }
      for (c <- conceptList) {
        // println("concept list" + c)
      }

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    new Message(mtype, message.get("NameSpace").get.toString, message.get("Name").get.toString(), physicalName, extractVersion(message), message.get("Description").get.toString(), message.get("Fixed").get.toString(), null, tdataexists, tdata, null, pkg, conceptList, null, null, null, null, null, 0)
  }
  * 
  */

  // Make sure the version is in the format of nn.nn.nn
  private def extractVersion(message: Map[String, Any]): String = {
    MdMgr.FormatVersion(message.getOrElse("Version", "0").toString)
  }

  private def getMsgorCntrObj(message: Map[String, Any], mtype: String, mdMgr: MdMgr, recompile: Boolean = false): Message = {
    var ele: List[Element] = null
    var tdata: TransformData = null
    var tdataexists: Boolean = false
    var container: Message = null
    val tkey: String = "TransformData"
    var pKey: String = null
    var prmryKey: String = null
    var partitionKeysList: List[String] = null
    var primaryKeysList: List[String] = null
    var conceptsList: List[String] = null
    var msgVersion: String = ""
    var persistMsg: Boolean = false
    try {
      if (message != null) {
        if (message.getOrElse("NameSpace", null) == null)
          throw new Exception("Please provide the Name space in the message definition ")

        if (message.getOrElse("Name", null) == null)
          throw new Exception("Please provide the Name of the message definition ")

        if (message.getOrElse("Version", null) == null)
          throw new Exception("Please provide the Version of the message definition ")

        if (message.getOrElse("Fixed", null) == null)
          throw new Exception("Please provide the type of the message definition (either Fixed or Mapped) ")

        val persist = message.getOrElse("Persist", "false").toString.toLowerCase

        if (persist.equals("true"))
          persistMsg = true

        for (key: String <- message.keys) {
          if (key.equals("Elements") || key.equals("Fields") || key.equals("Concepts")) {
            ele = getElementsObj(message, key).asInstanceOf[List[Element]]
            // container = getElementsObj(message, key)._2.asInstanceOf[Message]
          }

          if (mtype.equals("Message") && message.contains(tkey)) {
            if (key.equals(tkey)) {
              tdataexists = true
              tdata = getTransformData(message, key)
            }
          }

          if (key.equals("PartitionKey")) {
            var partitionKeys = message.getOrElse("PartitionKey", null)
            if (partitionKeys != null) {
              partitionKeysList = partitionKeys.asInstanceOf[List[String]]
              partitionKeysList = partitionKeysList.map(p => p.toLowerCase())
              for (partitionKey: String <- partitionKeysList) {
                if (partitionKeysList.size == 1)
                  pKey = partitionKey.toLowerCase()
              }
            }
          }

          if (key.equals("PrimaryKey")) {
            var primaryKeys = message.getOrElse("PrimaryKey", null)

            if (primaryKeys != null) {
              primaryKeysList = primaryKeys.asInstanceOf[List[String]]
              primaryKeysList = primaryKeysList.map(p => p.toLowerCase())
            }
          }

          if (key.equals("Concepts")) {
            conceptsList = getConcepts(message, key)
          }
        }
        if (message.get("Fixed").get.toString().toLowerCase == "true" && ele == null)
          throw new Exception("Either Fields or Elements or Concepts  do not exist in " + message.get("Name").get.toString())

        if (recompile) {
          msgVersion = getRecompiledMsgContainerVersion(mtype, message.get("NameSpace").get.toString, message.get("Name").get.toString(), mdMgr)
        } else {
          msgVersion = extractVersion(message)
        }

      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    val cur_time = System.currentTimeMillis
    val physicalName: String = pkg + "." + message.get("NameSpace").get.toString + "_" + message.get("Name").get.toString() + "_" + MdMgr.ConvertVersionToLong(msgVersion).toString + "_" + cur_time
    new Message(mtype, message.get("NameSpace").get.toString, message.get("Name").get.toString(), physicalName, msgVersion, message.get("Description").get.toString(), message.get("Fixed").get.toString(), persistMsg, ele, tdataexists, tdata, null, pkg.trim(), conceptsList, null, null, null, partitionKeysList, primaryKeysList, cur_time)
  }

  private def getTransformData(message: Map[String, Any], tkey: String): TransformData = {
    var iarr: Array[String] = null
    var oarr: Array[String] = null
    var karr: Array[String] = null
    type tMap = Map[String, Any]

    if (message.get(tkey).get.isInstanceOf[tMap]) {
      val tmap: Map[String, Any] = message.get(tkey).get.asInstanceOf[Map[String, Any]]
      for (key <- tmap.keys) {
        if (key.equals("Input"))
          iarr = gettData(tmap, key)
        if (key.equals("Output"))
          oarr = gettData(tmap, key)
        if (key.equals("Keys"))
          karr = gettData(tmap, key)
      }
    }
    new TransformData(iarr, oarr, karr)
  }

  private def gettData(tmap: Map[String, Any], key: String): Array[String] = {
    type tList = List[String]
    var tlist: List[String] = null
    if (tmap.contains(key) && tmap.get(key).get.isInstanceOf[tList])
      tlist = tmap.get(key).get.asInstanceOf[List[String]]
    tlist.toArray
  }

  private def getElementsObj(message: Map[String, Any], key: String): List[Element] = {
    // type list = List[Element]
    var elist: List[Element] = null
    var containerList: List[String] = null
    var container: Message = null
    var lbuffer = new ListBuffer[Element]
    var conceptStr: String = "Concepts"

    try {
      if (key.equals("Elements") || key.equals("Fields")) {
        // val (elist, container) = getElements(message, key).asInstanceOf[List[Element]]
        elist = getElements(message, key).asInstanceOf[List[Element]]
        // container = getElements(message, key)._2.asInstanceOf[Message]
      } else if (key.equals("Concepts")) {
        elist = getConceptData(message.get(conceptStr).get.asInstanceOf[List[String]], conceptStr)

      } else throw new Exception("Either Fields or Elements or Concepts  do not exist in " + key + " json")
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    elist
  }

  private def getConceptData(ccpts: List[String], key: String): List[Element] = {
    var element: Element = null
    var lbuffer = new ListBuffer[Element]
    type string = String;
    if (ccpts == null) throw new Exception("Concepts List is null")
    try {
      for (l <- ccpts)
        lbuffer += new Element(null, l.toString(), l.toString(), null, key, null)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    lbuffer.toList
  }

  private def getConcepts(message: Map[String, Any], key: String): List[String] = {
    var list: List[Element] = Nil
    var lbuffer = new ListBuffer[Element]
    type ccptList = List[String]
    var conceptList: ccptList = null
    try {
      if (message.get(key).get.isInstanceOf[ccptList]) {
        //val eList = message.get(key).get.asInstanceOf[List[String]]
        conceptList = message.getOrElse("Concepts", null).asInstanceOf[ccptList]

      } else throw new Exception("Elements list do not exist in json")
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    conceptList
  }

  private def getElements(message: Map[String, Any], key: String): List[Element] = {
    // var fbuffer = new ListBuffer[Field]
    var lbuffer = new ListBuffer[Element]
    var container: Message = null
    type messageList = List[Map[String, Any]]
    type keyMap = Map[String, Any]
    type typList = List[String]
    var cntrList: List[String] = null
    try {
      if (message.get(key).get.isInstanceOf[messageList]) {
        val eList = message.get(key).get.asInstanceOf[List[Map[String, Any]]]
        for (l <- eList) {
          if (l.isInstanceOf[keyMap]) {
            val eMap: Map[String, Any] = l.asInstanceOf[Map[String, Any]]

            if (eMap.contains("Field")) {
              lbuffer += getElement(eMap)

            } else if (key.equals("Fields")) {
              lbuffer += getElementData(eMap.asInstanceOf[Map[String, String]], key)

            } else if (eMap.contains("Container") || eMap.contains("Message") || eMap.contains("Containers") || eMap.contains("Messages") || eMap.contains("Concept") || eMap.contains("Concepts")) {

              var key: String = ""
              if (eMap.contains("Container"))
                key = "Container"
              else if (eMap.contains("Containers"))
                key = "Containers"
              else if (eMap.contains("Message"))
                key = "Message"
              else if (eMap.contains("Messages"))
                key = "Messages"
              else if (eMap.contains("Concepts"))
                key = "Concepts"
              else if (eMap.contains("Concept"))
                key = "Concept"

              if (eMap.get(key).get.isInstanceOf[keyMap]) {
                val containerMap: Map[String, Any] = eMap.get(key).get.asInstanceOf[Map[String, Any]]
                lbuffer += getElementData(containerMap.asInstanceOf[Map[String, String]], key)
              } else if (eMap.get(key).get.isInstanceOf[typList]) {
                lbuffer ++= getConceptData(eMap.get(key).get.asInstanceOf[List[String]], key)
              } else if (eMap.get(key).get.isInstanceOf[String]) {
                var cntrList: ListBuffer[String] = new ListBuffer[String]()
                cntrList += eMap.get(key).get.toString
                lbuffer ++= getConceptData(cntrList.toList, key)
              }

            } else if (message.get("Fixed").get.toString().toLowerCase == "true") throw new Exception("Either Fields or Container or Message or Concepts  do not exist in " + key + " json")
          }
        }
      } else throw new Exception("Elements list do not exist in json")
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    //  println("fbuffer" + fbuffer.toList)
    //  val ele: Element = new Element(fbuffer.toList, container)
    //  println("ele " + ele.Fields(1).Name)
    // lbuffer += ele
    (lbuffer.toList)
  }

  /* def getContainerObj(eMap: Map[String, Any]): Message = {

    var lbuffer = new ListBuffer[Element]
    var ContainerType = ""
    var CollectionType = ""
    // message = processJsonMap("Container", eMap).asInstanceOf[Message]
    // get the Name, is is Array of Container from map
    //if container in message has fields get the array of fields
    //if the container has list of container name get the array of container
    //create a message object

    var fbuffer = new ListBuffer[Field]
    println("eMap " + eMap)
    if (eMap.contains("Fields")) {
      val eList = eMap.get("Fields").get.asInstanceOf[List[Map[String, Any]]]
      for (l <- eList)
        fbuffer += getElementData(eMap.asInstanceOf[Map[String, String]], "Fields")
      val ele: Element = new Element(fbuffer.toList, null)
      println("ele 11111 " + ele.Fields(1).Name)
      lbuffer += ele

    }
    if (eMap.contains("Type")) {
      ContainerType = eMap.get("Type").get.asInstanceOf[String]
      if (eMap.contains("CollectionType")) {
        CollectionType = eMap.get("CollectionType").get.asInstanceOf[String]
      }
    }

    new Message("Container", null, eMap.get("Name").get.toString(), null, null, null, null, lbuffer.toList, false, null, null, null, null, ContainerType, CollectionType, null)

  }*/

  private def getConcept(eMap: Map[String, Any], key: String): List[Field] = {
    var lbuffer = new ListBuffer[Field]
    var concept: Concept = null
    if (eMap == null) throw new Exception("Concept Map is null")
    try {
      for (eKey: String <- eMap.keys) {
        val namestr = eMap.get(eKey).get.toString.split("\\.")
        if ((namestr != null) && (namestr.size == 2)) {
          concept.NameSpace = Some(namestr(0))
          concept.Name = Some(namestr(1))
          //  concept.Type = MdMgr.GetMdMgr.Attribute(namestr(0), namestr(1)).tType.toString()
          // concept.Type = MdMgr.GetMdMgr.Attribute(key, ver, onlyActive)(namestr(0), namestr(1)).tType.toString()
          val e = getcElement(concept, key)
          lbuffer += e
          //   val attr = MdMgr.GetMdMgr.Attribute(namestr(0), namestr(1))
        }
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    lbuffer.toList
  }

  private def getcElement(concept: Concept, eType: String): Field = {
    new Field(concept.NameSpace.getOrElse(null), concept.Name.getOrElse(null), concept.Type.getOrElse(null), null, eType, null)
  }

  private def getElement(eMap: Map[String, Any]): Element = {
    var fld: Element = null
    type keyMap = Map[String, String]
    if (eMap == null) throw new Exception("element Map is null")
    try {
      for (eKey: String <- eMap.keys) {
        if (eMap.get(eKey).get.isInstanceOf[keyMap]) {
          val element = eMap.get(eKey).get.asInstanceOf[Map[String, String]]
          fld = getElementData(element, eKey)
        }
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    fld
  }

  private def getElementData(field: Map[String, String], key: String): Element = {
    var fld: Element = null
    var name: String = ""
    var fldTypeVer: String = null

    var namespace: String = ""
    var ttype: String = ""
    var collectionType: String = ""
    type string = String;
    if (field == null) throw new Exception("element Map is null")
    try {
      if (field.contains("NameSpace") && (field.get("NameSpace").get.isInstanceOf[string]))
        namespace = field.get("NameSpace").get.asInstanceOf[String]

      if (field.contains("Name") && (field.get("Name").get.isInstanceOf[String]))
        name = field.get("Name").get.asInstanceOf[String].toLowerCase()
      else throw new Exception("Field Name do not exist in " + key)

      if (field.contains("Type") && (field.get("Type").get.isInstanceOf[string])) {
        val fieldstr = field.get("Type").get.toString.split("\\.")
        if ((fieldstr != null) && (fieldstr.size == 2)) {
          namespace = fieldstr(0).toLowerCase()
          ttype = field.get("Type").get.asInstanceOf[String].toLowerCase()
        } else
          ttype = field.get("Type").get.asInstanceOf[String].toLowerCase()
        if (field.contains("CollectionType") && (field.get("CollectionType").get.isInstanceOf[string])) {
          collectionType = field.get("CollectionType").get.asInstanceOf[String].toLowerCase()
        }
        if (field.contains("Version") && (field.get("Version").get.isInstanceOf[string])) {
          fldTypeVer = field.get("Version").get.asInstanceOf[String].toLowerCase()
        }

      } else throw new Exception("Field Type do not exist in " + key)
      fld = new Element(namespace, name, ttype, collectionType, key, fldTypeVer)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    fld
  }

  private def processConcept(conceptsStr: String, formatType: String) {
    val json = parse(conceptsStr)
    //println(json.values.asInstanceOf[Map[Any, Any]].contains("Concepts"))
    implicit val jsonFormats: Formats = DefaultFormats
    val ConceptList = json.extract[ConceptList]
    ConceptList.Concepts.foreach { concept =>
      log.debug("NameSpace => " + concept.NameSpace.getOrElse(null));
      log.debug("Name => " + concept.Name.getOrElse(None));
      log.debug("Type => " + concept.Type.getOrElse(None));
      log.debug("=========>");
    }

  }
}
