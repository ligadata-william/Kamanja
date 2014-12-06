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
import com.ligadata.olep.metadata.MdMgr
import com.ligadata.olep.metadata.EntityType
import com.ligadata.olep.metadata.MessageDef
import com.ligadata.olep.metadata.BaseAttributeDef
import com.ligadata.olep.metadata.ContainerDef
import com.ligadata.olep.metadata.ArrayTypeDef
import com.ligadata.olep.metadata.ArrayBufTypeDef

trait Attrib {
  var NameSpace: String
  var Name: String
  var Type: String
}

class Message(var msgtype: String, var NameSpace: String, var Name: String, var PhysicalName: String, var Version: String, var Description: String, var Fixed: String, var Elements: List[Element], var TDataExists: Boolean, var TrfrmData: TransformData, var jarset: Set[String], var pkg: String, var concepts: List[String], var Ctype: String, var CCollectiontype: String, var Containers: List[String], var PartitionKey: List[String], var PrimaryKeys: List[String])
class TransformData(var input: Array[String], var output: Array[String], var keys: Array[String])
class Field(var NameSpace: String, var Name: String, var Ttype: String, var CollectionType: String, var Fieldtype: String, var FieldtypeVer: String)
class Element(var NameSpace: String, var Name: String, var Ttype: String, var CollectionType: String, var ElemType: String, var FieldtypeVer: String)
case class MessageException(message: String) extends Exception(message)

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

  private def error[T](prefix: String): Option[T] =
    throw MessageException("%s must be specified".format(prefix))

  //creates the class string

  private def createObj(msg: Message): (StringBuilder) = {
    var cobj: StringBuilder = new StringBuilder
    var tdataexists: String = ""
    var tattribs: String = ""
    val newline = "\n"
    val cbrace = "}"
    val isFixed = getIsFixed(msg)
    if (msg.msgtype.equals("Message")) {
      if (msg.TDataExists) {
        tdataexists = gettdataexists + msg.TDataExists.toString
        tattribs = tdataattribs(msg)
      } else {
        tdataexists = gettdataexists + msg.TDataExists.toString
        tattribs = notdataattribs
      }
      // cobj.append(tattribs + newline + tdataexists + newline + getMessageName(msg) + newline + getName(msg) + newline + getVersion(msg) + newline + createNewMessage(msg) + newline + isFixed + cbrace + newline)
      cobj.append(tattribs + newline + tdataexists + newline + getName(msg) + newline + getVersion(msg) + newline + createNewMessage(msg) + newline + isFixed + cbrace + newline)

    } else if (msg.msgtype.equals("Container")) {
      // cobj.append(getMessageName(msg) + newline + getName(msg) + newline + getVersion(msg) + newline + createNewContainer(msg) + newline + isFixed + cbrace + newline)
      cobj.append(getName(msg) + newline + getVersion(msg) + newline + createNewContainer(msg) + newline + isFixed + cbrace + newline)

    }
    cobj
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
  private def createNewMessage(msg: Message) = {
    "\toverride def CreateNewMessage: BaseMsg  = new " + msg.NameSpace + "_" + msg.Name + "_" + msg.Version.replaceAll("[.]", "").toInt + "()"
  }

  private def createNewContainer(msg: Message) = {
    "\toverride def CreateNewContainer: BaseContainer  = new " + msg.NameSpace + "_" + msg.Name + "_" + msg.Version.replaceAll("[.]", "").toInt + "()"
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

  private def assignJsonForArray(fname: String, typeImpl: String) = {
    """
		if (map.getOrElse("""" + fname + """", null).isInstanceOf[tList])
			arr = map.getOrElse("""" + fname + """", null).asInstanceOf[List[String]]
		if (arr != null) {
			""" + fname + """  = arr.map(v => """ + typeImpl + """(v)).toArray
		}
      """
  }

  private def assignJsonForCntrArrayBuffer(fname: String, typeImpl: String) = {
    """
	 
		if (map.getOrElse("""" + fname + """", null).isInstanceOf[tMap])
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
        inputData.cur_json = Option(map.getOrElse("hl7messages", null))
	    """ + mName + """.populate(inputData)
	    """
  }

  private def handleBaseTypes(keysSet: Set[String], fixed: String, typ: Option[com.ligadata.olep.metadata.BaseTypeDef], f: Element, msgVersion: String): (String, String, String, String, List[(String, String)], List[(String, String, String, String, Boolean, String)], String, String, String, Set[String]) = {
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
    var typeImpl = new StringBuilder(8 * 1024)
    var fname: String = ""
    try {

      if (typ.get.implementationName.isEmpty())
        throw new Exception("Implementation Name not found in metadata for namespace %s" + f.Ttype)

      if (typ.getOrElse("None").equals("None"))
        throw new Exception("Type not found in metadata for Name: " + f.Name + " , NameSpace: " + f.NameSpace + " , Version: " + msgVersion + " , Type : " + f.Ttype)

      // if (!typ.get.physicalName.equals("String")){
      //  argsList = (f.NameSpace, f.Name, f.NameSpace, typ.get.physicalName.substring(6, typ.get.physicalName.length()), false) :: argsList
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

      val dval: String = getDefVal(f.Ttype)
      list = (f.Name, f.Ttype) :: list

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
        keysStr.append("(\"" + f.Name + "\"," + typ.get.implementationName + "),")
      }

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (scalaclass.toString, assignCsvdata.toString, assignJsondata.toString, assignXmldata.toString, list, argsList, addMsg.toString, keysStr.toString, typeImpl.toString, jarset)
  }

  private def handleArrayType(typ: Option[com.ligadata.olep.metadata.BaseTypeDef], f: Element, fixed: String): (String, String, String, String, List[(String, String)], List[(String, String, String, String, Boolean, String)], String, Set[String], String) = {
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
    var keysStr = new StringBuilder(8 * 1024)
    var arrayType: ArrayTypeDef = null
    try {
      arrayType = typ.get.asInstanceOf[ArrayTypeDef]

      if (arrayType == null) throw new Exception("Array type " + f.Ttype + " do not exist")

      if ((arrayType.elemDef.physicalName.equals("String")) || (arrayType.elemDef.physicalName.equals("Int")) || (arrayType.elemDef.physicalName.equals("Float")) || (arrayType.elemDef.physicalName.equals("Double")) || (arrayType.elemDef.physicalName.equals("Char"))) {
        if (arrayType.elemDef.implementationName.isEmpty())
          throw new Exception("Implementation Name not found in metadata for namespace %s" + f.Ttype)
        else
          fname = arrayType.elemDef.implementationName + ".Input"

        if (fixed.toLowerCase().equals("true")) {
          assignCsvdata.append("%s%s = list(inputdata.curPos).split(arrvaldelim, -1).map(v => %s(v));\n%sinputdata.curPos = inputdata.curPos+1\n".format(pad2, f.Name, fname, pad2))
        } else if (fixed.toLowerCase().equals("false")) {
          assignCsvdata.append(" ")
        }
        assignJsondata.append(assignJsonForArray(f.Name, fname))

      } else {
        if (arrayType.elemDef.tTypeType.toString().toLowerCase().equals("tcontainer")) {
          assignCsvdata.append(newline + getArrayStr(f.Name, arrayType.elemDef.physicalName) + newline + "\t\tinputdata.curPos = inputdata.curPos+1" + newline)
          assignJsondata.append(assignJsonForCntrArrayBuffer(f.Name, arrayType.elemDef.physicalName))
          keysStr.append("(\"" + f.Name + "\",")
        }
        if (arrayType.elemDef.tTypeType.toString().toLowerCase().equals("tmessage"))
          if (typ.get.typeString.toString().split("\\[").size == 2) {
            addMsg.append(pad2 + "if(curVal._2.compareToIgnoreCase(\"" + f.Name + "\") == 0) {" + newline + "\t")
            addMsg.append(pad3 + f.Name + " += msg.asInstanceOf[" + typ.get.typeString.toString().split("\\[")(1) + newline + pad3 + "} else ")
          }
        //assignCsvdata.append(newline + "//Array of " + arrayType.elemDef.physicalName + "not handled at this momemt" + newline)

      }
      scalaclass = scalaclass.append("%svar %s: %s = _ ;%s".format(pad1, f.Name, typ.get.typeString, newline))

      argsList = (f.NameSpace, f.Name, typ.get.NameSpace, typ.get.Name, false, null) :: argsList
      log.trace("typ.get.typeString " + typ.get.typeString)

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
    (scalaclass.toString, assignCsvdata.toString, assignJsondata.toString, assignXmldata.toString, list, argsList, addMsg.toString, jarset, keysStr.toString)
  }

  private def handleArrayBuffer(msgNameSpace: String, typ: Option[com.ligadata.olep.metadata.BaseTypeDef], f: Element): (String, String, String, String, List[(String, String)], List[(String, String, String, String, Boolean, String)], String, Set[String], String) = {
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
    var arrayBufType: ArrayBufTypeDef = null
    var keysStr = new StringBuilder(8 * 1024)

    try {
      arrayBufType = typ.get.asInstanceOf[ArrayBufTypeDef]
      if (arrayBufType == null) throw new Exception("Array Byffer of " + f.Ttype + " do not exists throwing Null Pointer")

      argsList = (msgNameSpace, f.Name, arrayBufType.NameSpace, arrayBufType.Name, false, null) :: argsList
      scalaclass = scalaclass.append("%svar %s: %s = new %s;%s".format(pad1, f.Name, typ.get.typeString, typ.get.typeString, newline))

      if (f.ElemType.equals("Container")) {
        assignCsvdata.append("%s//%s Implementation of Array Buffer of Container is not handled %s".format(pad2, f.Name, newline))

        if (typ.get.typeString.toString().split("\\[").size == 2) {
          assignJsondata.append(assignJsonForCntrArrayBuffer(f.Name, typ.get.typeString.toString().split("\\[")(1).split("\\]")(0)))
        }

        // assignJsondata.append("%s%s.assignJsonData(map)%s".format(pad1, f.Name, newline))
        // assignCsvdata.append(newline + getArrayStr(f.Name, arrayBufType.elemDef.physicalName) + newline + "\t\tinputdata.curPos = inputdata.curPos+1" + newline)
      } else if (f.ElemType.equals("Message")) {
        //  assignCsvdata.append("%s//%s Implementation of messages is not handled at this time%s".format(pad2, f.Name, newline))
        //  assignJsondata.append("%s//%s Implementation of messages is not handled %s".format(pad2, f.Name, newline))
        if (typ.get.typeString.toString().split("\\[").size == 2) {
          addMsg.append(pad2 + "if(curVal._2.compareToIgnoreCase(\"" + f.Name + "\") == 0) {" + newline + "\t")
          addMsg.append(pad3 + f.Name + " += msg.asInstanceOf[" + typ.get.typeString.toString().split("\\[")(1) + newline + pad3 + "} else ")
        }
      }
      keysStr.append("\"" + f.Name + "\",")
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
    (scalaclass.toString, assignCsvdata.toString, assignJsondata.toString, assignXmldata.toString, list, argsList, addMsg.toString, jarset, keysStr.toString)
  }

  private def handleContainer(mdMgr: MdMgr, ftypeVersion: Int, f: Element): (String, String, String, String, List[(String, String)], List[(String, String, String, String, Boolean, String)], String, Set[String], String) = {
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

    try {
      var ctrDef: ContainerDef = mdMgr.Container(f.Ttype, ftypeVersion, true).getOrElse(null)
      if (ctrDef == null) throw new Exception("Container  " + f.Ttype + " do not exists throwing null pointer")

      scalaclass = scalaclass.append("%svar %s:%s = new %s();%s".format(pad1, f.Name, ctrDef.PhysicalName, ctrDef.PhysicalName, newline))
      assignCsvdata.append("%s%s.populate(inputdata);\n%sinputdata.curPos = inputdata.curPos+1\n".format(pad2, f.Name, pad2))
      assignJsondata.append(assignJsonDataMessage(f.Name))
      assignXmldata.append("%s%s.populate(xmlData)%s".format(pad3, f.Name, newline))

      if ((ctrDef.dependencyJarNames != null) && (ctrDef.jarName != null)) {
        jarset = jarset + ctrDef.JarName ++ ctrDef.dependencyJarNames
      } else if ((ctrDef.jarName != null))
        jarset = jarset + ctrDef.JarName
      else if (ctrDef.dependencyJarNames != null)
        jarset = jarset ++ ctrDef.dependencyJarNames
      // val typ = MdMgr.GetMdMgr.Type(f.Ttype, ftypeVersion, true)
      argsList = (f.NameSpace, f.Name, ctrDef.NameSpace, ctrDef.Name, false, null) :: argsList
      keysStr.append("(\"" + f.Name + "\",")
      // argsList = (f.NameSpace, f.Name, typ.get.NameSpace, typ.get.Name, false, null) :: argsList

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (scalaclass.toString, assignCsvdata.toString, assignJsondata.toString, assignXmldata.toString, list, argsList, addMsg.toString, jarset, keysStr.toString)
  }

  private def handleMessage(mdMgr: MdMgr, ftypeVersion: Int, f: Element): (String, String, String, String, List[(String, String)], List[(String, String, String, String, Boolean, String)], String, Set[String]) = {

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

    try {

      var msgDef: MessageDef = mdMgr.Message(f.Ttype, ftypeVersion, true).getOrElse(null)
      if (msgDef == null) throw new Exception(f.Ttype + " do not exists throwing null pointer")

      scalaclass = scalaclass.append("%svar %s:%s = new %s();%s".format(pad1, f.Name, msgDef.PhysicalName, msgDef.PhysicalName, newline))
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

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (scalaclass.toString, assignCsvdata.toString, assignJsondata.toString, assignXmldata.toString, list, argsList, addMsg.toString, jarset)
  }

  private def handleConcept(mdMgr: MdMgr, ftypeVersion: Int, f: Element): (String, String, String, String, List[(String, String)], List[(String, String, String, String, Boolean, String)], String, Set[String]) = {

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

    try {

      var attribute: BaseAttributeDef = mdMgr.Attribute(f.Ttype, ftypeVersion, true).getOrElse(null)

      if (attribute == null) throw new Exception("Attribute " + f.Ttype + " do not exists throwing Null pointer")

      if ((attribute.typeString.equals("String")) || (attribute.typeString.equals("Int")) || (attribute.typeString.equals("Float")) || (attribute.typeString.equals("Double")) || (attribute.typeString.equals("Char"))) {
        if (attribute.typeDef.implementationName.isEmpty())
          throw new Exception("Implementation Name not found in metadata for namespace %s" + f.Ttype)
        else
          fname = attribute.typeDef.implementationName + ".Input"

        if (f.Name.split("\\.").size == 2) {

          val dval: String = getDefVal("system." + attribute.typeString.toLowerCase())

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

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (scalaclass.toString, assignCsvdata.toString, assignJsondata.toString, assignXmldata.toString, list, argsList, addMsg.toString, jarset)
  }
  //generates the variables string and assign string
  private def classStr(message: Message, mdMgr: MdMgr): (String, String, String, String, Int, List[(String, String)], List[(String, String, String, String, Boolean, String)], String) = {
    var scalaclass = new StringBuilder(8 * 1024)
    var assignCsvdata = new StringBuilder(8 * 1024)
    var assignJsondata = new StringBuilder(8 * 1024)
    var assignXmldata = new StringBuilder(8 * 1024)
    var addMsg = new StringBuilder(8 * 1024)
    var keysStr = new StringBuilder(8 * 1024)
    var msgAndCntnrsStr = new StringBuilder(8 * 1024)
    var typeImpl = new StringBuilder(8 * 1024)
    var keysVarStr: String = ""
    var MsgsAndCntrsVarStr: String = ""
    var typeImplStr: String = ""

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

    var ftypeVersion: Int = -1
    var addMessage: String = ""

    //  scalaclass = scalaclass.append(getIsFixed(message) + newline + getMessageName(message) + newline + getName(message) + newline + getVersion(message) + newline + newline)
    try {
      scalaclass = scalaclass.append(getIsFixed(message) + newline + getName(message) + newline + getVersion(message) + newline + newline)
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

      if (paritionkeys != null)
        paritionkeys.foreach { partitionkey =>
          keys = keys + partitionkey.toLowerCase()
        }

      if (primaryKeys != null)
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
            ftypeVersion = message.Version.replaceAll("[.]", "").toInt

          if ((f.ElemType.equals("Field")) || (f.ElemType.equals("Fields"))) {
            log.trace("message.Version " + message.Version.replaceAll("[.]", "").toInt)

            val typ = MdMgr.GetMdMgr.Type(f.Ttype, ftypeVersion, true) // message.Version.toInt

            if (typ.getOrElse("None").equals("None"))
              throw new Exception("Type not found in metadata for Name: " + f.Name + " , NameSpace: " + f.NameSpace + " , Version: " + message.Version + " , Type : " + f.Ttype)

            if (typ.get.tType == null) throw new Exception("tType in Type do not exists")

            if (typ.get.tType.toString().equals("tArray")) {

              val (arr_1, arr_2, arr_3, arr_4, arr_5, arr_6, arr_7, arr_8, arr_9) = handleArrayType(typ, f, message.Fixed)

              scalaclass = scalaclass.append(arr_1)
              assignCsvdata = assignCsvdata.append(arr_2)
              assignJsondata = assignJsondata.append(arr_3)

              assignXmldata = assignXmldata.append(arr_4)
              list = arr_5
              argsList = argsList ::: arr_6
              addMsg = addMsg.append(arr_7)
              jarset = jarset ++ arr_8
              msgAndCntnrsStr = msgAndCntnrsStr.append(arr_9)

              //       =  assignCsvdata.toString, assignJsondata.toString, assignXmldata.toString,  list, argsList, addMsg.toString)  = 
            } else if (typ.get.tType.toString().equals("tHashMap")) {

              assignCsvdata.append(newline + "//HashMap not handled at this momemt" + newline)
              assignJsondata.append(newline + "//HashMap not handled at this momemt" + newline)
            } else if (typ.get.tType.toString().equals("tTreeSet")) {

              assignCsvdata.append(newline + "//Tree Set not handled at this momemt" + newline)
              assignJsondata.append(newline + "//Tree Set not handled at this momemt" + newline)
            } else {

              val (baseTyp_1, baseTyp_2, baseTyp_3, baseTyp_4, baseTyp_5, baseTyp_6, baseTyp_7, baseTyp_8, baseTyp_9, baseTyp_10) = handleBaseTypes(keys, message.Fixed, typ, f, message.Version)
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
            }
            count = count + 1

          } else if ((f.ElemType.equals("Container")) || (f.ElemType.equals("Message"))) {

            val typ = MdMgr.GetMdMgr.Type(f.Ttype, ftypeVersion, true) // message.Version.toInt

            if (typ.getOrElse("None").equals("None"))
              throw new Exception("Type not found in metadata for Name: " + f.Name + " , NameSpace: " + f.NameSpace + " , Version: " + message.Version + " , Type : " + f.Ttype)

            if (typ.getOrElse(null) != null) {
              if (typ.get.tType.toString().equals("tArrayBuf")) {
                val (arrayBuf_1, arrayBuf_2, arrayBuf_3, arrayBuf_4, arrayBuf_5, arrayBuf_6, arrayBuf_7, arrayBuf_8, arrayBuf_9) = handleArrayBuffer(message.NameSpace, typ, f)
                scalaclass = scalaclass.append(arrayBuf_1)
                assignCsvdata = assignCsvdata.append(arrayBuf_2)
                assignJsondata = assignJsondata.append(arrayBuf_3)
                assignXmldata = assignXmldata.append(arrayBuf_4)
                list = arrayBuf_5
                argsList = argsList ::: arrayBuf_6
                addMsg = addMsg.append(arrayBuf_7)
                jarset = jarset ++ arrayBuf_8
                msgAndCntnrsStr = msgAndCntnrsStr.append(arrayBuf_9)

              } else {

                if (f.ElemType.equals("Container")) {

                  val (cntnr_1, cntnr_2, cntnr_3, cntnr_4, cntnr_5, cntnr_6, cntnr_7, cntnr_8, cntnr_9) = handleContainer(mdMgr, ftypeVersion, f)
                  scalaclass = scalaclass.append(cntnr_1)
                  assignCsvdata = assignCsvdata.append(cntnr_2)
                  assignJsondata = assignJsondata.append(cntnr_3)
                  assignXmldata = assignXmldata.append(cntnr_4)
                  list = cntnr_5
                  argsList = argsList ::: cntnr_6
                  addMsg = addMsg.append(cntnr_7)
                  jarset = jarset ++ cntnr_8
                  msgAndCntnrsStr = msgAndCntnrsStr.append(cntnr_9)

                } else if (f.ElemType.equals("Message")) {
                  val (msg_1, msg_2, msg_3, msg_4, msg_5, msg_6, msg_7, msg_8) = handleMessage(mdMgr, ftypeVersion, f)
                  scalaclass = scalaclass.append(msg_1)
                  assignCsvdata = assignCsvdata.append(msg_2)
                  assignJsondata = assignJsondata.append(msg_3)
                  assignXmldata = assignXmldata.append(msg_4)
                  list = msg_5
                  argsList = argsList ::: msg_6
                  addMsg = addMsg.append(msg_7)
                  jarset = jarset ++ msg_8
                }
              }
            }
          } else if (f.ElemType.equals("Concept")) {
            val (ccpt_1, ccpt_2, ccpt_3, ccpt_4, ccpt_5, ccpt_6, ccpt_7, ccpt_8) = handleConcept(mdMgr, ftypeVersion, f)
            scalaclass = scalaclass.append(ccpt_1)
            assignCsvdata = assignCsvdata.append(ccpt_2)
            assignJsondata = assignJsondata.append(ccpt_3)
            assignXmldata = assignXmldata.append(ccpt_4)
            list = ccpt_5
            argsList = argsList ::: ccpt_6
            addMsg = addMsg.append(ccpt_7)
            jarset = jarset ++ ccpt_8
          }

        }
      if (message.concepts != null) {

      }

      if (message.Fixed.toLowerCase().equals("false")) {
        if (keysStr != null && keysStr.toString.trim != "")
          keysVarStr = getKeysStr(keysStr.toString)
        MsgsAndCntrsVarStr = getMsgAndCntnrs(msgAndCntnrsStr.toString)
        //typeImplStr = getTypeImplStr(typeImpl.toString)
        scalaclass.append(keysVarStr + newline + pad1 + typeImplStr + newline + pad1 + MsgsAndCntrsVarStr)

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
      val partitionKeys = if (message.PartitionKey != null) ("Array(" + message.PartitionKey.map(p => p.toLowerCase).mkString(", ") + ")") else ""
      val prmryKeys = if (message.PrimaryKeys != null) ("Array(" + message.PrimaryKeys.map(p => p.toLowerCase).mkString(", ") + ")") else ""

      scalaclass = scalaclass.append(partitionkeyStr(partitionKeys) + newline + primarykeyStr(prmryKeys) + newline + getsetMethods(message.Fixed))

      if (addMsg.size > 5)
        addMessage = addMsg.toString.substring(0, addMsg.length - 5)
      log.trace("final arglist " + argsList)
      if (jarset != null)
        message.jarset = jarset

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }

    (scalaclass.toString, assignCsvdata.toString, assignJsondata.toString, assignXmldata.toString, count, list, argsList, addMessage)

  }

  private def getKeysStr(keysStr: String) = {
    """    var keys = Map(""" + keysStr.toString.substring(0, keysStr.toString.length - 1) + ") \n " +
      """
  	var fields: Map[String, Any] = new HashMap[String, Any];
	"""
  }

  private def getMsgAndCntnrs(msgsAndCntnrs: String): String = {
    var str = ""
    if (msgsAndCntnrs != null && msgsAndCntnrs.toString.trim != "")
      str = "val messagesAndContainers = Set(" + msgsAndCntnrs.toString.substring(0, msgsAndCntnrs.toString.length - 1) + ") \n "
    else
      str = "val messagesAndContainers = Set(\"\") \n "

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

  private def importStmts(msgtype: String): String = {
    var imprt: String = ""
    if (msgtype.equals("Message"))
      imprt = "import com.ligadata.OnLEPBase.{BaseMsg, BaseMsgObj, TransformMessage, BaseContainer}"
    else if (msgtype.equals("Container"))
      imprt = "import com.ligadata.OnLEPBase.{BaseMsg, BaseContainer, BaseContainerObj}"

    """
package com.ligadata.messagescontainers
    
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.Formats
import scala.xml.XML
import scala.xml.Elem
import com.ligadata.OnLEPBase.{InputData, DelimitedData, JsonData, XmlData}
import com.ligadata.BaseTypes._
import scala.collection.mutable.{ Map, HashMap }
""" + imprt

  }

  private def classname(msg: Message): (StringBuilder, StringBuilder) = {
    var sname: String = ""
    var oname: String = ""
    var clssb: StringBuilder = new StringBuilder()
    var objsb: StringBuilder = new StringBuilder()
    val ver = msg.Version.replaceAll("[.]", "").toInt.toString
    val xtends: String = "extends"
    val space = " "
    val uscore = "_"
    val cls = "class"
    val obj = "object"
    if (msg.msgtype.equals("Message")) {
      oname = "BaseMsgObj {"
      sname = "BaseMsg {"
    } else if (msg.msgtype.equals("Container")) {
      oname = "BaseContainerObj {"
      sname = "BaseContainer {"
    }
    val clsstr = cls + space + msg.NameSpace + uscore + msg.Name + uscore + ver + space + xtends + space + sname
    val objstr = obj + space + msg.NameSpace + uscore + msg.Name + uscore + ver + space + xtends + space + oname

    (clssb.append(clsstr), objsb.append(objstr))
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

  private def getsetMethods(msgFixed: String): String = {

    var getsetters = new StringBuilder(8 * 1024)
    if (msgFixed.toLowerCase().equals("true")) {
      getsetters = getsetters.append("""
    override def set(key: String, value: Any): Unit = { throw new Exception("set function is not yet implemented") }
    override def get(key: String): Any = { throw new Exception("get function is not yet implemented") }
    override def getOrElse(key: String, default: Any): Any = { throw new Exception("getOrElse function is not yet implemented") }
    """)
    } else if (msgFixed.toLowerCase().equals("false")) {

      getsetters = getsetters.append("""
    override def set(key: String, value: Any): Unit = { throw new Exception("set function is not yet implemented") }
    override def get(key: String): Any = {
      fields.get(key)
    }

    override def getOrElse(key: String, default: Any): Any = {
      fields.getOrElse(key, default)
    }
    """)

    }
    getsetters.toString
  }
  //input function conversion
  private def getDefVal(valType: String): String = {
    valType match {
      case "system.int" => "0"
      case "system.float" => "0"
      case "system.boolean" => "false"
      case "system.double" => "0.0"
      case "system.long" => "0"
      case "system.char" => "\' \'"
      case "system.string" => "\"\""
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
  	inputdata.curPos
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
  	inputdata.curPos
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

  private def assignJsonData(assignJsonData: String) = {
    """
  private def assignJsonData(json: JsonData) : Unit =  {
	type tList = List[String]
	type tMap = Map[String, Any]
	var arr: List[String] = null
    var list : List[Map[String, Any]] = null 
	try{ 
	  	json.root_json = Option(parse(json.dataInput).values.asInstanceOf[Map[String, Any]])
      	json.cur_json = json.root_json

	 	val map = json.cur_json.get.asInstanceOf[Map[String, Any]]
	  	if (map == null)
        	throw new Exception("Invalid json data")
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
    """
  private def assignJsonData(json: JsonData) : Unit =  {
	type tList = List[String]
	type tMap = Map[String, Any]
	var arr: List[String] = null
    var list : List[Map[String, Any]] = null 
    var keySet: Set[Any] = Set();
	try{
	  	json.root_json = Option(parse(json.dataInput).values.asInstanceOf[Map[String, Any]])
      	json.cur_json = json.root_json

	 	val map = json.cur_json.get.asInstanceOf[Map[String, Any]]
	  	if (map == null)
        	throw new Exception("Invalid json data")
	  	val msgsAndCntrs : scala.collection.mutable.Map[String, Any] = null
    
	  	// Traverse through whole map and make KEYS are lowercase and populate
	  	map.foreach(kv => {
	  		val key = kv._1.toLowerCase
	  		val typConv = keys.getOrElse(key, null)
	  		if (typConv != null && kv._2 != null) {
                // Cast to correct type
            	val v1 = typConv.Input(kv._2.toString)
            	fields.put(key, v1)
	  		}
	  		else {	 // Is this key is a message or container???
	  		if (messagesAndContainers(key))  
	  		   msgsAndCntrs.put(key, kv._2)
	  		else
            	fields.put(key, kv._2)
	  		}
	  	})
	 
    """ + assignJsonData +
      """
    
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

  def processMsgDef(jsonstr: String, msgDfType: String, mdMgr: MdMgr): (String, ContainerDef) = {
    var classname: String = null
    var ver: String = null
    var classstr_1: String = null
    var containerDef: ContainerDef = null
    try {
      if (mdMgr == null)
        throw new Exception("MdMgr is not found")
      if (msgDfType.equals("JSON")) {
        var message: Message = null
        message = processJson(jsonstr, mdMgr).asInstanceOf[Message]

        if (message.Fixed.equals("true")) {
          val (classStrVal, containerDefVal) = createFixedMsgClass(message, mdMgr)
          classstr_1 = classStrVal
          containerDef = containerDefVal

        } else if (message.Fixed.equals("false")) {
          val (classStrVal, containerDefVal) = createNonFixedMsgClass(message, mdMgr)
          classstr_1 = classStrVal
          containerDef = containerDefVal
        }

      } else throw new Exception("MsgDef Type JSON is only supported")
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (classstr_1, containerDef)
  }

  private def createFixedMsgClass(message: Message, mdMgr: MdMgr): (String, ContainerDef) = {
    var containerDef: ContainerDef = null
    var classstr_1: String = null

    try {
      val (classname, ver, classstr, list, argsList) = createClassStr(message, mdMgr)
      classstr_1 = classstr
      createScalaFile(classstr, ver, classname)
      val cname = message.pkg + "." + message.NameSpace + "_" + message.Name.toString() + "_" + message.Version.replaceAll("[.]", "").toInt.toString

      if (message.msgtype.equals("Message"))
        containerDef = createFixedMsgDef(message, list, mdMgr, argsList)
      else if (message.msgtype.equals("Container"))
        containerDef = createFixedContainerDef(message, list, mdMgr, argsList)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (classstr_1, containerDef)
  }

  private def createNonFixedMsgClass(message: Message, mdMgr: MdMgr): (String, ContainerDef) = {
    var containerDef: ContainerDef = null
    var classstr_1: String = null
    try {
      val (classname, ver, classstr, list, argsList) = createMappedClassStr(message, mdMgr)
      classstr_1 = classstr
      createScalaFile(classstr, ver, classname)
      val cname = message.pkg + "." + message.NameSpace + "_" + message.Name.toString() + "_" + message.Version.replaceAll("[.]", "").toInt.toString

      if (message.msgtype.equals("Message"))
        containerDef = createMappedMsgDef(message, list, mdMgr, argsList)
      else if (message.msgtype.equals("Container"))
        containerDef = createMappedContainerDef(message, list, mdMgr, argsList)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (classstr_1, containerDef)
  }

  private def createClassStr(message: Message, mdMgr: MdMgr): (String, String, String, List[(String, String)], List[(String, String, String, String, Boolean, String)]) = {
    var scalaclass = new StringBuilder(8 * 1024)
    val ver = message.Version.replaceAll("[.]", "").toInt.toString
    val newline = "\n"
    var addMsgStr: String = ""
    var list_msg = List[(String, String)]()
    var argsList_msg = List[(String, String, String, String, Boolean, String)]()

    try {
      val (classstr, csvassignstr, jsonstr, xmlStr, count, list, argsList, addMsg) = classStr(message, mdMgr)
      list_msg = list
      argsList_msg = argsList
      addMsgStr = addMessage(addMsg, message)
      val (btrait, striat, csetters) = getBaseTrait(message)
      val cobj = createObj(message)
      val isFixed = getIsFixed(message)
      val (clsstr, objstr) = classname(message)
      scalaclass = scalaclass.append(importStmts(message.msgtype) + newline + newline + objstr + newline + cobj.toString + newline + clsstr.toString + newline)
      scalaclass = scalaclass.append(classstr + csetters + addMsgStr + populate + populatecsv(csvassignstr, count) + populateJson + assignJsonData(jsonstr) + assignXmlData(xmlStr) + " \n}")
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (message.Name, ver.toString, scalaclass.toString, list_msg, argsList_msg)
  }

  private def createMappedClassStr(message: Message, mdMgr: MdMgr): (String, String, String, List[(String, String)], List[(String, String, String, String, Boolean, String)]) = {
    var scalaclass = new StringBuilder(8 * 1024)
    val ver = message.Version.replaceAll("[.]", "").toInt.toString
    val newline = "\n"
    var addMsgStr: String = ""
    var list_msg = List[(String, String)]()
    var argsList_msg = List[(String, String, String, String, Boolean, String)]()
    try {
      val (classstr, csvassignstr, jsonstr, xmlStr, count, list, argsList, addMsg) = classStr(message, mdMgr)
      list_msg = list
      argsList_msg = argsList
      addMsgStr = addMessage(addMsg, message)
      val (btrait, striat, csetters) = getBaseTrait(message)
      val cobj = createObj(message)
      val isFixed = getIsFixed(message)
      val (clsstr, objstr) = classname(message)
      scalaclass = scalaclass.append(importStmts(message.msgtype) + newline + newline + objstr + newline + cobj.toString + newline + clsstr.toString + newline)
      scalaclass = scalaclass.append(classstr + csetters + addMsgStr + populate + populateMappedCSV(csvassignstr, count) + populateJson + assignMappedJsonData(jsonstr) + assignMappedXmlData(xmlStr) + " \n}")
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (message.Name, ver.toString, scalaclass.toString, list_msg, argsList_msg)

  }
  private def getFields = {
    """
   var fields: Map[String, Any] = new HashMap[String, Any];
    """
  }

  private def createMappedContainerDef(msg: Message, list: List[(String, String)], mdMgr: MdMgr, argsList: List[(String, String, String, String, Boolean, String)]): ContainerDef = {
    var containerDef: ContainerDef = new ContainerDef()

    try {

      if (msg.PartitionKey != null)
        containerDef = mdMgr.MakeMappedContainer(msg.NameSpace, msg.Name, msg.PhysicalName, argsList, msg.Version.replaceAll("[.]", "").toInt, null, msg.jarset.toArray, null, null, msg.PartitionKey.toArray)
      else
        containerDef = mdMgr.MakeMappedContainer(msg.NameSpace, msg.Name, msg.PhysicalName, argsList, msg.Version.replaceAll("[.]", "").toInt, null, msg.jarset.toArray, null, null, null)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    containerDef
  }

  private def createMappedMsgDef(msg: Message, list: List[(String, String)], mdMgr: MdMgr, argsList: List[(String, String, String, String, Boolean, String)]): MessageDef = {
    var msgDef: MessageDef = new MessageDef()
    try {
      if (msg.PartitionKey != null)
        msgDef = mdMgr.MakeMappedMsg(msg.NameSpace, msg.Name, msg.PhysicalName, argsList, msg.Version.replaceAll("[.]", "").toInt, null, msg.jarset.toArray, null, null, msg.PartitionKey.toArray)
      else
        msgDef = mdMgr.MakeMappedMsg(msg.NameSpace, msg.Name, msg.PhysicalName, argsList, msg.Version.replaceAll("[.]", "").toInt, null, msg.jarset.toArray, null, null, null)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    msgDef
  }

  private def createFixedContainerDef(msg: Message, list: List[(String, String)], mdMgr: MdMgr, argsList: List[(String, String, String, String, Boolean, String)]): ContainerDef = {
    var containerDef: ContainerDef = new ContainerDef()
    try {
      if (msg.PartitionKey != null)
        containerDef = mdMgr.MakeFixedContainer(msg.NameSpace, msg.Name, msg.PhysicalName, argsList, msg.Version.replaceAll("[.]", "").toInt, null, msg.jarset.toArray, null, null, msg.PartitionKey.toArray)
      else
        containerDef = mdMgr.MakeFixedContainer(msg.NameSpace, msg.Name, msg.PhysicalName, argsList, msg.Version.replaceAll("[.]", "").toInt, null, msg.jarset.toArray, null, null, null)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    containerDef
  }

  private def createFixedMsgDef(msg: Message, list: List[(String, String)], mdMgr: MdMgr, argsList: List[(String, String, String, String, Boolean, String)]): MessageDef = {
    var msgDef: MessageDef = new MessageDef()

    try {
      if (msg.PartitionKey != null)
        msgDef = mdMgr.MakeFixedMsg(msg.NameSpace, msg.Name, msg.PhysicalName, argsList, msg.Version.replaceAll("[.]", "").toInt, null, msg.jarset.toArray, null, null, msg.PartitionKey.toArray)
      else
        msgDef = mdMgr.MakeFixedMsg(msg.NameSpace, msg.Name, msg.PhysicalName, argsList, msg.Version.replaceAll("[.]", "").toInt, null, msg.jarset.toArray, null, null, null)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    msgDef
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
  private def processJson(json: String, mdMgr: MdMgr): Message = {
    var message: Message = null
    var jtype: String = null
    //val parsed = JSON.parseFull(json)
    val map = parse(json).values.asInstanceOf[Map[String, Any]]
    // val map = parsed.get.asInstanceOf[Map[String, Any]]
    //  val map = parsed
    var key: String = ""
    try {
      jtype = geJsonType(map)
      message = processJsonMap(jtype, map).asInstanceOf[Message]

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

  private def processJsonMap(key: String, map: Map[String, Any]): Message = {
    var msg1: Message = null
    type messageMap = Map[String, Any]
    try {
      if (map.contains(key)) {
        if (map.get(key).get.isInstanceOf[messageMap]) {
          val message = map.get(key).get.asInstanceOf[Map[String, Any]]
          // if (message.get("Fixed").get.equals("true")) {
          msg1 = getMsgorCntrObj(message, key).asInstanceOf[Message]
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

  private def geKVMsgorCntrObj(message: Map[String, Any], mtype: String): Message = {
    var pkg: String = "com.ligadata.messagescontainers"
    val physicalName: String = pkg + "." + message.get("NameSpace").get.toString + "_" + message.get("Name").get.toString() + "_" + message.get("Version").get.toString().replaceAll("[.]", "").toInt.toString
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
    new Message(mtype, message.get("NameSpace").get.toString, message.get("Name").get.toString(), physicalName, message.get("Version").get.toString(), message.get("Description").get.toString(), message.get("Fixed").get.toString(), null, tdataexists, tdata, null, pkg, conceptList, null, null, null, null, null)
  }

  private def getMsgorCntrObj(message: Map[String, Any], mtype: String): Message = {
    var ele: List[Element] = null
    var tdata: TransformData = null
    var tdataexists: Boolean = false
    var container: Message = null
    var pkg: String = "com.ligadata.messagescontainers"
    val tkey: String = "TransformData"
    var pKey: String = null
    var prmryKey: String = null
    var partitionKeysList: List[String] = null
    var primaryKeysList: List[String] = null
    var conceptsList: List[String] = null
    try {
      if (message != null) {
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
            partitionKeysList = message.getOrElse("PartitionKey", null).asInstanceOf[List[String]]
            for (partitionKey: String <- partitionKeysList) {
              if (partitionKeysList.size == 1)
                pKey = partitionKey.toLowerCase()
            }
          }

          if (key.equals("PrimaryKey")) {
            primaryKeysList = message.getOrElse("PrimaryKey", null).asInstanceOf[List[String]]
            for (primaryKey: String <- primaryKeysList) {
              if (primaryKeysList.size == 1)
                prmryKey = primaryKey.toLowerCase()
            }
          }

          if (key.equals("Concepts")) {
            conceptsList = getConcepts(message, key)
          }
        }
        if (ele == null)
          throw new Exception("Either Fields or Elements or Concepts  do not exist in " + message.get("Name").get.toString())
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    val physicalName: String = pkg + "." + message.get("NameSpace").get.toString + "_" + message.get("Name").get.toString() + "_" + message.get("Version").get.toString().replaceAll("[.]", "").toInt.toString
    new Message(mtype, message.get("NameSpace").get.toString, message.get("Name").get.toString(), physicalName, message.get("Version").get.toString(), message.get("Description").get.toString(), message.get("Fixed").get.toString(), ele, tdataexists, tdata, null, pkg, conceptsList, null, null, null, partitionKeysList, primaryKeysList)
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

            } else throw new Exception("Either Fields or Container or Message or Concepts  do not exist in " + key + " json")
          }
        }
      } else throw new Exception("Elements not list do not exist in json")
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
      log.info("NameSpace => " + concept.NameSpace.getOrElse(null));
      log.info("Name => " + concept.Name.getOrElse(None));
      log.info("Type => " + concept.Type.getOrElse(None));
      log.info("=========>");
    }

  }

}
