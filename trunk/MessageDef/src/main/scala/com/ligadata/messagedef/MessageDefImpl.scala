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

class Message(var msgtype: String, var NameSpace: String, var Name: String, var PhysicalName: String, var Version: String, var Description: String, var Fixed: String, var Elements: List[Element], var TDataExists: Boolean, var TrfrmData: TransformData, var jarset: Set[String], var pkg: String, var concepts: List[String], var Ctype: String, var CCollectiontype: String, var Containers: List[String], var PartitionKey: List[String])
class TransformData(var input: Array[String], var output: Array[String], var keys: Array[String])
class Field(var NameSpace: String, var Name: String, var Ttype: String, var CollectionType: String, var Fieldtype: String)
class Element(var NameSpace: String, var Name: String, var Ttype: String, var CollectionType: String, var ElemType: String)
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

  def error[T](prefix: String): Option[T] =
    throw MessageException("%s must be specified".format(prefix))

  //creates the class string
  def createClassStr(message: Message, mdMgr: MdMgr): (String, String, String, List[(String, String)], List[(String, String, String, String, Boolean, String)]) = {
    var scalaclass = new StringBuilder(8 * 1024)
    val ver = message.Version.replaceAll("[.]", "").toInt.toString
    val newline = "\n"
    val (classstr, csvassignstr, jsonstr, xmlStr, count, list, argsList) = classStr(message, mdMgr)
    try {
      val (btrait, striat, csetters) = getBaseTrait(message)
      val cobj = createObj(message)
      val isFixed = getIsFixed(message)
      val (clsstr, objstr) = classname(message)
      scalaclass = scalaclass.append(importStmts(message.msgtype) + newline + newline + objstr + newline + cobj.toString + newline + clsstr.toString + newline)
      scalaclass = scalaclass.append(classstr + csetters + populate + populatecsv.toString + csvAssign(csvassignstr, count) + populateJson + assignJsonData(jsonstr) + populateXml + assignXmlData(xmlStr) + " \n}")
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    (message.Name, ver.toString, scalaclass.toString, list, argsList)
  }

  def createObj(msg: Message): (StringBuilder) = {
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
      cobj.append(tattribs + newline + tdataexists + newline + getMessageName(msg) + newline + getVersion(msg) + newline + createNewMessage(msg) + newline + isFixed + cbrace + newline)

    } else if (msg.msgtype.equals("Container")) {
      cobj.append(getMessageName(msg) + newline + getVersion(msg) + newline + createNewContainer(msg) + newline + isFixed + cbrace + newline)

    }
    cobj
  }
  def getMessageName(msg: Message) = {
    if (msg.msgtype.equals("Message"))
      "\tdef getMessageName: String = " + "\"" + msg.NameSpace + "." + msg.Name + "\""
    else if (msg.msgtype.equals("Container"))
      "\tdef getContainerName: String = " + "\"" + msg.NameSpace + "." + msg.Name + "\""

  }

  def getVersion(msg: Message) = {
    "\tdef getVersion: String = " + "\"" + msg.Version + "\""

  }
  def createNewMessage(msg: Message) = {
    "\tdef CreateNewMessage: BaseMsg  = new " + msg.NameSpace + "_" + msg.Name + "_" + msg.Version.replaceAll("[.]", "").toInt + "()"
  }

  def createNewContainer(msg: Message) = {
    "\tdef CreateNewContainer: BaseContainer  = new " + msg.NameSpace + "_" + msg.Name + "_" + msg.Version.replaceAll("[.]", "").toInt + "()"
  }

  def gettdataexists = {
    "\tdef NeedToTransformData: Boolean = "
  }
  def notdataattribs = {
    "\tdef TransformDataAttributes: TransformMessage = null"
  }

  def tdataattribs(msg: Message) = {
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

  def gettdataattribs(a: Array[String]): String = {
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

  def getBaseTrait(message: Message): (String, String, String) = {
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

  //generates the variables string and assign string
  def classStr(message: Message, mdMgr: MdMgr): (String, String, String, String, Int, List[(String, String)], List[(String, String, String, String, Boolean, String)]) = {
    var scalaclass = new StringBuilder(8 * 1024)
    var assignCsvdata = new StringBuilder(8 * 1024)
    var assignJsondata = new StringBuilder(8 * 1024)
    var assignXmldata = new StringBuilder(8 * 1024)
    var list = List[(String, String)]()
    var argsList = List[(String, String, String, String, Boolean, String)]()
    val pad1 = "\t"
    val pad2 = "\t\t"
    val pad3 = "\t\t\t"
    val newline = "\n"
    var jarset: Set[String] = Set();
    var arrayType: ArrayTypeDef = null
    var arrayBufType: ArrayBufTypeDef = null
    var fname: String = ""
    var count: Int = 0
    var concepts: String = "concepts"

    scalaclass = scalaclass.append(getIsFixed(message) + newline + getMessageName(message) + newline + getVersion(message) + newline)
    // for (e <- message.Elements) {
    //  var fields = e.Fields
    if (message.Elements != null)
      for (f <- message.Elements) {
        // val typ = MdMgr.GetMdMgr.Type(key, ver, onlyActive)(f.Ttype)
        //val attr = MdMgr.GetMdMgr.Attribute(message.NameSpace, message.Name)
        //  val container = MdMgr.GetMdMgr.Containers(onlyActive, latestVersion)

        if ((f.ElemType.equals("Field")) || (f.ElemType.equals("Fields"))) {
          val typ = MdMgr.GetMdMgr.Type(f.Ttype, message.Version.replaceAll("[.]", "").toInt, true) // message.Version.toInt

          if (typ.get.tType.toString().equals("tArray")) {
            arrayType = typ.get.asInstanceOf[ArrayTypeDef]

            if (arrayType.elemDef.implementationName.isEmpty())
              throw new Exception("Implementation Name not found in metadata for namespace %s" + f.Ttype)

            else fname = arrayType.elemDef.implementationName + ".Input"

            argsList = (f.NameSpace, f.Name, typ.get.NameSpace, typ.get.Name, false, null) :: argsList
            scalaclass = scalaclass.append("%svar %s: %s = _ ;%s".format(pad1, f.Name, typ.get.typeString, newline))

            assignCsvdata.append("%s%s = list(idx).split(arrvaldelim, -1).map(v => %s(v));\n%sidx = idx+1\n".format(pad2, f.Name, fname, pad2))

            if ((arrayType.dependencyJarNames != null) && (arrayType.JarName != null))
              jarset = jarset + arrayType.JarName ++ arrayType.dependencyJarNames
            else if (arrayType.JarName != null)
              jarset = jarset + arrayType.JarName

          } else {
            if (typ.get.implementationName.isEmpty())
              throw new Exception("Implementation Name not found in metadata for namespace %s" + f.Ttype)

            if (typ.isEmpty)
              throw new Exception("Type %s not found in metadata for namespace %s" + f.Ttype)
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

            val dval: String = getDefVal(f.Ttype)
            list = (f.Name, f.Ttype) :: list

            scalaclass = scalaclass.append("%svar %s:%s = _ ;%s".format(pad1, f.Name, typ.get.physicalName, newline))

            assignCsvdata.append("%s%s = %s(list(idx));\n%sidx = idx+1\n".format(pad2, f.Name, fname, pad2))
            assignJsondata.append("%s %s = %s(map.getOrElse(\"%s\", %s).toString)%s".format(pad1, f.Name, fname, f.Name, dval, newline))
            assignXmldata.append("%sval _%sval_  = (xml \\\\ \"%s\").text.toString %s%sif (_%sval_  != \"\")%s%s =  %s( _%sval_ ) else %s = %s%s".format(pad3, f.Name, f.Name, newline, pad3, f.Name, pad2, f.Name, fname, f.Name, f.Name, dval, newline))

          }
          count = count + 1

        } else if ((f.ElemType.equals("Container")) || (f.ElemType.equals("Message"))) {

          val typ = MdMgr.GetMdMgr.Type(f.Ttype, message.Version.replaceAll("[.]", "").toInt, true) // message.Version.toInt

          if (typ.get.tType.toString().equals("tArrayBuf")) {

            arrayBufType = typ.get.asInstanceOf[ArrayBufTypeDef]

            argsList = (message.NameSpace, f.Name, arrayBufType.NameSpace, arrayBufType.Name, false, null) :: argsList
            scalaclass = scalaclass.append("%svar %s: %s = new %s;%s".format(pad1, f.Name, typ.get.typeString, typ.get.typeString, newline))
            assignCsvdata.append("%s//%s Implementation of messages is not handled at this time".format(pad2, f.Name))

            if ((arrayBufType.dependencyJarNames != null) && (arrayBufType.JarName != null))
              jarset = jarset + arrayBufType.JarName ++ arrayBufType.dependencyJarNames
            else if (arrayBufType.JarName != null)
              jarset = jarset + arrayBufType.JarName

          } else {

            if (f.ElemType.equals("Container")) {

              var ctrDef: ContainerDef = mdMgr.Container(f.Ttype, message.Version.replaceAll("[.]", "").toInt, true).getOrElse(null)

              scalaclass = scalaclass.append("%svar %s:%s = new %s();%s".format(pad1, f.Name, ctrDef.PhysicalName, ctrDef.PhysicalName, newline))
              assignCsvdata.append("%sidx = %s.assignCSV(list, idx);\n%sidx = idx+1\n".format(pad2, f.Name, pad2))
              assignJsondata.append("%s%s.assignJsonData(map)%s".format(pad1, f.Name, newline))
              assignXmldata.append("%s%s.assignXml(xml)%s".format(pad3, f.Name, newline))

              if ((ctrDef.dependencyJarNames != null) && (ctrDef.jarName != null)) {
                jarset = jarset + ctrDef.JarName ++ ctrDef.dependencyJarNames
              } else if ((ctrDef.jarName != null))
                jarset = jarset + ctrDef.JarName
              val typ = MdMgr.GetMdMgr.Type(f.Ttype, message.Version.replaceAll("[.]", "").toInt, true)

              argsList = (f.NameSpace, f.Name, typ.get.NameSpace, typ.get.Name, false, null) :: argsList

            } else if (f.ElemType.equals("Message")) {

              var msgDef: MessageDef = mdMgr.Message(f.Ttype, message.Version.replaceAll("[.]", "").toInt, true).getOrElse(null)

              scalaclass = scalaclass.append("%svar %s:%s = new %s();%s".format(pad1, f.Name, msgDef.PhysicalName, msgDef.PhysicalName, newline))
              assignCsvdata.append("%sidx = %s.assignCSV(list, idx);\n%sidx = idx+1\n".format(pad2, f.Name, pad2))
              //  assignJsondata.append("%s%s.assignJsonData(map)%s".format(pad1, f.Name, newline))
              //  assignXmldata.append("%s%s.assignXml(xml)%s".format(pad3, f.Name, newline))

              if ((msgDef.dependencyJarNames != null) && (msgDef.jarName != null)) {
                jarset = jarset + msgDef.JarName ++ msgDef.dependencyJarNames
              } else if ((msgDef.jarName != null))
                jarset = jarset + msgDef.JarName

              argsList = (f.NameSpace, f.Name, typ.get.NameSpace, typ.get.Name, false, null) :: argsList
              /*
         * Messages in Message is not handled at this moment
       
        if (typ.get.tType.toString().equals("tArrayBuf")) {
          arrayBufType = typ.get.asInstanceOf[ArrayBufTypeDef]

          println("arrayBufType " + arrayBufType.arrayDims + "    " + arrayBufType.typeString)

          argsList = (message.NameSpace, f.Name, f.NameSpace, arrayBufType.Name, false, null) :: argsList
          scalaclass = scalaclass.append("%svar %s: %s = new %s;%s".format(pad1, f.Name, arrayBufType.typeString, arrayBufType.typeString, newline))
          assignCsvdata.append("%s//%s Implementation of messages is not handled at this time\n\n".format(pad2, f.Name))
          assignJsondata.append("%s//%s Implementation of messages is not handled at this time\n\n".format(pad2, f.Name))
          assignXmldata.append("%s//%s Implementation of messages is not handled at this time\n\n".format(pad2, f.Name))

          println("scalaclass " + scalaclass)

          if ((arrayBufType.dependencyJarNames != null) && (arrayBufType.JarName != null))
            jarset = jarset + arrayBufType.JarName ++ arrayBufType.dependencyJarNames
          else if (arrayBufType.JarName != null)
            jarset = jarset + arrayBufType.JarName

        } else {  */

            }
          }
        } else if (f.ElemType.equals("Concepts")) {

          var attribute: BaseAttributeDef = mdMgr.Attribute(f.Ttype, message.Version.replaceAll("[.]", "").toInt, true).asInstanceOf[BaseAttributeDef]
          scalaclass = scalaclass.append("%svar %s:%s = new %s();%s".format(pad1, f.Name, attribute.PhysicalName, attribute.PhysicalName, newline))
          assignCsvdata.append("%sidx = %s.assignCSV(list, idx);\n%sidx = idx+1\n".format(pad2, f.Name, pad2))
         
          if ((attribute.dependencyJarNames != null) && (attribute.jarName != null)) {
            jarset = jarset + attribute.JarName ++ attribute.dependencyJarNames
          } else if ((attribute.jarName != null))
            jarset = jarset + attribute.JarName

          argsList = (attribute.NameSpace, attribute.Name, attribute.typeDef.NameSpace, attribute.typeDef.Name, false, null) :: argsList

        }

      }
    if (message.concepts != null) {

    }
    if (message.PartitionKey != null)
      scalaclass = scalaclass.append(partitionkeyStr(message))

    log.trace("final arglist " + argsList)
    if (jarset != null)
      message.jarset = jarset
    (scalaclass.toString, assignCsvdata.toString, assignJsondata.toString, assignXmldata.toString, count, list, argsList)

  }

  def getIsFixed(message: Message): String = {
    val pad1 = "\t"
    val pad2 = "\t\t"
    val pad3 = "\t\t\t"
    val newline = "\n"
    val isf: String = "false"
    var isfixed = new StringBuilder(8 * 1024)
    if (message.Fixed.equals("true"))
      isfixed = isfixed.append("%sdef IsFixed:Boolean = %s;%s%sdef IsKv:Boolean = %s;%s".format(pad1, message.Fixed, newline, pad1, isf, newline))
    else
      isfixed = isfixed.append("%sdef IsFixed:Boolean = %s;%s%sdef IsKv:Boolean = %s;%s".format(pad1, isf, newline, pad1, message.Fixed, newline))

    isfixed.toString
  }

  def importStmts(msgtype: String): String = {
    var imprt: String = ""
    if (msgtype.equals("Message"))
      imprt = "import com.ligadata.OnLEPBase.{BaseMsg, BaseMsgObj, TransformMessage}"
    else if (msgtype.equals("Container"))
      imprt = "import com.ligadata.OnLEPBase.{BaseContainer, BaseContainerObj}"

    """
package com.ligadata.messagedef
    
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.Formats
import scala.xml.XML
import scala.xml.Elem
import com.ligadata.OnLEPBase.{InputData, DelimitedData, JsonData, XmlData}
import com.ligadata.BaseTypes._
""" + imprt

  }

  def classname(msg: Message): (StringBuilder, StringBuilder) = {
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

  def traitBaseContainer = {
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

  def partitionkeyStr(msg: Message): String = {

    "\n	override def getKeyData: String = " + msg.PartitionKey(0)

  }

  
  def inputData = {
    """ 
trait InputData {
	var dataInput: String
}

class DelimitedData(var dataInput: String, var dataDelim: String) extends InputData(){ }
class JsonData(var dataInput: String) extends InputData(){ }
class XmlData(var dataInput: String) extends InputData(){ }
	  """
  }

  //input function conversion
  def getDefVal(valType: String): String = {
    valType match {
      case "System.Int" => "0"
      case "System.Float" => "0"
      case "System.Boolean" => "false"
      case "System.Double" => "0.0"
      case "System.Long" => "0"
      case "System.Char" => "\' \'"
      case "System.String" => "\"\""
      case _ => ""
    }
  }

  def getMemberType(valType: String): String = {
    valType match {
      case "System.Int" => "Int"
      case "System.Float" => "Float"
      case "System.Boolean" => "Boolean"
      case "System.Double" => "Double"
      case "System.Long" => "Long"
      case "System.Char" => "OptChar"
      case "System.String" => "String"
      case _ => ""
    }
  }

  def cSetter() = {
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
  }
  //populate method in msg-TransactionMsg class
  def populate = {
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

  //populateCSV fucntion in meg class
  def populatecsv = {
    """
  private def populateCSV(inputdata:DelimitedData): Unit = { 
	val delimiter = inputdata.dataDelim
	val dataStr = inputdata.dataInput
	val list = inputdata.dataInput.split(delimiter)
	val idx: Int = assignCsv(list,0)
   }
	 """
  }

  ////csvAssign fucntion in meg class
  def csvAssign(assignCsvdata: String, count: Int): String = {
    """
  private def assignCsv(list:Array[String], startIdx:Int) : Int = {
	var idx = startIdx
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
  	idx
  }
	  """
  }

  def populateJson = {
    """
  private def populateJson(json:JsonData) : Unit = {
	try{
    	if(json == null) throw new Exception("Invalid json data")
     	val parsed = parse(json.dataInput).values.asInstanceOf[Map[String, Any]]
     	assignJsonData(parsed.get.asInstanceOf[Map[String, Any]])
	}catch{
	  case e:Exception =>{
   	    e.printStackTrace()
   	  	throw e	    	
	  }
	}
  }
	  """
  }

  def assignJsonData(assignJsonData: String) = {
    """
  private def assignJsonData(map:Map[String,Any]) : Unit =  {
    try{
	  if(map == null)  throw new Exception("Invalid json data")
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

  def populateXml = {
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

  def assignXmlData(xmlData: String) = {
    """
  def assignXml(xml:Elem) : Unit = {
	try{
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

  //creates the message class file
  def createScalaFile(scalaClass: String, version: String, className: String): Unit = {
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

          val (classname, ver, classstr, list, argsList) = createClassStr(message, mdMgr)
          classstr_1 = classstr
          createScalaFile(classstr, ver, classname)
          val cname = message.pkg + "." + message.NameSpace + "_" + message.Name.toString() + "_" + message.Version.replaceAll("[.]", "").toInt.toString

          if (message.msgtype.equals("Message"))
            containerDef = createMsgDef(message, list, mdMgr, argsList)
          else if (message.msgtype.equals("Container"))
            containerDef = createContainerDef(message, list, mdMgr, argsList)
        } else if (message.Fixed.equals("false")) {
          val (classname, ver, classstr, list, argsList) = createClassStr(message, mdMgr)
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

  def createContainerDef(msg: Message, list: List[(String, String)], mdMgr: MdMgr, argsList: List[(String, String, String, String, Boolean, String)]): ContainerDef = {
    var containerDef: ContainerDef = new ContainerDef()
    containerDef = mdMgr.MakeFixedContainer(msg.NameSpace, msg.Name, msg.PhysicalName, argsList, msg.Version.replaceAll("[.]", "").toInt, null, msg.jarset.toArray, null, null, msg.PartitionKey.toArray)
    containerDef
  }

  def createMsgDef(msg: Message, list: List[(String, String)], mdMgr: MdMgr, argsList: List[(String, String, String, String, Boolean, String)]): MessageDef = {
    var msgDef: MessageDef = new MessageDef()
    msgDef = mdMgr.MakeFixedMsg(msg.NameSpace, msg.Name, msg.PhysicalName, argsList, msg.Version.replaceAll("[.]", "").toInt, null, msg.jarset.toArray, null, null, msg.PartitionKey.toArray)
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
  def processJson(json: String, mdMgr: MdMgr): Message = {
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

  def geJsonType(map: Map[String, Any]): String = {
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

  def processJsonMap(key: String, map: Map[String, Any]): Message = {
    var msg1: Message = null
    type messageMap = Map[String, Any]
    try {
      if (map.contains(key)) {
        if (map.get(key).get.isInstanceOf[messageMap]) {
          val message = map.get(key).get.asInstanceOf[Map[String, Any]]
          if (message.get("Fixed").get.equals("true")) {
            msg1 = getMsgorCntrObj(message, key).asInstanceOf[Message]
          } else if (message.get("Fixed").get.equals("false")) {
            msg1 = geKVMsgorCntrObj(message, key).asInstanceOf[Message]
          } else throw new Exception("Message in json is not Map")

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
    var pkg: String = "com.ligadata.messagedef"
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
    new Message(mtype, message.get("NameSpace").get.toString, message.get("Name").get.toString(), physicalName, message.get("Version").get.toString(), message.get("Description").get.toString(), message.get("Fixed").get.toString(), null, tdataexists, tdata, null, pkg, conceptList, null, null, null, null)
  }

  def getMsgorCntrObj(message: Map[String, Any], mtype: String): Message = {
    var ele: List[Element] = null
    var tdata: TransformData = null
    var tdataexists: Boolean = false
    var container: Message = null
    var pkg: String = "com.ligadata.messagedef"
    val tkey: String = "TransformData"
    var pKey: String = ""
    var partitionKeysList: List[String] = null
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
                pKey = partitionKey
            }
          }

          if (key.equals("Concepts")) {
            conceptsList = getConcepts(message, key)
          }
        }
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    val physicalName: String = pkg + "." + message.get("NameSpace").get.toString + "_" + message.get("Name").get.toString() + "_" + message.get("Version").get.toString().replaceAll("[.]", "").toInt.toString
    new Message(mtype, message.get("NameSpace").get.toString, message.get("Name").get.toString(), physicalName, message.get("Version").get.toString(), message.get("Description").get.toString(), message.get("Fixed").get.toString(), ele, tdataexists, tdata, null, pkg, conceptsList, null, null, null, partitionKeysList)
  }

  def getTransformData(message: Map[String, Any], tkey: String): TransformData = {
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

  def gettData(tmap: Map[String, Any], key: String): Array[String] = {
    type tList = List[String]
    var tlist: List[String] = null
    if (tmap.contains(key) && tmap.get(key).get.isInstanceOf[tList])
      tlist = tmap.get(key).get.asInstanceOf[List[String]]
    tlist.toArray
  }

  def getElementsObj(message: Map[String, Any], key: String): List[Element] = {
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

      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    elist
  }

  def getConceptData(ccpts: List[String], key: String): List[Element] = {
    var element: Element = null
    var lbuffer = new ListBuffer[Element]
    type string = String;
    if (ccpts == null) throw new Exception("Concepts List is null")
    try {
      for (l <- ccpts)
        lbuffer += new Element(null, l.toString(), null, null, key)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    lbuffer.toList
  }

  def getConcepts(message: Map[String, Any], key: String): List[String] = {
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

  def getElements(message: Map[String, Any], key: String): List[Element] = {
    // var fbuffer = new ListBuffer[Field]
    var lbuffer = new ListBuffer[Element]
    var container: Message = null
    type messageList = List[Map[String, Any]]
    type keyMap = Map[String, Any]
    try {
      if (message.get(key).get.isInstanceOf[messageList]) {
        val eList = message.get(key).get.asInstanceOf[List[Map[String, Any]]]
        for (l <- eList) {
          if (l.isInstanceOf[keyMap]) {
            val eMap: Map[String, Any] = l.asInstanceOf[Map[String, Any]]
            if (eMap.contains("Concept")) {
              // lbuffer += getConcept(eMap, "Concept")

            } else if (eMap.contains("Field")) {
              lbuffer += getElement(eMap)
            } else if (key.equals("Fields")) {
              lbuffer += getElementData(eMap.asInstanceOf[Map[String, String]], key)
            } else if (eMap.contains("Container")) {
              val containerMap: Map[String, Any] = eMap.get("Container").get.asInstanceOf[Map[String, Any]]
              // container = getContainerObj(containerMap)
              lbuffer += getElementData(containerMap.asInstanceOf[Map[String, String]], "Container")

            } else if (eMap.contains("Message")) {
              val msgMap: Map[String, Any] = eMap.get("Message").get.asInstanceOf[Map[String, Any]]
              // container = getContainerObj(containerMap)
              lbuffer += getElementData(msgMap.asInstanceOf[Map[String, String]], "Message")

            } else if (eMap.contains("Concepts")) {
              val msgMap: Map[String, Any] = eMap.get("Concepts").get.asInstanceOf[Map[String, Any]]
              // container = getContainerObj(containerMap)
              lbuffer += getElementData(msgMap.asInstanceOf[Map[String, String]], "Concepts")

            }
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

  def getConcept(eMap: Map[String, Any], key: String): List[Field] = {
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

  def getcElement(concept: Concept, eType: String): Field = {
    new Field(concept.NameSpace.getOrElse(null), concept.Name.getOrElse(null), concept.Type.getOrElse(null), null, eType)
  }

  def getElement(eMap: Map[String, Any]): Element = {
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

  def getElementData(field: Map[String, String], key: String): Element = {
    var fld: Element = null
    var name: String = ""

    var namespace: String = ""
    var ttype: String = ""
    var collectionType: String = ""
    type string = String;
    if (field == null) throw new Exception("element Map is null")
    try {
      if (field.contains("NameSpace") && (field.get("NameSpace").get.isInstanceOf[string]))
        namespace = field.get("NameSpace").get.asInstanceOf[String]

      if (field.contains("Name") && (field.get("Name").get.isInstanceOf[String]))
        name = field.get("Name").get.asInstanceOf[String]

      if (field.contains("Type") && (field.get("Type").get.isInstanceOf[string])) {
        val fieldstr = field.get("Type").get.toString.split("\\.")
        if ((fieldstr != null) && (fieldstr.size == 2)) {
          namespace = fieldstr(0)
          ttype = field.get("Type").get.asInstanceOf[String]
        } else
          ttype = field.get("Type").get.asInstanceOf[String]
        if (field.contains("CollectionType") && (field.get("CollectionType").get.isInstanceOf[string])) {
          collectionType = field.get("CollectionType").get.asInstanceOf[String]
        }
      }
      fld = new Element(namespace, name, ttype, collectionType, key)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    fld
  }

  def processConcept(conceptsStr: String, formatType: String) {
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

  /*
   * get non fixed message object
   * 
   */

}
