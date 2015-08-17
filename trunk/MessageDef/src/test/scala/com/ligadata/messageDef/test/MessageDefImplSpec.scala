package com.ligadata.messagedef

import org.scalatest._

import java.io._
import com.ligadata.kamanja.metadata.MdMgr
import com.ligadata.kamanja.metadataload.MetadataLoad
import scala.io.Source
import com.ligadata.kamanja.metadata.ContainerDef
import scala.util.parsing.json.JSON
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.Formats
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.core.JsonProcessingException
import scala.util.control._
import java.net.URL;
import java.net.URLClassLoader;

/**
 * @author eshanhaval
 */
class MessageDefImplSpec extends FlatSpec with PrivateMethodTester with BeforeAndAfterAll {
  
  private val FileName = "/tmp/com.kamanja.test_0.0.1.scala"
  private var reader : FileReader = _
  private var msg : MessageDefImpl = _
  private var mdLoader: MetadataLoad = _
  private var json100 : String = _
  private var classStr200 : String = _
  private var classStr200_java : String = _
  private var msgDef200 : ContainerDef = _
  private var classStr200_1 : String = _
  private var classStr200_java_1 : String = _
  private val testMSG1FilePath : String = "/tmp/testing/messages/hl7Test.json"
  var classpath = ClassLoader.getSystemClassLoader
  
  val inputJsonStr = "{\"Message\" : {\"NameSpace\" : \"System\",\"Name\" : \"msg1\",\"Version\" : \"00.01.00\",\"Description\" : \"Hello World \",\"Fixed\" : \"true\",\"Elements\" : [{\"Field\" : {\"Name\" : \"Id\",\"Type\" : \"System.Int\"}},{\"Field\" : {\"Name\" : \"Name\",\"Type\" : \"System.String\"}},{\"Field\" : {\"Name\" : \"Score\",\"Type\" : \"System.Int\"}}]}}"
  
  def getClassPath() :String = {
    val res = classpath.asInstanceOf[URLClassLoader].getURLs
    val strBuilder : StringBuilder = null
    for(i<-res){
      strBuilder.append(i.getFile+":")
    }
    return strBuilder.toString().substring(0, strBuilder.length-1)
  }
  
  override def beforeAll(){
    /*
        def createScalaFile(scalaClass: String, version: String, className: String, clstype: String): Unit = {
        try {
          val writer = new PrintWriter(new File("/tmp/" + className + "_" + version + clstype))
          writer.write(scalaClass.toString)
          writer.close()
        } catch {
          case e: Exception => {
            e.printStackTrace()
            throw e
          }
        }
      }
 
    msg = new MessageDefImpl()
    mdLoader = new MetadataLoad(MdMgr.GetMdMgr, "", "", "", "")
    mdLoader.initialize
    val msg1Data ="{'Message' : {'NameSpace' : 'System','Name' : 'msg1','Version' : '00.01.00','Description' : 'Hello World ','Fixed' : 'true','Elements' : [{'Field' : {'Name' : 'Id','Type' : 'System.Int'}},{'Field' : {'Name' : 'Name','Type' : 'System.String'}},{'Field' : {'Name' : 'Score','Type' : 'System.Int'}}]}}"

    val f = new File(testMSG1FilePath)
    val writer = new FileWriter(f)
    try{
      writer.write(msg1Data)
    }
    finally{
      writer.close()
    }
     */  
  }
  
  override def afterAll(){
    reader.close()
    val file = new File(FileName)
    if(file.exists() && !file.isDirectory())
      file.delete()
  }
  
  it should "return the appropriate deserialzed writeInt type" in {
    val input: String = "int"
    val expected : String = "writeInt"
    val actual : String = new MessageDefImpl getSerializedType input
    assert(expected === actual)
  }
  
  it should "return the appropriate deserialzed writeUTF type" in {
    val input: String = "string"
    val expected : String = "writeUTF"
    val actual : String = new MessageDefImpl getSerializedType input
    assert(expected === actual)
  }
  
  it should "return the appropriate deserialzed writeLong type" in {
    val input: String = "long"
    val expected : String = "writeLong"
    val actual : String = new MessageDefImpl getSerializedType input
    assert(expected === actual)
  }
  
  it should "return the appropriate deserialzed writeDouble type" in {
    val input: String = "double"
    val expected : String = "writeDouble"
    val actual : String = new MessageDefImpl getSerializedType input
    assert(expected === actual)
  }
  
  it should "return the appropriate deserialzed writeBoolean type" in {
    val input: String = "boolean"
    val expected : String = "writeBoolean"
    val actual : String = new MessageDefImpl getSerializedType input
    assert(expected === actual)
  }
  
  it should "return the appropriate deserialzed writeChar type" in {
    val input: String = "char"
    val expected : String = "writeChar"
    val actual : String = new MessageDefImpl getSerializedType input
    assert(expected === actual)
  }
  
  it should "return the appropriate deserialzed writeFloat type" in {
    val input: String = "float"
    val expected : String = "writeFloat"
    val actual : String = new MessageDefImpl getSerializedType input
    assert(expected === actual)
  }
  
  it should "return the appropriate deserialzed dummyfloat type" in {
    val input: String = "dummyfloat"
    val expected : String = ""
    val actual : String = new MessageDefImpl getSerializedType input
    assert(expected === actual)
  }
  
  it should "create a file" in {
    val inputScalaClass = "com.kamanja.test"
    val inputVersion = "0.0.1"
    val inputClassName = "DemoTestClass"  
    val inputFileName = "/tmp/" + inputClassName + "_" + inputVersion + ".scala"
    val invokePrivateMethod = PrivateMethod[Unit]('createScalaFile)
    val actual = new MessageDefImpl() invokePrivate invokePrivateMethod(inputScalaClass,inputVersion,inputClassName)
    var builder = new StringBuilder
    val f = new File(inputFileName) 
    var checkFile = false
    if(f.exists() && !f.isDirectory()){
      checkFile = true
        reader = new FileReader(inputFileName)
        var c = reader.read()
          while (c != -1) {
           builder.append(c.toChar)
           c = reader.read()
         }
     }
    assert(checkFile == true && builder.toString() == inputScalaClass)
  }
  
  it should "generated message definition should be successfully interpreted" in {
    //def processMsgDef(jsonstr: String, msgDfType: String, mdMgr: MdMgr, recompile: Boolean = false): ((String, String), ContainerDef, (String, String)) = {
    //val inputJsonStr = "{\"Message\" : {\"NameSpace\" : \"System\",\"Name\" : \"msg1\",\"Version\" : \"00.01.00\",\"Description\" : \"Hello World \",\"Fixed\" : \"true\",\"Elements\" : [{\"Field\" : {\"Name\" : \"Id\",\"Type\" : \"System.Int\"}},{\"Field\" : {\"Name\" : \"Name\",\"Type\" : \"System.String\"}},{\"Field\" : {\"Name\" : \"Score\",\"Type\" : \"System.Int\"}}]}}"
    val inputMsgDfType = "JSON"
    val inputRecompile = false
    var msg : MessageDefImpl = new MessageDefImpl()
    val mdLoader: MetadataLoad = new MetadataLoad(MdMgr.GetMdMgr, "", "", "", "")
    mdLoader.initialize
    val inputMdMgr = MdMgr.GetMdMgr
    val actual = msg.processMsgDef(inputJsonStr.toString, inputMsgDfType.toString(), inputMdMgr, inputRecompile)
    var settings = new scala.tools.nsc.Settings()
    val origBootclasspath = settings.bootclasspath.value
    settings.bootclasspath.value = getClassPath
    val out = new java.io.StringWriter()
    var interpreter = new scala.tools.nsc.Interpreter(settings,new PrintWriter(out))
    val returnScala = interpreter.interpret(actual._1._1.substring(actual._1._1.toString().indexOf("import")))
    val returnScalaVersion = interpreter.interpret(actual._3._1.substring(actual._3._1.toString().indexOf("import")))
    
    assert(returnScala === "success" && returnScalaVersion === "success")
  }
  
  it should "match Mapped Previous Message Version" in {
    val expected : String =  """
    prevVerObj.fields.foreach(field => {

          if (prevVerMatchKeys.contains(field)) {
            fields(field._1.toLowerCase) = (field._2._1, field._2._2)
          } else {
            fields(field._1.toLowerCase) =  (0, ValueToString(field._2._2))
          }

        })

    """
    val invokePrivateMethod = PrivateMethod[String]('mappedMsgPrevVerDeser)
    val actual = new MessageDefImpl() invokePrivate invokePrivateMethod()
    assert(expected === actual)
  }
  
  it should "check whether the String is valid json" in {
    val invokePrivateMethod = PrivateMethod[Message]('processJson)
    val actual = new MessageDefImpl() invokePrivate invokePrivateMethod(inputJsonStr, MdMgr.GetMdMgr, false)
   /*
    val objectMapper = new ObjectMapper()
    try{
      val jsonNode = objectMapper.readTree(actual.toString())
    }
    catch{ 
    case e : JsonProcessingException => {e.printStackTrace()}
    }
    assert(true === true)
    */
    var settings = new scala.tools.nsc.Settings()
    val origBootclasspath = settings.bootclasspath.value
    settings.bootclasspath.value = getClassPath
    val out = new java.io.StringWriter()
    var interpreter = new scala.tools.nsc.Interpreter(settings,new PrintWriter(out))
    val returnScala = interpreter.interpret(actual.toString())
    assert(returnScala === "success")
  } 
  
  it should "return appropriate json type" in {
    val expected : String = "Message"
    val map = parse(inputJsonStr).values.asInstanceOf[Map[String, Any]]
    val invokePrivateMethod = PrivateMethod[String]('geJsonType)
    val actual = new MessageDefImpl() invokePrivate invokePrivateMethod(map)
    assert(actual === expected)
  }
  
  it should "return a message object" in {
    val inputMessage = parse(inputJsonStr).values.asInstanceOf[Map[String, Any]]
    val invokePrivateMethod = PrivateMethod[String]('geJsonType)
    val inputKey = new MessageDefImpl() invokePrivate invokePrivateMethod(inputMessage)
    val inputMdgMgr = MdMgr.GetMdMgr
    val inputRecompile = false
    val invokePvtMthd = PrivateMethod[Message]('processJsonMap)
    val actual = new MessageDefImpl() invokePrivate invokePvtMthd(inputMessage)
    
    var settings = new scala.tools.nsc.Settings()
    val origBootclasspath = settings.bootclasspath.value
    settings.bootclasspath.value = getClassPath
    val out = new java.io.StringWriter()
    var interpreter = new scala.tools.nsc.Interpreter(settings,new PrintWriter(out))
    val returnScala = interpreter.interpret(actual.toString())
    assert(returnScala === "success")
    
  }
  
  it should "return a transform object" in {
    val inputMessage = parse(inputJsonStr).values.asInstanceOf[Map[String, Any]]
    val inputTKey = "TransformData"
    val invokePvtMthd = PrivateMethod[TransformData]('getTransformData)
    val actual = new MessageDefImpl() invokePrivate invokePvtMthd(inputMessage,inputTKey)
    
    var settings = new scala.tools.nsc.Settings()
    val origBootclasspath = settings.bootclasspath.value
    settings.bootclasspath.value = getClassPath
    
    val out = new java.io.StringWriter()
    var interpreter = new scala.tools.nsc.Interpreter(settings,new PrintWriter(out))
    val returnScala = interpreter.interpret(actual.toString())
    assert(returnScala === "success")
  }
  
  it should "return message map as array" in {
    val inputMessage = parse(inputJsonStr).values.asInstanceOf[Map[String, Any]]
    val inputTKey = "Input"
    val tmap: Map[String, Any] = inputMessage.get(inputTKey).get.asInstanceOf[Map[String, Any]]
    val invokePvtMthd = PrivateMethod[Array[String]]('gettData)
    val actual = new MessageDefImpl() invokePrivate invokePvtMthd(tmap,inputTKey)
    //bug bug : How to check the result
  }
  
  it should "return a object of type List[Element]" in {
    val inputMessage = parse(inputJsonStr).values.asInstanceOf[Map[String, Any]]
    val inputKey = "Elements"
    val invokePvtMthd = PrivateMethod[List[Element]]('getElementsObj)
    val actual = new MessageDefImpl() invokePrivate invokePvtMthd(inputMessage,inputKey)
    //bug bug : How to check the result
  }
  
  it should "return a List[Element]" in {
    val inputKey = "Concepts"
    val msgMap = parse(inputJsonStr).values.asInstanceOf[Map[String, Any]]
    val inputMessage= msgMap.get(inputKey).get.asInstanceOf[List[String]]
    val invokePvtMthd = PrivateMethod[List[Element]]('getConceptData)
    val actual = new MessageDefImpl() invokePrivate invokePvtMthd(inputMessage,inputKey)
    //bug bug : How to check the result
  }
  
  it should "return a List[String] object" in {
    val map = parse(inputJsonStr).values.asInstanceOf[Map[String, Any]]
    val inputKey = "Concepts"
    val invokePvtMthd = PrivateMethod[List[Element]]('getConcepts)
    val actual = new MessageDefImpl() invokePrivate invokePvtMthd(map,inputKey)
    //Bug Bug how to check the result
  }
  
  it should "return a List[Element] object" in {
    val inputMessage = parse(inputJsonStr).values.asInstanceOf[Map[String, Any]]
    val inputKey = "Elements"
    val invokePvtMthd = PrivateMethod[List[Element]]('getElements)
    val actual = new MessageDefImpl() invokePrivate invokePvtMthd(inputMessage,inputKey)
    //Bug Bug how to check the result
    //Is the input key supplied correct?
  }
  
  it should "return a List[Field] object" in {
    val inputKey = "Concept"
    val msgMap = parse(inputJsonStr).values.asInstanceOf[Map[String, Any]]
    val inputMessage= msgMap.get(inputKey).get.asInstanceOf[List[String]]
    val invokePvtMthd = PrivateMethod[List[Element]]('getConcept)
    val actual = new MessageDefImpl() invokePrivate invokePvtMthd(inputMessage,inputKey)
    //bug bug : How to check the result
    //Is the input key supplied correct?
  }
  
  it should "return a Element object" in {
    val tmap = parse(inputJsonStr).values.asInstanceOf[Map[String, Any]]
    val inputKey = "Elements"
    val inputMessage = tmap.get(inputKey).get.asInstanceOf[List[Map[String, Any]]]
    val loop = new Breaks
    loop.breakable{
      for(l <- inputMessage)
      {
        val eMap: Map[String, Any] = l.asInstanceOf[Map[String, Any]]
        if (eMap.contains("Field")) {
          val invokePvtMthd = PrivateMethod[List[Element]]('getElement)
               val actual = new MessageDefImpl() invokePrivate invokePvtMthd(eMap)
        }
        loop.break()
      }//Bug Bug How to check the result
    }
  }
  
  
  
}