package com.ligadata.messagedef

import scala.util.parsing.json.JSON
import scala.reflect.runtime.universe
import scala.io.Source
import java.io.File
import org.apache.log4j.Logger

import com.ligadata.fatafat.metadata.MdMgr
import com.ligadata.fatafat.metadata.ContainerDef
import com.ligadata.fatafat.metadata.MessageDef
import scala.collection.mutable.ListBuffer
import com.ligadata.fatafat.metadataload.MetadataLoad
import java.io.File
import java.io.PrintWriter

trait LogTrait {
  val loggerName = this.getClass.getName()
  val logger = Logger.getLogger(loggerName)
}

object TestMessageDefImpl extends LogTrait {
  def main(args: Array[String]) {
    var msg: MessageDefImpl = new MessageDefImpl()
    val mdLoader: MetadataLoad = new MetadataLoad(MdMgr.GetMdMgr, "", "", "", "")
    mdLoader.initialize
    processFixedBeneficiary(msg)
    //   processTestSample(msg)
    //   val outputmsgStr: String = Source.fromFile("/tmp/outputMsgDef/outputMsg1.json").getLines.mkString
    //TestOutputMsg.test
    //    val res: String = com.ligadata.MetadataAPI.MetadataAPIOutputMsg.AddOutputMessage(outputmsgStr, "JSON")
    //  println("result  "+res)

    //   val res1: String = com.ligadata.MetadataAPI.MetadataAPIOutputMsg.RemoveOutputMsg("Barclays", "SkyBranchBneOut", 1000000)
    //   println("result  "+res1)

  }
  private def processContainers(msg: MessageDefImpl): Unit = {
    val json1: String = Source.fromFile("/tmp/RichTesting/hl7history.json").getLines.mkString
    val ((classStr1: String, classStr1_java: String), msgDef1: ContainerDef, (classStr2: String, classStr2_java: String)) = msg.processMsgDef(json1.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddContainer(msgDef1)
    MdMgr.GetMdMgr.AddArray(MdMgr.sysNS, "ArrayOfHL7History", MdMgr.sysNS, "HL7History", 1, 100)
  //  createScalaFile(classStr1, msgDef1.Version.toString, msgDef1.FullName)
   // createScalaFile(classStr2, msgDef1.Version.toString, msgDef1.FullName)

    /*  val json2: String = Source.fromFile("/tmp/RichTesting/inpatientclaimhistory.json").getLines.mkString
    val (classStr2: String, msgDef2: ContainerDef) = msg.processMsgDef(json2.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddContainer(msgDef2)
    MdMgr.GetMdMgr.AddArray(MdMgr.sysNS, "ArrayOfInpatientClaimHistory", MdMgr.sysNS, "InpatientClaimHistory", 1, 100)
    createScalaFile(classStr2, msgDef2.Version.toString, msgDef2.FullName)

    val json3: String = Source.fromFile("/tmp/RichTesting/outpatientclaimhistory.json").getLines.mkString
    val (classStr3: String, msgDef3: ContainerDef) = msg.processMsgDef(json3.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddContainer(msgDef3)
    MdMgr.GetMdMgr.AddArray(MdMgr.sysNS, "ArrayOfOutpatientClaimHistory", MdMgr.sysNS, "OutpatientClaimHistory", 1, 100)
    createScalaFile(classStr3, msgDef3.Version.toString, msgDef3.FullName)

    val json4: String = Source.fromFile("/tmp/RichTesting/macroStateSimple.json").getLines.mkString
    val (classStr4: String, msgDef4: ContainerDef) = msg.processMsgDef(json4.toString(), "JSON", MdMgr.GetMdMgr)
    //  MdMgr.GetMdMgr.AddContainer(msgDef100)
    //  MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfHL7", MdMgr.sysNS, "HL7", 1, 100)
    createScalaFile(classStr4, msgDef4.Version.toString, msgDef4.FullName)

    val json5: String = Source.fromFile("/tmp/RichTesting/macroStateSimpleWithContainerArrays.json").getLines.mkString
    val (classStr5: String, msgDef5: ContainerDef) = msg.processMsgDef(json5.toString(), "JSON", MdMgr.GetMdMgr)
    //   MdMgr.GetMdMgr.AddContainer(msgDef5)
    //  MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfHL7", MdMgr.sysNS, "HL7", 1, 100)
    createScalaFile(classStr5, msgDef5.Version.toString, msgDef5.FullName)

    //  val json4: String = Source.fromFile("/tmp/RichTesting/macroStateSimple.json").getLines.mkString
    // val (classStr5: String, msgDef5: ContainerDef) = msg.processMsgDef(json4.toString(), "JSON", MdMgr.GetMdMgr)
    //  MdMgr.GetMdMgr.AddContainer(msgDef100)
    //  MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfHL7", MdMgr.sysNS, "HL7", 1, 100)
    //  createScalaFile(classStr4, msgDef4.Version.toString, msgDef4.FullName)

    val outpjsonI: String = Source.fromFile("/tmp/Mapped/Containers/IdCodeDim.json").getLines.mkString

    val (classStrOutI: String, msgDefOutI: ContainerDef) = msg.processMsgDef(outpjsonI.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddContainer(msgDefOutI)
    MdMgr.GetMdMgr.AddArray(MdMgr.sysNS, "ArrayOfIdCodeDim", MdMgr.sysNS, "IdCodeDim", 1, 100)
    createScalaFile(classStrOutI, msgDefOutI.Version.toString, msgDefOutI.FullName)

    val outpjson: String = Source.fromFile("/tmp/Mapped/Containers/hcpcsCodes.json").getLines.mkString

    val (classStrOut: String, msgDefOut: ContainerDef) = msg.processMsgDef(outpjson.toString(), "JSON", MdMgr.GetMdMgr)
    createScalaFile(classStrOut, msgDefOut.Version.toString, msgDefOut.FullName)

    //    val outpjsonh: String = Source.fromFile("/tmp/testing/hcpcsCodes.json").getLines.mkString

    // val (classStrOuth: String, msgDefOuth: ContainerDef) = msg.processMsgDef(outpjsonh.toString(), "JSON", MdMgr.GetMdMgr)

    //MdMgr.GetMdMgr.AddContainer(msgDef)

    // val json: String = Source.fromFile("heirachial1.json").getLines.mkString
    // val json1: String = Source.fromFile("heirarchialstruct.json").getLines.mkString

    // var ctrDef: ContainerDef = MdMgr.GetMdMgr.Container("System.LineItmsInfo", 100, true).get
     * 
     */

  }

  private def processMsgContainers(msg: MessageDefImpl): Unit = {

    /* val json100: String = Source.fromFile("/tmp/Mapped/Containers/hl7.json").getLines.mkString
    val (classStr100: String, msgDef100: ContainerDef) = msg.processMsgDef(json100.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddContainer(msgDef100)
    MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfHL7", MdMgr.sysNS, "HL7", 1, 100)
    createScalaFile(classStr100, msgDef100.Version.toString, msgDef100.FullName)

    val inpjson: String = Source.fromFile("/tmp/Mapped/Containers/inpatientclaim.json").getLines.mkString
    val (classStrIn: String, msgDefIn: ContainerDef) = msg.processMsgDef(inpjson.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddContainer(msgDefIn)
    MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfInpatientClaim", MdMgr.sysNS, "inpatientclaim", 1, 100)
    createScalaFile(classStrIn, msgDefIn.Version.toString, msgDefIn.FullName)

    val idjson: String = Source.fromFile("/tmp/Mapped/Containers/outpatientclaim.json").getLines.mkString
    val (classStrOut1: String, msgDefOut1: ContainerDef) = msg.processMsgDef(idjson.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddContainer(msgDefOut1)
    MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfOutpatientClaim", MdMgr.sysNS, "OutpatientClaim", 1, 100)
    createScalaFile(classStrOut1, msgDefOut1.Version.toString, msgDefOut1.FullName)

    val benjson: String = Source.fromFile("/tmp/Mapped/Containers/beneficiary.json").getLines.mkString
    val (classStrBen: String, msgDefBen: ContainerDef) = msg.processMsgDef(benjson.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddContainer(msgDefBen)
    createScalaFile(classStrBen, msgDefBen.Version.toString, msgDefBen.FullName)

    //val inpjson200: String = Source.fromFile("/tmp/Mapped/Containers/inpatientclaim201.json").getLines.mkString
    //val (classStrIn200: String, msgDefIn200: ContainerDef) = msg.processMsgDef(inpjson200.toString(), "JSON", MdMgr.GetMdMgr)
    //createScalaFile(classStrIn200, msgDefIn200.Version.toString, msgDefIn200.FullName)
    //   MdMgr.GetMdMgr.AddContainer(msgDefIn200)
    //  MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfInpatientClaim", MdMgr.sysNS, "inpatientclaim", 1, 200)

    val benjson200: String = Source.fromFile("/tmp/Mapped/Containers/beneficiary200.json").getLines.mkString
    val (classStrben200: String, msgDefben200: ContainerDef) = msg.processMsgDef(benjson200.toString(), "JSON", MdMgr.GetMdMgr)
    createScalaFile(classStrben200, msgDefben200.Version.toString, msgDefben200.FullName)
    * 
    */

  }
  /*
  private def processTestSample(msg: MessageDefImpl): Unit = {

    // val json100: String = Source.fromFile("/tmp/Edifecs/test.json").getLines.mkString
    // val (classStrtest: String, msgDeftest: MessageDef) = msg.processMsgDef(json100.toString(), "JSON", MdMgr.GetMdMgr)
    //createScalaFile(classStrtest, msgDeftest.Version.toString, msgDeftest.FullName)

    //val json100: String = Source.fromFile("/tmp/testing/messages/test.json").getLines.mkString
    //val (classStr100: String, msgDef100: MessageDef) = msg.processMsgDef(json100.toString(), "JSON", MdMgr.GetMdMgr)
    //MdMgr.GetMdMgr.AddMsg(msgDef100)
    //MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "test", MdMgr.sysNS, "test", 1, 100)
    //createScalaFile(classStr100, msgDef100.Version.toString, msgDef100.FullName)

    val a: String = Source.fromFile("/tmp/Sample/1.json").getLines.mkString
    val (classStr200: String, msgDef200: MessageDef) = msg.processMsgDef(a.toString(), "JSON", MdMgr.GetMdMgr, false)
    createScalaFile(classStr200, msgDef200.Version.toString, msgDef200.FullName)

    val b: String = Source.fromFile("/tmp/Sample/2.json").getLines.mkString
    val (classStrIn: String, msgDefIn: MessageDef) = msg.processMsgDef(b.toString(), "JSON", MdMgr.GetMdMgr)
    createScalaFile(classStrIn, msgDefIn.Version.toString, msgDefIn.FullName)

    val c: String = Source.fromFile("/tmp/Sample/3.json").getLines.mkString
    val (classStrOut1: String, msgDefOut1: ContainerDef) = msg.processMsgDef(c.toString(), "JSON", MdMgr.GetMdMgr)
    createScalaFile(classStrOut1, msgDefOut1.Version.toString, msgDefOut1.FullName)

    val d: String = Source.fromFile("/tmp/Sample/4.json").getLines.mkString
    val (classStrOut2: String, msgDefOut2: ContainerDef) = msg.processMsgDef(d.toString(), "JSON", MdMgr.GetMdMgr)

    createScalaFile(classStrOut2, msgDefOut2.Version.toString, msgDefOut2.FullName)

  }
*/
  private def processFixedBeneficiary(msg: MessageDefImpl): Unit = {

    // val json100: String = Source.fromFile("/tmp/Edifecs/test.json").getLines.mkString
    // val (classStrtest: String, msgDeftest: MessageDef) = msg.processMsgDef(json100.toString(), "JSON", MdMgr.GetMdMgr)
    //createScalaFile(classStrtest, msgDeftest.Version.toString, msgDeftest.FullName)

    //val json100: String = Source.fromFile("/tmp/testing/messages/test.json").getLines.mkString
    //val (classStr100: String, msgDef100: MessageDef) = msg.processMsgDef(json100.toString(), "JSON", MdMgr.GetMdMgr)
    //MdMgr.GetMdMgr.AddMsg(msgDef100)
    //MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "test", MdMgr.sysNS, "test", 1, 100)
    //createScalaFile(classStr100, msgDef100.Version.toString, msgDef100.FullName)

    val json100: String = Source.fromFile("/tmp/testing/messages/hl7.json").getLines.mkString
    val ((classStr200: String, classStr200_java: String), msgDef200: ContainerDef, (classStr200_1: String, classStr200_java_1: String)) = msg.processMsgDef(json100.toString(), "JSON", MdMgr.GetMdMgr, false)
    MdMgr.GetMdMgr.AddContainer(msgDef200)
    MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfHL7", MdMgr.sysNS, "HL7", 1, 100)
    createScalaFile(classStr200, msgDef200.Version.toString, msgDef200.FullName, ".scala")
    createScalaFile(classStr200_java, msgDef200.Version.toString, msgDef200.FullName + "_java", ".java")
    createScalaFile(classStr200_1, "", msgDef200.FullName + "_1", ".scala")
    createScalaFile(classStr200_java_1, "", msgDef200.FullName + "_1_java", ".java")

   val inpjson: String = Source.fromFile("/tmp/testing/messages/inpatientclaim.json").getLines.mkString
    val ((classStrIn: String, classStrInJava: String), msgDefIn: ContainerDef, (classStrIn1: String, classStrIn1_java: String)) = msg.processMsgDef(inpjson.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddContainer(msgDefIn)
    MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfInpatientClaim", MdMgr.sysNS, "inpatientclaim", 1, 100)
    createScalaFile(classStrIn, msgDefIn.Version.toString, msgDefIn.FullName,  ".scala")
    createScalaFile(classStrInJava, msgDefIn.Version.toString, msgDefIn.FullName + "_java", ".java")
    createScalaFile(classStrIn1, "", msgDefIn.FullName + "_1" , ".scala")
    createScalaFile(classStrIn1_java, "", msgDefIn.FullName + "_1_java", ".java")

    val idjson: String = Source.fromFile("/tmp/testing/messages/outpatientclaim.json").getLines.mkString
    val ((classStrOut1: String, classStrOut1_java: String), msgDefOut1: ContainerDef, (classStrOut11: String, classStrOut11_java: String)) = msg.processMsgDef(idjson.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddContainer(msgDefOut1)
    MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfOutpatientClaim", MdMgr.sysNS, "OutpatientClaim", 1, 100)
    createScalaFile(classStrOut1, msgDefOut1.Version.toString, msgDefOut1.FullName, ".scala")
    createScalaFile(classStrOut1_java, msgDefOut1.Version.toString, msgDefOut1.FullName + "_java", ".java")
    createScalaFile(classStrOut11, "", msgDefOut1.FullName + "_1",  ".scala")
    createScalaFile(classStrOut11_java, "", msgDefOut1.FullName + "_1_java", ".java")

    val benjson: String = Source.fromFile("/tmp/testing/messages/beneficiary.json").getLines.mkString
    val ((classStrBen: String, classStrBen_java: String), msgDefBen: ContainerDef, (classStrBen1: String, classStrBen1_java: String)) = msg.processMsgDef(benjson.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddContainer(msgDefBen)
    createScalaFile(classStrBen, msgDefBen.Version.toString, msgDefBen.FullName,  ".scala")
    createScalaFile(classStrBen_java, msgDefBen.Version.toString, msgDefBen.FullName + "_java", ".java")
    createScalaFile(classStrBen1, "", msgDefBen.FullName + "_1",  ".scala")
    createScalaFile(classStrBen1_java, "", msgDefBen.FullName + "_1_java", ".java")
/*
    val json200: String = Source.fromFile("/tmp/testing/messages/hl7_201.json").getLines.mkString
    val ((classStr200_2: String, classStr200_2_java: String), msgDef200_1: ContainerDef, (classStr2001_1: String, classStr2001_1_java: String)) = msg.processMsgDef(json200.toString(), "JSON", MdMgr.GetMdMgr)
    // MdMgr.GetMdMgr.AddContainer(msgDef200_1)
    // MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfHL7", MdMgr.sysNS, "HL7", 1, 200)
    createScalaFile(classStr200_2, msgDef200_1.Version.toString, msgDef200_1.FullName,  ".scala")
    createScalaFile(classStr200_2, msgDef200_1.Version.toString, msgDef200_1.FullName + "_java", ".java")
    createScalaFile(classStr2001_1, "", msgDef200_1.FullName + "_2",  ".scala")
    createScalaFile(classStr2001_1_java, "", msgDef200_1.FullName + "_2_java", ".java")

    
    val benjson200: String = Source.fromFile("/tmp/testing/messages/beneficiary201.json").getLines.mkString
    val (classStrBen200: String, msgDefBen200: ContainerDef) = msg.processMsgDef(benjson200.toString(), "JSON", MdMgr.GetMdMgr)
    createScalaFile(classStrBen200, msgDefBen200.Version.toString, msgDefBen200.FullName)

 val inpjson200: String = Source.fromFile("/tmp/testing/messages/inpatientclaim201.json").getLines.mkString
    val (classStrIn200: String, msgDefIn200: MessageDef) = msg.processMsgDef(inpjson200.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddMsg(msgDefIn200)
    MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfInpatientClaim", MdMgr.sysNS, "inpatientclaim", 1, 200)
    createScalaFile(classStrIn200, msgDefIn200.Version.toString, msgDefIn200.FullName)
*/
    //   val json: String = Source.fromFile("/tmp/testing/messages/hl7.json").getLines.mkString

    //     val (classStr: String, msgDef: ContainerDef) = msg.processMsgDef(json.toString(), "JSON", MdMgr.GetMdMgr)

    //    val benjsonNew: String = Source.fromFile("/tmp/testing/messages/beneficiary.json").getLines.mkString
    //    val (benclassStr: String, benmsgDef: ContainerDef) = msg.processMsgDef(benjsonNew.toString(), "JSON", MdMgr.GetMdMgr)

    //mgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfOutpatientClaim", MdMgr.sysNS, "OutpatientClaim", 1, baseTypesVer)
    //	mgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfHL7", MdMgr.sysNS, "HL7", 1, baseTypesVer)

  }
  /*
  private def processMappedBeneficiary(msg: MessageDefImpl): Unit = {
    val json: String = Source.fromFile("/tmp/Mapped/Messages/hl7.json").getLines.mkString
    val (classStr: String, msgDef: MessageDef) = msg.processMsgDef(json.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddMsg(msgDef)
    MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfHL7", MdMgr.sysNS, "hl7", 1, 100)
    createScalaFile(classStr, msgDef.Version.toString, msgDef.FullName)

    val inpjson: String = Source.fromFile("/tmp/Mapped/Messages/inpatientclaim.json").getLines.mkString
    val (classStrIn: String, msgDefIn: MessageDef) = msg.processMsgDef(inpjson.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddMsg(msgDefIn)
    MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfInpatientClaim", MdMgr.sysNS, "inpatientclaim", 1, 100)
    createScalaFile(classStrIn, msgDefIn.Version.toString, msgDefIn.FullName)

    val idjson: String = Source.fromFile("/tmp/Mapped/Messages/outpatientclaim.json").getLines.mkString
    val (classStrOut1: String, msgDefOut1: MessageDef) = msg.processMsgDef(idjson.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddMsg(msgDefOut1)
    MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfOutpatientClaim", MdMgr.sysNS, "outpatientClaim", 1, 100)
    createScalaFile(classStrOut1, msgDefOut1.Version.toString, msgDefOut1.FullName)

    val benjson: String = Source.fromFile("/tmp/Mapped/Messages/beneficiary.json").getLines.mkString
    val (classStrBen: String, msgDefBen: MessageDef) = msg.processMsgDef(benjson.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddMsg(msgDefBen)
    MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfBeneficiary", MdMgr.sysNS, "beneficiary", 1, 100)

    createScalaFile(classStrBen, msgDefBen.Version.toString, msgDefBen.FullName)

    /*
    val inpjson1: String = Source.fromFile("/tmp/Mapped/Messages/inpatientclaim1.json").getLines.mkString
    val (classStrIn1: String, msgDefIn1: MessageDef) = msg.processMsgDef(inpjson1.toString(), "JSON", MdMgr.GetMdMgr, true)
    createScalaFile(classStrIn1, msgDefIn1.Version.toString, msgDefIn1.FullName)
*/

    val benjson1: String = Source.fromFile("/tmp/Mapped/Messages/beneficiary1.json").getLines.mkString
    val (classStrBen1: String, msgDefBen1: MessageDef) = msg.processMsgDef(benjson1.toString(), "JSON", MdMgr.GetMdMgr)
    createScalaFile(classStrBen1, msgDefBen1.Version.toString, msgDefBen1.FullName)

    /*  val json100: String = Source.fromFile("/tmp/Mapped/Messages/hl7_200.json").getLines.mkString
    val (classStr100: String, msgDef100: MessageDef) = msg.processMsgDef(json100.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddMsg(msgDef100)
    MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfHL7", MdMgr.sysNS, "HL7", 1, 100)

    *
    */
  }
  */
  private def createScalaFile(scalaClass: String, version: String, className: String, clstype:String): Unit = {
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
}

