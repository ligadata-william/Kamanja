package com.ligadata.messagedef

import scala.util.parsing.json.JSON
import scala.reflect.runtime.universe
import scala.io.Source
import java.io.File
import org.apache.log4j.Logger

import com.ligadata.olep.metadata.MdMgr
import com.ligadata.olep.metadata.ContainerDef
import com.ligadata.olep.metadata.MessageDef
import scala.collection.mutable.ListBuffer
import com.ligadata.olep.metadataload.MetadataLoad

trait LogTrait {
  val loggerName = this.getClass.getName()
  val logger = Logger.getLogger(loggerName)
}

object TestMessageDefImpl extends LogTrait {
  def main(args: Array[String]) {
    var msg: MessageDefImpl = new MessageDefImpl()

    val mdLoader: MetadataLoad = new MetadataLoad(MdMgr.GetMdMgr, "", "", "", "")
    mdLoader.initialize
    //  val json: String = Source.fromFile("messageTOutPatient.json").getLines.mkString
    // val json: String = Source.fromFile("ContainerFF.json").getLines.mkString
    // val json: String = Source.fromFile("beneficiary.json").getLines.mkString
    // val json: String = Source.fromFile("OutPatient.json").getLines.mkString
    //  val json: String = Source.fromFile("dictionaryMessage.json").getLines.mkString

    //  val ccptjson: String = Source.fromFile("/tmp/inputMsg/concept.json").getLines.mkString

    //  val (classStr: String, msgDef: ContainerDef) = msg.processMsgDef(ccptjson.toString(), "JSON", MdMgr.GetMdMgr)

    val json: String = Source.fromFile("/tmp/Mapped/Messages/hl7.json").getLines.mkString

    val (classStr: String, msgDef: ContainerDef) = msg.processMsgDef(json.toString(), "JSON", MdMgr.GetMdMgr)
     MdMgr.GetMdMgr.AddContainer(msgDef)
    MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfHL7", MdMgr.sysNS, "HL7", 1, 100)

    val inpjson: String = Source.fromFile("/tmp/Mapped/Messages/inpatientclaim.json").getLines.mkString
    val (classStrIn: String, msgDefIn: ContainerDef) = msg.processMsgDef(inpjson.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddContainer(msgDefIn)
    MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfInpatientClaim", MdMgr.sysNS, "inpatientclaim", 1, 100)

    val idjson: String = Source.fromFile("/tmp/Mapped/Messages/outpatientclaim.json").getLines.mkString
    val (classStrOut1: String, msgDefOut1: ContainerDef) = msg.processMsgDef(idjson.toString(), "JSON", MdMgr.GetMdMgr)
    MdMgr.GetMdMgr.AddContainer(msgDefOut1)
    MdMgr.GetMdMgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfOutpatientClaim", MdMgr.sysNS, "OutpatientClaim", 1, 100)

    val benjson: String = Source.fromFile("/tmp/Mapped/Messages/beneficiary.json").getLines.mkString
    val (classStrBen: String, msgDefBen: ContainerDef) = msg.processMsgDef(benjson.toString(), "JSON", MdMgr.GetMdMgr)


    //mgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfOutpatientClaim", MdMgr.sysNS, "OutpatientClaim", 1, baseTypesVer)
    //	mgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfHL7", MdMgr.sysNS, "HL7", 1, baseTypesVer)

    val outpjsonI: String = Source.fromFile("/tmp/Mapped/Containers/IdCodeDim.json").getLines.mkString

    val (classStrOutI: String, msgDefOutI: ContainerDef) = msg.processMsgDef(outpjsonI.toString(), "JSON", MdMgr.GetMdMgr)
   MdMgr.GetMdMgr.AddContainer(msgDefOutI)
    MdMgr.GetMdMgr.AddArray(MdMgr.sysNS, "ArrayOfIdCodeDim", MdMgr.sysNS, "IdCodeDim", 1, 100)

    val outpjson: String = Source.fromFile("/tmp/Mapped/Containers/hcpcsCodes.json").getLines.mkString

    val (classStrOut: String, msgDefOut: ContainerDef) = msg.processMsgDef(outpjson.toString(), "JSON", MdMgr.GetMdMgr)

    //    val outpjsonh: String = Source.fromFile("/tmp/testing/hcpcsCodes.json").getLines.mkString

    // val (classStrOuth: String, msgDefOuth: ContainerDef) = msg.processMsgDef(outpjsonh.toString(), "JSON", MdMgr.GetMdMgr)

    //MdMgr.GetMdMgr.AddContainer(msgDef)

    // val json: String = Source.fromFile("heirachial1.json").getLines.mkString
    // val json1: String = Source.fromFile("heirarchialstruct.json").getLines.mkString

    // var ctrDef: ContainerDef = MdMgr.GetMdMgr.Container("System.LineItmsInfo", 100, true).get

    //  println("44444444" +ctrDef.PhysicalName)
    //  val (classStr1: String, msgDef1: MessageDef) = msg.processMsgDef(json1.toString(), "JSON", MdMgr.GetMdMgr)

    //val json: String = Source.fromFile("concept.json").getLines.mkString
    //  val json: String = Source.fromFile("dictionaryMessage.json").getLines.mkString

    //msg.processConcept(json, "JSON")
    //   msg.processMsgDef(json.toString(), "JSON", MdMgr.GetMdMgr)
  }

}