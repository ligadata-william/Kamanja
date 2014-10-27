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
import com.ligadata.edifecs.MetadataLoad

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

    //   val json: String = Source.fromFile("/tmp/inputMsg/HL7_MsgDef_2.json").getLines.mkString

    //   val (classStr: String, msgDef: ContainerDef) = msg.processMsgDef(json.toString(), "JSON", MdMgr.GetMdMgr)

    //    val inpjson: String = Source.fromFile("/tmp/inputMsg/InpatientClaim_MsgDef_2.json").getLines.mkString

    //   val (classStrIn: String, msgDefIn: ContainerDef) = msg.processMsgDef(json.toString(), "JSON", MdMgr.GetMdMgr)

    val outpjson: String = Source.fromFile("/tmp/inputMsg/OutpatientClaim_MsgDef_2.json").getLines.mkString

    val (classStrOut: String, msgDefOut: ContainerDef) = msg.processMsgDef(outpjson.toString(), "JSON", MdMgr.GetMdMgr)

    //  val benjson: String = Source.fromFile("/tmp/inputMsg/Beneficiary_MsgDef_1.json").getLines.mkString

    //  val (classStrBen: String, msgDefBen: ContainerDef) = msg.processMsgDef(benjson.toString(), "JSON", MdMgr.GetMdMgr)

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