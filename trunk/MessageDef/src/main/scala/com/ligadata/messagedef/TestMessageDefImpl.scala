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
   //  val json: String = Source.fromFile("ContainerFE.json").getLines.mkString
    //val json: String = Source.fromFile("beneficiary.json").getLines.mkString
    val json: String = Source.fromFile("OutPatient.json").getLines.mkString

    val (classStr: String, msgDef: MessageDef) = msg.processMsgDef(json.toString(), "JSON", MdMgr.GetMdMgr)

  }
}