package com.ligadata.outputmsgdef

import org.scalatest.FunSuite
import com.ligadata.FatafatBase.BaseMsg
import com.ligadata.FatafatBase.{ DelimitedData, InputData }
import com.ligadata.fatafat.metadata._
import scala.collection.mutable.ArrayBuffer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite
import org.json4s.jackson.JsonMethods._
import scala.xml.XML
import com.ligadata.FatafatBase.{ InputData, DelimitedData, JsonData, XmlData }
import com.ligadata.BaseTypes._
import java.io.{ DataInputStream, DataOutputStream }
import com.ligadata.FatafatBase.{ BaseMsg, BaseMsgObj, TransformMessage, MdBaseResolveInfo }
import com.ligadata.messagescontainers._
import com.ligadata.messagedef._
trait MsgMdlInitialize1 extends BeforeAndAfterEach { this: Suite =>

  //val builder = new StringBuilder

  var ModelReslts: Map[String, Array[(String, String)]] = Map[String, Array[(String, String)]]()
  var HL7Message: System_HL7_1000001_1430446865995 = new System_HL7_1000001_1430446865995();
  var InPMessage: System_InpatientClaim_1000001_1430446866091 = new System_InpatientClaim_1000001_1430446866091();
  var OutPMessage: System_OutpatientClaim_1000001_1430446866113 = new System_OutpatientClaim_1000001_1430446866113();
  var BenMessage: System_Beneficiary_1000001_1430446866134 = new System_Beneficiary_1000001_1430446866134();

  var outputMsgDefs: ArrayBuffer[OutputMsgDef] = new ArrayBuffer[OutputMsgDef]()
  var outputMsgDef: OutputMsgDef = new OutputMsgDef()
  val namespace = "System".toLowerCase()
  val name = "OutputMsg1".toLowerCase()
  val version = MdMgr.ConvertVersionToLong("00.01.00")
  val queue = "TestOutMq_1".toLowerCase()
  var outputFormat: String = "{ \"_dd\": \"${system.beneficiary.hl7messages.clm_id}\", \"_hd\": \"${system.beneficiary.desynpuf_id}\", \"_ad\": \"${system.copdriskassessment.aatdeficiency}\", \"_bd\": \"${system.beneficiary.inpatient_claims.clm_id}\"}"

  override def beforeEach() {
    //create message from msgdef 

    ModelReslts = Map("System.COPDRiskAssessment" -> Array(("AATDeficiency", "true"), ("COPDSymptoms", "false"), ("COPDSeverity", "1a"), ("ChronicSputum", "false"), ("AYearAgo", "20140126"), ("Age", "70"), ("Dyspnoea", "false")))
    val benFormat = "100,19440701,,2,2,0,5,400,0,0,0,0,2,2,2,2,2,2,2,2,2,2,2,0,0,0,0,0,0,0,0,0"
    val inpFormat = "100,201,1,20131127,20131103,2302KU,4000,0,9833008208,1723547751,,20131127,,0,1024,0,0,7,20131103,208,4311~999999~1234566,34590,"
    val outpFormat = "100,201,1,20131212,20131212,1100SK,200,0,1298826910,,,0,,0,40,,93303,,"

    val hl7Format = "100,201,20140808,20140808,19230901,,2,2,0,10,260,12,12,0,0,1,1,1,1,2,1,1,1,1,2,1,86,7,4,19,12,11,9,63,14,11,17,,10,23,1,,34,10,31,1,1,,,4,,,,2,2,8,,3,9,,13,1,5,16,18,2,2,4,1,,,,,,173,75.7,159.6,57.6,172.3,161.7,153.2,,,1,0"
    val inputData = new DelimitedData(hl7Format, ",")
    inputData.tokens = inputData.dataInput.split(inputData.dataDelim, -1)
    inputData.curPos = 0
    var retInpData: InputData = null
    retInpData = inputData
    HL7Message.populate(retInpData)

    val inputDataInp = new DelimitedData(inpFormat, ",")
    inputData.tokens = inputData.dataInput.split(inputData.dataDelim, -1)
    inputData.curPos = 0
    var retInpData1: InputData = null
    retInpData1 = inputData
    InPMessage.populate(retInpData1)

    val inputDataOutP = new DelimitedData(outpFormat, ",")
    inputData.tokens = inputData.dataInput.split(inputData.dataDelim, -1)
    inputData.curPos = 0
    var retInpData2: InputData = null
    retInpData2 = inputData
    OutPMessage.populate(retInpData2)

    val inputDataBen = new DelimitedData(benFormat, ",")
    inputData.tokens = inputData.dataInput.split(inputData.dataDelim, -1)
    inputData.curPos = 0
    var retInpData3: InputData = null
    retInpData3 = inputData
    BenMessage.AddMessage(Array(("0", "outpatient_claims")), OutPMessage)
    BenMessage.AddMessage(Array(("0", "inpatient_claims")), InPMessage)
    BenMessage.AddMessage(Array(("0", "hl7messages")), HL7Message)
    BenMessage.populate(retInpData3)

    println("Benficiray data " + BenMessage.desynpuf_id)

    //val partionFieldKeys = Array[(String, Array[(String, String)], String, String)]()//List("${System.HL7.clm_id}", "${System.HL7.desynpuf_id}")
    var partionFieldKeys = Array(("system.beneficiary", Array(("desynpuf_id", "system.string")), "fixedmessage", "15001"))
    var dfaults = scala.collection.mutable.Map(("system.beneficiary.desynpuf_id" -> "15001"))
    var dataDeclrtion = scala.collection.mutable.Map(("delim" -> ","))
    val value1 = scala.collection.mutable.Set((Array(("inpatient_claims", "system.arraybufferofinpatientclaim"), ("clm_id", "system.long")), null), (Array(("hl7messages", "system.arraybufferofhl7"), ("clm_id", "system.long")), null), (Array(("desynpuf_id", "system.string")), "15001"))
    val value2 = scala.collection.mutable.Set((Array(("aatdeficiency", "system.int")), ""))

    var Fields = scala.collection.mutable.Map((("system.beneficiary", "fixedmessage") -> value1), (("system.copdriskassessment", "model") -> value2))

    outputMsgDef = MdMgr.GetMdMgr.MakeOutputMsg(namespace, name, version, queue, partionFieldKeys, dfaults, dataDeclrtion, Fields, outputFormat)

    MdMgr.GetMdMgr.AddOutputMsg(outputMsgDef)
    outputMsgDefs = outputMsgDefs :+ outputMsgDef
    println("size  " + outputMsgDefs.size)

    super.beforeEach() // To be stackable, must call super.beforeEach
  }

  override def afterEach() {
    try super.afterEach() // To be stackable, must call super.afterEach
    finally {
      //   MdMgr.GetMdMgr.RemoveOutputMsg(namespace, name, version)
    }
  }
}

class OutputMsgDefTestSuite1 extends FunSuite with MsgMdlInitialize1 {

  // val list = inputdata.tokens

  //HL7Message.populate(retInpData)

  /** Test case 1 **/
  TestMessageDefImpl.test
  test("check Output Msg Generator") {

    val outputresult = OutputMsgGenerator.generateOutputMsg(BenMessage, ModelReslts, outputMsgDefs.toArray)
    //val claimid = BenMessage.clm_id.toString
    //  val pufid = "15001"
    //  val aatdeficiency = "true"
    //  val outputFrmtRslt = "{ \"_dd\": \"" + claimid + "\", \"_hd\": \"" + pufid + "\", \"_ad\": \"" + aatdeficiency + "\"}"
    // assert(outputresult.size === 1)
    // println("outputFrmtRslt  " + outputFrmtRslt)
    //   assert(outputresult(0)._1 === queue)
    //   assert(outputresult(0)._2 === Array("15001", claimid))
    //   assert(outputresult(0)._3 === outputFrmtRslt)

  }

}

