
package com.ligadata.OnLEPBankPoc

import com.ligadata.OnLEPBase.{ InputData, DelimitedData, JsonData, XmlData }
import com.ligadata.OnLEPBase.{ BaseMsg, BaseMsgObj, TransformMessage, BaseContainer }
import com.ligadata.olep.metadata._

import com.ligadata.BaseTypes._


object BankPocMsg_100 extends BaseMsgObj {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def NeedToTransformData: Boolean = true

  def TransformDataAttributes: TransformMessage = {
    val transMsg = new TransformMessage
    transMsg.messageType = "Ligadata.BankPocMsg"
    transMsg.outputKeys = Array("CUST_ID", "ENT_ACC_NUM")
    transMsg.inputFields = Array("CUST_ID", "ENT_ACC_NUM", "ENT_SEG_TYP", "RISK_TIER_ID", "ENT_DTE", "ENT_TME", "ENT_SRC", "ENT_AMT_TYP", "ENT_AMT", "ODR_LMT", "ANT_LMT", "UNP_BUFF", "EB_BUFFER", "OD_BUFFER", "LB_LIMIT", "RUN_LDG_XAU", "Partition Key", "Entry Sequence Number", "STO Source Code", "STO Center Code", "STO Seq Num", "CDI Seq Num", "BBank Posting Date", "BBAnk ATM LTerm", "BBank ATM Tran Number", "BBank Posting Date2", "BBank BCH Num", "BBank BCH Tran Num", "Term Addr", "Term Seq No", "Posting Date", "Version Num", "Reject Reason", "Reject Account", "Original Branch", "Original Account", "Branch", "Product ID", "Business Class", "Op Limit Ind", "Bad Doubtful Ind", "Refer Ind", "Refer Stream", "Refer Reason", "Target Source Id", "Entry Amount Time", "Entry Source", "Entry Type", "Entry Code", "Entry TLA", "Entry Desc", "Benorig Branch", "Benorig Account", "Source Rec Ref", "Benrem User No", "CLRD Notice Ind", "Num Items", "Amount Cash", "Amount Uncleared Day 0", "Amount Uncleared Day 1", "Amount Uncleared Day 2", "Amount Uncleared Day 3", "Amount Uncleared Day 4", "Sub Branch", "Waste Type", "Same Day Entry Type", "Entry Type Ind", "Sort Code", "Service Branch", "Service Branch Account", "Days Notice", "Stops Flag", "Authorisation Date Time", "Authorisation Balance Type", "Authorisation System ID", "Authorisation Type", "Authorisation Status", "Matching System ID", "Reserve Limit", "Last Nights Ledger Balance", "Last Nights Clrd Int Balance", "Last Nights Clrd Fate Balance", "Amoubt Clrd Int Today", "Run C Int Balance Excluding Auths", "Run C Fate Balance Exceluding Auths", "Pred EOD Ledger Balance", "Pred CFI Ledger Balance", "Pred CFF Ledger Balance", "Funds Available Balance", "Amount CMTD outstanding ents", "Number CMTD Outstanding ents", "Amount Non CMTD Outstanding Ents", "Number Non CMTD Outstanding Ents", "Fate Clearance Amount Day 1", "Fate Clearance Amount Day 2", "Fate Clearance Amount Day 3", "Fate Clearance Amount Day 4", "Fate Clearance Amount Day 5", "Fate Clearance Amount Day 6", "Fate Clearance Amount Day 7", "Interest Clearance Amount Day 1", "Interest Clearance Amount Day 2", "Interest Clearance Amount Day 3", "Interest Clearance Amount Day 4", "Interest Clearance Amount Day 5", "PNP Error Flag", "NAR Error Flag", "Auths Error Flag", "Auths SDEP Type", "Narrative Line 1", "Narrative Line 2", "Narrative Line 3", "Narrative Line 4", "Narrative Line 5")
    transMsg.outputFields = Array("CUST_ID", "ENT_ACC_NUM", "ENT_SEG_TYP", "RISK_TIER_ID", "ENT_DTE", "ENT_TME", "ENT_SRC", "ENT_AMT_TYP", "ENT_AMT", "ODR_LMT", "ANT_LMT", "UNP_BUFF", "EB_BUFFER", "OD_BUFFER", "LB_LIMIT", "RUN_LDG_XAU")
    transMsg
  }

  def getMessageName: String = "Ligadata.BankPocMsg"
  def getVersion: String = "00.01.00"
  def CreateNewMessage: BaseMsg = new BankPocMsg_100
}

class BankPocMsg_100 extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "Ligadata.BankPocMsg"
  def getVersion: String = "00.01.00"

  var CUST_ID: Long = 0
  var ENT_ACC_NUM: Long = 0;
  var ENT_SEG_TYP: String = "";
  var ENT_DTE: Int = 0;
  var ENT_TME: Int = 0;
  var ENT_SRC: String = ""
  var ENT_AMT_TYP: Int = 0;
  var ENT_AMT: Double = 0;
  var ODR_LMT: Double = 0;
  var ANT_LMT: Double = 0;
  
  var UNP_BUFF: Double = 0;
  var EB_BUFFER: Double = 0;
  var OD_BUFFER: Double = 0;
  var LB_LIMIT: Double = 0;

  var RUN_LDG_XAU: Double = 0;
 
  override def toString : String = {

    val buffer : StringBuilder = new StringBuilder
	val name : String = getClass.getName()
	buffer.append(s"class $name {\n")
	buffer.append(s"\tCUST_ID     : Long   = ${CUST_ID.toString}\n")
	buffer.append(s"\tENT_ACC_NUM : Long   = ${ENT_ACC_NUM.toString}\n")
	buffer.append(s"\tENT_SEG_TYP : String = ${ENT_SEG_TYP.toString}\n")
	buffer.append(s"\tENT_DTE     : Int    = ${ENT_DTE.toString}\n")
	buffer.append(s"\tENT_TME     : Int    = ${ENT_TME.toString}\n")
	buffer.append(s"\tENT_SRC     : String = ${ENT_SRC.toString}\n")
	buffer.append(s"\tENT_AMT_TYP : Int    = ${ENT_AMT_TYP.toString}\n")
	buffer.append(s"\tENT_AMT     : Double = ${ENT_AMT.toString}\n")
	buffer.append(s"\tODR_LMT     : Double = ${ODR_LMT.toString}\n")
	buffer.append(s"\tANT_LMT     : Double = ${ANT_LMT.toString}\n")
	buffer.append(s"\tUNP_BUFF    : Double = ${UNP_BUFF.toString}\n")
	buffer.append(s"\tEB_BUFFER   : Double = ${EB_BUFFER.toString}\n")
	buffer.append(s"\tOD_BUFFER   : Double = ${OD_BUFFER.toString}\n")
	buffer.append(s"\tLB_LIMIT    : Double = ${LB_LIMIT.toString}\n")
	buffer.append(s"\tRUN_LDG_XAU : Double = ${RUN_LDG_XAU.toString}\n")
	buffer.append(s"}\n")

	buffer.toString
  }
  

  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter)
    assignCsv(list, 0)
  }

  def assignCsv(list: Array[String], startIdx: Int): Int = {
    var idx = startIdx
    try {
      if (list.size < 15)
        throw new Exception("Incorrect input data size")
      CUST_ID = LongImpl.Input(list(idx).trim)
      idx = idx + 1
      ENT_ACC_NUM = LongImpl.Input(list(idx));
      idx = idx + 1
      ENT_SEG_TYP = StringImpl.Input(list(idx).trim)
      idx = idx + 1
      ENT_DTE = IntImpl.Input(list(idx))
      idx = idx + 1
      ENT_TME = IntImpl.Input(list(idx))
      idx = idx + 1
      ENT_SRC = StringImpl.Input(list(idx).trim)
      idx = idx + 1
      ENT_AMT_TYP = IntImpl.Input(list(idx));
      idx = idx + 1
      ENT_AMT = DoubleImpl.Input(list(idx));
      idx = idx + 1
      ODR_LMT = DoubleImpl.Input(list(idx));
      idx = idx + 1
      ANT_LMT = DoubleImpl.Input(list(idx));
      idx = idx + 1
	  UNP_BUFF = DoubleImpl.Input(list(idx));
      idx = idx + 1
	  EB_BUFFER = DoubleImpl.Input(list(idx));
      idx = idx + 1
	  OD_BUFFER = DoubleImpl.Input(list(idx));
      idx = idx + 1
	  LB_LIMIT = DoubleImpl.Input(list(idx));
      idx = idx + 1
      RUN_LDG_XAU = DoubleImpl.Input(list(idx));
      idx = idx + 1

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    idx
  }
}

class AlertHistory_100 extends BaseContainer {
  var CUST_ID: Long = _
  var ENT_ACC_NUM: Long = _
  var LB001Sent: Int = _
  var OD001Sent: Int = _
  var OD002Sent: Int = _
  var OD003Sent: Int = _
  var NO001Sent: Int = _
  var EB001Sent: Int = _
  var EB002Sent: Int = _
  var UTFSent: Int = _
  var PTFSent: Int = _
  var EventDate: Int = _

  override def toString : String = {

    val buffer : StringBuilder = new StringBuilder
	val name : String = getClass.getName()
	buffer.append(s"class $name {\n")
	buffer.append(s"\tCUST_ID     : Long = ${CUST_ID.toString}\n")
	buffer.append(s"\tENT_ACC_NUM : Long = ${ENT_ACC_NUM.toString}\n")
	buffer.append(s"\tLB001Sent   : Int  = ${LB001Sent.toString}\n")
	buffer.append(s"\tOD001Sent   : Int  = ${OD001Sent.toString}\n")
	buffer.append(s"\tOD002Sent   : Int  = ${OD002Sent.toString}\n")
	buffer.append(s"\tOD003Sent   : Int  = ${OD003Sent.toString}\n")
	buffer.append(s"\tNO001Sent   : Int  = ${NO001Sent.toString}\n")
	buffer.append(s"\tEB001Sent   : Int  = ${EB001Sent.toString}\n")
	buffer.append(s"\tEB002Sent   : Int  = ${EB002Sent.toString}\n")
	buffer.append(s"\tUTFSent     : Int  = ${UTFSent.toString}\n")
	buffer.append(s"\tPTFSent     : Int  = ${PTFSent.toString}\n")
	buffer.append(s"\tEventDate   : Int  = ${EventDate.toString}\n")
	buffer.append(s"}\n")

	buffer.toString
  }
  
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def populate(inputdata: InputData): Unit = {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }
  def get(key: String): Any = null
  def getOrElse(key: String, default: Any): Any = null
  def set(key: String, value: Any): Unit = {}
  def getContainerName: String = "AlertHistory"
  def getVersion: String = "00.01.00"

  def Reset: Unit = {
    CUST_ID = 0
    ENT_ACC_NUM = 0
    LB001Sent = 0
    OD001Sent = 0
    OD002Sent = 0
    OD003Sent = 0
    NO001Sent = 0
    EB001Sent = 0
    EB002Sent = 0
    UTFSent = 0
    PTFSent = 0
    EventDate = 0
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter)
    assignCsv(list, 0)
  }

  def assignCsv(list: Array[String], startIdx: Int): Int = {
    var idx = startIdx
    try {
      if (list.size < 11)
        throw new Exception("Incorrect input data size")
      CUST_ID   = LongImpl.Input(list(idx)); idx = idx + 1
      ENT_ACC_NUM = LongImpl.Input(list(idx)); idx = idx + 1
      LB001Sent = IntImpl.Input(list(idx)); idx = idx + 1
      OD001Sent = IntImpl.Input(list(idx)); idx = idx + 1
      OD002Sent = IntImpl.Input(list(idx)); idx = idx + 1
      OD003Sent = IntImpl.Input(list(idx)); idx = idx + 1
      NO001Sent = IntImpl.Input(list(idx)); idx = idx + 1
      EB001Sent = IntImpl.Input(list(idx)); idx = idx + 1
      EB002Sent = IntImpl.Input(list(idx)); idx = idx + 1
      UTFSent = IntImpl.Input(list(idx)); idx = idx + 1
      PTFSent = IntImpl.Input(list(idx)); idx = idx + 1
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    idx
  }
}

class AlertParameters_100 extends BaseContainer {
  var ALERT: String = _
  var MAX_ALERTS_PER_DAY: Int = _
  var ONLINE_START_TIME: Long = _
  var ONLINE_END_TIME: Long = _
  var ALERT_EXPIRY_TIME: Long = _

  override def toString : String = {

    val buffer : StringBuilder = new StringBuilder
	val name : String = getClass.getName()
	buffer.append(s"class $name {\n")
	buffer.append(s"\tALERT              : String = ${ALERT.toString}\n")
	buffer.append(s"\tMAX_ALERTS_PER_DAY : Int    = ${MAX_ALERTS_PER_DAY.toString}\n")
	buffer.append(s"\tONLINE_START_TIME  : Long   = ${ONLINE_START_TIME.toString}\n")
	buffer.append(s"\tONLINE_END_TIME    : Long   = ${ONLINE_END_TIME.toString}\n")
	buffer.append(s"\tALERT_EXPIRY_TIME  : Long   = ${ALERT_EXPIRY_TIME.toString}\n")
	buffer.append(s"}\n")

	buffer.toString
  }
  
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def populate(inputdata: InputData): Unit = {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }
  def get(key: String): Any = null
  def getOrElse(key: String, default: Any): Any = null
  def set(key: String, value: Any): Unit = {}
  def getContainerName: String = "AlertParameters"
  def getVersion: String = "00.01.00"

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter)
    assignCsv(list, 0)
  }

  def assignCsv(list: Array[String], startIdx: Int): Int = {
    var idx = startIdx
    try {
      if (list.size < 5)
        throw new Exception("Incorrect input data size")
      ALERT = StringImpl.Input(list(idx)); idx = idx + 1
      MAX_ALERTS_PER_DAY = IntImpl.Input(list(idx)); idx = idx + 1
      ONLINE_START_TIME = LongImpl.Input(list(idx)); idx = idx + 1
      ONLINE_END_TIME = LongImpl.Input(list(idx)); idx = idx + 1
      ALERT_EXPIRY_TIME = LongImpl.Input(list(idx)); idx = idx + 1
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    idx
  }
}


class CustomerPreferences_100 extends BaseContainer {
  var CUST_ID: Long = _
  var SORT_CODE: Int = _
  var ENT_ACC_NUM: Long = _
  var ACCT_SHORT_NM: String = _
  var RISK_TIER_ID: String = _
  var LB_REG_FLG: Int = _
  var LB_LIMIT: Double = _
  var OD_REG_FLG: Int = _
  var MAX_EB_CNT: Int = _
  var LAST_UPDATE_TS: String = _
  var CONT_ID: Long = _
  var MOBILE_NUMBER: String = _
  var DECEASED_MARKER: Long = _
  var OD_T2_LIMIT: Double = _
  var OD_T1_LIMIT: Double = _
  var NO_FACTOR: Double = _

  override def toString : String = {

    val buffer : StringBuilder = new StringBuilder
	val name : String = getClass.getName()
	buffer.append(s"class $name {\n")
	buffer.append(s"\tCUST_ID          : Long     = ${CUST_ID.toString}\n")
	buffer.append(s"\tSORT_CODE        : Int      = ${SORT_CODE.toString}\n")
	buffer.append(s"\tENT_ACC_NUM      : Long     = ${ENT_ACC_NUM.toString}\n")
	buffer.append(s"\tACCT_SHORT_NM    : String   = ${ACCT_SHORT_NM.toString}\n")
	buffer.append(s"\tRISK_TIER_ID     : Long     = ${RISK_TIER_ID.toString}\n")
	buffer.append(s"\tLB_REG_FLG       : Int      = ${LB_REG_FLG.toString}\n")
	buffer.append(s"\tLB_LIMIT         : Double   = ${LB_LIMIT.toString}\n")
	buffer.append(s"\tOD_REG_FLG       : Int      = ${OD_REG_FLG.toString}\n")
	buffer.append(s"\tMAX_EB_CNT       : Int      = ${MAX_EB_CNT.toString}\n")
	buffer.append(s"\tLAST_UPDATE_TS   : String   = ${LAST_UPDATE_TS.toString}\n")
	buffer.append(s"\tCONT_ID          : Long     = ${CONT_ID.toString}\n")
	buffer.append(s"\tMOBILE_NUMBER    : String   = ${MOBILE_NUMBER.toString}\n")
	buffer.append(s"\tDECEASED_MARKER  : Long     = ${DECEASED_MARKER.toString}\n")
	buffer.append(s"\tOD_T2_LIMIT      : Double   = ${OD_T2_LIMIT.toString}\n")
	buffer.append(s"\tOD_T1_LIMIT      : Double   = ${OD_T1_LIMIT.toString}\n")
	buffer.append(s"\tNO_FACTOR        : Double   = ${NO_FACTOR.toString}\n")
	buffer.append(s"}\n")

	buffer.toString
  }
  
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def populate(inputdata: InputData): Unit = {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }
  def get(key: String): Any = null
  def getOrElse(key: String, default: Any): Any = null
  def set(key: String, value: Any): Unit = {}
  def getContainerName: String = "CustomerPreferences"
  def getVersion: String = "00.01.00"

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter)
    assignCsv(list, 0)
  }

  def populateStrings(list: Array[String]) = {
    assignCsv(list, 0)
  }

  def assignCsv(list: Array[String], startIdx: Int): Int = {
    var idx = startIdx
    try {
      if (list.size < 16)
        throw new Exception("Incorrect input data size")
      CUST_ID = LongImpl.Input(list(idx)); idx = idx + 1
      SORT_CODE = IntImpl.Input(list(idx)); idx = idx + 1
      ENT_ACC_NUM = LongImpl.Input(list(idx)); idx = idx + 1
      ACCT_SHORT_NM = StringImpl.Input(list(idx)); idx = idx + 1
      RISK_TIER_ID = StringImpl.Input(list(idx)); idx = idx + 1
      LB_REG_FLG = IntImpl.Input(list(idx)); idx = idx + 1
      LB_LIMIT = DoubleImpl.Input(list(idx)); idx = idx + 1
      OD_REG_FLG = IntImpl.Input(list(idx)); idx = idx + 1
      MAX_EB_CNT = IntImpl.Input(list(idx)); idx = idx + 1
      LAST_UPDATE_TS = StringImpl.Input(list(idx)); idx = idx + 1
      CONT_ID = LongImpl.Input(list(idx)); idx = idx + 1
      MOBILE_NUMBER = StringImpl.Input(list(idx)); idx = idx + 1
      DECEASED_MARKER = LongImpl.Input(list(idx)); idx = idx + 1
      OD_T2_LIMIT = DoubleImpl.Input(list(idx)); idx = idx + 1
      OD_T1_LIMIT = DoubleImpl.Input(list(idx)); idx = idx + 1
      NO_FACTOR = DoubleImpl.Input(list(idx)); idx = idx + 1
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    idx
  }
}


class TukTier_100 extends BaseContainer {
    
  var TIERSET_ID: String = _
  var T1_START_DATE: Int = _
  var T1_END_DATE: Int = _
  var T1_START_AMT: Double = _
  var T1_END_AMT: Double = _
  var T1_FEE: Double = _
  var T1_MAX_PER_PERIOD_CNT: Int = _
  var T2_START_DATE: Int = _
  var T2_END_DATE: Int = _
  var T2_START_AMT: Double = _
  var T2_END_AMT: Double = _
  var T2_FEE: Double = _
  var T2_MAX_PER_PERIOD_CNT: Int = _
  var T3_START_DATE: Int = _
  var T3_END_DATE: Int = _
  var T3_START_AMT: Double = _
  var T3_END_AMT: Double = _
  var T3_FEE: Double = _
  var T3_MAX_PER_PERIOD_CNT: Int = _
  var T4_START_DATE: Int = _
  var T4_END_DATE: Int = _
  var T4_START_AMT: Double = _
  var T4_END_AMT: Double = _
  var T4_FEE: Double = _
  var T4_MAX_PER_PERIOD_CNT: Int = _
  var T5_START_DATE: Int = _
  var T5_END_DATE: Int = _
  var T5_START_AMT: Double = _
  var T5_END_AMT: Double = _
  var T5_FEE: Double = _
  var T5_MAX_PER_PERIOD_CNT: Int = _
  var T6_START_DATE: Int = _
  var T6_END_DATE: Int = _
  var T6_START_AMT: Double = _
  var T6_END_AMT: Double = _
  var T6_FEE: Double = _
  var T6_MAX_PER_PERIOD_CNT: Int = _


  override def toString : String = {

    val buffer : StringBuilder = new StringBuilder
	val name : String = getClass.getName()
	
	buffer.append(s"\tTIERSET_ID                : Long    = ${TIERSET_ID.toString}\n")
	buffer.append(s"\tT1_START_DATE             : Int     = ${T1_START_DATE.toString}\n")
	buffer.append(s"\tT1_END_DATE               : Int     = ${T1_END_DATE.toString}\n")
	buffer.append(s"\tT1_START_AMT              : Double  = ${T1_START_AMT.toString}\n")
	buffer.append(s"\tT1_END_AMT                : Double  = ${T1_END_AMT.toString}\n")
	buffer.append(s"\tT1_FEE                    : Double  = ${T1_FEE.toString}\n")
	buffer.append(s"\tT1_MAX_PER_PERIOD_CNT     : Int     = ${T1_MAX_PER_PERIOD_CNT.toString}\n")
	buffer.append(s"\tT2_START_DATE             : Int     = ${T2_START_DATE.toString}\n")
	buffer.append(s"\tT2_END_DATE               : Int     = ${T2_END_DATE.toString}\n")
	buffer.append(s"\tT2_START_AMT              : Double  = ${T2_START_AMT.toString}\n")
	buffer.append(s"\tT2_END_AMT                : Double  = ${T2_END_AMT.toString}\n")
	buffer.append(s"\tT2_FEE                    : Double  = ${T2_FEE.toString}\n")
	buffer.append(s"\tT2_MAX_PER_PERIOD_CNT     : Int     = ${T2_MAX_PER_PERIOD_CNT.toString}\n")
	buffer.append(s"\tT3_START_DATE             : Int     = ${T3_START_DATE.toString}\n")
	buffer.append(s"\tT3_END_DATE               : Int     = ${T3_END_DATE.toString}\n")
	buffer.append(s"\tT3_START_AMT              : Double  = ${T3_START_AMT.toString}\n")
	buffer.append(s"\tT3_END_AMT                : Double  = ${T3_END_AMT.toString}\n")
	buffer.append(s"\tT3_FEE                    : Double  = ${T3_FEE.toString}\n")
	buffer.append(s"\tT3_MAX_PER_PERIOD_CNT     : Int     = ${T3_MAX_PER_PERIOD_CNT.toString}\n")
	buffer.append(s"\tT4_START_DATE             : Int     = ${T4_START_DATE.toString}\n")
	buffer.append(s"\tT4_END_DATE               : Int     = ${T4_END_DATE.toString}\n")
	buffer.append(s"\tT4_START_AMT              : Double  = ${T4_START_AMT.toString}\n")
	buffer.append(s"\tT4_END_AMT                : Double  = ${T4_END_AMT.toString}\n")
	buffer.append(s"\tT4_FEE                    : Double  = ${T4_FEE.toString}\n")
	buffer.append(s"\tT4_MAX_PER_PERIOD_CNT     : Int     = ${T4_MAX_PER_PERIOD_CNT.toString}\n")
	buffer.append(s"\tT5_START_DATE             : Int     = ${T5_START_DATE.toString}\n")
	buffer.append(s"\tT5_END_DATE               : Int     = ${T5_END_DATE.toString}\n")
	buffer.append(s"\tT5_START_AMT              : Double  = ${T5_START_AMT.toString}\n")
	buffer.append(s"\tT5_END_AMT                : Double  = ${T5_END_AMT.toString}\n")
	buffer.append(s"\tT5_FEE                    : Double  = ${T5_FEE.toString}\n")
	buffer.append(s"\tT5_MAX_PER_PERIOD_CNT     : Int     = ${T5_MAX_PER_PERIOD_CNT.toString}\n")
	buffer.append(s"\tT6_START_DATE             : Int     = ${T6_START_DATE.toString}\n")
	buffer.append(s"\tT6_END_DATE               : Int     = ${T6_END_DATE.toString}\n")
	buffer.append(s"\tT6_START_AMT              : Double  = ${T6_START_AMT.toString}\n")
	buffer.append(s"\tT6_END_AMT                : Double  = ${T6_END_AMT.toString}\n")
	buffer.append(s"\tT6_FEE                    : Double  = ${T6_FEE.toString}\n")
	buffer.append(s"\tT6_MAX_PER_PERIOD_CNT     : Int     = ${T6_MAX_PER_PERIOD_CNT.toString}\n")
	
	buffer.toString
  }
  
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def populate(inputdata: InputData): Unit = {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }
  def get(key: String): Any = null
  def getOrElse(key: String, default: Any): Any = null
  def set(key: String, value: Any): Unit = {}
  def getContainerName: String = "TukTier"
  def getVersion: String = "00.01.00"

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter)
    assignCsv(list, 0)
  }

  def populateStrings(list: Array[String]) = {
    assignCsv(list, 0)
  }

  def assignCsv(list: Array[String], startIdx: Int): Int = {
    var idx = startIdx
    try {
      if (list.size < 37)
        throw new Exception("Incorrect input data size")
      
      TIERSET_ID = StringImpl.Input(list(idx)); idx = idx + 1
      T1_START_DATE = IntImpl.Input(list(idx)); idx = idx + 1
      T1_END_DATE = IntImpl.Input(list(idx)); idx = idx + 1
      T1_START_AMT = DoubleImpl.Input(list(idx)); idx = idx + 1
      T1_END_AMT = DoubleImpl.Input(list(idx)); idx = idx + 1
      T1_FEE = DoubleImpl.Input(list(idx)); idx = idx + 1
      T1_MAX_PER_PERIOD_CNT = IntImpl.Input(list(idx)); idx = idx + 1
      T2_START_DATE = IntImpl.Input(list(idx)); idx = idx + 1
      T2_END_DATE = IntImpl.Input(list(idx)); idx = idx + 1
      T2_START_AMT = DoubleImpl.Input(list(idx)); idx = idx + 1
      T2_END_AMT = DoubleImpl.Input(list(idx)); idx = idx + 1
      T2_FEE = DoubleImpl.Input(list(idx)); idx = idx + 1
      T2_MAX_PER_PERIOD_CNT = IntImpl.Input(list(idx)); idx = idx + 1
      T3_START_DATE = IntImpl.Input(list(idx)); idx = idx + 1
      T3_END_DATE = IntImpl.Input(list(idx)); idx = idx + 1
      T3_START_AMT = DoubleImpl.Input(list(idx)); idx = idx + 1
      T3_END_AMT = DoubleImpl.Input(list(idx)); idx = idx + 1
      T3_FEE = DoubleImpl.Input(list(idx)); idx = idx + 1
      T3_MAX_PER_PERIOD_CNT = IntImpl.Input(list(idx)); idx = idx + 1
      T4_START_DATE = IntImpl.Input(list(idx)); idx = idx + 1
      T4_END_DATE = IntImpl.Input(list(idx)); idx = idx + 1
      T4_START_AMT = DoubleImpl.Input(list(idx)); idx = idx + 1
      T4_END_AMT = DoubleImpl.Input(list(idx)); idx = idx + 1
      T4_FEE = DoubleImpl.Input(list(idx)); idx = idx + 1
      T4_MAX_PER_PERIOD_CNT = IntImpl.Input(list(idx)); idx = idx + 1
      T5_START_DATE = IntImpl.Input(list(idx)); idx = idx + 1
      T5_END_DATE = IntImpl.Input(list(idx)); idx = idx + 1
      T5_START_AMT = DoubleImpl.Input(list(idx)); idx = idx + 1
      T5_END_AMT = DoubleImpl.Input(list(idx)); idx = idx + 1
      T5_FEE = DoubleImpl.Input(list(idx)); idx = idx + 1
      T5_MAX_PER_PERIOD_CNT = IntImpl.Input(list(idx)); idx = idx + 1
      T6_START_DATE = IntImpl.Input(list(idx)); idx = idx + 1
      T6_END_DATE = IntImpl.Input(list(idx)); idx = idx + 1
      T6_START_AMT = DoubleImpl.Input(list(idx)); idx = idx + 1
      T6_END_AMT = DoubleImpl.Input(list(idx)); idx = idx + 1
      T6_FEE = DoubleImpl.Input(list(idx)); idx = idx + 1
      T6_MAX_PER_PERIOD_CNT = IntImpl.Input(list(idx)); idx = idx + 1

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    idx
  }
}

						        

