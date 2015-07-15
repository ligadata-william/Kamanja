package com.ligadata.models.samples.models

import com.ligadata.FatafatBase.{ BaseMsg, BaseContainer, RddUtils, RddDate, BaseContainerObj, MessageContainerBase, RDDObject, RDD }
import com.ligadata.FatafatBase.{ TimeRange, ModelBaseObj, ModelBase, ModelResultBase, TransactionContext, ModelContext }
import com.ligadata.messagescontainers.System.V1000000._
import RddUtils._
import RddDate._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import java.io.{ DataInputStream, DataOutputStream }

object LowBalanceAlert2 extends ModelBaseObj {
  override def IsValidMessage(msg: MessageContainerBase): Boolean = return msg.isInstanceOf[TransactionMsg]
  override def CreateNewModel(mdlCtxt: ModelContext): ModelBase = return new LowBalanceAlert2(mdlCtxt)
  override def ModelName(): String = "System.LowBalanceAlert2" // Model Name
  override def Version(): String = "0.0.1" // Model Version
  override def CreateResultObject(): ModelResultBase = new LowBalanceAlertResult2()
}

class LowBalanceAlertResult2 extends ModelResultBase {
  var custId: Long = 0;
  var branchId: Int = 0;
  var accNo: Long = 0;
  var curBalance: Double = 0
  var alertType: String = ""
  var triggerTime: Long = 0

  def withCustId(cId: Long): LowBalanceAlertResult2 = {
    custId = cId
    this
  }

  def withBranchId(bId: Int): LowBalanceAlertResult2 = {
    branchId = bId
    this
  }

  def withAccNo(aNo: Long): LowBalanceAlertResult2 = {
    accNo = aNo
    this
  }

  def withCurBalance(curBal: Double): LowBalanceAlertResult2 = {
    curBalance = curBal
    this
  }

  def withAlertType(alertTyp: String): LowBalanceAlertResult2 = {
    alertType = alertTyp
    this
  }

  def withTriggerTime(triggerTm: Long): LowBalanceAlertResult2 = {
    triggerTime = triggerTm
    this
  }

  override def toJson: List[org.json4s.JsonAST.JObject] = {
    val json = List(
      ("CustId" -> custId) ~
        ("BranchId" -> branchId) ~
        ("AccNo" -> accNo) ~
        ("CurBalance" -> curBalance) ~
        ("AlertType" -> alertType) ~
        ("TriggerTime" -> triggerTime))
    return json
  }

  override def toString: String = {
    compact(render(toJson))
  }

  override def get(key: String): Any = {
    if (key.compareToIgnoreCase("custId") == 0) return custId
    if (key.compareToIgnoreCase("branchId") == 0) return branchId
    if (key.compareToIgnoreCase("accNo") == 0) return accNo
    if (key.compareToIgnoreCase("curBalance") == 0) return curBalance
    if (key.compareToIgnoreCase("alertType") == 0) return alertType
    if (key.compareToIgnoreCase("triggerTime") == 0) return triggerTime
    return null
  }

  override def asKeyValuesMap: Map[String, Any] = {
    val map = scala.collection.mutable.Map[String, Any]()
    map("custid") = custId
    map("branchid") = branchId
    map("accno") = accNo
    map("curbalance") = curBalance
    map("alerttype") = alertType
    map("triggertime") = triggerTime
    map.toMap
  }

  override def Deserialize(dis: DataInputStream): Unit = {
    // BUGBUG:: Yet to implement
  }

  override def Serialize(dos: DataOutputStream): Unit = {
    // BUGBUG:: Yet to implement
  }
}

class LowBalanceAlert2(mdlCtxt: ModelContext) extends ModelBase(mdlCtxt, LowBalanceAlert2) {
  override def execute(emitAllResults: Boolean): ModelResultBase = {
    // First check the preferences and decide whether to continue or not
    val gPref = GlobalPreferences.getRecentOrNew(Array("Type1"))
    val pref = CustPreferences.getRecentOrNew
    if (pref.multidayminbalancealertoptout == false)
      return null

    // Check if at least min number of hours elapsed since last alert  
    val curDtTmInMs = RddDate.currentGmtDateTime
    val alertHistory = CustAlertHistory.getRecentOrNew
    if (curDtTmInMs.timeDiffInHrs(RddDate(alertHistory.alertdttminms)) < gPref.minalertdurationinhrs)
      return null

    // get history of transaction whose balance is less than minAlertBalance in last N days
    val lookBackTime = curDtTmInMs.lastNdays(gPref.numlookbackdaysformultidayminbalancealert)
    val rcntTxns = TransactionMsg.getRDD(lookBackTime, { trnsaction: MessageContainerBase =>
      {
        val trn = trnsaction.asInstanceOf[TransactionMsg]
        trn.balance < gPref.minalertbalance
      }
    })

    // quick check to whether it is worth doing group by operation
    if (rcntTxns.isEmpty || rcntTxns.count < gPref.maxnumdaysallowedwithminbalance)
      return null

    // compute distinct days in the retrieved history (number of days with balance less than minAlertBalance)
    // and check if those days meet the threshold
    val daysWhenBalanceIsLessThanMin = rcntTxns.groupBy({ trn => trn.date }).count.toInt
    if (daysWhenBalanceIsLessThanMin <= gPref.maxnumdaysallowedwithminbalance)
      return null

    // create new alert history record and persist (if policy is to keep only one, this will replace existing one)
    CustAlertHistory.build.withalertdttminms(curDtTmInMs.getDateTimeInMs).withalerttype("tooManyMinBalanceDays").withnumdayswithlessbalance(daysWhenBalanceIsLessThanMin).Save

    val rcntTxn = TransactionMsg.getRecent
    val curTmInMs = curDtTmInMs.getDateTimeInMs
    
    // results
    new LowBalanceAlertResult2().withCustId(rcntTxn.get.custid).withBranchId(rcntTxn.get.branchid).withAccNo(rcntTxn.get.accno).withCurBalance(rcntTxn.get.balance).withAlertType("lowBalanceAlert2").withTriggerTime(curTmInMs)
  }
}

