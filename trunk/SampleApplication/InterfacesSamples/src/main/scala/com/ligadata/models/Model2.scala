package com.ligadata.models

import com.ligadata.messagescontainers._
import com.ligadata.FatafatBase.{ BaseMsg, BaseContainer, RDDBase, BaseContainerObj, MdBaseResolveInfo, MessageContainerBase, RDDObject, RDD, TimeRange, ModelBaseObj, ModelBase, ModelResult, TransactionContext }

// Model2:

// We generate an alert if min balance < 100 in last 30 days (rolling time window) and if we haven't issued any 
// alert in last two days and if minBalanceAlertOptOut is N.

object System_Model2_1000000 extends ModelBaseObj {
  override def IsValidMessage(msg: MessageContainerBase): Boolean = {
    return false
  }
  override def CreateNewModel(txnContext: TransactionContext): ModelBase = {
    return new System_Model1_1000000(txnContext)
  }
}

class System_Model2_1000000(txnContext: TransactionContext) extends ModelBase {
  override def execute(emitAllResults: Boolean): ModelResult = {
    val custInfo = System_CustomerPreferences_1000000.GetRecentRDDForCurrentPartitionKey
    if (custInfo.isEmpty || custInfo.last.get.minbalancealertoptout != "N")
      return null
    val currentDate = current_date
    val custAlertHistory = System_AlertHistory_1000000.GetRecentRDDForCurrentPartitionKey
    if (custAlertHistory.isEmpty == false && ((currentDate - custAlertHistory.last.get.lastalertdate) < 2)) // 2 days check
      return null
    val monthAgo = TimeRange.MonthAgo(currentDate)
    val monthTxns = System_TransactionMsg_1000000.GetRDDForCurrentPartitionKey(new TimeRange(monthAgo, currentDate), (t: System_TransactionMsg_1000000) => { true })

    val minBal = monthTxns.min((prevBal: Option[Double], txn: System_TransactionMsg_1000000) => { if (prevBal != None && prevBal.get < txn.balance) prevBal else Some(txn.balance) })

    if (minBal != None && minBal.get < 100) {
      // ... Prepare results here ...
      new ModelResult(dateMillis, nowStr, getModelName, getModelVersion, results)
    } else {
      null
    }
  }
}

