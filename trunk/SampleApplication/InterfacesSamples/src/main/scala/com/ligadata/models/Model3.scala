package com.ligadata.models

import com.ligadata.messagescontainers._
import com.ligadata.FatafatBase.{ BaseMsg, BaseContainer, RDDBase, BaseContainerObj, MdBaseResolveInfo, MessageContainerBase, RDDObject, RDD, TimeRange, ModelBaseObj, ModelBase, ModelResult, TransactionContext }

// Model3:

// We generate an alert if min balance < 100 more than 3 times in last 30 days (rolling time window) and if we haven't issued any 
// alert in last two days and if minBalanceAlertOptOut is N.

object System_Model3_1000000 extends ModelBaseObj {
  override def IsValidMessage(msg: MessageContainerBase): Boolean = {
    return false
  }
  override def CreateNewModel(txnContext: TransactionContext): ModelBase = {
    return new System_Model1_1000000(txnContext)
  }
} 

class System_Model3_1000000(txnContext: TransactionContext) extends ModelBase {
  override def execute(emitAllResults : Boolean) : ModelResult = {
    val custInfo = System_CustomerPreferences_1000000.GetRecentRDDForCurrentPartitionKey
    if (custInfo.isEmpty || custInfo.last.get.minbalancealertoptout != "N")
      return null
    val currentDate = current_date
    val custAlertHistory = System_AlertHistory_1000000.GetRecentRDDForCurrentPartitionKey
    if (custAlertHistory.isEmpty == false && ((currentDate - custAlertHistory.last.get.lastalertdate) < 2)) // 2 days check
      return null
    val monthAgo = TimeRange.MonthAgo(currentDate)
    val minBalLastMonthTxns = System_TransactionMsg_1000000.GetRDDForCurrentPartitionKey(new TimeRange(monthAgo, currentDate), (t: System_TransactionMsg_1000000) => { t.balance < 100 })

    if (minBalLastMonthTxns.size > 2) {
      // ... Prepare results here ...
      new ModelResult(dateMillis, nowStr, getModelName, getModelVersion, results) 
    }
    else {
      null
    }
  }
}

