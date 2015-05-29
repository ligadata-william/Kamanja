package com.ligadata.models

import com.ligadata.messagescontainers._
import com.ligadata.FatafatBase.{ BaseMsg, BaseContainer, RDDBase, BaseContainerObj, MdBaseResolveInfo, MessageContainerBase, RDDObject, RDD, TimeRange, ModelBaseObj, ModelBase, ModelResult, TransactionContext }

// Model1:

// We generate an alert if min balance < 100 and if we haven't issued any 
// alert today and if minBalanceAlertOptOut is N.

object System_Model1_1000000 extends ModelBaseObj {
  override def IsValidMessage(msg: MessageContainerBase): Boolean = {
    return false
  }
  override def CreateNewModel(txnContext: TransactionContext): ModelBase = {
    return new System_Model1_1000000(txnContext)
  }
} 

class System_Model1_1000000(txnContext: TransactionContext) extends ModelBase {
  override def execute(emitAllResults : Boolean) : ModelResult = {
    val custInfo = System_CustomerPreferences_1000000.GetRecentRDDForCurrentPartitionKey
    if (custInfo.isEmpty || custInfo.last.get.minbalancealertoptout != "N")
      return null
    val currentDate = current_date
    val custAlertHistory = System_AlertHistory_1000000.GetRecentRDDForCurrentPartitionKey
    if (custAlertHistory.isEmpty == false && currentDate <= custAlertHistory.last.get.lastalertdate) // today
      return null
    val rcntTxn = System_TransactionMsg_1000000.GetRecentRDDForCurrentPartitionKey
    if (rcntTxn.isEmpty || rcntTxn.last.get.balance >= 100)
      return null

    // ... Prepare results here ...
    new ModelResult(dateMillis, nowStr, getModelName, getModelVersion, results) 
  }
}
