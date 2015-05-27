package com.ligadata.models

// Model1:

// We generate an alert if min balance < 100 and if we haven't issued any 
// alert today and if minBalanceAlertOptOut is N.

object System_Model1_1000000 extends ModelBaseObj {
  override def IsValidMessage(msg: MessageContainerBase): Boolean = ...
  override def CreateNewModel(txnContext: TransactionContext): ModelBase = ...
} 

class System_Model1_1000000(txnContext: TransactionContext) extends ModelBase {
  override def execute(emitAllResults : Boolean) : ModelResult = {
    val custInfo = ${System.CustomerInfo}.GetRecentRDDForCurrentPartitionKey
    if (custInfo.isEmpty || custInfo.last.get.minbalancealertoptout != "N")
      return null
    val currentDate = current_date
    val custAlertHistory = ${System.CustomerAlertHistory}.GetRecentRDDForCurrentPartitionKey
    if (custAlertHistory.isEmpty == false && currentDate <= custAlertHistory.last.get.lastalertdate) // today
      return null
    val rcntTxn = ${System.TransactionMsg}.GetRecentRDDForCurrentPartitionKey
    if (rcntTxn.isEmpty || txn.balance >= 100)
      return null

    // ... Prepare results here ...
    new ModelResult(dateMillis, nowStr, getModelName, getModelVersion, results) 
  }
}
