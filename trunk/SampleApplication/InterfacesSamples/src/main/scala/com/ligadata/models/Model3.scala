package com.ligadata.models

// Model3:

// We generate an alert if min balance < 100 more than 3 times in last 30 days (rolling time window) and if we haven't issued any 
// alert in last two days and if minBalanceAlertOptOut is N.

object System_Model3_1000000 extends ModelBaseObj {
  override def IsValidMessage(msg: MessageContainerBase): Boolean = ...
  override def CreateNewModel(txnContext: TransactionContext): ModelBase = ...
} 

class System_Model3_1000000(txnContext: TransactionContext) extends ModelBase {
  override def execute(emitAllResults : Boolean) : ModelResult = {
    val custInfo = ${System.CustomerInfo}.GetRecentRDDForCurrentPartitionKey
    if (custInfo.isEmpty || custInfo.last.get.minbalancealertoptout != "N")
      return null
    val currentDate = current_date
    val custAlertHistory = ${System.CustomerAlertHistory}.GetRecentRDDForCurrentPartitionKey
    if (custAlertHistory.isEmpty == false && ((currentDate - custAlertHistory.last.get.lastalertdate) < 2)) // 2 days check
      return null
    val monthAgo = TimeRange.MonthAgo(currentDate)
    val minBalLastMonthTxns = ${System.TransactionMsg}.GetRDDForCurrentPartitionKey(new TimeRange(monthAgo, currentDate), (t: TransactionMsg) => { txn.balance < 100 })

    if (minBalLastMonthTxns.size > 2) {
      // ... Prepare results here ...
      new ModelResult(dateMillis, nowStr, getModelName, getModelVersion, results) 
    }
    else {
      null
    }
  }
}

