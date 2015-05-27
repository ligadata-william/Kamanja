package com.ligadata.models

// Model2:

// We generate an alert if min balance < 100 in last 30 days (rolling time window) and if we haven't issued any 
// alert in last two days and if minBalanceAlertOptOut is N.

object System_Model2_1000000 extends ModelBaseObj {
  override def IsValidMessage(msg: MessageContainerBase): Boolean = ...
  override def CreateNewModel(txnContext: TransactionContext): ModelBase = ...
} 

class System_Model2_1000000(txnContext: TransactionContext) extends ModelBase {
  override def execute(emitAllResults : Boolean) : ModelResult = {
    val custInfo = ${System.CustomerInfo}.GetRecentRDDForCurrentPartitionKey
    if (custInfo.isEmpty || custInfo.last.get.minbalancealertoptout != "N")
      return null
    val currentDate = current_date
    val custAlertHistory = ${System.CustomerAlertHistory}.GetRecentRDDForCurrentPartitionKey
    if (custAlertHistory.isEmpty == false && ((currentDate - custAlertHistory.last.get.lastalertdate) < 2)) // 2 days check
      return null
    val monthAgo = TimeRange.MonthAgo(currentDate)
    val monthTxns = ${System.TransactionMsg}.GetRDDForCurrentPartitionKey(new TimeRange(monthAgo, currentDate), (t: TransactionMsg) => {true})

    val minBal = monthTxns.min((prevBal: Option[Double], txn: TransactionMsg) => { if (prevBal != None && prevBal.get < txn.balance) prevBal else Some(txn.balance) })

    if (minBal != None && minBal.get < 100) {
      // ... Prepare results here ...
      new ModelResult(dateMillis, nowStr, getModelName, getModelVersion, results) 
    }
    else {
      null
    }
  }
}

