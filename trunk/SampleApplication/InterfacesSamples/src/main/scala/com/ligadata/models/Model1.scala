package com.ligadata.models.samples

import com.ligadata.messagescontainers._
import com.ligadata.FatafatBase.{ BaseMsg, BaseContainer, RDDBase, BaseContainerObj, MessageContainerBase, RDDObject, RDD }
import com.ligadata.FatafatBase.{ TimeRange, ModelBaseObj, ModelBase, ModelResult, TransactionContext }

// Model: LowBalanceAlert
// Description: Generate low balance alerts based on current amount after each transaction performed by a customer
// Conditions: Generate an alert 
//   if current balance < 100 (configurable) and 
//   if low balance alert hasn't been issued in 48 hours and 
//   if customer preferences has minBalanceAlertOptOut is false.
// Inputs:
//   CustPreferences     - one record for each customer
//   CustAlertHistory    - one record for each customer (no history accessed even if system maintains)
//   GlobalPreferences   - one global record where model level constants are configured
//   CustTransaction     - current transaction message on which this model makes decision
//                         no transaction history is used to make alert decision
//                         balance attribute in transaction reflects the current balance after this transaction
// Output:
//   CustAlertHistory    - a new entry is created - system decides on how to store/how many to keep based on policies
//   LowBalanceAlertRslt - a new entry is created - system decides what to do with the generated object
//

object LowBalanceAlert extends ModelBaseObj {
  override def IsValidMessage(msg: MessageContainerBase): Boolean = return msg.isInstanceOf[CustTransaction]
  override def CreateNewModel(txnContext: TransactionContext): ModelBase = return new LowBalanceAlert(txnContext)
} 

class LowBalanceAlert(ctxt: TransactionContext) extends ModelBase {
  override def execute(emitAllResults : Boolean) : Option[ModelResult] = {
    val globalPref = GlobalPreferences.getRecen(ctxt)
    val pref = CustPreferences.getRecent(ctxt)
    if (pref.isEmpty || pref.get.minBalanceAlertOptout == false)
      return None
    val curDt = currentDateTime
    val custAlertHistory = CustAlertHistory.GetRecent(ctxt)
    if (custAlertHistory.isEmpty == false && timeDiffInHrs(curDt, custAlertHistory.get.lastAlertDt) < globalPref.minAlertDurationInHrs)
      return None
    val rcntTxn = CustTransaction.GetRecent(ctxt)
    if (rcntTxn.isEmpty || rcntTxn.get.balance >= globalPref.minAlertBalance)
      return None
    // create new alert history record and persist (if policy is to keep only one, this will replace existing one)
    custAlertHistory.Builder(ctxt).withLastAlertDt(curDt).build.save
    // ... Prepare results here ...
    ModelResult.Builder(ctxt).withResult(new LowBalanceAlertResult(ctxt)) 
  }
}
