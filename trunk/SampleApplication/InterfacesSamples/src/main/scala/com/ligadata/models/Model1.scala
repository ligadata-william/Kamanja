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

// Note:
//   There is no need to pass context in every call to Builder or getRecent as the context is stored
//   in thread local storage and accessible to all methods used in the call path.
//   Of course, this assumes model doesn't launch thread and make calls to these objects.
//   If model needs to launch another thread of use thread pool where it needs to make calls to
//   the engine provided interface, we need more complex implementation and that is topic for future.
//   
class LowBalanceAlert(ctxt: TransactionContext) extends ModelBase {
  override def execute(emitAllResults : Boolean) : Option[ModelResult] = {
    val globalPref = GlobalPreferences.getRecent
    val pref = CustPreferences.getRecent
    if (pref.isEmpty || pref.get.minBalanceAlertOptout == false)
      return None
    val curDt = currentDateTime
    val custAlertHistory = CustAlertHistory.GetRecent
    if (custAlertHistory.isEmpty == false && timeDiffInHrs(curDt, custAlertHistory.get.lastAlertDt) < globalPref.minAlertDurationInHrs)
      return None
    val rcntTxn = CustTransaction.GetRecent
    if (rcntTxn.isEmpty || rcntTxn.get.balance >= globalPref.minAlertBalance)
      return None
    // create new alert history record and persist (if policy is to keep only one, this will replace existing one)
    custAlertHistory.Builder.withLastAlertDt(curDt).withLastAlertType("lowBalanceAlert").build.save
    // ... Prepare results here ... need to populate result object with appropriate attributes
    ModelResult.Builder.withResult(new LowBalanceAlertResult(ctxt)) 
  }
}

// Model: LowBalanceAlert2
// Description: Generate low balance alerts based on current amount after each transaction performed by a customer
//              This model uses more complex logic than simply comparing the current balance
//
// Conditions: Generate an alert 
//   if current balance < 100 (configurable) and 
//   if number of days that balance < 100 in last 30 days is more than 3 days
//   if low balance alert hasn't been issued in 48 hours and 
//   if customer preferences has minBalanceAlertOptOut is false.
// Inputs:
//   CustPreferences     - one record for each customer
//   CustAlertHistory    - one record for each customer (no history accessed even if system maintains)
//   GlobalPreferences   - one global record where model level constants are configured
//   CustTransaction     - current transaction message and history of messages in last 30 days
//                         balance attribute in transaction reflects the current balance after this transaction
// Output:
//   CustAlertHistory    - a new entry is created - system decides on how to store/how many to keep based on policies
//   LowBalanceAlertRslt - a new entry is created - system decides what to do with the generated object
//

object LowBalanceAlert2 extends ModelBaseObj {
  override def IsValidMessage(msg: MessageContainerBase): Boolean = return msg.isInstanceOf[CustTransaction]
  override def CreateNewModel(txnContext: TransactionContext): ModelBase = return new LowBalanceAlert2(txnContext)
} 

class LowBalanceAlert2(ctxt: TransactionContext) extends ModelBase {
  override def execute(emitAllResults : Boolean) : Option[ModelResult] = {
    val globalPref = GlobalPreferences.getRecent
    val pref = CustPreferences.getRecent
    if (pref.isEmpty || pref.get.multiDayMinBalanceAlertOptout == false)
      return None
    val curDt = currentDateTime
    val custAlertHistory = CustAlertHistory.GetRecent
    if (custAlertHistory.isEmpty == false && timeDiffInHrs(curDt, custAlertHistory.get.lastAlertDt) < globalPref.minAlertDurationInHrs)
      return None
    val lookBackTime = curDt.lastNdays(globalPref.numLookbackDaysForMultiDayMinBalanceAlert)
    val rcntTxns = CustTransaction.GetRDD(lookBackTime, { trn => trn.balance < 100 })
    if (rcntTxn.isEmpty || rcntTxns.count < globalPref.maxNumDaysAllowedWithMinBalance)
      return None
    val daysWhenBalanceIsLessThanMin = rcntTxns.groupBy({trn => trn.transactionTime.toDate}).count
    if(daysWhenBalanceIsLessThanMin <= globalPref.maxNumDaysAllowedWithMinBalance)
      return None
    // create new alert history record and persist (if policy is to keep only one, this will replace existing one)
    custAlertHistory.Builder(ctxt).withLastAlertDt(curDt).
                     withLastAlertType("tooManyMinBalanceDays").withNumTimes(daysWhenBalanceIsLessThanMin).build.save
    // ... Prepare results here ... need to populate result object with appropriate attributes
    ModelResult.Builder(ctxt).withResult(new LowBalanceAlertResult2(ctxt)) 
  }
}
