package com.ligadata.models.samples.models

import com.ligadata.FatafatBase.{ BaseMsg, BaseContainer, RddUtils, RddDate, BaseContainerObj, MessageContainerBase, RDDObject, RDD }
import com.ligadata.FatafatBase.{ TimeRange, ModelBaseObj, ModelBase, ModelResult, TransactionContext, ModelContext }
import com.ligadata.samples.messages.{ CustAlertHistory, GlobalPreferences, CustPreferences, CustTransaction }
import RddUtils._
import RddDate._

// Implementation notes:
//   There is no need to pass context in every call to Builder or getRecent as the context is stored
//   in thread local storage and accessible to all methods used in the call path.
//   Of course, this assumes model doesn't launch thread and make calls to these objects.
//   If model needs to launch another thread of use thread pool where it needs to make calls to
//   the engine provided interface, we need more complex implementation and that is topic for future.
//   
//
// Messages : CustTransaction, CustAlertHisotry, CustPreferences, GlobalPreferences
// 
// CustTransaction    - Time series and primary input message on which models act (fact)
// CustPreferences    - Preference information maintained one record per customer (dimension)
// CustAlertHistory   - Time series information generated conditionally by models (fact) 
//                    - could be think as dimension if only recent message is maintained.
// ModelPreferences   - Common preferences to control model parameters. Only one instance per model is maintained.
// 
// Each CustTransaction message is associated with a customer (customerid field) and these 
// messages are partitioned by that field.
// The other messages CustAlertHistory, CustPreferences are also associated with a customer and these are  
// linked with CustTransaction message via common key - customer id. This allows to retrieve related messages
// without explicitly specifying in the higher level API calls.
// 

// Model: LowBalanceAlert
// Description: Generate low balance alerts based on current balance after each transaction performed by a customer
// Conditions: Generate an alert 
//   if current balance < 100 (configurable) and 
//   if low balance alert hasn't been issued in 48 hours and 
//   if customer preferences has minBalanceAlertOptOut is false.
//
// Inputs:
//   CustPreferences     - one record for each customer
//   CustAlertHistory    - one record for each customer (no history accessed even if system maintains)
//   ModelPreferences    - one global record where model level constants are configured
//   CustTransaction     - current transaction message on which this model makes decision
//                         no transaction history is used to make alert decision
//                         balance attribute in transaction reflects the current balance after this transaction
// Output:
//   CustAlertHistory    - a new entry is created - system decides on how to store/how many to keep based on policies
//   LowBalanceAlertRslt - a new entry is created - system decides what to do with the generated object
//

object LowBalanceAlert extends ModelBaseObj {
  override def IsValidMessage(msg: MessageContainerBase): Boolean = return msg.isInstanceOf[CustTransaction]
  override def CreateNewModel(mdlCtxt: ModelContext): ModelBase = return new LowBalanceAlert(mdlCtxt)
  override def ModelName: String = "LowBalanceAlert" // Model Name
  override def Version: String = "0.0.1" // Model Version
}

class LowBalanceAlertResult(ctxt: TransactionContext) {
}

class LowBalanceAlert(mdlCtxt: ModelContext) extends ModelBase(mdlCtxt, LowBalanceAlert) {
  override def execute(emitAllResults: Boolean): ModelResult = {
    // First check the preferences and decide whether to continue or not
    val gPref = GlobalPreferences.getRecentOrNew
    val pref = CustPreferences.getRecentOrNew
    if (pref.minBalanceAlertOptout == false)
      return null

    // Check if at least min number of hours elapsed since last alert  
    val curDt = RddDate.currentDateTime
    val alertHistory = CustAlertHistory.getRecentOrNew
    if (curDt.timeDiffInHrs(alertHistory.alertDt) < gPref.minAlertDurationInHrs)
      return null

    // continue with alert generation only if balance from current transaction is less than threshold
    val rcntTxn = CustTransaction.getRecent
    if (rcntTxn.isEmpty || rcntTxn.get.balance >= gPref.minAlertBalance)
      return null

    // create new alert history record and persist (if policy is to keep only one, this will replace existing one)
    CustAlertHistory.build.withAlertDt(curDt).withAlertType("lowBalanceAlert").save
    // ... Prepare results here ... need to populate result object with appropriate attributes
    ModelResult.builder.withResult(new LowBalanceAlertResult(mdlCtxt.txnContext)).build
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
//
// Inputs:
//   CustPreferences     - one record for each customer
//   CustAlertHistory    - one record for each customer (no history accessed even if system maintains)
//   ModelPreferences    - one global record where model level constants are configured
//   CustTransaction     - current transaction message and history of messages in last 30 days
//                         balance attribute in transaction reflects the current balance after this transaction
// Output:
//   CustAlertHistory    - a new entry is created - system decides on how to store/how many to keep based on policies
//   LowBalanceAlertRslt - a new entry is created - system decides what to do with the generated object
//

object LowBalanceAlert2 extends ModelBaseObj {
  override def IsValidMessage(msg: MessageContainerBase): Boolean = return msg.isInstanceOf[CustTransaction]
  override def CreateNewModel(mdlCtxt: ModelContext): ModelBase = return new LowBalanceAlert2(mdlCtxt)
  override def ModelName: String = "LowBalanceAlert2" // Model Name
  override def Version: String = "0.0.1" // Model Version
}

class LowBalanceAlertResult2(ctxt: TransactionContext) {
}

class LowBalanceAlert2(mdlCtxt: ModelContext) extends ModelBase(mdlCtxt, LowBalanceAlert) {
  override def execute(emitAllResults: Boolean): ModelResult = {
    // First check the preferences and decide whether to continue or not
    val gPref = GlobalPreferences.getRecentOrNew
    val pref = CustPreferences.getRecentOrNew
    if (pref.multiDayMinBalanceAlertOptout == false)
      return null

    // Check if at least min number of hours elapsed since last alert  
    val curDt = currentDateTime
    val alertHistory = CustAlertHistory.getRecentOrNew
    if (curDt.timeDiffInHrs(alertHistory.alertDt) < gPref.minAlertDurationInHrs)
      return null

    // get history of transaction whose balance is less than minAlertBalance in last N days
    val lookBackTime = curDt.lastNdays(gPref.numLookbackDaysForMultiDayMinBalanceAlert)
    val rcntTxns = CustTransaction.getRDD(lookBackTime, { trnsaction: MessageContainerBase =>
      {
        val trn = trnsaction.asInstanceOf[CustTransaction]
        trn.balance < gPref.minAlertBalance
      }
    })

    // quick check to whether it is worth doing group by operation
    if (rcntTxns.isEmpty || rcntTxns.count < gPref.maxNumDaysAllowedWithMinBalance)
      return null

    // compute distinct days in the retrieved history (number of days with balance less than minAlertBalance)
    // and check if those days meet the threshold
    val daysWhenBalanceIsLessThanMin = rcntTxns.groupBy({ trn => trn.date }).count.toInt
    if (daysWhenBalanceIsLessThanMin <= gPref.maxNumDaysAllowedWithMinBalance)
      return null

    // create new alert history record and persist (if policy is to keep only one, this will replace existing one)
    CustAlertHistory.build.withAlertDt(curDt).withAlertType("tooManyMinBalanceDays").withNumDays(daysWhenBalanceIsLessThanMin).save
    // ... Prepare results here ... need to populate result object with appropriate attributes
    ModelResult.builder.withResult(new LowBalanceAlertResult2(mdlCtxt.txnContext)).build
  }
}

// Model: LocationAlert
// Description: Generate alert based on locations of transaction transaction .
//              This model uses more complex logic of maintaining than simply comparing the current balance
//
// Conditions: Generate an alert 
//   if current location is more than < 100 (configurable) and 
//   if number of days that balance < 100 in last 30 days is more than 3 days
//   if low balance alert hasn't been issued in 48 hours and 
//   if customer preferences has minBalanceAlertOptOut is false.
//
// Inputs:
//   CustPreferences     - one record for each customer
//   CustAlertHistory    - one record for each customer (no history accessed even if system maintains)
//   ModelPreferences    - one global record where model level constants are configured
//   CustTransaction     - current transaction message and history of messages in last 30 days
//                         balance attribute in transaction reflects the current balance after this transaction
// Output:
//   CustAlertHistory    - a new entry is created - system decides on how to store/how many to keep based on policies
//   LowBalanceAlertRslt - a new entry is created - system decides what to do with the generated object
//

