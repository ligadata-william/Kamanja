/**
 *
 * Emergency Borrowing Alert (EB1, EB2)
 *
 * 1) If the TGNERQT.ANTICIPATORY_LIMIT is less than zero, the account has an EB limit. If no EB limit held (i.e. TGNERQT.ANTICIPATORY_LIMIT not < 0) an EB alert will not be triggered and processing will skip to Overdraft Alerts.
 *
 * 2) IF (RUN_LEDG_BAL_EXCL_AUTHS equal or greater than (OVERDRAFT_LIMIT + ANTICIPATORY_LIMIT + UNP-BUFF) and (RUN-LEDG-BAL-EXCL-AUTHS less than OVERDRAFT_LIMIT + EB_BUFFER)
 * (balance is within the Emergency Borrowing tier)
 * Continue
 * ELSE
 * Exit the model (consider the Overdraft Limit alerts)
 *
 * 3) Calculation - Calculate PREVIOUS_BAL as RUN_LEDG_BAL_EXCL_AUTHS minus ENTRY_AMOUNT
 * IF PREVIOUS_BAL equal or greater than (OVERDRAFT_LIMIT + EB_BUFFER) and
 * RUN-LEDG-BAL-EXCL-AUTHS less than (OVERDRAFT_LIMIT + EB_BUFFER)
 * (i.e. the debit has caused the balance to move into the EB tier)
 * Continue to next step below
 * ELSE
 * No EB alert required.  Terminate processing for the event as the Overdraft and Low Balance alert would not be triggered as balance is in the EB tier.
 *
 * 4) Offline Alert (current time between 17:00 and 07:30). Determine whether a pending alert has already been written for the same business date to the Alert History Table (i.e. any one of LBN001, OD1, OD2, OD3, NOL001, EB1, or EB2).  If not already present, write it to the table.
 *
 * 5) Online Alert.  Determine whether an alert higher in the alert hierarchy has already been triggered today, and whether the daily threshold for EB alerts has been reached.  In either case, an alert would not be triggered.
 * IF number of EB alerts sent today (from Alert History Table) equals the daily threshold (configurable parameter set to 1)
 * No EB alert required.  Terminate processing for the event as the Overdraft and Low Balance alerts would not be triggered either, on account of the fact that an EB alert has already been sent and is higher in the alert hierarchy.
 * ELSE - Determine which flavor of alert is required
 * IF UTF (unpaid transaction fee) or PTF (paid transaction fee) alert has already been triggered today for the customer/account
 * No EB alert required.  Terminate processing for the event as the Overdraft and Low Balance alerts would not be triggered either, on account of the fact that a UTF/PTF alert has already been sent and is higher in the alert hierarchy.  Skip all subsquent processing.
 * ELSE
 * EB Alert is required
 * If an EB alert is required, this step determines whether an EB001 or EB002 alert is required depending on the number of EB fees charged, which will have been loaded into Consolidated Preference Data table for the alert.
 * IF MAX_EB_CNT = ‘N’
 * EB1 alert required (borrower hit the limit)
 * ELSE
 * EB2 alert required
 *
 * 6) Pass back specific parameters required for the alert, i.e:
 * Key Stage Event ID (Alert Type) = EB1 or EB2
 * Balance = RUN-LEDG-BAL-EXCL-AUTHS
 * Account Short Name (from Customer Preferences)
 * Expiry Date (set according to rule 001 on Alert Parameters table, i.e. from TGNERQT.DATE)
 * Expiry Time (set from Expiry Time on Alert Parameters Table).
 *
 */

package com.ligadata.OnLEPTestModel

import scala.collection.mutable.ArrayBuffer
import scala.collection.breakOut
import com.ligadata.OnLEPBase.{ BaseMsg, BaseMsgObj, ModelBase, ModelBaseObj, EnvContext, ModelResult, Result, MinVarType }
import com.ligadata.OnLEPBankPoc.{ BankPocMsg_100, CustomerPreferences_100, AlertParameters_100, AlertHistory_100 }
import com.ligadata.Utils.Utils

object EmergencyBorrowAlert_100 extends ModelBaseObj {
  def IsValidMessage(msg: BaseMsg): Boolean = true
  def CreateNewModel(envContext: EnvContext, msg: BaseMsg, tenantId: String): ModelBase = new EmergencyBorrowAlert_100(envContext, msg, getModelName, getVersion, tenantId)

  def getModelName: String = "EmergencyBorrowAlert"
  def getVersion: String = "00.01.00"
}

class EmergencyBorrowAlert_100(val gCtx: EnvContext, val msg: BaseMsg, val modelName: String, val modelVersion: String, val tenantId: String) extends ModelBase {

  val UNP_BUFF: Double = 0
  val EB_BUFFER: Double = 15.0

  /* for now we are processing "fixed" msg type */
  val _event = if (msg != null) msg.asInstanceOf[BankPocMsg_100] else null

  val custAccKey = _event.CUST_ID.toString + "." +  _event.ENT_ACC_NUM.toString
  val tmpCustomerPrefs = gCtx.getObject("Ligadata.CustomerPreferences", custAccKey)
  var tmpAlertHistory = gCtx.getObject("Ligadata.AlertHistory", custAccKey) // We are saving & getting it per day basis
  val tmpAlertParameters = gCtx.getObject("Ligadata.AlertParameters", "EB")

  val _customerPrefs = if (tmpCustomerPrefs != null) tmpCustomerPrefs.asInstanceOf[CustomerPreferences_100] else new CustomerPreferences_100()
  var _alertHistory = if (tmpAlertHistory != null) tmpAlertHistory.asInstanceOf[AlertHistory_100] else new AlertHistory_100()
  val _alertParameters = if (tmpAlertHistory != null) tmpAlertHistory.asInstanceOf[AlertParameters_100] else new AlertParameters_100()

  if (_event.ENT_DTE != _alertHistory.EventDate)
    _alertHistory.Reset

  /** helper function */
  def Between(thisOne: Int, leftMargin: Int, rightMargin: Int, inclusive: Boolean): Boolean = {
    if (inclusive) (thisOne >= leftMargin && thisOne <= rightMargin) else (thisOne > leftMargin && thisOne < rightMargin)
  }

  def Between(thisOne: Long, leftMargin: Long, rightMargin: Long, inclusive: Boolean): Boolean = {
    if (inclusive) (thisOne >= leftMargin && thisOne <= rightMargin) else (thisOne > leftMargin && thisOne < rightMargin)
  }

  def prepareEmptyResult(outputCurrentModel: Boolean): ModelResult = {
    if (outputCurrentModel)
      prepareResults(false, false, "")
    else
      null
  }

  def execute(outputCurrentModel: Boolean): ModelResult = {
    def executeInternal() : ModelResult = {
    if (AcctHasEBLimit && InEmergencyBorrowRange && DebitHasCausedEBCondition(CalculatePreviousBal)) {
      val offlnEvnt = OfflineEvent
      if ((offlnEvnt && shouldOfflineEventBeRecorded) || (offlnEvnt == false && shouldOnlineEventBeIssued))
         prepareResults(true, offlnEvnt, determineWhichEBType)
      else
        prepareEmptyResult(outputCurrentModel)
    } else
       prepareEmptyResult(outputCurrentModel)
    }
    executeInternal()
  }

  private def prepareResults(predict: Boolean, offlnEvnt: Boolean, alertType: String): ModelResult = {
    var results: ArrayBuffer[Result] = new ArrayBuffer[Result]()

    results += new Result("SendResult", MinVarType.Predicted, predict.toString)
    if (predict) {
      val accShrtNm = _customerPrefs.ACCT_SHORT_NM
      val expiryDate = _event.ENT_DTE
      val expiryTime = _alertParameters.ALERT_EXPIRY_TIME
      val mobileNumber = _customerPrefs.MOBILE_NUMBER
      val runLedgBalExclAuths = _event.RUN_LDG_XAU

      results += new Result("OfflineEvent", MinVarType.Supplementary, offlnEvnt.toString)
      results += new Result("AlertType", MinVarType.Supplementary, alertType)
      results += new Result("Balance", MinVarType.Supplementary, runLedgBalExclAuths.toString)
      results += new Result("AccountShortName", MinVarType.Supplementary, accShrtNm.toString)
      results += new Result("ExpiryDate", MinVarType.Supplementary, expiryDate.toString)
      results += new Result("ExpiryTime", MinVarType.Supplementary, expiryTime.toString)
      results += new Result("MobileNumber", MinVarType.Supplementary, mobileNumber.toString)

      if (alertType.equalsIgnoreCase("EB001"))
        _alertHistory.EB001Sent += 1
      else
        _alertHistory.EB002Sent += 1

      // While writing we need these two values
      _alertHistory.CUST_ID = _event.CUST_ID
      _alertHistory.ENT_ACC_NUM = _event.ENT_ACC_NUM
      _alertHistory.EventDate = _event.ENT_DTE

      gCtx.setObject("Ligadata.AlertHistory", custAccKey, _alertHistory) // We are saving & getting it per day basis
    }

    new ModelResult(_event.ENT_DTE, Utils.GetCurDtTmStr, getModelName, getVersion, results.toArray)
  }

  /**
   * 1) If the TGNERQT.ANTICIPATORY_LIMIT is less than zero, the account has an EB limit. If
   * no EB limit held (i.e. TGNERQT.ANTICIPATORY_LIMIT not < 0) an EB alert will not be triggered and
   * processing will skip to Overdraft Alerts.
   */
  private def AcctHasEBLimit: Boolean = {
    val limit = _event.ANT_LMT
    val doesIt = (limit < 0.0)
    doesIt
  }

  /**
   * 2) IF (RUN_LEDG_BAL_EXCL_AUTHS equal or greater than (OVERDRAFT_LIMIT + ANTICIPATORY_LIMIT + UNP-BUFF) and (RUN-LEDG-BAL-EXCL-AUTHS less than OVERDRAFT_LIMIT + EB_BUFFER)
   * (balance is within the Emergency Borrowing tier)
   * Continue
   * ELSE
   * Exit the model (consider the Overdraft Limit alerts)
   */

  private def InEmergencyBorrowRange: Boolean = {
    val runLedgBalExclAuths = _event.RUN_LDG_XAU
    val overdraftLimit = _event.ODR_LMT
    val anticipatoryLimit = _event.ANT_LMT
    val unpBuff = UNP_BUFF
    val ebBuff = EB_BUFFER

    val isIt = ((runLedgBalExclAuths > (overdraftLimit + anticipatoryLimit + unpBuff)) && (runLedgBalExclAuths < overdraftLimit + ebBuff))
    isIt
  }

  /**
   *
   *
   * 3) Calculation - Calculate PREVIOUS_BAL as RUN_LEDG_BAL_EXCL_AUTHS minus ENTRY_AMOUNT
   * IF PREVIOUS_BAL equal or greater than (OVERDRAFT_LIMIT + EB_BUFFER) and
   * RUN-LEDG-BAL-EXCL-AUTHS less than (OVERDRAFT_LIMIT + EB_BUFFER)
   * (i.e. the debit has caused the balance to move into the EB tier)
   * Continue to next step below
   * ELSE
   * No EB alert required.  Terminate processing for the _event as the Overdraft and Low Balance alert would not be triggered as balance is in the EB tier.
   */

  private def CalculatePreviousBal: Double = {
    val runLedgBalExclAuths = _event.RUN_LDG_XAU
    val entryAmt = _event.ENT_AMT
    runLedgBalExclAuths - entryAmt
  }

  private def DebitHasCausedEBCondition(previousBalance: Double): Boolean = {
    val runLedgBalExclAuths = _event.RUN_LDG_XAU
    val overdraftLimit = _event.ODR_LMT
    val ebBuff = EB_BUFFER
    val sumOverDraftEmerBorrowsToday = overdraftLimit + ebBuff
    val hasIt = (previousBalance >= sumOverDraftEmerBorrowsToday && runLedgBalExclAuths < sumOverDraftEmerBorrowsToday)
    hasIt
  }

  /**
   * 4) Offline Alert (current time between 17:00 and 07:30). Determine whether a pending alert has already been written for the same business date to the Alert History Table (i.e. any one of LBN001, OD1, OD2, OD3, NOL001, EB1, or EB2).  If not already present, write it to the table.
   */

  private def OfflineEvent: Boolean = {
    /** PRECISE TIME ENTRY Format: OHHMMSSCC, WHERE CC REPRESENTS HUNDREDTHS OF A SECOND */
    val hours = (_event.ENT_TME / 1000000) % 100
    val minutes = (_event.ENT_TME / 10000) % 100
    val seconds = (_event.ENT_TME / 100) % 100

    val evtseconds = hours * 60 * 60 + minutes * 60 + seconds

    val startOfDayInSeconds = _alertParameters.ONLINE_START_TIME
    val endOfDayInSeconds = _alertParameters.ONLINE_END_TIME

    (!Between(evtseconds, startOfDayInSeconds, endOfDayInSeconds, true))
  }

  private def shouldOfflineEventBeRecorded: Boolean = {
    val totalAlertsForToday = _alertHistory.LB001Sent +
      _alertHistory.OD001Sent +
      _alertHistory.OD002Sent +
      _alertHistory.OD003Sent +
      _alertHistory.NO001Sent +
      _alertHistory.EB001Sent +
      _alertHistory.EB002Sent;

    totalAlertsForToday == 0
  }

  /**
   * 5) Online Alert.  Determine whether an alert higher in the alert hierarchy has already been triggered today, and whether the daily threshold for EB alerts has been reached.  In either case, an alert would not be triggered.
   */

  private def shouldOnlineEventBeIssued: Boolean = {
    val ebDailyThreshold = _alertParameters.MAX_ALERTS_PER_DAY

    val numEBAlertsSentToday = _alertHistory.EB001Sent + _alertHistory.EB002Sent
    val numUTF_PTF_AlertsSentToday = _alertHistory.UTFSent + _alertHistory.PTFSent

    if (numEBAlertsSentToday >= ebDailyThreshold) {
      return false
    }

    if (numUTF_PTF_AlertsSentToday > 0) {
      return false
    }

    return true
  }

  private def determineWhichEBType: String = {
    var numEBAlertsSentToday: Int = 0

    if (_customerPrefs.MAX_EB_CNT == 'N') {
      "EB001"
    } else {
      "EB002"
    }
  }
}
