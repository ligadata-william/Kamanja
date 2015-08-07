package com.ligadata.models.samples.models

import com.ligadata.KamanjaBase.{ BaseMsg, BaseContainer, RddUtils, RddDate, BaseContainerObj, MessageContainerBase, RDDObject, RDD }
import com.ligadata.KamanjaBase.{ TimeRange, ModelBaseObj, ModelBase, ModelResultBase, TransactionContext, ModelContext }
import com.ligadata.messagescontainers.System.V1000000._
import RddUtils._
import RddDate._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import java.io.{ DataInputStream, DataOutputStream }

object RddTest1 extends ModelBaseObj {
  override def IsValidMessage(msg: MessageContainerBase): Boolean = return msg.isInstanceOf[TransactionMsg]
  override def CreateNewModel(mdlCtxt: ModelContext): ModelBase = return new RddTest1(mdlCtxt)
  override def ModelName(): String = "System.RddTest1" // Model Name
  override def Version(): String = "0.0.1" // Model Version
  override def CreateResultObject(): ModelResultBase = new RddTest1Result()
}

class RddTest1Result extends ModelResultBase {
  var allrdd_count: Long = 0;
  var allrdd_map: List[(Long, Int, Long, Double, String)] = null
  var allrdd_filter1_5: List[(Long, Int, Long, Double, String)] = null
  var allrdd_flatMap: List[Double] = null
  // var allrdd_groupBy: List[(Long, Int, Long, Double, String)] = null
  var allrdd_foreach: List[(Long, Int, Long, Double, String)] = null
  var allrdd_first: (Long, Int, Long, Double, String) = null
  var allrdd_last: (Long, Int, Long, Double, String) = null
  var allrdd_maxAccNo: Long = 0
  var allrdd_minAccNo: Long = 0
  var allrdd_maxBalance: Double = 0
  var allrdd_minBalance: Double = 0
  var allrdd_isEmpty: Boolean = false

  var filterrdd_size: Long = 0;
  var filterrdd_map: List[(Long, Int, Long, Double, String)] = null
  var filterrdd_filter1_5: List[(Long, Int, Long, Double, String)] = null
  var filterrdd_flatMap: List[Double] = null
  // var filterrdd_groupBy: List[(Long, Int, Long, Double, String)] = null
  var filterrdd_foreach: List[(Long, Int, Long, Double, String)] = null
  var filterrdd_first: (Long, Int, Long, Double, String) = null
  var filterrdd_last: (Long, Int, Long, Double, String) = null
  var filterrdd_maxAccNo: Long = 0
  var filterrdd_minAccNo: Long = 0
  var filterrdd_maxBalance: Double = 0
  var filterrdd_minBalance: Double = 0
  var filterrdd_isEmpty: Boolean = false

  var currdd_count: Long = 0;
  var currdd_map: List[(Long, Int, Long, Double, String)] = null
  var currdd_filter1_5: List[(Long, Int, Long, Double, String)] = null
  var currdd_flatMap: List[Double] = null
  // var currdd_groupBy: List[(Long, Int, Long, Double, String)] = null
  var currdd_foreach: List[(Long, Int, Long, Double, String)] = null
  var currdd_first: (Long, Int, Long, Double, String) = null
  var currdd_last: (Long, Int, Long, Double, String) = null
  var currdd_maxAccNo: Long = 0
  var currdd_minAccNo: Long = 0
  var currdd_maxBalance: Double = 0
  var currdd_minBalance: Double = 0
  var currdd_isEmpty: Boolean = false

  var curfltrrdd_size: Long = 0;
  var curfltrrdd_map: List[(Long, Int, Long, Double, String)] = null
  var curfltrrdd_filter1_5: List[(Long, Int, Long, Double, String)] = null
  var curfltrrdd_flatMap: List[Double] = null
  // var curfltrrdd_groupBy: List[(Long, Int, Long, Double, String)] = null
  var curfltrrdd_foreach: List[(Long, Int, Long, Double, String)] = null
  var curfltrrdd_first: (Long, Int, Long, Double, String) = null
  var curfltrrdd_last: (Long, Int, Long, Double, String) = null
  var curfltrrdd_maxAccNo: Long = 0
  var curfltrrdd_minAccNo: Long = 0
  var curfltrrdd_maxBalance: Double = 0
  var curfltrrdd_minBalance: Double = 0
  var curfltrrdd_isEmpty: Boolean = false


  override def toJson: List[org.json4s.JsonAST.JObject] = {
    val json = List(
      ("allrdd_count" -> allrdd_count) ~
        ("allrdd_map" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(allrdd_map)) ~
        ("allrdd_filter1_5" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(allrdd_filter1_5)) ~
        ("allrdd_flatMap" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(allrdd_flatMap)) ~
        ("allrdd_foreach" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(allrdd_foreach)) ~
        ("allrdd_first" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(allrdd_first)) ~
        ("allrdd_last" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(allrdd_last)) ~
        ("allrdd_maxAccNo" -> allrdd_maxAccNo) ~
        ("allrdd_minAccNo" -> allrdd_minAccNo) ~
        ("allrdd_maxBalance" -> allrdd_maxBalance) ~
        ("allrdd_minBalance" -> allrdd_minBalance) ~
        ("allrdd_isEmpty" -> allrdd_isEmpty) ~
        ("filterrdd_size" -> filterrdd_size) ~
        ("filterrdd_map" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(filterrdd_map)) ~
        ("filterrdd_filter1_5" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(filterrdd_filter1_5)) ~
        ("filterrdd_flatMap" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(filterrdd_flatMap)) ~
        ("filterrdd_foreach" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(filterrdd_foreach)) ~
        ("filterrdd_first" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(filterrdd_first)) ~
        ("filterrdd_last" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(filterrdd_last)) ~
        ("filterrdd_maxAccNo" -> filterrdd_maxAccNo) ~
        ("filterrdd_minAccNo" -> filterrdd_minAccNo) ~
        ("filterrdd_maxBalance" -> filterrdd_maxBalance) ~
        ("filterrdd_minBalance" -> filterrdd_minBalance) ~
        ("filterrdd_isEmpty" -> filterrdd_isEmpty) ~
        ("currdd_count" -> currdd_count) ~
        ("currdd_map" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(currdd_map)) ~
        ("currdd_filter1_5" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(currdd_filter1_5)) ~
        ("currdd_flatMap" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(currdd_flatMap)) ~
        ("currdd_foreach" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(currdd_foreach)) ~
        ("currdd_first" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(currdd_first)) ~
        ("currdd_last" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(currdd_last)) ~
        ("currdd_maxAccNo" -> currdd_maxAccNo) ~
        ("currdd_minAccNo" -> currdd_minAccNo) ~
        ("currdd_maxBalance" -> currdd_maxBalance) ~
        ("currdd_minBalance" -> currdd_minBalance) ~
        ("currdd_isEmpty" -> currdd_isEmpty) ~
        ("curfltrrdd_size" -> curfltrrdd_size) ~
        ("curfltrrdd_map" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(curfltrrdd_map)) ~
        ("curfltrrdd_filter1_5" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(curfltrrdd_filter1_5)) ~
        ("curfltrrdd_flatMap" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(curfltrrdd_flatMap)) ~
        ("curfltrrdd_foreach" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(curfltrrdd_foreach)) ~
        ("curfltrrdd_first" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(curfltrrdd_first)) ~
        ("curfltrrdd_last" -> com.ligadata.KamanjaBase.ModelsResults.ValueString(curfltrrdd_last)) ~
        ("curfltrrdd_maxAccNo" -> curfltrrdd_maxAccNo) ~
        ("curfltrrdd_minAccNo" -> curfltrrdd_minAccNo) ~
        ("curfltrrdd_maxBalance" -> curfltrrdd_maxBalance) ~
        ("curfltrrdd_minBalance" -> curfltrrdd_minBalance) ~
        ("curfltrrdd_isEmpty" -> curfltrrdd_isEmpty) 
      )
    return json
  }

  override def toString: String = {
    compact(render(toJson))
  }

  override def get(key: String): Any = {
    return null
  }

  override def asKeyValuesMap: Map[String, Any] = {
    val map = scala.collection.mutable.Map[String, Any]()
    map.toMap
  }

  override def Deserialize(dis: DataInputStream): Unit = {
    // BUGBUG:: Yet to implement
  }

  override def Serialize(dos: DataOutputStream): Unit = {
    // BUGBUG:: Yet to implement
  }
}

class RddTest1(mdlCtxt: ModelContext) extends ModelBase(mdlCtxt, RddTest1) {
  def findMinAccNo(prevVal: Option[Long], curObj: TransactionMsg) : Long = {
    if (prevVal == None || prevVal.get > curObj.accno)
       return curObj.accno
    prevVal.get
  }

  def findMaxAccNo(prevVal: Option[Long], curObj: TransactionMsg) : Long = {
    if (prevVal == None || prevVal.get < curObj.accno)
       return curObj.accno
    prevVal.get
  }

  def findMinBal(prevVal: Option[Double], curObj: TransactionMsg) : Double = {
    if (prevVal == None || prevVal.get > curObj.balance)
       return curObj.balance
    prevVal.get
  }

  def findMaxBal(prevVal: Option[Double], curObj: TransactionMsg) : Double = {
    if (prevVal == None || prevVal.get < curObj.balance)
       return curObj.balance
    prevVal.get
  }

  def getVal(curObj: Option[TransactionMsg]) : (Long, Int, Long, Double, String) = {
    if (curObj == None)
       return (0, 0, 0, 0, "")
    val v = curObj.get
    (v.custid, v.branchid, v.accno, v.balance, v.transtype)
  }

  def getTuple(curObj: TransactionMsg) : (Long, Int, Long, Double, String) = {
    (curObj.custid, curObj.branchid, curObj.accno, curObj.balance, curObj.transtype)
  }

  def filter_between_50_5000(curObj: TransactionMsg) : Boolean = {
    (curObj.balance >= 50 && curObj.balance <= 5000)
  }

  override def execute(emitAllResults: Boolean): ModelResultBase = {
    val curDtTmInMs = RddDate.currentGmtDateTime
    val lookBackTime = curDtTmInMs.lastNdays(30)
    val allTxns = TransactionMsg.getRDD(lookBackTime)
    val fltrTxns = TransactionMsg.getRDD(lookBackTime, { trnsaction: MessageContainerBase =>
      {
        val trn = trnsaction.asInstanceOf[TransactionMsg]
        (trn.balance >= 50 && trn.balance <= 10000)
      }
    })
    val curUsrTxns = TransactionMsg.getRDDForCurrKey(lookBackTime)
    val curUsrFltrTxns = TransactionMsg.getRDDForCurrKey(lookBackTime, { trnsaction: MessageContainerBase =>
      {
        val trn = trnsaction.asInstanceOf[TransactionMsg]
        (trn.balance >= 50 && trn.balance <= 10000)
      }
    })

    val result = new RddTest1Result()

    result.allrdd_count = allTxns.count
    result.allrdd_map = allTxns.map(getTuple).toList
    result.allrdd_filter1_5 = allTxns.filter(filter_between_50_5000).map(getTuple).toList
    result.allrdd_flatMap = allTxns.flatMap((v:TransactionMsg) => { List(v.balance) }).toList
 // result.allrdd_groupBy: List[(Long, Int, Long, Double, String)] = null
    var allrdd_foreach_val: List[(Long, Int, Long, Double, String)] = List[(Long, Int, Long, Double, String)]()
    allTxns.foreach((v:TransactionMsg) => {
      allrdd_foreach_val = allrdd_foreach_val :+ (v.custid, v.branchid, v.accno, v.balance, v.transtype)
    })

    result.allrdd_foreach = allrdd_foreach_val
    result.allrdd_first = getVal(allTxns.first())
    result.allrdd_last = getVal(allTxns.last())
    val allrdd_maxAccNo = allTxns.max(findMaxAccNo)
    val allrdd_minAccNo = allTxns.min(findMinAccNo)
    val allrdd_maxBal = allTxns.max(findMaxBal)
    val allrdd_minBal = allTxns.min(findMinBal)
    result.allrdd_maxAccNo = if (allrdd_maxAccNo == None) 0 else allrdd_maxAccNo.get
    result.allrdd_minAccNo = if (allrdd_minAccNo == None) 0 else allrdd_minAccNo.get
    result.allrdd_maxBalance = if (allrdd_maxBal == None) 0 else allrdd_maxBal.get
    result.allrdd_minBalance = if (allrdd_minBal == None) 0 else allrdd_minBal.get
    result.allrdd_isEmpty = allTxns.isEmpty

    result.filterrdd_size = fltrTxns.size
    result.filterrdd_map = fltrTxns.map(getTuple).toList
    result.filterrdd_filter1_5 = fltrTxns.filter(filter_between_50_5000).map(getTuple).toList
    result.filterrdd_flatMap = fltrTxns.flatMap((v:TransactionMsg) => { List(v.balance) }).toList
 // result.filterrdd_groupBy: List[(Long, Int, Long, Double, String)] = null
    var filterrdd_foreach_val: List[(Long, Int, Long, Double, String)] = List[(Long, Int, Long, Double, String)]()
    fltrTxns.foreach((v:TransactionMsg) => {
      filterrdd_foreach_val = filterrdd_foreach_val :+ (v.custid, v.branchid, v.accno, v.balance, v.transtype)
    })

    result.filterrdd_foreach = filterrdd_foreach_val
    result.filterrdd_first = getVal(fltrTxns.first())
    result.filterrdd_last = getVal(fltrTxns.last())
    val filterrdd_maxAccNo = fltrTxns.max(findMaxAccNo)
    val filterrdd_minAccNo = fltrTxns.min(findMinAccNo)
    val filterrdd_maxBal = fltrTxns.max(findMaxBal)
    val filterrdd_minBal = fltrTxns.min(findMinBal)
    result.filterrdd_maxAccNo = if (filterrdd_maxAccNo == None) 0 else filterrdd_maxAccNo.get
    result.filterrdd_minAccNo = if (filterrdd_minAccNo == None) 0 else filterrdd_minAccNo.get
    result.filterrdd_maxBalance = if (filterrdd_maxBal == None) 0 else filterrdd_maxBal.get
    result.filterrdd_minBalance = if (filterrdd_minBal == None) 0 else filterrdd_minBal.get
    result.filterrdd_isEmpty = fltrTxns.isEmpty

    result.currdd_count = curUsrTxns.count
    result.currdd_map = curUsrTxns.map(getTuple).toList
    result.currdd_filter1_5 = curUsrTxns.filter(filter_between_50_5000).map(getTuple).toList
    result.currdd_flatMap = curUsrTxns.flatMap((v:TransactionMsg) => { List(v.balance) }).toList
 // result.currdd_groupBy: List[(Long, Int, Long, Double, String)] = null
    var currdd_foreach_val: List[(Long, Int, Long, Double, String)] = List[(Long, Int, Long, Double, String)]()
    curUsrTxns.foreach((v:TransactionMsg) => {
      currdd_foreach_val = currdd_foreach_val :+ (v.custid, v.branchid, v.accno, v.balance, v.transtype)
    })

    result.currdd_foreach = currdd_foreach_val
    result.currdd_first = getVal(curUsrTxns.first())
    result.currdd_last = getVal(curUsrTxns.last())
    val currdd_maxAccNo = curUsrTxns.max(findMaxAccNo)
    val currdd_minAccNo = curUsrTxns.min(findMinAccNo)
    val currdd_maxBal = curUsrTxns.max(findMaxBal)
    val currdd_minBal = curUsrTxns.min(findMinBal)
    result.currdd_maxAccNo = if (currdd_maxAccNo == None) 0 else currdd_maxAccNo.get
    result.currdd_minAccNo = if (currdd_minAccNo == None) 0 else currdd_minAccNo.get
    result.currdd_maxBalance = if (currdd_maxBal == None) 0 else currdd_maxBal.get
    result.currdd_minBalance = if (currdd_minBal == None) 0 else currdd_minBal.get
    result.currdd_isEmpty = curUsrTxns.isEmpty

    result.curfltrrdd_size = curUsrFltrTxns.size
    result.curfltrrdd_map = curUsrFltrTxns.map(getTuple).toList
    result.curfltrrdd_filter1_5 = curUsrFltrTxns.filter(filter_between_50_5000).map(getTuple).toList
    result.curfltrrdd_flatMap = curUsrFltrTxns.flatMap((v:TransactionMsg) => { List(v.balance) }).toList
 // result.curfltrrdd_groupBy: List[(Long, Int, Long, Double, String)] = null
    var curfltrrdd_foreach_val: List[(Long, Int, Long, Double, String)] = List[(Long, Int, Long, Double, String)]()
    curUsrFltrTxns.foreach((v:TransactionMsg) => {
      curfltrrdd_foreach_val = curfltrrdd_foreach_val :+ (v.custid, v.branchid, v.accno, v.balance, v.transtype)
    })

    result.curfltrrdd_foreach = curfltrrdd_foreach_val
    result.curfltrrdd_first = getVal(curUsrFltrTxns.first())
    result.curfltrrdd_last = getVal(curUsrFltrTxns.last())
    val curfltrrdd_maxAccNo = curUsrFltrTxns.max(findMaxAccNo)
    val curfltrrdd_minAccNo = curUsrFltrTxns.min(findMinAccNo)
    val curfltrrdd_maxBal = curUsrFltrTxns.max(findMaxBal)
    val curfltrrdd_minBal = curUsrFltrTxns.min(findMinBal)
    result.curfltrrdd_maxAccNo = if (curfltrrdd_maxAccNo == None) 0 else curfltrrdd_maxAccNo.get
    result.curfltrrdd_minAccNo = if (curfltrrdd_minAccNo == None) 0 else curfltrrdd_minAccNo.get
    result.curfltrrdd_maxBalance = if (curfltrrdd_maxBal == None) 0 else curfltrrdd_maxBal.get
    result.curfltrrdd_minBalance = if (curfltrrdd_minBal == None) 0 else curfltrrdd_minBal.get
    result.curfltrrdd_isEmpty = curUsrFltrTxns.isEmpty

    return result
  }
}

