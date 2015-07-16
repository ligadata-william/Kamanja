package com.ligadata.models.samples.models

import com.ligadata.FatafatBase.{ BaseMsg, BaseContainer, RddUtils, RddDate, BaseContainerObj, MessageContainerBase, RDDObject, RDD }
import com.ligadata.FatafatBase.{ TimeRange, ModelBaseObj, ModelBase, ModelResultBase, TransactionContext, ModelContext }
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
  var one_count: Long = 0;
  var one_map: List[(Long, Int, Long, Double, String)] = null
  var one_filter1_5: List[(Long, Int, Long, Double, String)] = null
  var one_flatMap: List[Double] = null
  // var one_groupBy: List[(Long, Int, Long, Double, String)] = null
  var one_foreach: List[(Long, Int, Long, Double, String)] = null
  var one_first: (Long, Int, Long, Double, String) = null
  var one_last: (Long, Int, Long, Double, String) = null
  var one_maxAccNo: Long = 0
  var one_minAccNo: Long = 0
  var one_maxBalance: Double = 0
  var one_minBalance: Double = 0
  var one_isEmpty: Boolean = false

  var two_size: Long = 0;
  var two_map: List[(Long, Int, Long, Double, String)] = null
  var two_filter1_5: List[(Long, Int, Long, Double, String)] = null
  var two_flatMap: List[Double] = null
  // var two_groupBy: List[(Long, Int, Long, Double, String)] = null
  var two_foreach: List[(Long, Int, Long, Double, String)] = null
  var two_first: (Long, Int, Long, Double, String) = null
  var two_last: (Long, Int, Long, Double, String) = null
  var two_maxAccNo: Long = 0
  var two_minAccNo: Long = 0
  var two_maxBalance: Double = 0
  var two_minBalance: Double = 0
  var two_isEmpty: Boolean = false

  override def toJson: List[org.json4s.JsonAST.JObject] = {
    val json = List(
      ("one_count" -> one_count) ~
        ("one_map" -> com.ligadata.FatafatBase.ModelsResults.ValueString(one_map)) ~
        ("one_filter1_5" -> com.ligadata.FatafatBase.ModelsResults.ValueString(one_filter1_5)) ~
        ("one_flatMap" -> com.ligadata.FatafatBase.ModelsResults.ValueString(one_flatMap)) ~
        ("one_foreach" -> com.ligadata.FatafatBase.ModelsResults.ValueString(one_foreach)) ~
        ("one_first" -> com.ligadata.FatafatBase.ModelsResults.ValueString(one_first)) ~
        ("one_last" -> com.ligadata.FatafatBase.ModelsResults.ValueString(one_last)) ~
        ("one_maxAccNo" -> one_maxAccNo) ~
        ("one_minAccNo" -> one_minAccNo) ~
        ("one_maxBalance" -> one_maxBalance) ~
        ("one_minBalance" -> one_minBalance) ~
        ("one_isEmpty" -> one_isEmpty) ~
        ("two_size" -> two_size) ~
        ("two_map" -> com.ligadata.FatafatBase.ModelsResults.ValueString(two_map)) ~
        ("two_filter1_5" -> com.ligadata.FatafatBase.ModelsResults.ValueString(two_filter1_5)) ~
        ("two_flatMap" -> com.ligadata.FatafatBase.ModelsResults.ValueString(two_flatMap)) ~
        ("two_foreach" -> com.ligadata.FatafatBase.ModelsResults.ValueString(two_foreach)) ~
        ("two_first" -> com.ligadata.FatafatBase.ModelsResults.ValueString(two_first)) ~
        ("two_last" -> com.ligadata.FatafatBase.ModelsResults.ValueString(two_last)) ~
        ("two_maxAccNo" -> two_maxAccNo) ~
        ("two_minAccNo" -> two_minAccNo) ~
        ("two_maxBalance" -> two_maxBalance) ~
        ("two_minBalance" -> two_minBalance) ~
        ("two_isEmpty" -> two_isEmpty) 
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

    val result = new RddTest1Result()

    result.one_count = allTxns.count
    result.one_map = allTxns.map(getTuple).toList
    result.one_filter1_5 = allTxns.filter(filter_between_50_5000).map(getTuple).toList
    result.one_flatMap = allTxns.flatMap((v:TransactionMsg) => { List(v.balance) }).toList
 // result.one_groupBy: List[(Long, Int, Long, Double, String)] = null
    var one_foreach_val: List[(Long, Int, Long, Double, String)] = List[(Long, Int, Long, Double, String)]()
    allTxns.foreach((v:TransactionMsg) => {
      one_foreach_val = one_foreach_val :+ (v.custid, v.branchid, v.accno, v.balance, v.transtype)
    })

    result.one_foreach = one_foreach_val
    result.one_first = getVal(allTxns.first())
    result.one_last = getVal(allTxns.last())
    val one_maxAccNo = allTxns.max(findMaxAccNo)
    val one_minAccNo = allTxns.min(findMinAccNo)
    val one_maxBal = allTxns.max(findMaxBal)
    val one_minBal = allTxns.min(findMinBal)
    result.one_maxAccNo = if (one_maxAccNo == None) 0 else one_maxAccNo.get
    result.one_minAccNo = if (one_minAccNo == None) 0 else one_minAccNo.get
    result.one_maxBalance = if (one_maxBal == None) 0 else one_maxBal.get
    result.one_minBalance = if (one_minBal == None) 0 else one_minBal.get
    result.one_isEmpty = allTxns.isEmpty

    result.two_size = fltrTxns.size
    result.two_map = fltrTxns.map(getTuple).toList
    result.two_filter1_5 = fltrTxns.filter(filter_between_50_5000).map(getTuple).toList
    result.two_flatMap = fltrTxns.flatMap((v:TransactionMsg) => { List(v.balance) }).toList
 // result.two_groupBy: List[(Long, Int, Long, Double, String)] = null
    var two_foreach_val: List[(Long, Int, Long, Double, String)] = List[(Long, Int, Long, Double, String)]()
    fltrTxns.foreach((v:TransactionMsg) => {
      two_foreach_val = two_foreach_val :+ (v.custid, v.branchid, v.accno, v.balance, v.transtype)
    })

    result.two_foreach = two_foreach_val
    result.two_first = getVal(fltrTxns.first())
    result.two_last = getVal(fltrTxns.last())
    val two_maxAccNo = fltrTxns.max(findMaxAccNo)
    val two_minAccNo = fltrTxns.min(findMinAccNo)
    val two_maxBal = fltrTxns.max(findMaxBal)
    val two_minBal = fltrTxns.min(findMinBal)
    result.two_maxAccNo = if (two_maxAccNo == None) 0 else two_maxAccNo.get
    result.two_minAccNo = if (two_minAccNo == None) 0 else two_minAccNo.get
    result.two_maxBalance = if (two_maxBal == None) 0 else two_maxBal.get
    result.two_minBalance = if (two_minBal == None) 0 else two_minBal.get
    result.two_isEmpty = fltrTxns.isEmpty

    return result
  }
}

