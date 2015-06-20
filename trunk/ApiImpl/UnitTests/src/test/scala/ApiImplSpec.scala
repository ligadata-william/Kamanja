package com.ligadata.automation.unittests.apiimpl

import com.ligadata.FatafatBase._

import org.scalatest._
import Matchers._

import util.control.Breaks._
import scala.io._
import java.util.Date
import java.io._

import sys.process._
import org.apache.log4j._

case class Customer(name: String, accountId: String, accountBalance: Long, accountType: Long)
case class BusinessCustomer(name: String, accountId: String, accountBalance: Long, businessName: String)

class ApiImplSpec extends FunSpec with BeforeAndAfter with BeforeAndAfterAll with GivenWhenThen {

  private val loggerName = this.getClass.getName
  private val logger = Logger.getLogger(loggerName)

  override def beforeAll = {
    try {
      logger.info("Root of Resource Directory at run-time => " + getClass.getResource("/").getPath)
      logger.info("Finished startup initialization")
    }
    catch {
      case e: Exception => throw new Exception("Failed to execute set up properly\n" + e)
    }
  }

  def isLowBalance(x:Customer) : Boolean = {
    if( x.accountBalance < 500 ){
      true
    }
    else{
      false
    }
  }

  def findCurMin(prevVal:Option[Customer],curObj:Customer) : Customer = {
    if ( prevVal == None  || prevVal.get.accountBalance > curObj.accountBalance )
      return curObj
    prevVal.get
  }

  def findCurMax(prevVal:Option[Customer],curObj:Customer) : Customer = {
    if ( prevVal == None  || prevVal.get.accountBalance < curObj.accountBalance )
      return curObj
    prevVal.get
  }
      

  def makeBusinessCustomer(x:Customer) : BusinessCustomer = {
    new BusinessCustomer(x.name,x.accountId,x.accountBalance,x.name + "Inc")
  }

  def makeTraversableOnce(x:Customer) : TraversableOnce[BusinessCustomer] = {
    List(BusinessCustomer(x.name,x.accountId,x.accountBalance,x.name + "Inc"))
  }

  def groupByAccountType(curObj: Customer) : Long = {
    curObj.accountType
  }

   def keyByName(curObj: Customer) : Long = {
    curObj.name.replaceAll("customer","").toLong
  }

  describe("Unit Tests for RDDImpl functions") {
    var custArray = new Array[Customer](0)
    var custArray1 = new Array[Customer](0)
    var rdd:RDD[Customer] = null
    var rdd1:RDD[Customer] = null

    it("add entries into collections"){
      val c1 = new Customer("customer1","100101101",499,1);
      custArray = custArray :+ c1
      val c2 = new Customer("customer2","100101102",500,1);
      custArray = custArray :+ c2
      val c3 = new Customer("customer3","100101103",200,2);
      custArray = custArray :+ c3
      val c4 = new Customer("customer4","100101104",700,2);
      custArray = custArray :+ c4
      val c5 = new Customer("customer5","100101105",300,2);
      custArray = custArray :+ c5

      rdd = RDD.makeRDD(custArray)
      assert(rdd.size == 5)

      val c6 = new Customer("customer6","100101106",500,2);
      custArray1 = custArray1 :+ c6
      val c7 = new Customer("customer7","100101107",200,2);
      custArray1 = custArray1 :+ c7

      rdd1 = RDD.makeRDD(custArray1)
      assert(rdd1.size == 2)
    }

    it("verify first entry from collection"){
      val fst = rdd.first().get
      assert(fst != null)
      assert(fst.name == "customer1")
      assert(fst.accountId == "100101101")
      assert(fst.accountBalance == 499)
    }

    it("verify last entry from collection"){
      val lst = rdd.last().get
      assert(lst != null)
      assert(lst.name == "customer5")
      assert(lst.accountId == "100101105")
      assert(lst.accountBalance == 300)
    }

    it("min  entry from collection"){
      val min = rdd.min(findCurMin)
      assert(min != None)
      assert(min.get.name == "customer3")
    }

    it("max  entry from collection"){
      val max = rdd.min(findCurMax)
      assert(max != None)
      assert(max.get.name == "customer4")
    }

    it("create a new collection of different type using map"){
      var businessAccounts = rdd.map(makeBusinessCustomer)
      assert(businessAccounts.size == rdd.size)
      
      businessAccounts.foreach(a => {
	assert(a.getClass.getSimpleName == "BusinessCustomer")
      })
    }


    it("create a flatMap from given collectoion"){
      var fMap = rdd.flatMap(makeTraversableOnce)
      assert(fMap.size == rdd.size)
    }

    it("filter entries from collection"){
      var lowBalanceAccounts = rdd.filter(isLowBalance)
      assert(lowBalanceAccounts.size == 3)

      // do we maintain any order after filtering
      val fst = lowBalanceAccounts.first().get
      assert(fst != null)
      assert(fst.name == "customer1")
      val lst = lowBalanceAccounts.last().get
      assert(lst != null)
      assert(lst.name == "customer5")
    }


    it("Union of two RDDs using union function"){
      var union = rdd.union(rdd1)
      assert(union.size == 7)
      // do we maintain any order after union
      val fst = union.first().get
      assert(fst != null)
      assert(fst.name == "customer1")
      val lst = union.last().get
      assert(lst != null)
      assert(lst.name == "customer7")
    }

    it("Union of two RDDs using ++ operator"){
      var union = rdd ++ rdd1
      assert(union.size == 7)
      // do we maintain any order after union
      val fst = union.first().get
      assert(fst != null)
      assert(fst.name == "customer1")
      val lst = union.last().get
      assert(lst != null)
      assert(lst.name == "customer7")
    }

    ignore("Intersection of two RDDs using intersection function"){
      var x = rdd.intersection(rdd1)
      assert(x.size == 0)

      var union = rdd ++ rdd1
      assert(union.size == 7)

      var custArray2 = new Array[Customer](0)
      val c8 = new Customer("customer6","100101106",500,2);
      custArray2 = custArray2 :+ c8
      val c9 = new Customer("customer7","100101107",200,2);
      custArray2 = custArray2 :+ c9
      var rdd2 = RDD.makeRDD(custArray2)
      assert(rdd2.size == 2)

      var x1 = union.intersection(rdd2)
      assert(x1.size == 2)
    }

    it("Group the records of RDD using groupbBy function using a column"){
      var rddGroups = rdd.groupBy(groupByAccountType)
      rddGroups.foreach(rd => {
	rd._1 match {
	  case 1 => {
	    val consumerAccounts = rd._2
	    assert(consumerAccounts != null)
	    assert(consumerAccounts.size == 2)
	  }
	  case 2 => {
	    val businessAccounts = rd._2
	    assert(businessAccounts != null)
	    assert(businessAccounts.size == 3)
	  }
	  case _ => logger.info("Unexpected AccounType => " + rd._1)
	}
      })
    }

    it("Change the key value for each row of RDD"){
      var rdd1 = rdd.keyBy(keyByName)
      assert(rdd1 != null)
      assert(rdd1.size == 5)
      rdd1.toArray.foreach(x => {
	assert(x._1  == x._2.name.substring(8).toLong)
      })
    }
  }
}

