package com.ligadata.samples.messages

import java.util.Date
import org.json4s.jackson.JsonMethods._
import com.ligadata.FatafatBase.{ InputData, DelimitedData, JsonData, XmlData }
import java.io.{DataInputStream, DataOutputStream}
import com.ligadata.FatafatBase.{BaseMsg, BaseMsgObj, TransformMessage, BaseContainer, BaseContainerObj, MdBaseResolveInfo, RDDObject, RDD, TimeRange}

object CustAlertHistory extends BaseContainerObj with RDDObject[CustAlertHistory, CustAlertHistoryBuilder] {
  type T = CustAlertHistory
  
  override def FullName: String = "com.ligadata.samples.messages.CustAlertHistory"
  override def NameSpace: String = "com.ligadata.samples.messages"
  override def Name: String = "CustAlertHistory"
  override def Version: String = "000000.000001.000000"
  override def CreateNewContainer: BaseContainer = new CustAlertHistory()
  override def IsFixed: Boolean = true;
  override def IsKv: Boolean = false;
  override def CanPersist: Boolean = true;

  def builder = new CustAlertHistoryBuilder
  
  val partitionKeys: Array[String] = null
  val partKeyPos = Array(0)
  val default =  new CustAlertHistory

  override def PartitionKeyData(inputdata: InputData): Array[String] = Array[String]()
  override def PrimaryKeyData(inputdata: InputData): Array[String] = Array[String]()

  // Get recent entry for the given key
  override def getRecent(key: Array[String]): Option[T] = None
  override def getRecentOrDefault(key: Array[String]): T = null
  
  override def getOne(tmRange: TimeRange, f: T => Boolean): Option[T] = None
  override def getOne(key: Array[String], tmRange: TimeRange, f: T => Boolean): Option[T] = None
  override def getOneOrDefault(key: Array[String], tmRange: TimeRange, f: T => Boolean): T = null
  override def getOneOrDefault(tmRange: TimeRange, f: T => Boolean): T = null

  // Get for Current Key
  override def getRecent: Option[T] = { None }  
  override def getRecentOrDefault: T = null
  override def getRDDForCurrKey(f: CustAlertHistory => Boolean): RDD[T] = null
  override def getRDDForCurrKey(tmRange: TimeRange, f: T => Boolean): RDD[T] = null
  
  // With too many messages, these may fail - mostly useful for message types where number of messages are relatively small 
  override def getRDD(tmRange: TimeRange, func: T => Boolean) : RDD[T] = null
  override def getRDD(tmRange: TimeRange) : RDD[T] = null
  override def getRDD(func: T => Boolean) : RDD[T] = null
  
  override def getRDD(key: Array[String], tmRange: TimeRange, func: T => Boolean) : RDD[T] = null
  override def getRDD(key: Array[String], func: T => Boolean) : RDD[T] = null
  override def getRDD(key: Array[String], tmRange: TimeRange) : RDD[T] = null
}

class CustAlertHistoryBuilder {
  def withAlertDt(curDt : Date) : CustAlertHistoryBuilder = {this}
  def withAlertType(alertType: String) : CustAlertHistoryBuilder = {this}
  def withNumDays(daysWithLessBalance : Int) : CustAlertHistoryBuilder = {this}
  
  def build : CustAlertHistory = null
  
  var custid: Long = 0;
  var branchid: Int = 0;
  var accno: Long = 0;
  var lastAlertDt: Date = _;
}

class CustAlertHistory extends BaseContainer {
  override def IsFixed: Boolean = true;
  override def IsKv: Boolean = false;

  override def CanPersist: Boolean = true;

  override def FullName: String = "com.ligadata.samples.messages.CustAlertHistory"
  override def NameSpace: String = "com.ligadata.samples.messages"
  override def Name: String = "CustAlertHistory"
  override def Version: String = "000000.000001.000000"

  var custid: Long = 0;
  var branchid: Int = 0;
  var accno: Long = 0;
  var lastalertdate: Int = 0;
  var lastAlertDt: Date = _;

  def save = {}
  override def PartitionKeyData: Array[String] = null
  override def PrimaryKeyData: Array[String] = null
  override def set(key: String, value: Any): Unit = { }
  override def get(key: String): Any = null
  override def getOrElse(key: String, default: Any): Any = { throw new Exception("getOrElse function is not yet implemented") }
  private def getByName(key: String): Any = null
  private def getWithReflection(key: String): Any = null
  override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = {}
  override def GetMessage(childPath: Array[(String, String)], primaryKey: Array[String]): com.ligadata.FatafatBase.BaseMsg = null
  def populate(inputdata: InputData) = {}
  private def populateCSV(inputdata: DelimitedData): Unit = {}
  private def populateJson(json: JsonData): Unit = {}
  def CollectionAsArrString(v: Any): Array[String] = null
  private def assignJsonData(json: JsonData): Unit = {}
  private def populateXml(xmlData: XmlData): Unit = {}
  override def Serialize(dos: DataOutputStream): Unit = {}
  override def Deserialize(dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, savedDataVersion: String): Unit = {}
  def ConvertPrevToNewVerObj(obj: Any): Unit = {}
}


object CustPreferences extends BaseContainerObj with RDDObject[CustPreferences, CustPreferencesBuilder] {
  type T = CustPreferences

  override def FullName: String = "com.ligadata.samples.messages.CustPreferences"
  override def NameSpace: String = "com.ligadata.samples.messages"
  override def Name: String = "CustPreferences"
  override def Version: String = "000000.000001.000000"
  override def CreateNewContainer: BaseContainer = new CustPreferences()
  override def IsFixed: Boolean = true;
  override def IsKv: Boolean = false;
  override def CanPersist: Boolean = true;

  def builder = new CustPreferencesBuilder
  
  val partitionKeys: Array[String] = null
  val partKeyPos = Array(0)
  val default =  new CustPreferences

  override def PartitionKeyData(inputdata: InputData): Array[String] = Array[String]()
  override def PrimaryKeyData(inputdata: InputData): Array[String] = Array[String]()

  // Get recent entry for the given key
  override def getRecent(key: Array[String]): Option[T] = None
  override def getRecentOrDefault(key: Array[String]): T = null
  
  override def getOne(tmRange: TimeRange, f: T => Boolean): Option[T] = None
  override def getOne(key: Array[String], tmRange: TimeRange, f: T => Boolean): Option[T] = None
  override def getOneOrDefault(key: Array[String], tmRange: TimeRange, f: T => Boolean): T = null
  override def getOneOrDefault(tmRange: TimeRange, f: T => Boolean): T = null

  // Get for Current Key
  override def getRecent: Option[T] = { None }  
  override def getRecentOrDefault: T = null
  override def getRDDForCurrKey(f: CustPreferences => Boolean): RDD[T] = null
  override def getRDDForCurrKey(tmRange: TimeRange, f: T => Boolean): RDD[T] = null
  
  // With too many messages, these may fail - mostly useful for message types where number of messages are relatively small 
  override def getRDD(tmRange: TimeRange, func: T => Boolean) : RDD[T] = null
  override def getRDD(tmRange: TimeRange) : RDD[T] = null
  override def getRDD(func: T => Boolean) : RDD[T] = null
  
  override def getRDD(key: Array[String], tmRange: TimeRange, func: T => Boolean) : RDD[T] = null
  override def getRDD(key: Array[String], func: T => Boolean) : RDD[T] = null
  override def getRDD(key: Array[String], tmRange: TimeRange) : RDD[T] = null
}

class CustPreferencesBuilder {
  var custid: Long = 0;
  var branchid: Int = 0;
  var accno: Long = 0;
  var minBalanceAlertOptout = false;
  var overdraftlimit: Double = 0;
  var multiDayMinBalanceAlertOptout = false;
}

class CustPreferences extends BaseContainer {
  override def IsFixed: Boolean = true;
  override def IsKv: Boolean = false;

  override def CanPersist: Boolean = true;

  override def FullName: String = "com.ligadata.samples.messages.CustPreferences"
  override def NameSpace: String = "com.ligadata.samples.messages"
  override def Name: String = "CustPreferences"
  override def Version: String = "000000.000001.000000"

  var custid: Long = 0;
  var branchid: Int = 0;
  var accno: Long = 0;
  var minBalanceAlertOptout = false;
  var overdraftlimit: Double = 0;
  var multiDayMinBalanceAlertOptout = false;
  
  def save = {}
  override def PartitionKeyData: Array[String] = null
  override def PrimaryKeyData: Array[String] = null
  override def set(key: String, value: Any): Unit = { }
  override def get(key: String): Any = null
  override def getOrElse(key: String, default: Any): Any = { throw new Exception("getOrElse function is not yet implemented") }
  private def getByName(key: String): Any = null
  private def getWithReflection(key: String): Any = null
  override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = {}
  override def GetMessage(childPath: Array[(String, String)], primaryKey: Array[String]): com.ligadata.FatafatBase.BaseMsg = null
  def populate(inputdata: InputData) = {}
  private def populateCSV(inputdata: DelimitedData): Unit = {}
  private def populateJson(json: JsonData): Unit = {}
  def CollectionAsArrString(v: Any): Array[String] = null
  private def assignJsonData(json: JsonData): Unit = {}
  private def populateXml(xmlData: XmlData): Unit = {}
  override def Serialize(dos: DataOutputStream): Unit = {}
  override def Deserialize(dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, savedDataVersion: String): Unit = {}
  def ConvertPrevToNewVerObj(obj: Any): Unit = {}
}

object CustTransaction extends BaseMsgObj with RDDObject[CustTransaction, CustTransactionBuilder] {
  type T = CustTransaction

  override def TransformDataAttributes: TransformMessage = null
  override def NeedToTransformData: Boolean = false
  override def FullName: String = "com.ligadata.samples.messages.CustTransaction"
  override def NameSpace: String = "com.ligadata.samples.messages"
  override def Name: String = "CustTransaction"
  override def Version: String = "000000.000001.000000"
  override def CreateNewMessage: BaseMsg = new CustTransaction()
  override def IsFixed: Boolean = true;
  override def IsKv: Boolean = false;
  override def CanPersist: Boolean = true;

  val partitionKeys: Array[String] = null
  val partKeyPos = Array(0)
  val default =  new CustTransaction
    
  def builder = new CustTransactionBuilder
  override def PartitionKeyData(inputdata: InputData): Array[String] = Array[String]()
  override def PrimaryKeyData(inputdata: InputData): Array[String] = Array[String]()

  // Get recent entry for the given key
  override def getRecent(key: Array[String]): Option[T] = None
  override def getRecentOrDefault(key: Array[String]): T = null
  
  override def getOne(tmRange: TimeRange, f: T => Boolean): Option[T] = None
  override def getOne(key: Array[String], tmRange: TimeRange, f: T => Boolean): Option[T] = None
  override def getOneOrDefault(key: Array[String], tmRange: TimeRange, f: T => Boolean): T = null
  override def getOneOrDefault(tmRange: TimeRange, f: T => Boolean): T = null

  // Get for Current Key
  override def getRecent: Option[T] = { None }  
  override def getRecentOrDefault: T = null
  override def getRDDForCurrKey(f: CustTransaction => Boolean): RDD[T] = null
  override def getRDDForCurrKey(tmRange: TimeRange, f: T => Boolean): RDD[T] = null
  
  // With too many messages, these may fail - mostly useful for message types where number of messages are relatively small 
  override def getRDD(tmRange: TimeRange, func: T => Boolean) : RDD[T] = null
  override def getRDD(tmRange: TimeRange) : RDD[T] = null
  override def getRDD(func: T => Boolean) : RDD[T] = null
  
  override def getRDD(key: Array[String], tmRange: TimeRange, func: T => Boolean) : RDD[T] = null
  override def getRDD(key: Array[String], func: T => Boolean) : RDD[T] = null
  override def getRDD(key: Array[String], tmRange: TimeRange) : RDD[T] = null
}

class CustTransactionBuilder {
}

class CustTransaction extends BaseMsg {
  override def IsFixed: Boolean = true;
  override def IsKv: Boolean = false;

  override def CanPersist: Boolean = true;

  override def FullName: String = "com.ligadata.samples.messages.CustTransaction"
  override def NameSpace: String = "com.ligadata.samples.messages"
  override def Name: String = "CustTransaction"
  override def Version: String = "000000.000001.000000"

  var custid: Long = 0;
  var branchid: Int = 0;
  var accno: Long = 0;
  var amount: Double = 0;
  var balance: Double = 0;
  var date: Int = 0;
  var time: Int = 0;
  var locationid: Int = 0;
  var transtype: String = "";

  def save = {}
  override def PartitionKeyData: Array[String] = null
  override def PrimaryKeyData: Array[String] = null
  override def set(key: String, value: Any): Unit = { }
  override def get(key: String): Any = null
  override def getOrElse(key: String, default: Any): Any = { throw new Exception("getOrElse function is not yet implemented") }
  private def getByName(key: String): Any = null
  private def getWithReflection(key: String): Any = null
  override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = {}
  override def GetMessage(childPath: Array[(String, String)], primaryKey: Array[String]): com.ligadata.FatafatBase.BaseMsg = null
  def populate(inputdata: InputData) = {}
  private def populateCSV(inputdata: DelimitedData): Unit = {}
  private def populateJson(json: JsonData): Unit = {}
  def CollectionAsArrString(v: Any): Array[String] = null
  private def assignJsonData(json: JsonData): Unit = {}
  private def populateXml(xmlData: XmlData): Unit = {}
  override def Serialize(dos: DataOutputStream): Unit = {}
  override def Deserialize(dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, savedDataVersion: String): Unit = {}
  def ConvertPrevToNewVerObj(obj: Any): Unit = {}
}

object GlobalPreferences extends BaseContainerObj with RDDObject[GlobalPreferences, GlobalPreferencesBuilder] {
  type T = GlobalPreferences

  override def FullName: String = "com.ligadata.samples.messages.GlobalPreferences"
  override def NameSpace: String = "com.ligadata.samples.messages"
  override def Name: String = "GlobalPreferences"
  override def Version: String = "000000.000001.000000"
  override def CreateNewContainer: BaseContainer = new GlobalPreferences()
  override def IsFixed: Boolean = true;
  override def IsKv: Boolean = false;
  override def CanPersist: Boolean = true;

  def builder = new GlobalPreferencesBuilder
  
  val partitionKeys: Array[String] = null
  val partKeyPos = Array(0)
  val default = new GlobalPreferencesBuilder
  
  override def PartitionKeyData(inputdata: InputData): Array[String] = Array[String]()
  override def PrimaryKeyData(inputdata: InputData): Array[String] = Array[String]()

  // Get recent entry for the given key
  override def getRecent(key: Array[String]): Option[T] = None
  override def getRecentOrDefault(key: Array[String]): T = null
  
  override def getOne(tmRange: TimeRange, f: T => Boolean): Option[T] = None
  override def getOne(key: Array[String], tmRange: TimeRange, f: T => Boolean): Option[T] = None
  override def getOneOrDefault(key: Array[String], tmRange: TimeRange, f: T => Boolean): T = null
  override def getOneOrDefault(tmRange: TimeRange, f: T => Boolean): T = null

  // Get for Current Key
  override def getRecent: Option[T] = { None }  
  override def getRecentOrDefault: T = null
  override def getRDDForCurrKey(f: GlobalPreferences => Boolean): RDD[T] = null
  override def getRDDForCurrKey(tmRange: TimeRange, f: T => Boolean): RDD[T] = null
  
  // With too many messages, these may fail - mostly useful for message types where number of messages are relatively small 
  override def getRDD(tmRange: TimeRange, func: T => Boolean) : RDD[T] = null
  override def getRDD(tmRange: TimeRange) : RDD[T] = null
  override def getRDD(func: T => Boolean) : RDD[T] = null
  
  override def getRDD(key: Array[String], tmRange: TimeRange, func: T => Boolean) : RDD[T] = null
  override def getRDD(key: Array[String], func: T => Boolean) : RDD[T] = null
  override def getRDD(key: Array[String], tmRange: TimeRange) : RDD[T] = null
}

class GlobalPreferencesBuilder  {
  def build : GlobalPreferences = null
  
// define all withXXX methods to set values in the builder  
  var overDraftLimit: Double = 0.0;
  var minAlertBalance: Double = 0.0
  var minAlertDurationInHrs: Int = 48;
  var numLookbackDaysForMultiDayMinBalanceAlert: Int = 30;
  var maxNumDaysAllowedWithMinBalance: Int = 2;
}

class GlobalPreferences extends BaseContainer {
  override def IsFixed: Boolean = true;
  override def IsKv: Boolean = false;

  override def CanPersist: Boolean = true;

  override def FullName: String = "com.ligadata.samples.messages.GlobalPreferences"
  override def NameSpace: String = "com.ligadata.samples.messages"
  override def Name: String = "GlobalPreferences"
  override def Version: String = "000000.000001.000000"

  var overDraftLimit: Double = 0.0;
  var minAlertBalance: Double = 0.0
  var minAlertDurationInHrs: Int = 48;
  var numLookbackDaysForMultiDayMinBalanceAlert: Int = 30;
  var maxNumDaysAllowedWithMinBalance: Int = 2;
  
  def save = {}
  override def PartitionKeyData: Array[String] = null
  override def PrimaryKeyData: Array[String] = null
  override def set(key: String, value: Any): Unit = { }
  override def get(key: String): Any = null
  override def getOrElse(key: String, default: Any): Any = { throw new Exception("getOrElse function is not yet implemented") }
  private def getByName(key: String): Any = null
  private def getWithReflection(key: String): Any = null
  override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = {}
  override def GetMessage(childPath: Array[(String, String)], primaryKey: Array[String]): com.ligadata.FatafatBase.BaseMsg = null
  def populate(inputdata: InputData) = {}
  private def populateCSV(inputdata: DelimitedData): Unit = {}
  private def populateJson(json: JsonData): Unit = {}
  def CollectionAsArrString(v: Any): Array[String] = null
  private def assignJsonData(json: JsonData): Unit = {}
  private def populateXml(xmlData: XmlData): Unit = {}
  override def Serialize(dos: DataOutputStream): Unit = {}
  override def Deserialize(dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, savedDataVersion: String): Unit = {}
  def ConvertPrevToNewVerObj(obj: Any): Unit = {}
}

