package com.ligadata.samples.messages

import java.util.Date
import org.json4s.jackson.JsonMethods._
import com.ligadata.FatafatBase.{ InputData, DelimitedData, JsonData, XmlData }
import java.io.{ DataInputStream, DataOutputStream }
import com.ligadata.FatafatBase.{ BaseMsg, BaseMsgObj, TransformMessage, BaseContainer, BaseContainerObj, MdBaseResolveInfo, RDDObject, RDD, TimeRange, JavaRDDObject }

object CustAlertHistory extends RDDObject[CustAlertHistory] with BaseContainerObj {
  type T = CustAlertHistory

  override def FullName: String = "com.ligadata.samples.messages.CustAlertHistory"
  override def NameSpace: String = "com.ligadata.samples.messages"
  override def Name: String = "CustAlertHistory"
  override def Version: String = "000000.000001.000000"
  override def CreateNewContainer: BaseContainer = new CustAlertHistory()
  override def IsFixed: Boolean = true;
  override def IsKv: Boolean = false;
  override def CanPersist: Boolean = true;

  def build = new T
  def build(from: T) = new T(from)

  val partitionKeys: Array[String] = null
  val partKeyPos = Array(0)
  val default = new CustAlertHistory

  override def PartitionKeyData(inputdata: InputData): Array[String] = Array[String]()
  override def PrimaryKeyData(inputdata: InputData): Array[String] = Array[String]()
  override def getFullName = FullName
  override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this)
}

class CustAlertHistory extends BaseContainer {
  override def IsFixed: Boolean = CustAlertHistory.IsFixed;
  override def IsKv: Boolean = CustAlertHistory.IsKv;
  override def CanPersist: Boolean = CustAlertHistory.CanPersist;
  override def FullName: String = CustAlertHistory.FullName
  override def NameSpace: String = CustAlertHistory.NameSpace
  override def Name: String = CustAlertHistory.Name
  override def Version: String = CustAlertHistory.Version

  var custid: Long = 0;
  var branchid: Int = 0;
  var accno: Long = 0;
  var alertDtTmInMs: Long = _;
  var alertType: String = ""
  var numDaysWithLessBalance: Int = 0

  def this(from: CustAlertHistory) = { this }
  def withAlertDtTmInMs(curDtTmInMs: Long): CustAlertHistory = { this }
  def withAlertType(alertType: String): CustAlertHistory = { this }
  def withNumDays(daysWithLessBalance: Int): CustAlertHistory = { this }

  override def Save: Unit = { CustAlertHistory.saveOne(this) }
  override def PartitionKeyData: Array[String] = null
  override def PrimaryKeyData: Array[String] = null
  override def set(key: String, value: Any): Unit = {}
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

object CustPreferences extends RDDObject[CustPreferences] with BaseContainerObj {
  type T = CustPreferences

  override def FullName: String = "com.ligadata.samples.messages.CustPreferences"
  override def NameSpace: String = "com.ligadata.samples.messages"
  override def Name: String = "CustPreferences"
  override def Version: String = "000000.000001.000000"
  override def CreateNewContainer: BaseContainer = new CustPreferences()
  override def IsFixed: Boolean = true;
  override def IsKv: Boolean = false;
  override def CanPersist: Boolean = true;

  def build = new T
  def build(from: T) = new T(from)

  val partitionKeys: Array[String] = null
  val partKeyPos = Array(0)
  val default = new CustPreferences

  override def PartitionKeyData(inputdata: InputData): Array[String] = Array[String]()
  override def PrimaryKeyData(inputdata: InputData): Array[String] = Array[String]()

  override def getFullName = FullName
  override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this)
}

class CustPreferences extends BaseContainer {
  override def IsFixed: Boolean = CustPreferences.IsFixed;
  override def IsKv: Boolean = CustPreferences.IsKv;
  override def CanPersist: Boolean = CustPreferences.CanPersist;
  override def FullName: String = CustPreferences.FullName
  override def NameSpace: String = CustPreferences.NameSpace
  override def Name: String = CustPreferences.Name
  override def Version: String = CustPreferences.Version

  def this(from: CustPreferences) = { this }
  def withMinBalanceAlertOptout(curDt: Date): CustPreferences = { this }
  def withOverdraftlimit(alertType: String): CustPreferences = { this }
  def withMultiDayMinBalanceAlertOptout(daysWithLessBalance: Int): CustPreferences = { this }

  var custid: Long = 0;
  var branchid: Int = 0;
  var accno: Long = 0;
  var minBalanceAlertOptout = false;
  var overdraftlimit: Double = 0;
  var multiDayMinBalanceAlertOptout = false;

  override def Save: Unit = { CustPreferences.saveOne(this) }
  override def PartitionKeyData: Array[String] = null
  override def PrimaryKeyData: Array[String] = null
  override def set(key: String, value: Any): Unit = {}
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

object CustTransaction extends RDDObject[CustTransaction] with BaseMsgObj {
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
  val default = new CustTransaction

  def build = new T
  def build(from: T) = new T(from)

  override def PartitionKeyData(inputdata: InputData): Array[String] = Array[String]()
  override def PrimaryKeyData(inputdata: InputData): Array[String] = Array[String]()

  override def getFullName = FullName
  override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this)
}

class CustTransaction extends BaseMsg {
  override def IsFixed: Boolean = CustTransaction.IsFixed;
  override def IsKv: Boolean = CustTransaction.IsKv;
  override def CanPersist: Boolean = CustTransaction.CanPersist;
  override def FullName: String = CustTransaction.FullName
  override def NameSpace: String = CustTransaction.NameSpace
  override def Name: String = CustTransaction.Name
  override def Version: String = CustTransaction.Version

  def this(from: CustTransaction) = { this }

  var custid: Long = 0;
  var branchid: Int = 0;
  var accno: Long = 0;
  var amount: Double = 0;
  var balance: Double = 0;
  var date: Int = 0;
  var time: Int = 0;
  var locationid: Int = 0;
  var transtype: String = "";

  override def Save: Unit = { CustTransaction.saveOne(this) }
  override def PartitionKeyData: Array[String] = null
  override def PrimaryKeyData: Array[String] = null
  override def set(key: String, value: Any): Unit = {}
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

object GlobalPreferences extends RDDObject[GlobalPreferences] with BaseContainerObj {
  type T = GlobalPreferences

  override def FullName: String = "com.ligadata.samples.messages.GlobalPreferences"
  override def NameSpace: String = "com.ligadata.samples.messages"
  override def Name: String = "GlobalPreferences"
  override def Version: String = "000000.000001.000000"
  override def CreateNewContainer: BaseContainer = new GlobalPreferences()
  override def IsFixed: Boolean = true;
  override def IsKv: Boolean = false;
  override def CanPersist: Boolean = true;

  def build = new T
  def build(from: T) = new T(from)

  val partitionKeys: Array[String] = null
  val partKeyPos = Array(0)
  val default = new GlobalPreferences

  override def PartitionKeyData(inputdata: InputData): Array[String] = Array[String]()
  override def PrimaryKeyData(inputdata: InputData): Array[String] = Array[String]()

  override def getFullName = FullName
  override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this)
}

class GlobalPreferences extends BaseContainer {
  override def IsFixed: Boolean = GlobalPreferences.IsFixed;
  override def IsKv: Boolean = GlobalPreferences.IsKv;
  override def CanPersist: Boolean = GlobalPreferences.CanPersist;
  override def FullName: String = GlobalPreferences.FullName
  override def NameSpace: String = GlobalPreferences.NameSpace
  override def Name: String = GlobalPreferences.Name
  override def Version: String = GlobalPreferences.Version

  def this(from: GlobalPreferences) = { this }

  var overDraftLimit: Double = 0.0;
  var minAlertBalance: Double = 0.0
  var minAlertDurationInHrs: Int = 48;
  var numLookbackDaysForMultiDayMinBalanceAlert: Int = 30;
  var maxNumDaysAllowedWithMinBalance: Int = 2;

  override def Save: Unit = { GlobalPreferences.saveOne(this) }
  override def PartitionKeyData: Array[String] = null
  override def PrimaryKeyData: Array[String] = null
  override def set(key: String, value: Any): Unit = {}
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

