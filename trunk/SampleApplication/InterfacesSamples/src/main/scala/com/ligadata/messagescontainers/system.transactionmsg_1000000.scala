
package com.ligadata.messagescontainers

import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.Formats
import scala.xml.XML
import scala.xml.Elem
import com.ligadata.FatafatBase.{ InputData, DelimitedData, JsonData, XmlData }
import com.ligadata.BaseTypes._
import com.ligadata.FatafatBase.SerializeDeserialize
import java.io.{ DataInputStream, DataOutputStream, ByteArrayOutputStream }
import com.ligadata.FatafatBase.{ BaseMsg, BaseMsgObj, TransformMessage, BaseContainer, MdBaseResolveInfo, MessageContainerBase, RDDObject, RDD, TimeRange }

object System_TransactionMsg_1000000 extends BaseMsgObj with RDDObject {
  override def TransformDataAttributes: TransformMessage = null
  override def NeedToTransformData: Boolean = false
  override def FullName: String = "System.TransactionMsg"
  override def NameSpace: String = "System"
  override def Name: String = "TransactionMsg"
  override def Version: String = "000000.000001.000000"
  override def CreateNewMessage: BaseMsg = new System_TransactionMsg_1000000()
  override def IsFixed: Boolean = true;
  override def IsKv: Boolean = false;
  override def CanPersist: Boolean = true;

  val partitionKeys: Array[String] = Array("custid")
  val partKeyPos = Array(0)

  override def PartitionKeyData(inputdata: InputData): Array[String] = {
    if (partKeyPos.size == 0 || partitionKeys.size == 0)
      return Array[String]()
    if (inputdata.isInstanceOf[DelimitedData]) {
      val csvData = inputdata.asInstanceOf[DelimitedData]
      if (csvData.tokens == null) {
        return partKeyPos.map(pos => "")
      }
      return partKeyPos.map(pos => csvData.tokens(pos))
    } else if (inputdata.isInstanceOf[JsonData]) {
      val jsonData = inputdata.asInstanceOf[JsonData]
      val mapOriginal = jsonData.cur_json.get.asInstanceOf[Map[String, Any]]
      if (mapOriginal == null) {
        return partKeyPos.map(pos => "")
      }
      val map: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map[String, Any]()
      mapOriginal.foreach(kv => { map(kv._1.toLowerCase()) = kv._2 })
      return partitionKeys.map(key => map.getOrElse(key, "").toString)
    } else if (inputdata.isInstanceOf[XmlData]) {
      val xmlData = inputdata.asInstanceOf[XmlData]
      // Fix this
    } else throw new Exception("Invalid input data")
    return Array[String]()
  }

  val primaryKeys: Array[String] = Array("branchid", "accno")
  val prmryKeyPos = Array(1, 2)

  override def PrimaryKeyData(inputdata: InputData): Array[String] = {
    if (prmryKeyPos.size == 0 || primaryKeys.size == 0)
      return Array[String]()
    if (inputdata.isInstanceOf[DelimitedData]) {
      val csvData = inputdata.asInstanceOf[DelimitedData]
      if (csvData.tokens == null) {
        return prmryKeyPos.map(pos => "")
      }
      return prmryKeyPos.map(pos => csvData.tokens(pos))
    } else if (inputdata.isInstanceOf[JsonData]) {
      val jsonData = inputdata.asInstanceOf[JsonData]
      val mapOriginal = jsonData.cur_json.get.asInstanceOf[Map[String, Any]]
      if (mapOriginal == null) {
        return prmryKeyPos.map(pos => "")
      }
      val map: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map[String, Any]()
      mapOriginal.foreach(kv => { map(kv._1.toLowerCase()) = kv._2 })

      return primaryKeys.map(key => map.getOrElse(key, "").toString)
    } else if (inputdata.isInstanceOf[XmlData]) {
      val xmlData = inputdata.asInstanceOf[XmlData]
      // Fix this
    } else throw new Exception("Invalid input data")
    return Array[String]()
  }

  // Get Most Recent Message for Current Partition Key
  override def GetRecentRDDForCurrentPartitionKey: RDD[System_TransactionMsg_1000000] = { return null }

  // Get by Current (Partition) Key
  override def GetRDDForCurrentPartitionKey(tmRange: TimeRange, f: System_TransactionMsg_1000000 => Boolean): RDD[System_TransactionMsg_1000000] = { null }
  override def GetRDDForCurrentPartitionKey(f: System_TransactionMsg_1000000 => Boolean): RDD[System_TransactionMsg_1000000] = { null }
  override def GetRDDForCurrentPartitionKey: RDD[System_TransactionMsg_1000000] = { null } // Should return some error/exception on facts if the size is too big

  // Get by Partition Key
  override def GetRDDForPartitionKey(partitionKey: Array[String], tmRange: TimeRange, f: System_TransactionMsg_1000000 => Boolean): RDD[System_TransactionMsg_1000000] = { null }
  override def GetRDDForPartitionKey(partitionKey: Array[String], f: System_TransactionMsg_1000000 => Boolean): RDD[System_TransactionMsg_1000000] = { null }
  override def GetRDDForPartitionKey(partitionKey: Array[String]): RDD[System_TransactionMsg_1000000] = { null } // Should return some error/exception on facts if the size is too big
}

class System_TransactionMsg_1000000 extends BaseMsg {
  override def IsFixed: Boolean = true;
  override def IsKv: Boolean = false;

  override def CanPersist: Boolean = true;

  override def FullName: String = "System.TransactionMsg"
  override def NameSpace: String = "System"
  override def Name: String = "TransactionMsg"
  override def Version: String = "000000.000001.000000"

  var custid: Long = _;
  var branchid: Int = _;
  var accno: Long = _;
  var amount: Double = _;
  var balance: Double = _;
  var date: Int = _;
  var time: Int = _;
  var locationid: Int = _;
  var transtype: String = _;

  override def PartitionKeyData: Array[String] = Array(custid.toString)

  override def PrimaryKeyData: Array[String] = Array(branchid.toString, accno.toString)

  override def set(key: String, value: Any): Unit = { throw new Exception("set function is not yet implemented") }

  override def get(key: String): Any = {
    try {
      // Try with reflection
      return getWithReflection(key)
    } catch {
      case e: Exception => {
        // Call By Name
        return getByName(key)
      }
    }
  }
  override def getOrElse(key: String, default: Any): Any = { throw new Exception("getOrElse function is not yet implemented") }

  private def getByName(key: String): Any = {
    try {
      if (key.equals("custid")) return custid;
      if (key.equals("branchid")) return branchid;
      if (key.equals("accno")) return accno;
      if (key.equals("amount")) return amount;
      if (key.equals("balance")) return balance;
      if (key.equals("date")) return date;
      if (key.equals("time")) return time;
      if (key.equals("locationid")) return locationid;
      if (key.equals("transtype")) return transtype;

      // if (key.equals("desynpuf_id")) return desynpuf_id;
      //if (key.equals("clm_id")) return clm_id;
      return null;
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }

  private def getWithReflection(key: String): Any = {
    val ru = scala.reflect.runtime.universe
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val im = m.reflect(this)
    val fieldX = ru.typeOf[System_TransactionMsg_1000000].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
    val fmX = im.reflectField(fieldX)
    fmX.get
  }

  override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = {}

  override def GetMessage(childPath: Array[(String, String)], primaryKey: Array[String]): com.ligadata.FatafatBase.BaseMsg = {
    return null
  }

  def populate(inputdata: InputData) = {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else if (inputdata.isInstanceOf[JsonData])
      populateJson(inputdata.asInstanceOf[JsonData])
    else if (inputdata.isInstanceOf[XmlData])
      populateXml(inputdata.asInstanceOf[XmlData])
    else throw new Exception("Invalid input data")

  }

  private def populateCSV(inputdata: DelimitedData): Unit = {
    val list = inputdata.tokens
    val arrvaldelim = "~"
    try {
      if (list.size < 9) throw new Exception("Incorrect input data size")
      custid = com.ligadata.BaseTypes.LongImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      branchid = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      accno = com.ligadata.BaseTypes.LongImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      amount = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      balance = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      date = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      time = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      locationid = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      transtype = com.ligadata.BaseTypes.StringImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }

  private def populateJson(json: JsonData): Unit = {
    try {
      if (json == null || json.cur_json == null || json.cur_json == None) throw new Exception("Invalid json data")
      assignJsonData(json)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }

  def CollectionAsArrString(v: Any): Array[String] = {
    if (v.isInstanceOf[Set[_]]) {
      return v.asInstanceOf[Set[String]].toArray
    }
    if (v.isInstanceOf[List[_]]) {
      return v.asInstanceOf[List[String]].toArray
    }
    if (v.isInstanceOf[Array[_]]) {
      return v.asInstanceOf[Array[String]].toArray
    }
    throw new Exception("Unhandled Collection")
  }

  private def assignJsonData(json: JsonData): Unit = {
    type tList = List[String]
    type tMap = Map[String, Any]
    var list: List[Map[String, Any]] = null
    try {
      val mapOriginal = json.cur_json.get.asInstanceOf[Map[String, Any]]
      if (mapOriginal == null)
        throw new Exception("Invalid json data")

      val map: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map[String, Any]()
      mapOriginal.foreach(kv => { map(kv._1.toLowerCase()) = kv._2 })

      custid = com.ligadata.BaseTypes.LongImpl.Input(map.getOrElse("custid", 0).toString)
      branchid = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("branchid", 0).toString)
      accno = com.ligadata.BaseTypes.LongImpl.Input(map.getOrElse("accno", 0).toString)
      amount = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("amount", 0.0).toString)
      balance = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("balance", 0.0).toString)
      date = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("date", 0).toString)
      time = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("time", 0).toString)
      locationid = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("locationid", 0).toString)
      transtype = com.ligadata.BaseTypes.StringImpl.Input(map.getOrElse("transtype", "").toString)

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }

  private def populateXml(xmlData: XmlData): Unit = {
    try {
      val xml = XML.loadString(xmlData.dataInput)
      if (xml == null) throw new Exception("Invalid xml data")
      val _custidval_ = (xml \\ "custid").text.toString
      if (_custidval_ != "") custid = com.ligadata.BaseTypes.LongImpl.Input(_custidval_) else custid = 0
      val _branchidval_ = (xml \\ "branchid").text.toString
      if (_branchidval_ != "") branchid = com.ligadata.BaseTypes.IntImpl.Input(_branchidval_) else branchid = 0
      val _accnoval_ = (xml \\ "accno").text.toString
      if (_accnoval_ != "") accno = com.ligadata.BaseTypes.LongImpl.Input(_accnoval_) else accno = 0
      val _amountval_ = (xml \\ "amount").text.toString
      if (_amountval_ != "") amount = com.ligadata.BaseTypes.DoubleImpl.Input(_amountval_) else amount = 0.0
      val _balanceval_ = (xml \\ "balance").text.toString
      if (_balanceval_ != "") balance = com.ligadata.BaseTypes.DoubleImpl.Input(_balanceval_) else balance = 0.0
      val _dateval_ = (xml \\ "date").text.toString
      if (_dateval_ != "") date = com.ligadata.BaseTypes.IntImpl.Input(_dateval_) else date = 0
      val _timeval_ = (xml \\ "time").text.toString
      if (_timeval_ != "") time = com.ligadata.BaseTypes.IntImpl.Input(_timeval_) else time = 0
      val _locationidval_ = (xml \\ "locationid").text.toString
      if (_locationidval_ != "") locationid = com.ligadata.BaseTypes.IntImpl.Input(_locationidval_) else locationid = 0
      val _transtypeval_ = (xml \\ "transtype").text.toString
      if (_transtypeval_ != "") transtype = com.ligadata.BaseTypes.StringImpl.Input(_transtypeval_) else transtype = ""

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }

  override def Serialize(dos: DataOutputStream): Unit = {
    try {
      com.ligadata.BaseTypes.LongImpl.SerializeIntoDataOutputStream(dos, custid);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, branchid);
      com.ligadata.BaseTypes.LongImpl.SerializeIntoDataOutputStream(dos, accno);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, amount);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, balance);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, date);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, time);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, locationid);
      com.ligadata.BaseTypes.StringImpl.SerializeIntoDataOutputStream(dos, transtype);

    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  override def Deserialize(dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, savedDataVersion: String): Unit = {
    try {
      if (savedDataVersion == null || savedDataVersion.trim() == "")
        throw new Exception("Please provide Data Version")

      val prevVer = savedDataVersion.replaceAll("[.]", "").toLong
      val currentVer = Version.replaceAll("[.]", "").toLong

      if (prevVer == currentVer) {
        custid = com.ligadata.BaseTypes.LongImpl.DeserializeFromDataInputStream(dis);
        branchid = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        accno = com.ligadata.BaseTypes.LongImpl.DeserializeFromDataInputStream(dis);
        amount = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        balance = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        date = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        time = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        locationid = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        transtype = com.ligadata.BaseTypes.StringImpl.DeserializeFromDataInputStream(dis);

      } else throw new Exception("Current Message/Container Version " + currentVer + " should be greater than Previous Message Version " + prevVer + ".")

    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def ConvertPrevToNewVerObj(obj: Any): Unit = {}

}