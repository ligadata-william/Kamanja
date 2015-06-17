
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

import com.ligadata.FatafatBase.{ BaseMsg, BaseMsgObj, TransformMessage, BaseContainer, MdBaseResolveInfo, MessageContainerBase }

object System_Beneficiary_1000001_1430446866134 extends BaseMsgObj {
  override def TransformDataAttributes: TransformMessage = null
  override def NeedToTransformData: Boolean = false
  override def FullName: String = "System.Beneficiary"
  override def NameSpace: String = "System"
  override def Name: String = "Beneficiary"
  override def Version: String = "000000.000001.000001"
  override def CreateNewMessage: BaseMsg = new System_Beneficiary_1000001_1430446866134()
  override def IsFixed: Boolean = true;
  override def IsKv: Boolean = false;
  override def CanPersist: Boolean = true;

  val partitionKeys: Array[String] = Array("desynpuf_id")
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

  val primaryKeys: Array[String] = Array("desynpuf_id")
  val prmryKeyPos = Array(0)

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

}

class System_Beneficiary_1000001_1430446866134 extends BaseMsg {
  override def IsFixed: Boolean = true;
  override def IsKv: Boolean = false;

  override def FullName: String = "System.Beneficiary"
  override def NameSpace: String = "System"
  override def Name: String = "Beneficiary"
  override def Version: String = "000000.000001.000001"
    override def CanPersist: Boolean = true;

  var desynpuf_id: String = _;
  var bene_birth_dt: Int = _;
  var bene_death_dt: Int = _;
  var bene_sex_ident_cd: Int = _;
  var bene_race_cd: Int = _;
  var bene_esrd_ind: Char = _;
  var sp_state_code: Int = _;
  var bene_county_cd: Int = _;
  var bene_hi_cvrage_tot_mons: Int = _;
  var bene_smi_cvrage_tot_mons: Int = _;
  var bene_hmo_cvrage_tot_mons: Int = _;
  var plan_cvrg_mos_num: Int = _;
  var sp_alzhdmta: Int = _;
  var sp_chf: Int = _;
  var sp_chrnkidn: Int = _;
  var sp_cncr: Int = _;
  var sp_copd: Int = _;
  var sp_depressn: Int = _;
  var sp_diabetes: Int = _;
  var sp_ischmcht: Int = _;
  var sp_osteoprs: Int = _;
  var sp_ra_oa: Int = _;
  var sp_strketia: Int = _;
  var medreimb_ip: Double = _;
  var benres_ip: Double = _;
  var pppymt_ip: Double = _;
  var medreimb_op: Double = _;
  var benres_op: Double = _;
  var pppymt_op: Double = _;
  var medreimb_car: Double = _;
  var benres_car: Double = _;
  var pppymt_car: Double = _;
  var hl7message: com.ligadata.FatafatBase.BaseMsg = _;
  var inpatient_claims: scala.collection.mutable.ArrayBuffer[com.ligadata.messagescontainers.System_InpatientClaim_1000001_1430446866091] = new scala.collection.mutable.ArrayBuffer[com.ligadata.messagescontainers.System_InpatientClaim_1000001_1430446866091];
  var outpatient_claims: scala.collection.mutable.ArrayBuffer[com.ligadata.messagescontainers.System_OutpatientClaim_1000001_1430446866113] = new scala.collection.mutable.ArrayBuffer[com.ligadata.messagescontainers.System_OutpatientClaim_1000001_1430446866113];
  var hl7messages: scala.collection.mutable.ArrayBuffer[com.ligadata.messagescontainers.System_HL7_1000001_1430446865995] = new scala.collection.mutable.ArrayBuffer[com.ligadata.messagescontainers.System_HL7_1000001_1430446865995];

  override def PartitionKeyData: Array[String] = Array(desynpuf_id.toString)

  override def PrimaryKeyData: Array[String] = Array(desynpuf_id.toString)

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
      if (key.equals("desynpuf_id")) return desynpuf_id;
      if (key.equals("bene_birth_dt")) return bene_birth_dt;
      if (key.equals("bene_death_dt")) return bene_death_dt;
      if (key.equals("bene_sex_ident_cd")) return bene_sex_ident_cd;
      if (key.equals("bene_race_cd")) return bene_race_cd;
      if (key.equals("bene_esrd_ind")) return bene_esrd_ind;
      if (key.equals("sp_state_code")) return sp_state_code;
      if (key.equals("bene_county_cd")) return bene_county_cd;
      if (key.equals("bene_hi_cvrage_tot_mons")) return bene_hi_cvrage_tot_mons;
      if (key.equals("bene_smi_cvrage_tot_mons")) return bene_smi_cvrage_tot_mons;
      if (key.equals("bene_hmo_cvrage_tot_mons")) return bene_hmo_cvrage_tot_mons;
      if (key.equals("plan_cvrg_mos_num")) return plan_cvrg_mos_num;
      if (key.equals("sp_alzhdmta")) return sp_alzhdmta;
      if (key.equals("sp_chf")) return sp_chf;
      if (key.equals("sp_chrnkidn")) return sp_chrnkidn;
      if (key.equals("sp_cncr")) return sp_cncr;
      if (key.equals("sp_copd")) return sp_copd;
      if (key.equals("sp_depressn")) return sp_depressn;
      if (key.equals("sp_diabetes")) return sp_diabetes;
      if (key.equals("sp_ischmcht")) return sp_ischmcht;
      if (key.equals("sp_osteoprs")) return sp_osteoprs;
      if (key.equals("sp_ra_oa")) return sp_ra_oa;
      if (key.equals("sp_strketia")) return sp_strketia;
      if (key.equals("medreimb_ip")) return medreimb_ip;
      if (key.equals("benres_ip")) return benres_ip;
      if (key.equals("pppymt_ip")) return pppymt_ip;
      if (key.equals("medreimb_op")) return medreimb_op;
      if (key.equals("benres_op")) return benres_op;
      if (key.equals("pppymt_op")) return pppymt_op;
      if (key.equals("medreimb_car")) return medreimb_car;
      if (key.equals("benres_car")) return benres_car;
      if (key.equals("pppymt_car")) return pppymt_car;
      if (key.equals("hl7message")) return hl7message;
      if (key.equals("inpatient_claims")) return inpatient_claims;
      if (key.equals("outpatient_claims")) return outpatient_claims;
      if (key.equals("hl7messages")) return hl7messages;

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
    val fieldX = ru.typeOf[System_Beneficiary_1000001_1430446866134].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
    val fmX = im.reflectField(fieldX)
    fmX.get
  }

  override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = {
    if (childPath == null || childPath.size == 0) { // Invalid case
      return
    }
    val curVal = childPath(0)
    if (childPath.size == 1) { // If this is last level
      if (curVal._2.compareToIgnoreCase("hl7message") == 0) {
        hl7message = msg.asInstanceOf[com.ligadata.messagescontainers.System_HL7_1000001_1430446865995]
      } else if (curVal._2.compareToIgnoreCase("inpatient_claims") == 0) {
        inpatient_claims += msg.asInstanceOf[com.ligadata.messagescontainers.System_InpatientClaim_1000001_1430446866091]
      } else if (curVal._2.compareToIgnoreCase("outpatient_claims") == 0) {
        outpatient_claims += msg.asInstanceOf[com.ligadata.messagescontainers.System_OutpatientClaim_1000001_1430446866113]
      } else if (curVal._2.compareToIgnoreCase("hl7messages") == 0) {
        hl7messages += msg.asInstanceOf[com.ligadata.messagescontainers.System_HL7_1000001_1430446865995]
      }
    } else { // Yet to handle it. Make sure we add the message to one of the value given
      throw new Exception("Not yet handled messages more than one level")
    }
  }

  override def GetMessage(childPath: Array[(String, String)], primaryKey: Array[String]): com.ligadata.FatafatBase.BaseMsg = {
    if (childPath == null || childPath.size == 0 || primaryKey == null || primaryKey.size == 0) { // Invalid case
      return null
    }
    val curVal = childPath(0)
    if (childPath.size == 1) { // If this is last level
      if (curVal._2.compareToIgnoreCase("hl7message") == 0) {
        val pkd = hl7message.PrimaryKeyData
        if (pkd.sameElements(primaryKey)) {
          return hl7message
        }
      } else if (curVal._2.compareToIgnoreCase("inpatient_claims") == 0) {
        inpatient_claims.foreach(o => {
          if (o != null) {
            val pkd = o.PrimaryKeyData
            if (pkd.sameElements(primaryKey)) {
              return o
            }
          }
        })
      } else if (curVal._2.compareToIgnoreCase("outpatient_claims") == 0) {
        outpatient_claims.foreach(o => {
          if (o != null) {
            val pkd = o.PrimaryKeyData
            if (pkd.sameElements(primaryKey)) {
              return o
            }
          }
        })
      } else if (curVal._2.compareToIgnoreCase("hl7messages") == 0) {
        hl7messages.foreach(o => {
          if (o != null) {
            val pkd = o.PrimaryKeyData
            if (pkd.sameElements(primaryKey)) {
              return o
            }
          }
        })
      }
    } else { // Yet to handle it. Make sure we add the message to one of the value given
      throw new Exception("Not yet handled messages more than one level")
    }
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
      if (list.size < 32) throw new Exception("Incorrect input data size")
      desynpuf_id = com.ligadata.BaseTypes.StringImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      bene_birth_dt = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      bene_death_dt = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      bene_sex_ident_cd = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      bene_race_cd = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      bene_esrd_ind = com.ligadata.BaseTypes.CharImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      sp_state_code = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      bene_county_cd = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      bene_hi_cvrage_tot_mons = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      bene_smi_cvrage_tot_mons = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      bene_hmo_cvrage_tot_mons = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      plan_cvrg_mos_num = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      sp_alzhdmta = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      sp_chf = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      sp_chrnkidn = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      sp_cncr = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      sp_copd = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      sp_depressn = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      sp_diabetes = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      sp_ischmcht = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      sp_osteoprs = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      sp_ra_oa = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      sp_strketia = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      medreimb_ip = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      benres_ip = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      pppymt_ip = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      medreimb_op = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      benres_op = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      pppymt_op = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      medreimb_car = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      benres_car = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      pppymt_car = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      //Not Handling CSV Populate for member type Message
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

      desynpuf_id = com.ligadata.BaseTypes.StringImpl.Input(map.getOrElse("desynpuf_id", "").toString)
      bene_birth_dt = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("bene_birth_dt", 0).toString)
      bene_death_dt = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("bene_death_dt", 0).toString)
      bene_sex_ident_cd = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("bene_sex_ident_cd", 0).toString)
      bene_race_cd = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("bene_race_cd", 0).toString)
      bene_esrd_ind = com.ligadata.BaseTypes.CharImpl.Input(map.getOrElse("bene_esrd_ind", ' ').toString)
      sp_state_code = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("sp_state_code", 0).toString)
      bene_county_cd = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("bene_county_cd", 0).toString)
      bene_hi_cvrage_tot_mons = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("bene_hi_cvrage_tot_mons", 0).toString)
      bene_smi_cvrage_tot_mons = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("bene_smi_cvrage_tot_mons", 0).toString)
      bene_hmo_cvrage_tot_mons = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("bene_hmo_cvrage_tot_mons", 0).toString)
      plan_cvrg_mos_num = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("plan_cvrg_mos_num", 0).toString)
      sp_alzhdmta = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("sp_alzhdmta", 0).toString)
      sp_chf = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("sp_chf", 0).toString)
      sp_chrnkidn = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("sp_chrnkidn", 0).toString)
      sp_cncr = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("sp_cncr", 0).toString)
      sp_copd = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("sp_copd", 0).toString)
      sp_depressn = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("sp_depressn", 0).toString)
      sp_diabetes = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("sp_diabetes", 0).toString)
      sp_ischmcht = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("sp_ischmcht", 0).toString)
      sp_osteoprs = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("sp_osteoprs", 0).toString)
      sp_ra_oa = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("sp_ra_oa", 0).toString)
      sp_strketia = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("sp_strketia", 0).toString)
      medreimb_ip = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("medreimb_ip", 0.0).toString)
      benres_ip = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("benres_ip", 0.0).toString)
      pppymt_ip = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("pppymt_ip", 0.0).toString)
      medreimb_op = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("medreimb_op", 0.0).toString)
      benres_op = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("benres_op", 0.0).toString)
      pppymt_op = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("pppymt_op", 0.0).toString)
      medreimb_car = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("medreimb_car", 0.0).toString)
      benres_car = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("benres_car", 0.0).toString)
      pppymt_car = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("pppymt_car", 0.0).toString)
      //Not Handling JSON Populate for member type Message
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
      val _desynpuf_idval_ = (xml \\ "desynpuf_id").text.toString
      if (_desynpuf_idval_ != "") desynpuf_id = com.ligadata.BaseTypes.StringImpl.Input(_desynpuf_idval_) else desynpuf_id = ""
      val _bene_birth_dtval_ = (xml \\ "bene_birth_dt").text.toString
      if (_bene_birth_dtval_ != "") bene_birth_dt = com.ligadata.BaseTypes.IntImpl.Input(_bene_birth_dtval_) else bene_birth_dt = 0
      val _bene_death_dtval_ = (xml \\ "bene_death_dt").text.toString
      if (_bene_death_dtval_ != "") bene_death_dt = com.ligadata.BaseTypes.IntImpl.Input(_bene_death_dtval_) else bene_death_dt = 0
      val _bene_sex_ident_cdval_ = (xml \\ "bene_sex_ident_cd").text.toString
      if (_bene_sex_ident_cdval_ != "") bene_sex_ident_cd = com.ligadata.BaseTypes.IntImpl.Input(_bene_sex_ident_cdval_) else bene_sex_ident_cd = 0
      val _bene_race_cdval_ = (xml \\ "bene_race_cd").text.toString
      if (_bene_race_cdval_ != "") bene_race_cd = com.ligadata.BaseTypes.IntImpl.Input(_bene_race_cdval_) else bene_race_cd = 0
      val _bene_esrd_indval_ = (xml \\ "bene_esrd_ind").text.toString
      if (_bene_esrd_indval_ != "") bene_esrd_ind = com.ligadata.BaseTypes.CharImpl.Input(_bene_esrd_indval_) else bene_esrd_ind = ' '
      val _sp_state_codeval_ = (xml \\ "sp_state_code").text.toString
      if (_sp_state_codeval_ != "") sp_state_code = com.ligadata.BaseTypes.IntImpl.Input(_sp_state_codeval_) else sp_state_code = 0
      val _bene_county_cdval_ = (xml \\ "bene_county_cd").text.toString
      if (_bene_county_cdval_ != "") bene_county_cd = com.ligadata.BaseTypes.IntImpl.Input(_bene_county_cdval_) else bene_county_cd = 0
      val _bene_hi_cvrage_tot_monsval_ = (xml \\ "bene_hi_cvrage_tot_mons").text.toString
      if (_bene_hi_cvrage_tot_monsval_ != "") bene_hi_cvrage_tot_mons = com.ligadata.BaseTypes.IntImpl.Input(_bene_hi_cvrage_tot_monsval_) else bene_hi_cvrage_tot_mons = 0
      val _bene_smi_cvrage_tot_monsval_ = (xml \\ "bene_smi_cvrage_tot_mons").text.toString
      if (_bene_smi_cvrage_tot_monsval_ != "") bene_smi_cvrage_tot_mons = com.ligadata.BaseTypes.IntImpl.Input(_bene_smi_cvrage_tot_monsval_) else bene_smi_cvrage_tot_mons = 0
      val _bene_hmo_cvrage_tot_monsval_ = (xml \\ "bene_hmo_cvrage_tot_mons").text.toString
      if (_bene_hmo_cvrage_tot_monsval_ != "") bene_hmo_cvrage_tot_mons = com.ligadata.BaseTypes.IntImpl.Input(_bene_hmo_cvrage_tot_monsval_) else bene_hmo_cvrage_tot_mons = 0
      val _plan_cvrg_mos_numval_ = (xml \\ "plan_cvrg_mos_num").text.toString
      if (_plan_cvrg_mos_numval_ != "") plan_cvrg_mos_num = com.ligadata.BaseTypes.IntImpl.Input(_plan_cvrg_mos_numval_) else plan_cvrg_mos_num = 0
      val _sp_alzhdmtaval_ = (xml \\ "sp_alzhdmta").text.toString
      if (_sp_alzhdmtaval_ != "") sp_alzhdmta = com.ligadata.BaseTypes.IntImpl.Input(_sp_alzhdmtaval_) else sp_alzhdmta = 0
      val _sp_chfval_ = (xml \\ "sp_chf").text.toString
      if (_sp_chfval_ != "") sp_chf = com.ligadata.BaseTypes.IntImpl.Input(_sp_chfval_) else sp_chf = 0
      val _sp_chrnkidnval_ = (xml \\ "sp_chrnkidn").text.toString
      if (_sp_chrnkidnval_ != "") sp_chrnkidn = com.ligadata.BaseTypes.IntImpl.Input(_sp_chrnkidnval_) else sp_chrnkidn = 0
      val _sp_cncrval_ = (xml \\ "sp_cncr").text.toString
      if (_sp_cncrval_ != "") sp_cncr = com.ligadata.BaseTypes.IntImpl.Input(_sp_cncrval_) else sp_cncr = 0
      val _sp_copdval_ = (xml \\ "sp_copd").text.toString
      if (_sp_copdval_ != "") sp_copd = com.ligadata.BaseTypes.IntImpl.Input(_sp_copdval_) else sp_copd = 0
      val _sp_depressnval_ = (xml \\ "sp_depressn").text.toString
      if (_sp_depressnval_ != "") sp_depressn = com.ligadata.BaseTypes.IntImpl.Input(_sp_depressnval_) else sp_depressn = 0
      val _sp_diabetesval_ = (xml \\ "sp_diabetes").text.toString
      if (_sp_diabetesval_ != "") sp_diabetes = com.ligadata.BaseTypes.IntImpl.Input(_sp_diabetesval_) else sp_diabetes = 0
      val _sp_ischmchtval_ = (xml \\ "sp_ischmcht").text.toString
      if (_sp_ischmchtval_ != "") sp_ischmcht = com.ligadata.BaseTypes.IntImpl.Input(_sp_ischmchtval_) else sp_ischmcht = 0
      val _sp_osteoprsval_ = (xml \\ "sp_osteoprs").text.toString
      if (_sp_osteoprsval_ != "") sp_osteoprs = com.ligadata.BaseTypes.IntImpl.Input(_sp_osteoprsval_) else sp_osteoprs = 0
      val _sp_ra_oaval_ = (xml \\ "sp_ra_oa").text.toString
      if (_sp_ra_oaval_ != "") sp_ra_oa = com.ligadata.BaseTypes.IntImpl.Input(_sp_ra_oaval_) else sp_ra_oa = 0
      val _sp_strketiaval_ = (xml \\ "sp_strketia").text.toString
      if (_sp_strketiaval_ != "") sp_strketia = com.ligadata.BaseTypes.IntImpl.Input(_sp_strketiaval_) else sp_strketia = 0
      val _medreimb_ipval_ = (xml \\ "medreimb_ip").text.toString
      if (_medreimb_ipval_ != "") medreimb_ip = com.ligadata.BaseTypes.DoubleImpl.Input(_medreimb_ipval_) else medreimb_ip = 0.0
      val _benres_ipval_ = (xml \\ "benres_ip").text.toString
      if (_benres_ipval_ != "") benres_ip = com.ligadata.BaseTypes.DoubleImpl.Input(_benres_ipval_) else benres_ip = 0.0
      val _pppymt_ipval_ = (xml \\ "pppymt_ip").text.toString
      if (_pppymt_ipval_ != "") pppymt_ip = com.ligadata.BaseTypes.DoubleImpl.Input(_pppymt_ipval_) else pppymt_ip = 0.0
      val _medreimb_opval_ = (xml \\ "medreimb_op").text.toString
      if (_medreimb_opval_ != "") medreimb_op = com.ligadata.BaseTypes.DoubleImpl.Input(_medreimb_opval_) else medreimb_op = 0.0
      val _benres_opval_ = (xml \\ "benres_op").text.toString
      if (_benres_opval_ != "") benres_op = com.ligadata.BaseTypes.DoubleImpl.Input(_benres_opval_) else benres_op = 0.0
      val _pppymt_opval_ = (xml \\ "pppymt_op").text.toString
      if (_pppymt_opval_ != "") pppymt_op = com.ligadata.BaseTypes.DoubleImpl.Input(_pppymt_opval_) else pppymt_op = 0.0
      val _medreimb_carval_ = (xml \\ "medreimb_car").text.toString
      if (_medreimb_carval_ != "") medreimb_car = com.ligadata.BaseTypes.DoubleImpl.Input(_medreimb_carval_) else medreimb_car = 0.0
      val _benres_carval_ = (xml \\ "benres_car").text.toString
      if (_benres_carval_ != "") benres_car = com.ligadata.BaseTypes.DoubleImpl.Input(_benres_carval_) else benres_car = 0.0
      val _pppymt_carval_ = (xml \\ "pppymt_car").text.toString
      if (_pppymt_carval_ != "") pppymt_car = com.ligadata.BaseTypes.DoubleImpl.Input(_pppymt_carval_) else pppymt_car = 0.0
      //Not Handling XML Populate for member type Message
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }

  override def Serialize(dos: DataOutputStream): Unit = {
    try {
      com.ligadata.BaseTypes.StringImpl.SerializeIntoDataOutputStream(dos, desynpuf_id);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, bene_birth_dt);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, bene_death_dt);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, bene_sex_ident_cd);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, bene_race_cd);
      com.ligadata.BaseTypes.CharImpl.SerializeIntoDataOutputStream(dos, bene_esrd_ind);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, sp_state_code);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, bene_county_cd);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, bene_hi_cvrage_tot_mons);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, bene_smi_cvrage_tot_mons);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, bene_hmo_cvrage_tot_mons);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, plan_cvrg_mos_num);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, sp_alzhdmta);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, sp_chf);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, sp_chrnkidn);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, sp_cncr);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, sp_copd);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, sp_depressn);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, sp_diabetes);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, sp_ischmcht);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, sp_osteoprs);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, sp_ra_oa);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, sp_strketia);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, medreimb_ip);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, benres_ip);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, pppymt_ip);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, medreimb_op);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, benres_op);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, pppymt_op);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, medreimb_car);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, benres_car);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, pppymt_car);
      {
        if (hl7message == null) com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, 0);
        else {
          val bytes = SerializeDeserialize.Serialize(hl7message.asInstanceOf[com.ligadata.messagescontainers.System_HL7_1000001_1430446865995])
          com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, bytes.length);
          dos.write(bytes);
        }
      }
      if ((inpatient_claims == null) || (inpatient_claims.size == 0)) com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, 0);
      else {
        com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, inpatient_claims.size);
        inpatient_claims.foreach(obj => {
          val bytes = SerializeDeserialize.Serialize(obj)
          com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, bytes.length)
          dos.write(bytes)
        })
      }
      if ((outpatient_claims == null) || (outpatient_claims.size == 0)) com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, 0);
      else {
        com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, outpatient_claims.size);
        outpatient_claims.foreach(obj => {
          val bytes = SerializeDeserialize.Serialize(obj)
          com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, bytes.length)
          dos.write(bytes)
        })
      }
      if ((hl7messages == null) || (hl7messages.size == 0)) com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, 0);
      else {
        com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, hl7messages.size);
        hl7messages.foreach(obj => {
          val bytes = SerializeDeserialize.Serialize(obj)
          com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, bytes.length)
          dos.write(bytes)
        })
      }

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
        desynpuf_id = com.ligadata.BaseTypes.StringImpl.DeserializeFromDataInputStream(dis);
        bene_birth_dt = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        bene_death_dt = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        bene_sex_ident_cd = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        bene_race_cd = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        bene_esrd_ind = com.ligadata.BaseTypes.CharImpl.DeserializeFromDataInputStream(dis);
        sp_state_code = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        bene_county_cd = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        bene_hi_cvrage_tot_mons = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        bene_smi_cvrage_tot_mons = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        bene_hmo_cvrage_tot_mons = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        plan_cvrg_mos_num = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        sp_alzhdmta = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        sp_chf = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        sp_chrnkidn = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        sp_cncr = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        sp_copd = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        sp_depressn = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        sp_diabetes = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        sp_ischmcht = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        sp_osteoprs = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        sp_ra_oa = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        sp_strketia = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        medreimb_ip = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        benres_ip = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        pppymt_ip = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        medreimb_op = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        benres_op = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        pppymt_op = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        medreimb_car = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        benres_car = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        pppymt_car = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        {
          val length = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis)
          if (length > 0) {
            var bytes = new Array[Byte](length);
            dis.read(bytes);
            val inst = SerializeDeserialize.Deserialize(bytes, mdResolver, loader, false, "com.ligadata.messagescontainers.System_HL7_1000001_1430446865995");
            hl7message = inst.asInstanceOf[BaseMsg];
          }
        }
        {
          var arraySize = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
          val i: Int = 0;
          for (i <- 0 until arraySize) {
            var bytes = new Array[Byte](com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis))
            dis.read(bytes);
            val inst = SerializeDeserialize.Deserialize(bytes, mdResolver, loader, false, "com.ligadata.messagescontainers.System_InpatientClaim_1000001_1430446866091");
            inpatient_claims += inst.asInstanceOf[com.ligadata.messagescontainers.System_InpatientClaim_1000001_1430446866091];
          }

        }
        {
          var arraySize = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
          val i: Int = 0;
          for (i <- 0 until arraySize) {
            var bytes = new Array[Byte](com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis))
            dis.read(bytes);
            val inst = SerializeDeserialize.Deserialize(bytes, mdResolver, loader, false, "com.ligadata.messagescontainers.System_OutpatientClaim_1000001_1430446866113");
            outpatient_claims += inst.asInstanceOf[com.ligadata.messagescontainers.System_OutpatientClaim_1000001_1430446866113];
          }

        }
        {
          var arraySize = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
          val i: Int = 0;
          for (i <- 0 until arraySize) {
            var bytes = new Array[Byte](com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis))
            dis.read(bytes);
            val inst = SerializeDeserialize.Deserialize(bytes, mdResolver, loader, false, "com.ligadata.messagescontainers.System_HL7_1000001_1430446865995");
            hl7messages += inst.asInstanceOf[com.ligadata.messagescontainers.System_HL7_1000001_1430446865995];
          }

        }

      } else throw new Exception("Current Message/Container Version " + currentVer + " should be greater than Previous Message Version " + prevVer + ".")

    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def ConvertPrevToNewVerObj(obj: Any): Unit = {}

}