
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

object System_OutpatientClaim_1000001_1430446866113 extends BaseMsgObj {
  override def TransformDataAttributes: TransformMessage = null
  override def NeedToTransformData: Boolean = false
  override def FullName: String = "System.OutpatientClaim"
  override def NameSpace: String = "System"
  override def Name: String = "OutpatientClaim"
  override def Version: String = "000000.000001.000001"
  override def CreateNewMessage: BaseMsg = new System_OutpatientClaim_1000001_1430446866113()
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

  val primaryKeys: Array[String] = Array("desynpuf_id", "clm_id")
  val prmryKeyPos = Array(0, 1)

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

class System_OutpatientClaim_1000001_1430446866113 extends BaseMsg {
  override def IsFixed: Boolean = true;
  override def IsKv: Boolean = false;

  override def FullName: String = "System.OutpatientClaim"
  override def NameSpace: String = "System"
  override def Name: String = "OutpatientClaim"
  override def Version: String = "000000.000001.000001"
  override def CanPersist: Boolean = true;

  var desynpuf_id: String = _;
  var clm_id: Long = _;
  var segment: Int = _;
  var clm_from_dt: Int = _;
  var clm_thru_dt: Int = _;
  var prvdr_num: String = _;
  var clm_pmt_amt: Double = _;
  var nch_prmry_pyr_clm_pd_amt: Double = _;
  var at_physn_npi: Long = _;
  var op_physn_npi: Long = _;
  var ot_physn_npi: Long = _;
  var nch_bene_blood_ddctbl_lblty_am: Double = _;
  var icd9_dgns_cds: scala.Array[String] = _;
  var icd9_prcdr_cds: scala.Array[Int] = _;
  var nch_bene_ptb_ddctbl_amt: Double = _;
  var nch_bene_ptb_coinsrnc_amt: Double = _;
  var admtng_icd9_dgns_cd: String = _;
  var hcpcs_cds: scala.Array[Int] = _;

  override def PartitionKeyData: Array[String] = Array(desynpuf_id.toString)

  override def PrimaryKeyData: Array[String] = Array(desynpuf_id.toString, clm_id.toString)

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
      if (key.equals("clm_id")) return clm_id;
      if (key.equals("segment")) return segment;
      if (key.equals("clm_from_dt")) return clm_from_dt;
      if (key.equals("clm_thru_dt")) return clm_thru_dt;
      if (key.equals("prvdr_num")) return prvdr_num;
      if (key.equals("clm_pmt_amt")) return clm_pmt_amt;
      if (key.equals("nch_prmry_pyr_clm_pd_amt")) return nch_prmry_pyr_clm_pd_amt;
      if (key.equals("at_physn_npi")) return at_physn_npi;
      if (key.equals("op_physn_npi")) return op_physn_npi;
      if (key.equals("ot_physn_npi")) return ot_physn_npi;
      if (key.equals("nch_bene_blood_ddctbl_lblty_am")) return nch_bene_blood_ddctbl_lblty_am;
      if (key.equals("icd9_dgns_cds")) return icd9_dgns_cds;
      if (key.equals("icd9_prcdr_cds")) return icd9_prcdr_cds;
      if (key.equals("nch_bene_ptb_ddctbl_amt")) return nch_bene_ptb_ddctbl_amt;
      if (key.equals("nch_bene_ptb_coinsrnc_amt")) return nch_bene_ptb_coinsrnc_amt;
      if (key.equals("admtng_icd9_dgns_cd")) return admtng_icd9_dgns_cd;
      if (key.equals("hcpcs_cds")) return hcpcs_cds;

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
    val fieldX = ru.typeOf[System_OutpatientClaim_1000001_1430446866113].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
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
      if (list.size < 18) throw new Exception("Incorrect input data size")
      desynpuf_id = com.ligadata.BaseTypes.StringImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      clm_id = com.ligadata.BaseTypes.LongImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      segment = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      clm_from_dt = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      clm_thru_dt = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      prvdr_num = com.ligadata.BaseTypes.StringImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      clm_pmt_amt = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      nch_prmry_pyr_clm_pd_amt = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      at_physn_npi = com.ligadata.BaseTypes.LongImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      op_physn_npi = com.ligadata.BaseTypes.LongImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      ot_physn_npi = com.ligadata.BaseTypes.LongImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      nch_bene_blood_ddctbl_lblty_am = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      icd9_dgns_cds = list(inputdata.curPos).split(arrvaldelim, -1).map(v => com.ligadata.BaseTypes.StringImpl.Input(v));
      inputdata.curPos = inputdata.curPos + 1
      icd9_prcdr_cds = list(inputdata.curPos).split(arrvaldelim, -1).map(v => com.ligadata.BaseTypes.IntImpl.Input(v));
      inputdata.curPos = inputdata.curPos + 1
      nch_bene_ptb_ddctbl_amt = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      nch_bene_ptb_coinsrnc_amt = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      admtng_icd9_dgns_cd = com.ligadata.BaseTypes.StringImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      hcpcs_cds = list(inputdata.curPos).split(arrvaldelim, -1).map(v => com.ligadata.BaseTypes.IntImpl.Input(v));
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

      desynpuf_id = com.ligadata.BaseTypes.StringImpl.Input(map.getOrElse("desynpuf_id", "").toString)
      clm_id = com.ligadata.BaseTypes.LongImpl.Input(map.getOrElse("clm_id", 0).toString)
      segment = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("segment", 0).toString)
      clm_from_dt = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("clm_from_dt", 0).toString)
      clm_thru_dt = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("clm_thru_dt", 0).toString)
      prvdr_num = com.ligadata.BaseTypes.StringImpl.Input(map.getOrElse("prvdr_num", "").toString)
      clm_pmt_amt = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("clm_pmt_amt", 0.0).toString)
      nch_prmry_pyr_clm_pd_amt = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("nch_prmry_pyr_clm_pd_amt", 0.0).toString)
      at_physn_npi = com.ligadata.BaseTypes.LongImpl.Input(map.getOrElse("at_physn_npi", 0).toString)
      op_physn_npi = com.ligadata.BaseTypes.LongImpl.Input(map.getOrElse("op_physn_npi", 0).toString)
      ot_physn_npi = com.ligadata.BaseTypes.LongImpl.Input(map.getOrElse("ot_physn_npi", 0).toString)
      nch_bene_blood_ddctbl_lblty_am = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("nch_bene_blood_ddctbl_lblty_am", 0.0).toString)

      if (map.contains("icd9_dgns_cds")) {
        val arr = map.getOrElse("icd9_dgns_cds", null)
        if (arr != null) {
          val arrFld = CollectionAsArrString(arr)
          icd9_dgns_cds = arrFld.map(v => com.ligadata.BaseTypes.StringImpl.Input(v.toString)).toArray
        } else icd9_dgns_cds = new scala.Array[String](0)
      }

      if (map.contains("icd9_prcdr_cds")) {
        val arr = map.getOrElse("icd9_prcdr_cds", null)
        if (arr != null) {
          val arrFld = CollectionAsArrString(arr)
          icd9_prcdr_cds = arrFld.map(v => com.ligadata.BaseTypes.IntImpl.Input(v.toString)).toArray
        } else icd9_prcdr_cds = new scala.Array[Int](0)
      }
      nch_bene_ptb_ddctbl_amt = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("nch_bene_ptb_ddctbl_amt", 0.0).toString)
      nch_bene_ptb_coinsrnc_amt = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("nch_bene_ptb_coinsrnc_amt", 0.0).toString)
      admtng_icd9_dgns_cd = com.ligadata.BaseTypes.StringImpl.Input(map.getOrElse("admtng_icd9_dgns_cd", "").toString)

      if (map.contains("hcpcs_cds")) {
        val arr = map.getOrElse("hcpcs_cds", null)
        if (arr != null) {
          val arrFld = CollectionAsArrString(arr)
          hcpcs_cds = arrFld.map(v => com.ligadata.BaseTypes.IntImpl.Input(v.toString)).toArray
        } else hcpcs_cds = new scala.Array[Int](0)
      }

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
      val _clm_idval_ = (xml \\ "clm_id").text.toString
      if (_clm_idval_ != "") clm_id = com.ligadata.BaseTypes.LongImpl.Input(_clm_idval_) else clm_id = 0
      val _segmentval_ = (xml \\ "segment").text.toString
      if (_segmentval_ != "") segment = com.ligadata.BaseTypes.IntImpl.Input(_segmentval_) else segment = 0
      val _clm_from_dtval_ = (xml \\ "clm_from_dt").text.toString
      if (_clm_from_dtval_ != "") clm_from_dt = com.ligadata.BaseTypes.IntImpl.Input(_clm_from_dtval_) else clm_from_dt = 0
      val _clm_thru_dtval_ = (xml \\ "clm_thru_dt").text.toString
      if (_clm_thru_dtval_ != "") clm_thru_dt = com.ligadata.BaseTypes.IntImpl.Input(_clm_thru_dtval_) else clm_thru_dt = 0
      val _prvdr_numval_ = (xml \\ "prvdr_num").text.toString
      if (_prvdr_numval_ != "") prvdr_num = com.ligadata.BaseTypes.StringImpl.Input(_prvdr_numval_) else prvdr_num = ""
      val _clm_pmt_amtval_ = (xml \\ "clm_pmt_amt").text.toString
      if (_clm_pmt_amtval_ != "") clm_pmt_amt = com.ligadata.BaseTypes.DoubleImpl.Input(_clm_pmt_amtval_) else clm_pmt_amt = 0.0
      val _nch_prmry_pyr_clm_pd_amtval_ = (xml \\ "nch_prmry_pyr_clm_pd_amt").text.toString
      if (_nch_prmry_pyr_clm_pd_amtval_ != "") nch_prmry_pyr_clm_pd_amt = com.ligadata.BaseTypes.DoubleImpl.Input(_nch_prmry_pyr_clm_pd_amtval_) else nch_prmry_pyr_clm_pd_amt = 0.0
      val _at_physn_npival_ = (xml \\ "at_physn_npi").text.toString
      if (_at_physn_npival_ != "") at_physn_npi = com.ligadata.BaseTypes.LongImpl.Input(_at_physn_npival_) else at_physn_npi = 0
      val _op_physn_npival_ = (xml \\ "op_physn_npi").text.toString
      if (_op_physn_npival_ != "") op_physn_npi = com.ligadata.BaseTypes.LongImpl.Input(_op_physn_npival_) else op_physn_npi = 0
      val _ot_physn_npival_ = (xml \\ "ot_physn_npi").text.toString
      if (_ot_physn_npival_ != "") ot_physn_npi = com.ligadata.BaseTypes.LongImpl.Input(_ot_physn_npival_) else ot_physn_npi = 0
      val _nch_bene_blood_ddctbl_lblty_amval_ = (xml \\ "nch_bene_blood_ddctbl_lblty_am").text.toString
      if (_nch_bene_blood_ddctbl_lblty_amval_ != "") nch_bene_blood_ddctbl_lblty_am = com.ligadata.BaseTypes.DoubleImpl.Input(_nch_bene_blood_ddctbl_lblty_amval_) else nch_bene_blood_ddctbl_lblty_am = 0.0
      val _nch_bene_ptb_ddctbl_amtval_ = (xml \\ "nch_bene_ptb_ddctbl_amt").text.toString
      if (_nch_bene_ptb_ddctbl_amtval_ != "") nch_bene_ptb_ddctbl_amt = com.ligadata.BaseTypes.DoubleImpl.Input(_nch_bene_ptb_ddctbl_amtval_) else nch_bene_ptb_ddctbl_amt = 0.0
      val _nch_bene_ptb_coinsrnc_amtval_ = (xml \\ "nch_bene_ptb_coinsrnc_amt").text.toString
      if (_nch_bene_ptb_coinsrnc_amtval_ != "") nch_bene_ptb_coinsrnc_amt = com.ligadata.BaseTypes.DoubleImpl.Input(_nch_bene_ptb_coinsrnc_amtval_) else nch_bene_ptb_coinsrnc_amt = 0.0
      val _admtng_icd9_dgns_cdval_ = (xml \\ "admtng_icd9_dgns_cd").text.toString
      if (_admtng_icd9_dgns_cdval_ != "") admtng_icd9_dgns_cd = com.ligadata.BaseTypes.StringImpl.Input(_admtng_icd9_dgns_cdval_) else admtng_icd9_dgns_cd = ""

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
      com.ligadata.BaseTypes.LongImpl.SerializeIntoDataOutputStream(dos, clm_id);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, segment);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, clm_from_dt);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, clm_thru_dt);
      com.ligadata.BaseTypes.StringImpl.SerializeIntoDataOutputStream(dos, prvdr_num);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, clm_pmt_amt);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, nch_prmry_pyr_clm_pd_amt);
      com.ligadata.BaseTypes.LongImpl.SerializeIntoDataOutputStream(dos, at_physn_npi);
      com.ligadata.BaseTypes.LongImpl.SerializeIntoDataOutputStream(dos, op_physn_npi);
      com.ligadata.BaseTypes.LongImpl.SerializeIntoDataOutputStream(dos, ot_physn_npi);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, nch_bene_blood_ddctbl_lblty_am);
      if (icd9_dgns_cds == null || icd9_dgns_cds.size == 0) com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, 0);
      else {
        com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, icd9_dgns_cds.size);
        icd9_dgns_cds.foreach(v => {
          com.ligadata.BaseTypes.StringImpl.SerializeIntoDataOutputStream(dos, v);
        })
      }
      if (icd9_prcdr_cds == null || icd9_prcdr_cds.size == 0) com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, 0);
      else {
        com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, icd9_prcdr_cds.size);
        icd9_prcdr_cds.foreach(v => {
          com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, v);
        })
      }
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, nch_bene_ptb_ddctbl_amt);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, nch_bene_ptb_coinsrnc_amt);
      com.ligadata.BaseTypes.StringImpl.SerializeIntoDataOutputStream(dos, admtng_icd9_dgns_cd);
      if (hcpcs_cds == null || hcpcs_cds.size == 0) com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, 0);
      else {
        com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, hcpcs_cds.size);
        hcpcs_cds.foreach(v => {
          com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, v);
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
        clm_id = com.ligadata.BaseTypes.LongImpl.DeserializeFromDataInputStream(dis);
        segment = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        clm_from_dt = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        clm_thru_dt = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        prvdr_num = com.ligadata.BaseTypes.StringImpl.DeserializeFromDataInputStream(dis);
        clm_pmt_amt = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        nch_prmry_pyr_clm_pd_amt = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        at_physn_npi = com.ligadata.BaseTypes.LongImpl.DeserializeFromDataInputStream(dis);
        op_physn_npi = com.ligadata.BaseTypes.LongImpl.DeserializeFromDataInputStream(dis);
        ot_physn_npi = com.ligadata.BaseTypes.LongImpl.DeserializeFromDataInputStream(dis);
        nch_bene_blood_ddctbl_lblty_am = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        {
          var arraySize = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
          val i: Int = 0;
          if (arraySize > 0) {
            icd9_dgns_cds = new scala.Array[String](arraySize)
            for (i <- 0 until arraySize) {
              val inst = com.ligadata.BaseTypes.StringImpl.DeserializeFromDataInputStream(dis);
              icd9_dgns_cds(i) = inst;
            }

          }
        }
        {
          var arraySize = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
          val i: Int = 0;
          if (arraySize > 0) {
            icd9_prcdr_cds = new scala.Array[Int](arraySize)
            for (i <- 0 until arraySize) {
              val inst = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
              icd9_prcdr_cds(i) = inst;
            }

          }
        }
        nch_bene_ptb_ddctbl_amt = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        nch_bene_ptb_coinsrnc_amt = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        admtng_icd9_dgns_cd = com.ligadata.BaseTypes.StringImpl.DeserializeFromDataInputStream(dis);
        {
          var arraySize = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
          val i: Int = 0;
          if (arraySize > 0) {
            hcpcs_cds = new scala.Array[Int](arraySize)
            for (i <- 0 until arraySize) {
              val inst = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
              hcpcs_cds(i) = inst;
            }

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