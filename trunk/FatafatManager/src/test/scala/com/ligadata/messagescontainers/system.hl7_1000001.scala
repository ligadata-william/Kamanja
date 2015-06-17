
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

object System_HL7_1000001_1430446865995 extends BaseMsgObj {
  override def TransformDataAttributes: TransformMessage = null
  override def NeedToTransformData: Boolean = false
  override def FullName: String = "System.HL7"
  override def NameSpace: String = "System"
  override def Name: String = "HL7"
  override def Version: String = "000000.000001.000001"
  override def CreateNewMessage: BaseMsg = new System_HL7_1000001_1430446865995()
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

class System_HL7_1000001_1430446865995 extends BaseMsg {
  override def IsFixed: Boolean = true;
  override def IsKv: Boolean = false;

  override def FullName: String = "System.HL7"
  override def NameSpace: String = "System"
  override def Name: String = "HL7"
  override def Version: String = "000000.000001.000001"
    override def CanPersist: Boolean = true;

  var desynpuf_id: String = _;
  var clm_id: Long = _;
  var clm_from_dt: Int = _;
  var clm_thru_dt: Int = _;
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
  var age: Int = _;
  var infectious_parasitic_diseases: Int = _;
  var neoplasms: Int = _;
  var endocrine_nutritional_metabolic_diseases_immunity_disorders: Int = _;
  var diseases_blood_blood_forming_organs: Int = _;
  var mental_disorders: Int = _;
  var diseases_nervous_system_sense_organs: Int = _;
  var diseases_circulatory_system: Int = _;
  var diseases_respiratory_system: Int = _;
  var diseases_digestive_system: Int = _;
  var diseases_genitourinary_system: Int = _;
  var complications_of_pregnancy_childbirth_the_puerperium: Int = _;
  var diseases_skin_subcutaneous_tissue: Int = _;
  var diseases_musculoskeletal_system_connective_tissue: Int = _;
  var congenital_anomalies: Int = _;
  var certain_conditions_originating_in_the_perinatal_period: Int = _;
  var symptoms_signs_ill_defined_conditions: Int = _;
  var injury_poisoning: Int = _;
  var factors_influencing_health_status_contact_with_health_services: Int = _;
  var external_causes_of_injury_poisoning: Int = _;
  var hypothyroidism: Int = _;
  var infarction: Int = _;
  var alzheimer: Int = _;
  var alzheimer_related: Int = _;
  var anemia: Int = _;
  var asthma: Int = _;
  var atrial_fibrillation: Int = _;
  var hyperplasia: Int = _;
  var cataract: Int = _;
  var kidney_disease: Int = _;
  var pulmonary_disease: Int = _;
  var depression: Int = _;
  var diabetes: Int = _;
  var glaucoma: Int = _;
  var heart_failure: Int = _;
  var hip_pelvic_fracture: Int = _;
  var hyperlipidemia: Int = _;
  var hypertension: Int = _;
  var ischemic_heart_disease: Int = _;
  var osteoporosis: Int = _;
  var ra_oa: Int = _;
  var stroke: Int = _;
  var breast_cancer: Int = _;
  var colorectal_cancer: Int = _;
  var prostate_cancer: Int = _;
  var lung_cancer: Int = _;
  var endometrial_cancer: Int = _;
  var tobacco: Int = _;
  var height: Double = _;
  var weight: Double = _;
  var systolic: Double = _;
  var diastolic: Double = _;
  var totalcholesterol: Double = _;
  var ldl: Double = _;
  var triglycerides: Double = _;
  var shortnessofbreath: Int = _;
  var chestpain: String = _;
  var aatdeficiency: Int = _;

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
      if (key.equals("clm_id")) return clm_id;
      if (key.equals("clm_from_dt")) return clm_from_dt;
      if (key.equals("clm_thru_dt")) return clm_thru_dt;
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
      if (key.equals("age")) return age;
      if (key.equals("infectious_parasitic_diseases")) return infectious_parasitic_diseases;
      if (key.equals("neoplasms")) return neoplasms;
      if (key.equals("endocrine_nutritional_metabolic_diseases_immunity_disorders")) return endocrine_nutritional_metabolic_diseases_immunity_disorders;
      if (key.equals("diseases_blood_blood_forming_organs")) return diseases_blood_blood_forming_organs;
      if (key.equals("mental_disorders")) return mental_disorders;
      if (key.equals("diseases_nervous_system_sense_organs")) return diseases_nervous_system_sense_organs;
      if (key.equals("diseases_circulatory_system")) return diseases_circulatory_system;
      if (key.equals("diseases_respiratory_system")) return diseases_respiratory_system;
      if (key.equals("diseases_digestive_system")) return diseases_digestive_system;
      if (key.equals("diseases_genitourinary_system")) return diseases_genitourinary_system;
      if (key.equals("complications_of_pregnancy_childbirth_the_puerperium")) return complications_of_pregnancy_childbirth_the_puerperium;
      if (key.equals("diseases_skin_subcutaneous_tissue")) return diseases_skin_subcutaneous_tissue;
      if (key.equals("diseases_musculoskeletal_system_connective_tissue")) return diseases_musculoskeletal_system_connective_tissue;
      if (key.equals("congenital_anomalies")) return congenital_anomalies;
      if (key.equals("certain_conditions_originating_in_the_perinatal_period")) return certain_conditions_originating_in_the_perinatal_period;
      if (key.equals("symptoms_signs_ill_defined_conditions")) return symptoms_signs_ill_defined_conditions;
      if (key.equals("injury_poisoning")) return injury_poisoning;
      if (key.equals("factors_influencing_health_status_contact_with_health_services")) return factors_influencing_health_status_contact_with_health_services;
      if (key.equals("external_causes_of_injury_poisoning")) return external_causes_of_injury_poisoning;
      if (key.equals("hypothyroidism")) return hypothyroidism;
      if (key.equals("infarction")) return infarction;
      if (key.equals("alzheimer")) return alzheimer;
      if (key.equals("alzheimer_related")) return alzheimer_related;
      if (key.equals("anemia")) return anemia;
      if (key.equals("asthma")) return asthma;
      if (key.equals("atrial_fibrillation")) return atrial_fibrillation;
      if (key.equals("hyperplasia")) return hyperplasia;
      if (key.equals("cataract")) return cataract;
      if (key.equals("kidney_disease")) return kidney_disease;
      if (key.equals("pulmonary_disease")) return pulmonary_disease;
      if (key.equals("depression")) return depression;
      if (key.equals("diabetes")) return diabetes;
      if (key.equals("glaucoma")) return glaucoma;
      if (key.equals("heart_failure")) return heart_failure;
      if (key.equals("hip_pelvic_fracture")) return hip_pelvic_fracture;
      if (key.equals("hyperlipidemia")) return hyperlipidemia;
      if (key.equals("hypertension")) return hypertension;
      if (key.equals("ischemic_heart_disease")) return ischemic_heart_disease;
      if (key.equals("osteoporosis")) return osteoporosis;
      if (key.equals("ra_oa")) return ra_oa;
      if (key.equals("stroke")) return stroke;
      if (key.equals("breast_cancer")) return breast_cancer;
      if (key.equals("colorectal_cancer")) return colorectal_cancer;
      if (key.equals("prostate_cancer")) return prostate_cancer;
      if (key.equals("lung_cancer")) return lung_cancer;
      if (key.equals("endometrial_cancer")) return endometrial_cancer;
      if (key.equals("tobacco")) return tobacco;
      if (key.equals("height")) return height;
      if (key.equals("weight")) return weight;
      if (key.equals("systolic")) return systolic;
      if (key.equals("diastolic")) return diastolic;
      if (key.equals("totalcholesterol")) return totalcholesterol;
      if (key.equals("ldl")) return ldl;
      if (key.equals("triglycerides")) return triglycerides;
      if (key.equals("shortnessofbreath")) return shortnessofbreath;
      if (key.equals("chestpain")) return chestpain;
      if (key.equals("aatdeficiency")) return aatdeficiency;

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
    val fieldX = ru.typeOf[System_HL7_1000001_1430446865995].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
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
      if (list.size < 84) throw new Exception("Incorrect input data size")
      desynpuf_id = com.ligadata.BaseTypes.StringImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      clm_id = com.ligadata.BaseTypes.LongImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      clm_from_dt = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      clm_thru_dt = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
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
      age = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      infectious_parasitic_diseases = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      neoplasms = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      endocrine_nutritional_metabolic_diseases_immunity_disorders = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      diseases_blood_blood_forming_organs = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      mental_disorders = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      diseases_nervous_system_sense_organs = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      diseases_circulatory_system = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      diseases_respiratory_system = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      diseases_digestive_system = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      diseases_genitourinary_system = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      complications_of_pregnancy_childbirth_the_puerperium = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      diseases_skin_subcutaneous_tissue = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      diseases_musculoskeletal_system_connective_tissue = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      congenital_anomalies = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      certain_conditions_originating_in_the_perinatal_period = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      symptoms_signs_ill_defined_conditions = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      injury_poisoning = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      factors_influencing_health_status_contact_with_health_services = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      external_causes_of_injury_poisoning = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      hypothyroidism = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      infarction = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      alzheimer = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      alzheimer_related = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      anemia = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      asthma = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      atrial_fibrillation = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      hyperplasia = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      cataract = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      kidney_disease = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      pulmonary_disease = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      depression = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      diabetes = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      glaucoma = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      heart_failure = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      hip_pelvic_fracture = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      hyperlipidemia = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      hypertension = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      ischemic_heart_disease = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      osteoporosis = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      ra_oa = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      stroke = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      breast_cancer = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      colorectal_cancer = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      prostate_cancer = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      lung_cancer = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      endometrial_cancer = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      tobacco = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      height = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      weight = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      systolic = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      diastolic = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      totalcholesterol = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      ldl = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      triglycerides = com.ligadata.BaseTypes.DoubleImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      shortnessofbreath = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      chestpain = com.ligadata.BaseTypes.StringImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos + 1
      aatdeficiency = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
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
      clm_from_dt = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("clm_from_dt", 0).toString)
      clm_thru_dt = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("clm_thru_dt", 0).toString)
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
      age = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("age", 0).toString)
      infectious_parasitic_diseases = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("infectious_parasitic_diseases", 0).toString)
      neoplasms = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("neoplasms", 0).toString)
      endocrine_nutritional_metabolic_diseases_immunity_disorders = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("endocrine_nutritional_metabolic_diseases_immunity_disorders", 0).toString)
      diseases_blood_blood_forming_organs = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("diseases_blood_blood_forming_organs", 0).toString)
      mental_disorders = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("mental_disorders", 0).toString)
      diseases_nervous_system_sense_organs = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("diseases_nervous_system_sense_organs", 0).toString)
      diseases_circulatory_system = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("diseases_circulatory_system", 0).toString)
      diseases_respiratory_system = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("diseases_respiratory_system", 0).toString)
      diseases_digestive_system = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("diseases_digestive_system", 0).toString)
      diseases_genitourinary_system = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("diseases_genitourinary_system", 0).toString)
      complications_of_pregnancy_childbirth_the_puerperium = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("complications_of_pregnancy_childbirth_the_puerperium", 0).toString)
      diseases_skin_subcutaneous_tissue = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("diseases_skin_subcutaneous_tissue", 0).toString)
      diseases_musculoskeletal_system_connective_tissue = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("diseases_musculoskeletal_system_connective_tissue", 0).toString)
      congenital_anomalies = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("congenital_anomalies", 0).toString)
      certain_conditions_originating_in_the_perinatal_period = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("certain_conditions_originating_in_the_perinatal_period", 0).toString)
      symptoms_signs_ill_defined_conditions = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("symptoms_signs_ill_defined_conditions", 0).toString)
      injury_poisoning = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("injury_poisoning", 0).toString)
      factors_influencing_health_status_contact_with_health_services = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("factors_influencing_health_status_contact_with_health_services", 0).toString)
      external_causes_of_injury_poisoning = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("external_causes_of_injury_poisoning", 0).toString)
      hypothyroidism = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("hypothyroidism", 0).toString)
      infarction = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("infarction", 0).toString)
      alzheimer = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("alzheimer", 0).toString)
      alzheimer_related = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("alzheimer_related", 0).toString)
      anemia = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("anemia", 0).toString)
      asthma = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("asthma", 0).toString)
      atrial_fibrillation = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("atrial_fibrillation", 0).toString)
      hyperplasia = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("hyperplasia", 0).toString)
      cataract = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("cataract", 0).toString)
      kidney_disease = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("kidney_disease", 0).toString)
      pulmonary_disease = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("pulmonary_disease", 0).toString)
      depression = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("depression", 0).toString)
      diabetes = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("diabetes", 0).toString)
      glaucoma = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("glaucoma", 0).toString)
      heart_failure = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("heart_failure", 0).toString)
      hip_pelvic_fracture = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("hip_pelvic_fracture", 0).toString)
      hyperlipidemia = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("hyperlipidemia", 0).toString)
      hypertension = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("hypertension", 0).toString)
      ischemic_heart_disease = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("ischemic_heart_disease", 0).toString)
      osteoporosis = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("osteoporosis", 0).toString)
      ra_oa = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("ra_oa", 0).toString)
      stroke = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("stroke", 0).toString)
      breast_cancer = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("breast_cancer", 0).toString)
      colorectal_cancer = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("colorectal_cancer", 0).toString)
      prostate_cancer = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("prostate_cancer", 0).toString)
      lung_cancer = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("lung_cancer", 0).toString)
      endometrial_cancer = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("endometrial_cancer", 0).toString)
      tobacco = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("tobacco", 0).toString)
      height = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("height", 0.0).toString)
      weight = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("weight", 0.0).toString)
      systolic = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("systolic", 0.0).toString)
      diastolic = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("diastolic", 0.0).toString)
      totalcholesterol = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("totalcholesterol", 0.0).toString)
      ldl = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("ldl", 0.0).toString)
      triglycerides = com.ligadata.BaseTypes.DoubleImpl.Input(map.getOrElse("triglycerides", 0.0).toString)
      shortnessofbreath = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("shortnessofbreath", 0).toString)
      chestpain = com.ligadata.BaseTypes.StringImpl.Input(map.getOrElse("chestpain", "").toString)
      aatdeficiency = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("aatdeficiency", 0).toString)

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
      val _clm_from_dtval_ = (xml \\ "clm_from_dt").text.toString
      if (_clm_from_dtval_ != "") clm_from_dt = com.ligadata.BaseTypes.IntImpl.Input(_clm_from_dtval_) else clm_from_dt = 0
      val _clm_thru_dtval_ = (xml \\ "clm_thru_dt").text.toString
      if (_clm_thru_dtval_ != "") clm_thru_dt = com.ligadata.BaseTypes.IntImpl.Input(_clm_thru_dtval_) else clm_thru_dt = 0
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
      val _ageval_ = (xml \\ "age").text.toString
      if (_ageval_ != "") age = com.ligadata.BaseTypes.IntImpl.Input(_ageval_) else age = 0
      val _infectious_parasitic_diseasesval_ = (xml \\ "infectious_parasitic_diseases").text.toString
      if (_infectious_parasitic_diseasesval_ != "") infectious_parasitic_diseases = com.ligadata.BaseTypes.IntImpl.Input(_infectious_parasitic_diseasesval_) else infectious_parasitic_diseases = 0
      val _neoplasmsval_ = (xml \\ "neoplasms").text.toString
      if (_neoplasmsval_ != "") neoplasms = com.ligadata.BaseTypes.IntImpl.Input(_neoplasmsval_) else neoplasms = 0
      val _endocrine_nutritional_metabolic_diseases_immunity_disordersval_ = (xml \\ "endocrine_nutritional_metabolic_diseases_immunity_disorders").text.toString
      if (_endocrine_nutritional_metabolic_diseases_immunity_disordersval_ != "") endocrine_nutritional_metabolic_diseases_immunity_disorders = com.ligadata.BaseTypes.IntImpl.Input(_endocrine_nutritional_metabolic_diseases_immunity_disordersval_) else endocrine_nutritional_metabolic_diseases_immunity_disorders = 0
      val _diseases_blood_blood_forming_organsval_ = (xml \\ "diseases_blood_blood_forming_organs").text.toString
      if (_diseases_blood_blood_forming_organsval_ != "") diseases_blood_blood_forming_organs = com.ligadata.BaseTypes.IntImpl.Input(_diseases_blood_blood_forming_organsval_) else diseases_blood_blood_forming_organs = 0
      val _mental_disordersval_ = (xml \\ "mental_disorders").text.toString
      if (_mental_disordersval_ != "") mental_disorders = com.ligadata.BaseTypes.IntImpl.Input(_mental_disordersval_) else mental_disorders = 0
      val _diseases_nervous_system_sense_organsval_ = (xml \\ "diseases_nervous_system_sense_organs").text.toString
      if (_diseases_nervous_system_sense_organsval_ != "") diseases_nervous_system_sense_organs = com.ligadata.BaseTypes.IntImpl.Input(_diseases_nervous_system_sense_organsval_) else diseases_nervous_system_sense_organs = 0
      val _diseases_circulatory_systemval_ = (xml \\ "diseases_circulatory_system").text.toString
      if (_diseases_circulatory_systemval_ != "") diseases_circulatory_system = com.ligadata.BaseTypes.IntImpl.Input(_diseases_circulatory_systemval_) else diseases_circulatory_system = 0
      val _diseases_respiratory_systemval_ = (xml \\ "diseases_respiratory_system").text.toString
      if (_diseases_respiratory_systemval_ != "") diseases_respiratory_system = com.ligadata.BaseTypes.IntImpl.Input(_diseases_respiratory_systemval_) else diseases_respiratory_system = 0
      val _diseases_digestive_systemval_ = (xml \\ "diseases_digestive_system").text.toString
      if (_diseases_digestive_systemval_ != "") diseases_digestive_system = com.ligadata.BaseTypes.IntImpl.Input(_diseases_digestive_systemval_) else diseases_digestive_system = 0
      val _diseases_genitourinary_systemval_ = (xml \\ "diseases_genitourinary_system").text.toString
      if (_diseases_genitourinary_systemval_ != "") diseases_genitourinary_system = com.ligadata.BaseTypes.IntImpl.Input(_diseases_genitourinary_systemval_) else diseases_genitourinary_system = 0
      val _complications_of_pregnancy_childbirth_the_puerperiumval_ = (xml \\ "complications_of_pregnancy_childbirth_the_puerperium").text.toString
      if (_complications_of_pregnancy_childbirth_the_puerperiumval_ != "") complications_of_pregnancy_childbirth_the_puerperium = com.ligadata.BaseTypes.IntImpl.Input(_complications_of_pregnancy_childbirth_the_puerperiumval_) else complications_of_pregnancy_childbirth_the_puerperium = 0
      val _diseases_skin_subcutaneous_tissueval_ = (xml \\ "diseases_skin_subcutaneous_tissue").text.toString
      if (_diseases_skin_subcutaneous_tissueval_ != "") diseases_skin_subcutaneous_tissue = com.ligadata.BaseTypes.IntImpl.Input(_diseases_skin_subcutaneous_tissueval_) else diseases_skin_subcutaneous_tissue = 0
      val _diseases_musculoskeletal_system_connective_tissueval_ = (xml \\ "diseases_musculoskeletal_system_connective_tissue").text.toString
      if (_diseases_musculoskeletal_system_connective_tissueval_ != "") diseases_musculoskeletal_system_connective_tissue = com.ligadata.BaseTypes.IntImpl.Input(_diseases_musculoskeletal_system_connective_tissueval_) else diseases_musculoskeletal_system_connective_tissue = 0
      val _congenital_anomaliesval_ = (xml \\ "congenital_anomalies").text.toString
      if (_congenital_anomaliesval_ != "") congenital_anomalies = com.ligadata.BaseTypes.IntImpl.Input(_congenital_anomaliesval_) else congenital_anomalies = 0
      val _certain_conditions_originating_in_the_perinatal_periodval_ = (xml \\ "certain_conditions_originating_in_the_perinatal_period").text.toString
      if (_certain_conditions_originating_in_the_perinatal_periodval_ != "") certain_conditions_originating_in_the_perinatal_period = com.ligadata.BaseTypes.IntImpl.Input(_certain_conditions_originating_in_the_perinatal_periodval_) else certain_conditions_originating_in_the_perinatal_period = 0
      val _symptoms_signs_ill_defined_conditionsval_ = (xml \\ "symptoms_signs_ill_defined_conditions").text.toString
      if (_symptoms_signs_ill_defined_conditionsval_ != "") symptoms_signs_ill_defined_conditions = com.ligadata.BaseTypes.IntImpl.Input(_symptoms_signs_ill_defined_conditionsval_) else symptoms_signs_ill_defined_conditions = 0
      val _injury_poisoningval_ = (xml \\ "injury_poisoning").text.toString
      if (_injury_poisoningval_ != "") injury_poisoning = com.ligadata.BaseTypes.IntImpl.Input(_injury_poisoningval_) else injury_poisoning = 0
      val _factors_influencing_health_status_contact_with_health_servicesval_ = (xml \\ "factors_influencing_health_status_contact_with_health_services").text.toString
      if (_factors_influencing_health_status_contact_with_health_servicesval_ != "") factors_influencing_health_status_contact_with_health_services = com.ligadata.BaseTypes.IntImpl.Input(_factors_influencing_health_status_contact_with_health_servicesval_) else factors_influencing_health_status_contact_with_health_services = 0
      val _external_causes_of_injury_poisoningval_ = (xml \\ "external_causes_of_injury_poisoning").text.toString
      if (_external_causes_of_injury_poisoningval_ != "") external_causes_of_injury_poisoning = com.ligadata.BaseTypes.IntImpl.Input(_external_causes_of_injury_poisoningval_) else external_causes_of_injury_poisoning = 0
      val _hypothyroidismval_ = (xml \\ "hypothyroidism").text.toString
      if (_hypothyroidismval_ != "") hypothyroidism = com.ligadata.BaseTypes.IntImpl.Input(_hypothyroidismval_) else hypothyroidism = 0
      val _infarctionval_ = (xml \\ "infarction").text.toString
      if (_infarctionval_ != "") infarction = com.ligadata.BaseTypes.IntImpl.Input(_infarctionval_) else infarction = 0
      val _alzheimerval_ = (xml \\ "alzheimer").text.toString
      if (_alzheimerval_ != "") alzheimer = com.ligadata.BaseTypes.IntImpl.Input(_alzheimerval_) else alzheimer = 0
      val _alzheimer_relatedval_ = (xml \\ "alzheimer_related").text.toString
      if (_alzheimer_relatedval_ != "") alzheimer_related = com.ligadata.BaseTypes.IntImpl.Input(_alzheimer_relatedval_) else alzheimer_related = 0
      val _anemiaval_ = (xml \\ "anemia").text.toString
      if (_anemiaval_ != "") anemia = com.ligadata.BaseTypes.IntImpl.Input(_anemiaval_) else anemia = 0
      val _asthmaval_ = (xml \\ "asthma").text.toString
      if (_asthmaval_ != "") asthma = com.ligadata.BaseTypes.IntImpl.Input(_asthmaval_) else asthma = 0
      val _atrial_fibrillationval_ = (xml \\ "atrial_fibrillation").text.toString
      if (_atrial_fibrillationval_ != "") atrial_fibrillation = com.ligadata.BaseTypes.IntImpl.Input(_atrial_fibrillationval_) else atrial_fibrillation = 0
      val _hyperplasiaval_ = (xml \\ "hyperplasia").text.toString
      if (_hyperplasiaval_ != "") hyperplasia = com.ligadata.BaseTypes.IntImpl.Input(_hyperplasiaval_) else hyperplasia = 0
      val _cataractval_ = (xml \\ "cataract").text.toString
      if (_cataractval_ != "") cataract = com.ligadata.BaseTypes.IntImpl.Input(_cataractval_) else cataract = 0
      val _kidney_diseaseval_ = (xml \\ "kidney_disease").text.toString
      if (_kidney_diseaseval_ != "") kidney_disease = com.ligadata.BaseTypes.IntImpl.Input(_kidney_diseaseval_) else kidney_disease = 0
      val _pulmonary_diseaseval_ = (xml \\ "pulmonary_disease").text.toString
      if (_pulmonary_diseaseval_ != "") pulmonary_disease = com.ligadata.BaseTypes.IntImpl.Input(_pulmonary_diseaseval_) else pulmonary_disease = 0
      val _depressionval_ = (xml \\ "depression").text.toString
      if (_depressionval_ != "") depression = com.ligadata.BaseTypes.IntImpl.Input(_depressionval_) else depression = 0
      val _diabetesval_ = (xml \\ "diabetes").text.toString
      if (_diabetesval_ != "") diabetes = com.ligadata.BaseTypes.IntImpl.Input(_diabetesval_) else diabetes = 0
      val _glaucomaval_ = (xml \\ "glaucoma").text.toString
      if (_glaucomaval_ != "") glaucoma = com.ligadata.BaseTypes.IntImpl.Input(_glaucomaval_) else glaucoma = 0
      val _heart_failureval_ = (xml \\ "heart_failure").text.toString
      if (_heart_failureval_ != "") heart_failure = com.ligadata.BaseTypes.IntImpl.Input(_heart_failureval_) else heart_failure = 0
      val _hip_pelvic_fractureval_ = (xml \\ "hip_pelvic_fracture").text.toString
      if (_hip_pelvic_fractureval_ != "") hip_pelvic_fracture = com.ligadata.BaseTypes.IntImpl.Input(_hip_pelvic_fractureval_) else hip_pelvic_fracture = 0
      val _hyperlipidemiaval_ = (xml \\ "hyperlipidemia").text.toString
      if (_hyperlipidemiaval_ != "") hyperlipidemia = com.ligadata.BaseTypes.IntImpl.Input(_hyperlipidemiaval_) else hyperlipidemia = 0
      val _hypertensionval_ = (xml \\ "hypertension").text.toString
      if (_hypertensionval_ != "") hypertension = com.ligadata.BaseTypes.IntImpl.Input(_hypertensionval_) else hypertension = 0
      val _ischemic_heart_diseaseval_ = (xml \\ "ischemic_heart_disease").text.toString
      if (_ischemic_heart_diseaseval_ != "") ischemic_heart_disease = com.ligadata.BaseTypes.IntImpl.Input(_ischemic_heart_diseaseval_) else ischemic_heart_disease = 0
      val _osteoporosisval_ = (xml \\ "osteoporosis").text.toString
      if (_osteoporosisval_ != "") osteoporosis = com.ligadata.BaseTypes.IntImpl.Input(_osteoporosisval_) else osteoporosis = 0
      val _ra_oaval_ = (xml \\ "ra_oa").text.toString
      if (_ra_oaval_ != "") ra_oa = com.ligadata.BaseTypes.IntImpl.Input(_ra_oaval_) else ra_oa = 0
      val _strokeval_ = (xml \\ "stroke").text.toString
      if (_strokeval_ != "") stroke = com.ligadata.BaseTypes.IntImpl.Input(_strokeval_) else stroke = 0
      val _breast_cancerval_ = (xml \\ "breast_cancer").text.toString
      if (_breast_cancerval_ != "") breast_cancer = com.ligadata.BaseTypes.IntImpl.Input(_breast_cancerval_) else breast_cancer = 0
      val _colorectal_cancerval_ = (xml \\ "colorectal_cancer").text.toString
      if (_colorectal_cancerval_ != "") colorectal_cancer = com.ligadata.BaseTypes.IntImpl.Input(_colorectal_cancerval_) else colorectal_cancer = 0
      val _prostate_cancerval_ = (xml \\ "prostate_cancer").text.toString
      if (_prostate_cancerval_ != "") prostate_cancer = com.ligadata.BaseTypes.IntImpl.Input(_prostate_cancerval_) else prostate_cancer = 0
      val _lung_cancerval_ = (xml \\ "lung_cancer").text.toString
      if (_lung_cancerval_ != "") lung_cancer = com.ligadata.BaseTypes.IntImpl.Input(_lung_cancerval_) else lung_cancer = 0
      val _endometrial_cancerval_ = (xml \\ "endometrial_cancer").text.toString
      if (_endometrial_cancerval_ != "") endometrial_cancer = com.ligadata.BaseTypes.IntImpl.Input(_endometrial_cancerval_) else endometrial_cancer = 0
      val _tobaccoval_ = (xml \\ "tobacco").text.toString
      if (_tobaccoval_ != "") tobacco = com.ligadata.BaseTypes.IntImpl.Input(_tobaccoval_) else tobacco = 0
      val _heightval_ = (xml \\ "height").text.toString
      if (_heightval_ != "") height = com.ligadata.BaseTypes.DoubleImpl.Input(_heightval_) else height = 0.0
      val _weightval_ = (xml \\ "weight").text.toString
      if (_weightval_ != "") weight = com.ligadata.BaseTypes.DoubleImpl.Input(_weightval_) else weight = 0.0
      val _systolicval_ = (xml \\ "systolic").text.toString
      if (_systolicval_ != "") systolic = com.ligadata.BaseTypes.DoubleImpl.Input(_systolicval_) else systolic = 0.0
      val _diastolicval_ = (xml \\ "diastolic").text.toString
      if (_diastolicval_ != "") diastolic = com.ligadata.BaseTypes.DoubleImpl.Input(_diastolicval_) else diastolic = 0.0
      val _totalcholesterolval_ = (xml \\ "totalcholesterol").text.toString
      if (_totalcholesterolval_ != "") totalcholesterol = com.ligadata.BaseTypes.DoubleImpl.Input(_totalcholesterolval_) else totalcholesterol = 0.0
      val _ldlval_ = (xml \\ "ldl").text.toString
      if (_ldlval_ != "") ldl = com.ligadata.BaseTypes.DoubleImpl.Input(_ldlval_) else ldl = 0.0
      val _triglyceridesval_ = (xml \\ "triglycerides").text.toString
      if (_triglyceridesval_ != "") triglycerides = com.ligadata.BaseTypes.DoubleImpl.Input(_triglyceridesval_) else triglycerides = 0.0
      val _shortnessofbreathval_ = (xml \\ "shortnessofbreath").text.toString
      if (_shortnessofbreathval_ != "") shortnessofbreath = com.ligadata.BaseTypes.IntImpl.Input(_shortnessofbreathval_) else shortnessofbreath = 0
      val _chestpainval_ = (xml \\ "chestpain").text.toString
      if (_chestpainval_ != "") chestpain = com.ligadata.BaseTypes.StringImpl.Input(_chestpainval_) else chestpain = ""
      val _aatdeficiencyval_ = (xml \\ "aatdeficiency").text.toString
      if (_aatdeficiencyval_ != "") aatdeficiency = com.ligadata.BaseTypes.IntImpl.Input(_aatdeficiencyval_) else aatdeficiency = 0

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
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, clm_from_dt);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, clm_thru_dt);
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
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, age);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, infectious_parasitic_diseases);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, neoplasms);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, endocrine_nutritional_metabolic_diseases_immunity_disorders);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, diseases_blood_blood_forming_organs);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, mental_disorders);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, diseases_nervous_system_sense_organs);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, diseases_circulatory_system);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, diseases_respiratory_system);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, diseases_digestive_system);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, diseases_genitourinary_system);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, complications_of_pregnancy_childbirth_the_puerperium);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, diseases_skin_subcutaneous_tissue);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, diseases_musculoskeletal_system_connective_tissue);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, congenital_anomalies);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, certain_conditions_originating_in_the_perinatal_period);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, symptoms_signs_ill_defined_conditions);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, injury_poisoning);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, factors_influencing_health_status_contact_with_health_services);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, external_causes_of_injury_poisoning);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, hypothyroidism);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, infarction);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, alzheimer);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, alzheimer_related);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, anemia);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, asthma);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, atrial_fibrillation);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, hyperplasia);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, cataract);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, kidney_disease);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, pulmonary_disease);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, depression);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, diabetes);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, glaucoma);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, heart_failure);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, hip_pelvic_fracture);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, hyperlipidemia);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, hypertension);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, ischemic_heart_disease);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, osteoporosis);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, ra_oa);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, stroke);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, breast_cancer);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, colorectal_cancer);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, prostate_cancer);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, lung_cancer);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, endometrial_cancer);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, tobacco);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, height);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, weight);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, systolic);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, diastolic);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, totalcholesterol);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, ldl);
      com.ligadata.BaseTypes.DoubleImpl.SerializeIntoDataOutputStream(dos, triglycerides);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, shortnessofbreath);
      com.ligadata.BaseTypes.StringImpl.SerializeIntoDataOutputStream(dos, chestpain);
      com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, aatdeficiency);

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
        clm_from_dt = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        clm_thru_dt = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
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
        age = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        infectious_parasitic_diseases = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        neoplasms = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        endocrine_nutritional_metabolic_diseases_immunity_disorders = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        diseases_blood_blood_forming_organs = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        mental_disorders = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        diseases_nervous_system_sense_organs = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        diseases_circulatory_system = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        diseases_respiratory_system = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        diseases_digestive_system = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        diseases_genitourinary_system = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        complications_of_pregnancy_childbirth_the_puerperium = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        diseases_skin_subcutaneous_tissue = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        diseases_musculoskeletal_system_connective_tissue = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        congenital_anomalies = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        certain_conditions_originating_in_the_perinatal_period = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        symptoms_signs_ill_defined_conditions = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        injury_poisoning = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        factors_influencing_health_status_contact_with_health_services = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        external_causes_of_injury_poisoning = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        hypothyroidism = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        infarction = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        alzheimer = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        alzheimer_related = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        anemia = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        asthma = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        atrial_fibrillation = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        hyperplasia = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        cataract = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        kidney_disease = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        pulmonary_disease = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        depression = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        diabetes = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        glaucoma = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        heart_failure = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        hip_pelvic_fracture = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        hyperlipidemia = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        hypertension = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        ischemic_heart_disease = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        osteoporosis = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        ra_oa = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        stroke = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        breast_cancer = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        colorectal_cancer = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        prostate_cancer = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        lung_cancer = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        endometrial_cancer = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        tobacco = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        height = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        weight = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        systolic = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        diastolic = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        totalcholesterol = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        ldl = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        triglycerides = com.ligadata.BaseTypes.DoubleImpl.DeserializeFromDataInputStream(dis);
        shortnessofbreath = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
        chestpain = com.ligadata.BaseTypes.StringImpl.DeserializeFromDataInputStream(dis);
        aatdeficiency = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);

      } else throw new Exception("Current Message/Container Version " + currentVer + " should be greater than Previous Message Version " + prevVer + ".")

    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def ConvertPrevToNewVerObj(obj: Any): Unit = {}

}