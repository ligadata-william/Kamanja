package com.ligadata.edifecs

import scala.collection.mutable._
import com.ligadata.OnLEPBase.{ InputData, DelimitedData }
import com.ligadata.OnLEPBase.{ BaseMsg, BaseContainer, BaseMsgObj, TransformMessage }
import com.ligadata.olep.metadata._
import com.ligadata.BaseTypes._

class System_HL7_100 extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "System.HL7"
  def getVersion: String = "00.01.00"
  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter, -1)
    assignCsv(list, 0)
  }

  def assignCsv(list: Array[String], startIdx: Int): Int = {
    var idx = startIdx
    try {
      if (list.size < 86)
        throw new Exception("Incorrect input data size")

      Desynpuf_Id = StringImpl.Input(list(idx).trim)
      idx = idx + 1
      Clm_Id = LongImpl.Input(list(idx));
      idx = idx + 1
      Clm_From_Dt = IntImpl.Input(list(idx))
      idx = idx + 1
      Clm_Thru_Dt = IntImpl.Input(list(idx))
      idx = idx + 1
      Bene_Birth_Dt = IntImpl.Input(list(idx))
      idx = idx + 1
      Bene_Death_Dt = IntImpl.Input(list(idx))
      idx = idx + 1
      Bene_Sex_Ident_Cd = IntImpl.Input(list(idx))
      idx = idx + 1
      Bene_Race_Cd = IntImpl.Input(list(idx))
      idx = idx + 1
      Bene_Esrd_Ind = CharImpl.Input(list(idx));
      idx = idx + 1
      Sp_State_Code = IntImpl.Input(list(idx))
      idx = idx + 1
      Bene_County_Cd = IntImpl.Input(list(idx))
      idx = idx + 1
      Bene_Hi_Cvrage_Tot_Mons = IntImpl.Input(list(idx))
      idx = idx + 1
      Bene_Smi_Cvrage_Tot_Mons = IntImpl.Input(list(idx))
      idx = idx + 1
      Bene_Hmo_Cvrage_Tot_Mons = IntImpl.Input(list(idx))
      idx = idx + 1
      Plan_Cvrg_Mos_Num = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Alzhdmta = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Chf = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Chrnkidn = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Cncr = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Copd = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Depressn = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Diabetes = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Ischmcht = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Osteoprs = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Ra_Oa = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Strketia = IntImpl.Input(list(idx))
      idx = idx + 1
      Age = IntImpl.Input(list(idx))
      idx = idx + 1
      Infectious_Parasitic_Diseases = IntImpl.Input(list(idx))
      idx = idx + 1
      Neoplasms = IntImpl.Input(list(idx))
      idx = idx + 1
      Endocrine_Nutritional_Metabolic_Diseases_Immunity_Disorders = IntImpl.Input(list(idx))
      idx = idx + 1
      Diseases_Blood_Blood_Forming_Organs = IntImpl.Input(list(idx))
      idx = idx + 1
      Mental_Disorders = IntImpl.Input(list(idx))
      idx = idx + 1
      Diseases_Nervous_System_Sense_Organs = IntImpl.Input(list(idx))
      idx = idx + 1
      Diseases_Circulatory_System = IntImpl.Input(list(idx))
      idx = idx + 1
      Diseases_Respiratory_System = IntImpl.Input(list(idx))
      idx = idx + 1
      Diseases_Digestive_System = IntImpl.Input(list(idx))
      idx = idx + 1
      Diseases_Genitourinary_System = IntImpl.Input(list(idx))
      idx = idx + 1
      Complications_Of_Pregnancy_Childbirth_The_Puerperium = IntImpl.Input(list(idx))
      idx = idx + 1
      Diseases_Skin_Subcutaneous_Tissue = IntImpl.Input(list(idx))
      idx = idx + 1
      Diseases_Musculoskeletal_System_Connective_Tissue = IntImpl.Input(list(idx))
      idx = idx + 1
      Congenital_Anomalies = IntImpl.Input(list(idx))
      idx = idx + 1
      Certain_Conditions_Originating_In_The_Perinatal_Period = IntImpl.Input(list(idx))
      idx = idx + 1
      Symptoms_Signs_Ill_Defined_Conditions = IntImpl.Input(list(idx))
      idx = idx + 1
      Injury_Poisoning = IntImpl.Input(list(idx))
      idx = idx + 1
      Factors_Influencing_Health_Status_Contact_With_Health_Services = IntImpl.Input(list(idx))
      idx = idx + 1
      External_Causes_Of_Injury_Poisoning = IntImpl.Input(list(idx))
      idx = idx + 1
      Hypothyroidism = IntImpl.Input(list(idx))
      idx = idx + 1
      Infarction = IntImpl.Input(list(idx))
      idx = idx + 1
      Alzheimer = IntImpl.Input(list(idx))
      idx = idx + 1
      Alzheimer_Related = IntImpl.Input(list(idx))
      idx = idx + 1
      Anemia = IntImpl.Input(list(idx))
      idx = idx + 1
      Asthma = IntImpl.Input(list(idx))
      idx = idx + 1
      Atrial_Fibrillation = IntImpl.Input(list(idx))
      idx = idx + 1
      Hyperplasia = IntImpl.Input(list(idx))
      idx = idx + 1
      Cataract = IntImpl.Input(list(idx))
      idx = idx + 1
      Kidney_Disease = IntImpl.Input(list(idx))
      idx = idx + 1
      Pulmonary_Disease = IntImpl.Input(list(idx))
      idx = idx + 1
      Depression = IntImpl.Input(list(idx))
      idx = idx + 1
      Diabetes = IntImpl.Input(list(idx))
      idx = idx + 1
      Glaucoma = IntImpl.Input(list(idx))
      idx = idx + 1
      Heart_Failure = IntImpl.Input(list(idx))
      idx = idx + 1
      Hip_Pelvic_Fracture = IntImpl.Input(list(idx))
      idx = idx + 1
      Hyperlipidemia = IntImpl.Input(list(idx))
      idx = idx + 1
      Hypertension = IntImpl.Input(list(idx))
      idx = idx + 1
      Ischemic_Heart_Disease = IntImpl.Input(list(idx))
      idx = idx + 1
      Osteoporosis = IntImpl.Input(list(idx))
      idx = idx + 1
      Ra_Oa = IntImpl.Input(list(idx))
      idx = idx + 1
      Stroke = IntImpl.Input(list(idx))
      idx = idx + 1
      Breast_Cancer = IntImpl.Input(list(idx))
      idx = idx + 1
      Colorectal_Cancer = IntImpl.Input(list(idx))
      idx = idx + 1
      Prostate_Cancer = IntImpl.Input(list(idx))
      idx = idx + 1
      Lung_Cancer = IntImpl.Input(list(idx))
      idx = idx + 1
      Endometrial_Cancer = IntImpl.Input(list(idx))
      idx = idx + 1
      Tobacco = IntImpl.Input(list(idx))
      idx = idx + 1
      Height = DoubleImpl.Input(list(idx));
      idx = idx + 1
      Weight = DoubleImpl.Input(list(idx));
      idx = idx + 1
      Systolic = DoubleImpl.Input(list(idx));
      idx = idx + 1
      Diastolic = DoubleImpl.Input(list(idx));
      idx = idx + 1
      Totalcholesterol = DoubleImpl.Input(list(idx));
      idx = idx + 1
      Ldl = DoubleImpl.Input(list(idx));
      idx = idx + 1
      Triglycerides = DoubleImpl.Input(list(idx));
      idx = idx + 1
      Shortnessofbreath = IntImpl.Input(list(idx))
      idx = idx + 1
      Chestpain = StringImpl.Input(list(idx).trim)
      idx = idx + 1
      AATDeficiency = IntImpl.Input(list(idx))
      idx = idx + 1
      ChronicCough = IntImpl.Input(list(idx))
      idx = idx + 1
      ChronicSputum = IntImpl.Input(list(idx))
      idx = idx + 1

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    idx
  }
  
  override def getKeyData: String = Desynpuf_Id

  var Desynpuf_Id: String = _
  var Clm_Id: Long = _
  var Clm_From_Dt: Int = _
  var Clm_Thru_Dt: Int = _
  var Bene_Birth_Dt: Int = _
  var Bene_Death_Dt: Int = _
  var Bene_Sex_Ident_Cd: Int = _
  var Bene_Race_Cd: Int = _
  var Bene_Esrd_Ind: Char = _
  var Sp_State_Code: Int = _
  var Bene_County_Cd: Int = _
  var Bene_Hi_Cvrage_Tot_Mons: Int = _
  var Bene_Smi_Cvrage_Tot_Mons: Int = _
  var Bene_Hmo_Cvrage_Tot_Mons: Int = _
  var Plan_Cvrg_Mos_Num: Int = _
  var Sp_Alzhdmta: Int = _
  var Sp_Chf: Int = _
  var Sp_Chrnkidn: Int = _
  var Sp_Cncr: Int = _
  var Sp_Copd: Int = _
  var Sp_Depressn: Int = _
  var Sp_Diabetes: Int = _
  var Sp_Ischmcht: Int = _
  var Sp_Osteoprs: Int = _
  var Sp_Ra_Oa: Int = _
  var Sp_Strketia: Int = _
  var Age: Int = _
  var Infectious_Parasitic_Diseases: Int = _
  var Neoplasms: Int = _
  var Endocrine_Nutritional_Metabolic_Diseases_Immunity_Disorders: Int = _
  var Diseases_Blood_Blood_Forming_Organs: Int = _
  var Mental_Disorders: Int = _
  var Diseases_Nervous_System_Sense_Organs: Int = _
  var Diseases_Circulatory_System: Int = _
  var Diseases_Respiratory_System: Int = _
  var Diseases_Digestive_System: Int = _
  var Diseases_Genitourinary_System: Int = _
  var Complications_Of_Pregnancy_Childbirth_The_Puerperium: Int = _
  var Diseases_Skin_Subcutaneous_Tissue: Int = _
  var Diseases_Musculoskeletal_System_Connective_Tissue: Int = _
  var Congenital_Anomalies: Int = _
  var Certain_Conditions_Originating_In_The_Perinatal_Period: Int = _
  var Symptoms_Signs_Ill_Defined_Conditions: Int = _
  var Injury_Poisoning: Int = _
  var Factors_Influencing_Health_Status_Contact_With_Health_Services: Int = _
  var External_Causes_Of_Injury_Poisoning: Int = _
  var Hypothyroidism: Int = _
  var Infarction: Int = _
  var Alzheimer: Int = _
  var Alzheimer_Related: Int = _
  var Anemia: Int = _
  var Asthma: Int = _
  var Atrial_Fibrillation: Int = _
  var Hyperplasia: Int = _
  var Cataract: Int = _
  var Kidney_Disease: Int = _
  var Pulmonary_Disease: Int = _
  var Depression: Int = _
  var Diabetes: Int = _
  var Glaucoma: Int = _
  var Heart_Failure: Int = _
  var Hip_Pelvic_Fracture: Int = _
  var Hyperlipidemia: Int = _
  var Hypertension: Int = _
  var Ischemic_Heart_Disease: Int = _
  var Osteoporosis: Int = _
  var Ra_Oa: Int = _
  var Stroke: Int = _
  var Breast_Cancer: Int = _
  var Colorectal_Cancer: Int = _
  var Prostate_Cancer: Int = _
  var Lung_Cancer: Int = _
  var Endometrial_Cancer: Int = _
  var Tobacco: Int = _
  var Height: Double = _
  var Weight: Double = _
  var Systolic: Double = _
  var Diastolic: Double = _
  var Totalcholesterol: Double = _
  var Ldl: Double = _
  var Triglycerides: Double = _
  var Shortnessofbreath: Int = _
  var Chestpain: String = _

  /**
   * we leave these here even though we are rewriting these determinations in terms of icd9 codes
   *  These codes used in the less flexible version of the COPD.xml where names hardcoded instead of looked up in tables
   */
  var AATDeficiency: Int = _
  var ChronicCough: Int = _
  var ChronicSputum: Int = _

}

object System_HL7_100 extends BaseMsgObj {
  def TransformDataAttributes: TransformMessage = null
  def NeedToTransformData: Boolean = false
  def getMessageName: String = "System.HL7"
  def getVersion: String = "00.01.00"
  def CreateNewMessage: BaseMsg = new System_HL7_100()
  def IsFixed: Boolean = true;
  def IsKv: Boolean = false;
}

class System_InpatientClaim_100 extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "System.InpatientClaim"
  def getVersion: String = "00.01.00"
  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter, -1)
    assignCsv(list, 0)
  }

  def assignCsv(list: Array[String], startIdx: Int): Int = {
    val arrvaldelim = "~"
    var idx = startIdx
    try {
      if (list.size < 23)
        throw new Exception("Incorrect input data size")

      Desynpuf_Id = StringImpl.Input(list(idx).trim)
      idx = idx + 1
      Clm_Id = LongImpl.Input(list(idx));
      idx = idx + 1
      Segment = IntImpl.Input(list(idx))
      idx = idx + 1
      Clm_From_Dt = IntImpl.Input(list(idx))
      idx = idx + 1
      Clm_Thru_Dt = IntImpl.Input(list(idx))
      idx = idx + 1
      Prvdr_Num = StringImpl.Input(list(idx))
      idx = idx + 1
      Clm_Pmt_Amt = FloatImpl.Input(list(idx));
      idx = idx + 1
      Nch_Prmry_Pyr_Clm_Pd_Amt = FloatImpl.Input(list(idx));
      idx = idx + 1
      At_Physn_Npi = LongImpl.Input(list(idx));
      idx = idx + 1
      Op_Physn_Npi = LongImpl.Input(list(idx));
      idx = idx + 1
      Ot_Physn_Npi = LongImpl.Input(list(idx));
      idx = idx + 1
      Clm_Admsn_Dt = IntImpl.Input(list(idx))
      idx = idx + 1
      Admtng_Icd9_Dgns_Cd = StringImpl.Input(list(idx).trim)
      idx = idx + 1
      Clm_Pass_Thru_Per_Diem_Amt = FloatImpl.Input(list(idx));
      idx = idx + 1
      Nch_Bene_Ip_Ddctbl_Amt = FloatImpl.Input(list(idx));
      idx = idx + 1
      Nch_Bene_Pta_Coinsrnc_Lblty_Am = FloatImpl.Input(list(idx));
      idx = idx + 1
      Nch_Bene_Blood_Ddctbl_Lblty_Am = FloatImpl.Input(list(idx));
      idx = idx + 1
      Clm_Utlztn_Day_Cnt = IntImpl.Input(list(idx))
      idx = idx + 1
      Nch_Bene_Dschrg_Dt = IntImpl.Input(list(idx))
      idx = idx + 1
      Clm_Drg_Cd = IntImpl.Input(list(idx))
      idx = idx + 1
      /**
       * FIXME: BUGBUG: For now we are expecting ~ as separater between array values
       */
      Icd9_Dgns_Cds = list(idx).split(arrvaldelim, -1).map(v => StringImpl.Input(v))
      idx = idx + 1
      Icd9_Prcdr_Cds = list(idx).split(arrvaldelim, -1).map(v => IntImpl.Input(v))
      idx = idx + 1
      Hcpcs_Cds = list(idx).split(arrvaldelim, -1).map(v => IntImpl.Input(v))
      idx = idx + 1

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    idx
  }

  override def getKeyData: String = Desynpuf_Id

  var Desynpuf_Id: String = _
  var Clm_Id: Long = _
  var Segment: Int = _
  var Clm_From_Dt: Int = _
  var Clm_Thru_Dt: Int = _
  var Prvdr_Num: String = _
  var Clm_Pmt_Amt: Float = _
  var Nch_Prmry_Pyr_Clm_Pd_Amt: Float = _
  var At_Physn_Npi: Long = _
  var Op_Physn_Npi: Long = _
  var Ot_Physn_Npi: Long = _
  var Clm_Admsn_Dt: Int = _
  var Admtng_Icd9_Dgns_Cd: String = _
  var Clm_Pass_Thru_Per_Diem_Amt: Float = _
  var Nch_Bene_Ip_Ddctbl_Amt: Float = _
  var Nch_Bene_Pta_Coinsrnc_Lblty_Am: Float = _
  var Nch_Bene_Blood_Ddctbl_Lblty_Am: Float = _
  var Clm_Utlztn_Day_Cnt: Int = _
  var Nch_Bene_Dschrg_Dt: Int = _
  var Clm_Drg_Cd: Int = _
  var Icd9_Dgns_Cds: Array[String] = _
  var Icd9_Prcdr_Cds: Array[Int] = _
  var Hcpcs_Cds: Array[Int] = _
}

object System_InpatientClaim_100 extends BaseMsgObj {
  def TransformDataAttributes: TransformMessage = null
  def NeedToTransformData: Boolean = false
  def getMessageName: String = "System.InpatientClaim"
  def getVersion: String = "00.01.00"
  def CreateNewMessage: BaseMsg = new System_InpatientClaim_100()
  def IsFixed: Boolean = true;
  def IsKv: Boolean = false;
}

class System_OutpatientClaim_100 extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "System.OutpatientClaim"
  def getVersion: String = "00.01.00"
  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter, -1)
    assignCsv(list, 0)
  }

  def assignCsv(list: Array[String], startIdx: Int): Int = {
    val arrvaldelim = "~"
    var idx = startIdx
    try {
      if (list.size < 18)
        throw new Exception("Incorrect input data size")

      Desynpuf_Id = StringImpl.Input(list(idx).trim)
      idx = idx + 1
      Clm_Id = LongImpl.Input(list(idx));
      idx = idx + 1
      Segment = IntImpl.Input(list(idx))
      idx = idx + 1
      Clm_From_Dt = IntImpl.Input(list(idx))
      idx = idx + 1
      Clm_Thru_Dt = IntImpl.Input(list(idx))
      idx = idx + 1
      Prvdr_Num = StringImpl.Input(list(idx))
      idx = idx + 1
      Clm_Pmt_Amt = DoubleImpl.Input(list(idx))
      idx = idx + 1
      Nch_Prmry_Pyr_Clm_Pd_Amt = FloatImpl.Input(list(idx))
      idx = idx + 1
      At_Physn_Npi = LongImpl.Input(list(idx));
      idx = idx + 1
      Op_Physn_Npi = LongImpl.Input(list(idx));
      idx = idx + 1
      Ot_Physn_Npi = LongImpl.Input(list(idx));
      idx = idx + 1
      Nch_Bene_Blood_Ddctbl_Lblty_Am = FloatImpl.Input(list(idx))
      idx = idx + 1

      /**
       * FIXME: BUGBUG: For now we are expecting ~ as separater between array values
       */
      Icd9_Dgns_Cds = list(idx).split(arrvaldelim, -1).map(v => StringImpl.Input(v))
      idx = idx + 1
      Icd9_Prcdr_Cds = list(idx).split(arrvaldelim, -1).map(v => IntImpl.Input(v))
      idx = idx + 1
      Nch_Bene_Ptb_Ddctbl_Amt = FloatImpl.Input(list(idx))
      idx = idx + 1
      Nch_Bene_Ptb_Coinsrnc_Amt = FloatImpl.Input(list(idx))
      idx = idx + 1
      Admtng_Icd9_Dgns_Cd = StringImpl.Input(list(idx).trim)
      idx = idx + 1
      Hcpcs_Cds = list(idx).split(arrvaldelim, -1).map(v => IntImpl.Input(v))
      idx = idx + 1

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    idx
  }

  override def getKeyData: String = Desynpuf_Id

  var Desynpuf_Id: String = _
  var Clm_Id: Long = _
  var Segment: Int = _
  var Clm_From_Dt: Int = _
  var Clm_Thru_Dt: Int = _
  var Prvdr_Num: String= _
  var Clm_Pmt_Amt: Double = _
  var Nch_Prmry_Pyr_Clm_Pd_Amt: Float = _
  var At_Physn_Npi: Long = _
  var Op_Physn_Npi: Long = _
  var Ot_Physn_Npi: Long = _
  var Nch_Bene_Blood_Ddctbl_Lblty_Am: Float = _
  var Icd9_Dgns_Cds: Array[String] = _
  var Icd9_Prcdr_Cds: Array[Int] = _
  var Nch_Bene_Ptb_Ddctbl_Amt: Float = _
  var Nch_Bene_Ptb_Coinsrnc_Amt: Float = _
  var Admtng_Icd9_Dgns_Cd: String = _
  var Hcpcs_Cds: Array[Int] = _
}

object System_OutpatientClaim_100 extends BaseMsgObj {
  def TransformDataAttributes: TransformMessage = null
  def NeedToTransformData: Boolean = false
  def getMessageName: String = "System.OutpatientClaim"
  def getVersion: String = "00.01.00"
  def CreateNewMessage: BaseMsg = new System_OutpatientClaim_100()
  def IsFixed: Boolean = true;
  def IsKv: Boolean = false;
}

class System_Beneficiary_100 extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "System.Beneficiary"
  def getVersion: String = "00.01.00"
  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter, -1)
    assignCsv(list, 0)
  }

  def assignCsv(list: Array[String], startIdx: Int): Int = {
    var idx = startIdx
    try {
      if (list.size < 2)
        throw new Exception("Incorrect input data size")

      Desynpuf_Id = StringImpl.Input(list(idx).trim)
      idx = idx + 1
      Bene_Birth_Dt = IntImpl.Input(list(idx))
      idx = idx + 1
      Bene_Death_Dt = IntImpl.Input(list(idx))
      idx = idx + 1
      Bene_Sex_Ident_Cd = IntImpl.Input(list(idx))
      idx = idx + 1
      Bene_Race_Cd = IntImpl.Input(list(idx))
      idx = idx + 1
      Bene_Esrd_Ind = CharImpl.Input(list(idx))
      idx = idx + 1
      Sp_State_Code = IntImpl.Input(list(idx))
      idx = idx + 1
      Bene_County_Cd = IntImpl.Input(list(idx))
      idx = idx + 1
      Bene_Hi_Cvrage_Tot_Mons = IntImpl.Input(list(idx))
      idx = idx + 1
      Bene_Smi_Cvrage_Tot_Mons = IntImpl.Input(list(idx))
      idx = idx + 1
      Bene_Hmo_Cvrage_Tot_Mons = IntImpl.Input(list(idx))
      idx = idx + 1
      Plan_Cvrg_Mos_Num = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Alzhdmta = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Chf = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Chrnkidn = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Cncr = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Copd = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Depressn = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Diabetes = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Ischmcht = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Osteoprs = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Ra_Oa = IntImpl.Input(list(idx))
      idx = idx + 1
      Sp_Strketia = IntImpl.Input(list(idx))
      idx = idx + 1
      Medreimb_Ip = DoubleImpl.Input(list(idx))
      idx = idx + 1
      Benres_Ip = DoubleImpl.Input(list(idx))
      idx = idx + 1
      Pppymt_Ip = DoubleImpl.Input(list(idx))
      idx = idx + 1
      Medreimb_Op = DoubleImpl.Input(list(idx))
      idx = idx + 1
      Benres_Op = DoubleImpl.Input(list(idx))
      idx = idx + 1
      Pppymt_Op = DoubleImpl.Input(list(idx))
      idx = idx + 1
      Medreimb_Car = DoubleImpl.Input(list(idx))
      idx = idx + 1
      Benres_Car = DoubleImpl.Input(list(idx))
      idx = idx + 1
      Pppymt_Car = DoubleImpl.Input(list(idx))
      idx = idx + 1
      /**
       * NOTE NOTE:: For now We are not expecting messages data in this message/container data.
       */
      // Inpatient_Claims is messages ArrayBuffer, so we are not going to populate it here
      // Outpatient_Claims is messages ArrayBuffer, so we are not going to populate it here
      // HL7Messages is messages ArrayBuffer, so we are not going to populate it here
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    idx
  }

  override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = {
    if (childPath == null || childPath.size == 0) { // Invalid case
      return
    }
    val curVal = childPath(0)
    if (childPath.size == 1) { // If this is last level
      if (curVal._2.compareToIgnoreCase("Inpatient_Claims") == 0) {
        Inpatient_Claims += msg.asInstanceOf[com.ligadata.edifecs.System_InpatientClaim_100]
      } else if (curVal._2.compareToIgnoreCase("Outpatient_Claims") == 0) {
        Outpatient_Claims += msg.asInstanceOf[com.ligadata.edifecs.System_OutpatientClaim_100]
      } else if (curVal._2.compareToIgnoreCase("HL7Messages") == 0) {
        HL7Messages += msg.asInstanceOf[com.ligadata.edifecs.System_HL7_100]
      }
    } else { // Yet to handle it. Make sure we add the message to one of the value given
      throw new Exception("Not yet handled messages more than one level")
    }
  }

  override def getKeyData: String = Desynpuf_Id

  var Desynpuf_Id: String = _
  var Bene_Birth_Dt: Int = _
  var Bene_Death_Dt: Int = _
  var Bene_Sex_Ident_Cd: Int = _
  var Bene_Race_Cd: Int = _
  var Bene_Esrd_Ind: Char = _
  var Sp_State_Code: Int = _
  var Bene_County_Cd: Int = _
  var Bene_Hi_Cvrage_Tot_Mons: Int = _
  var Bene_Smi_Cvrage_Tot_Mons: Int = _
  var Bene_Hmo_Cvrage_Tot_Mons: Int = _
  var Plan_Cvrg_Mos_Num: Int = _
  var Sp_Alzhdmta: Int = _
  var Sp_Chf: Int = _
  var Sp_Chrnkidn: Int = _
  var Sp_Cncr: Int = _
  var Sp_Copd: Int = _
  var Sp_Depressn: Int = _
  var Sp_Diabetes: Int = _
  var Sp_Ischmcht: Int = _
  var Sp_Osteoprs: Int = _
  var Sp_Ra_Oa: Int = _
  var Sp_Strketia: Int = _
  var Medreimb_Ip: Double = _
  var Benres_Ip: Double = _
  var Pppymt_Ip: Double = _
  var Medreimb_Op: Double = _
  var Benres_Op: Double = _
  var Pppymt_Op: Double = _
  var Medreimb_Car: Double = _
  var Benres_Car: Double = _
  var Pppymt_Car: Double = _

  var Inpatient_Claims: ArrayBuffer[com.ligadata.edifecs.System_InpatientClaim_100] = new ArrayBuffer[com.ligadata.edifecs.System_InpatientClaim_100];
  var Outpatient_Claims: ArrayBuffer[com.ligadata.edifecs.System_OutpatientClaim_100] = new ArrayBuffer[com.ligadata.edifecs.System_OutpatientClaim_100];
  var HL7Messages: ArrayBuffer[com.ligadata.edifecs.System_HL7_100] = new ArrayBuffer[com.ligadata.edifecs.System_HL7_100];
}

object System_Beneficiary_100 extends BaseMsgObj {
  def TransformDataAttributes: TransformMessage = null
  def NeedToTransformData: Boolean = false
  def getMessageName: String = "System.Beneficiary"
  def getVersion: String = "00.01.00"
  def CreateNewMessage: BaseMsg = new System_Beneficiary_100()
  def IsFixed: Boolean = true;
  def IsKv: Boolean = false;
}

class ConflictMedicalCode extends BaseContainer {
  var Code1: String = ""; // Make sure we store only lowercase
  var Code2: String = ""; // Make sure we store only lowercase
  var Description: String = "";

  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getVersion: String = "00.01.00"
  def get(key: String): Any = null
  def getOrElse(key: String, default: Any): Any = null
  def set(key: String, value: Any): Unit = {}
  def getContainerName: String = { "ConflictMedicalCode" }
  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter, -1)
    /** ... */
  }
}

class IdCodeDim extends BaseContainer {

  var Id: Int = 0;
  var Code: String = "";
  var Description: String = "";

  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getVersion: String = "00.01.00"
  def get(key: String): Any = null
  def getOrElse(key: String, default: Any): Any = null
  def set(key: String, value: Any): Unit = {}
  def getContainerName: String = { "IdCodeDim" }
  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter, -1)
    /** ... */
  }
}

class hcpcsCodes extends BaseContainer {

  var idCodeArr: Array[IdCodeDim] = null
  var str2IdMap: HashMap[String, Int] = null

  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getVersion: String = "00.01.00"
  def get(key: String): Any = null
  def getOrElse(key: String, default: Any): Any = null
  def set(key: String, value: Any): Unit = {}
  def getContainerName: String = { "hcpcsCodes" }
  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter, -1)
    /** ... */
  }
}

class icd9DiagnosisCodes extends BaseContainer {

  var idCodeArr: Array[IdCodeDim] = null
  var str2IdMap: HashMap[String, Int] = null

  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getVersion: String = "00.01.00"
  def get(key: String): Any = null
  def getOrElse(key: String, default: Any): Any = null
  def set(key: String, value: Any): Unit = {}
  def getContainerName: String = { "icd9DiagnosisCodes" }
  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter, -1)
    /** ... */
  }
}

class icd9ProcedureCodes extends BaseContainer {

  var idCodeArr: Array[IdCodeDim] = null
  var str2IdMap: HashMap[String, Int] = null

  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getVersion: String = "00.01.00"
  def get(key: String): Any = null
  def getOrElse(key: String, default: Any): Any = null
  def set(key: String, value: Any): Unit = {}
  def getContainerName: String = { "icd9ProcedureCodes" }
  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter, -1)
    /** ... */
  }
}

class dgRelGrpCodes extends BaseContainer {

  var idCodeArr: Array[IdCodeDim] = null
  var str2IdMap: HashMap[String, Int] = null

  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getVersion: String = "00.01.00"
  def get(key: String): Any = null
  def getOrElse(key: String, default: Any): Any = null
  def set(key: String, value: Any): Unit = {}
  def getContainerName: String = { "dgRelGrpCodes" }
  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter, -1)
    /** ... */
  }
}

class lneProcIndTable extends BaseContainer {

  var idCodeArr: Array[IdCodeDim] = null
  var str2IdMap: HashMap[String, Int] = null

  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getVersion: String = "00.01.00"
  def get(key: String): Any = null
  def getOrElse(key: String, default: Any): Any = null
  def set(key: String, value: Any): Unit = {}
  def getContainerName: String = { "lneProcIndTable" }
  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter, -1)
    /** ... */
  }
}

class provNumTable extends BaseContainer {

  var idCodeArr: Array[IdCodeDim] = null
  var str2IdMap: HashMap[String, Int] = null

  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getVersion: String = "00.01.00"
  def get(key: String): Any = null
  def getOrElse(key: String, default: Any): Any = null
  def set(key: String, value: Any): Unit = {}
  def getContainerName: String = { "provNumTable" }
  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter, -1)
    /** ... */
  }
}

class conflictMedCds extends BaseContainer {

  var codeSet: TreeSet[String] = null

  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getVersion: String = "00.01.00"
  def get(key: String): Any = null
  def getOrElse(key: String, default: Any): Any = null
  def set(key: String, value: Any): Unit = {}
  def getContainerName: String = { "conflictMedCds" }
  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter, -1)
    /** ... */
  }
}

class SmokeCodes_100 extends BaseContainer {

  var icd9Code: String = _
  var icd9Descr: String = _

  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getVersion: String = "00.01.00"
  def get(key: String): Any = null
  def getOrElse(key: String, default: Any): Any = null
  def set(key: String, value: Any): Unit = {}
  def getContainerName: String = { "SmokeCodes_100" }
  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter, -1)
    assignCsv(list, 0)
  }

  def assignCsv(list: Array[String], startIdx: Int): Int = {
    var idx = startIdx
    try {
      if (list.size < 2)
        throw new Exception("Incorrect input data size")

      icd9Code = StringImpl.Input(list(idx).trim)
      idx = idx + 1
      icd9Descr = StringImpl.Input(list(idx).trim)
      idx = idx + 1

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    idx
  }
}

class EnvCodes_100 extends BaseContainer {

  var icd9Code: String = _
  var icd9Descr: String = _

  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getVersion: String = "00.01.00"
  def get(key: String): Any = null
  def getOrElse(key: String, default: Any): Any = null
  def set(key: String, value: Any): Unit = {}
  def getContainerName: String = { "EnvCodes_100" }
  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter, -1)
    assignCsv(list, 0)
  }

  def assignCsv(list: Array[String], startIdx: Int): Int = {
    var idx = startIdx
    try {
      if (list.size < 2)
        throw new Exception("Incorrect input data size")

      icd9Code = StringImpl.Input(list(idx).trim)
      idx = idx + 1
      icd9Descr = StringImpl.Input(list(idx).trim)
      idx = idx + 1

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    idx
  }
}

class CoughCodes_100 extends BaseContainer {

  var icd9Code: String = _
  var icd9Descr: String = _

  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getVersion: String = "00.01.00"
  def get(key: String): Any = null
  def getOrElse(key: String, default: Any): Any = null
  def set(key: String, value: Any): Unit = {}
  def getContainerName: String = { "CoughCodes_100" }
  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter, -1)
    assignCsv(list, 0)
  }

  def assignCsv(list: Array[String], startIdx: Int): Int = {
    var idx = startIdx
    try {
      if (list.size < 2)
        throw new Exception("Incorrect input data size")

      icd9Code = StringImpl.Input(list(idx).trim)
      idx = idx + 1
      icd9Descr = StringImpl.Input(list(idx).trim)
      idx = idx + 1

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    idx
  }
}

class SputumCodes_100 extends BaseContainer {

  var icd9Code: String = _
  var icd9Descr: String = _

  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getVersion: String = "00.01.00"
  def get(key: String): Any = null
  def getOrElse(key: String, default: Any): Any = null
  def set(key: String, value: Any): Unit = {}
  def getContainerName: String = { "SputumCodes_100" }
  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter, -1)
    assignCsv(list, 0)
  }

  def assignCsv(list: Array[String], startIdx: Int): Int = {
    var idx = startIdx
    try {
      if (list.size < 2)
        throw new Exception("Incorrect input data size")

      icd9Code = StringImpl.Input(list(idx).trim)
      idx = idx + 1
      icd9Descr = StringImpl.Input(list(idx).trim)
      idx = idx + 1

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    idx
  }
}

class DyspnoeaCodes_100 extends BaseContainer {

  var icd9Code: String = _
  var icd9Descr: String = _

  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getVersion: String = "00.01.00"
  def get(key: String): Any = null
  def getOrElse(key: String, default: Any): Any = null
  def set(key: String, value: Any): Unit = {}
  def getContainerName: String = { "DyspnoeaCodes_100" }
  def populate(inputdata: InputData) {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else throw new Exception("Invalid input data")
  }

  def populateCSV(inputdata: DelimitedData) = {
    val delimiter = inputdata.dataDelim
    val dataStr = inputdata.dataInput
    val list = inputdata.dataInput.split(delimiter, -1)
    assignCsv(list, 0)
  }

  def assignCsv(list: Array[String], startIdx: Int): Int = {
    var idx = startIdx
    try {
      if (list.size < 2)
        throw new Exception("Incorrect input data size")

      icd9Code = StringImpl.Input(list(idx).trim)
      idx = idx + 1
      icd9Descr = StringImpl.Input(list(idx).trim)
      idx = idx + 1

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    idx
  }
}




