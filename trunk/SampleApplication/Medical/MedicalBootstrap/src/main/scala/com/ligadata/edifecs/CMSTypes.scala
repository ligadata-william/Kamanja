package com.ligadata.edifecs

import scala.collection.mutable._
import com.ligadata.OnLEPBase.{InputData, DelimitedData}
import com.ligadata.OnLEPBase.{BaseMsg, BaseContainer, BaseMsgObj, TransformMessage}
import com.ligadata.olep.metadata._

class System_HL7_100 extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "System.HL7"
  def getVersion: String = "00.01.00"
  def populate(inputdata: InputData) {
     /** ... */
  }

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
}

object System_HL7_100 extends BaseMsgObj {
	def TransformDataAttributes: TransformMessage = null
	def NeedToTransformData: Boolean = false
	def getMessageName: String = "System.HL7"
	def getVersion: String = "00.01.00"
	def CreateNewMessage: BaseMsg  = new System_HL7_100()
	def IsFixed:Boolean = true;
	def IsKv:Boolean = false;
}

class System_InpatientClaim_100 extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "System.InpatientClaim"
  def getVersion: String = "00.01.00"
  def populate(inputdata: InputData) {
    /** ... */
  }

  var Desynpuf_Id: String = _
  var Clm_Id: Long = _
  var Segment: Int = _
  var Clm_From_Dt: Int = _
  var Clm_Thru_Dt: Int = _
  var Prvdr_Num: Int = _
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
	def CreateNewMessage: BaseMsg  = new System_InpatientClaim_100()
	def IsFixed:Boolean = true;
	def IsKv:Boolean = false;
}

class System_OutpatientClaim_100 extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "System.OutpatientClaim"
  def getVersion: String = "00.01.00"
  def populate(inputdata: InputData) {
    /** ... */
  }

  var Desynpuf_Id: String = _
  var Clm_Id: Long = _
  var Segment: Int = _
  var Clm_From_Dt: Int = _
  var Clm_Thru_Dt: Int = _
  var Prvdr_Num: Int = _
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
	def CreateNewMessage: BaseMsg  = new System_OutpatientClaim_100()
	def IsFixed:Boolean = true;
	def IsKv:Boolean = false;
}


class System_Beneficiary_100 extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "System.Beneficiary"
  def getVersion: String = "00.01.00"
  def populate(inputdata: InputData) {
    /** ... */
  }
  
  var Desynpuf_Id: String = _
  var Bene_Esrd_Ind: Char = _
  var Bene_Birth_Dt: Int = _
  var Bene_Death_Dt: Int = _
  var Bene_Sex_Ident_Cd: Int = _
  var Bene_Race_Cd: Int = _
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
	def CreateNewMessage: BaseMsg  = new System_Beneficiary_100()
	def IsFixed:Boolean = true;
	def IsKv:Boolean = false;
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
    val list = inputdata.dataInput.split(delimiter)
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
    val list = inputdata.dataInput.split(delimiter)
    /** ... */
  }
}

class hcpcsCodes extends BaseContainer {
  
  var idCodeArr : Array[IdCodeDim] = null
  var str2IdMap : HashMap[String,Int] = null

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
    val list = inputdata.dataInput.split(delimiter)
    /** ... */
  }
}

class icd9DiagnosisCodes extends BaseContainer {
  
  var idCodeArr : Array[IdCodeDim] = null
  var str2IdMap : HashMap[String,Int] = null

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
    val list = inputdata.dataInput.split(delimiter)
    /** ... */
  }
}

class icd9ProcedureCodes extends BaseContainer {
  
  var idCodeArr : Array[IdCodeDim] = null
  var str2IdMap : HashMap[String,Int] = null

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
    val list = inputdata.dataInput.split(delimiter)
    /** ... */
  }
}

class dgRelGrpCodes extends BaseContainer {
  
  var idCodeArr : Array[IdCodeDim] = null
  var str2IdMap : HashMap[String,Int] = null

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
    val list = inputdata.dataInput.split(delimiter)
    /** ... */
  }
}

class lneProcIndTable extends BaseContainer {
  
  var idCodeArr : Array[IdCodeDim] = null
  var str2IdMap : HashMap[String,Int] = null

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
    val list = inputdata.dataInput.split(delimiter)
    /** ... */
  }
}

class provNumTable extends BaseContainer {
  
  var idCodeArr : Array[IdCodeDim] = null
  var str2IdMap : HashMap[String,Int] = null

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
    val list = inputdata.dataInput.split(delimiter)
    /** ... */
  }
}

class conflictMedCds extends BaseContainer {
  
  var codeSet : TreeSet[String] = null

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
    val list = inputdata.dataInput.split(delimiter)
    /** ... */
  }
}

