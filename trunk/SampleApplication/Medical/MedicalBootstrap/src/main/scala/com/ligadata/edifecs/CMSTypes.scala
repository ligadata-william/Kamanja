package com.ligadata.edifecs

import scala.collection.mutable._
import com.ligadata.OnLEPBase.{InputData, DelimitedData}
import com.ligadata.OnLEPBase.{BaseMsg, BaseContainer}
import com.ligadata.olep.metadata._

class BeneficiaryBase extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "Edifecs.BeneficiaryBase"
  def getVersion: String = "00.01.00"
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
  
  var Desynpuf_Id: String = ""; // Make sure we store only lowercase
  var Bene_Esrd_Ind: Char = ' ';
  var Bene_Birth_Dt: Int = 0;
  var Bene_Death_Dt: Int = 0;
  var Bene_Sex_Ident_Cd: Int = 0;
  var Bene_Race_Cd: Int = 0;
  var Sp_State_Code: Int = 0;
  var Bene_County_Cd: Int = 0;
  var Bene_Hi_Cvrage_Tot_Mons: Int = 0;
  var Bene_Smi_Cvrage_Tot_Mons: Int = 0;
  var Bene_Hmo_Cvrage_Tot_Mons: Int = 0;
  var Plan_Cvrg_Mos_Num: Int = 0;
  var Sp_Alzhdmta: Int = 0;
  var Sp_Chf: Int = 0;
  var Sp_Chrnkidn: Int = 0;
  var Sp_Cncr: Int = 0;
  var Sp_Copd: Int = 0;
  var Sp_Depressn: Int = 0;
  var Sp_Diabetes: Int = 0;
  var Sp_Ischmcht: Int = 0;
  var Sp_Osteoprs: Int = 0;
  var Sp_Ra_Oa: Int = 0;
  var Sp_Strketia: Int = 0;
  var Medreimb_Ip: Double = 0;
  var Benres_Ip: Double = 0;
  var Pppymt_Ip: Double = 0;
  var Medreimb_Op: Double = 0;
  var Benres_Op: Double = 0;
  var Pppymt_Op: Double = 0;
  var Medreimb_Car: Double = 0;
  var Benres_Car: Double = 0;
  var Pppymt_Car: Double = 0;
}

class Beneficiary  extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "Edifecs.Beneficiary"
  def getVersion: String = "00.01.00"
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
  
  var beneficiaryBase: BeneficiaryBase = new BeneficiaryBase()
  var Inpatient_Claims: ArrayBuffer[InpatientClaim] = new ArrayBuffer[InpatientClaim];
  var Outpatient_Claims: ArrayBuffer[OutpatientClaim] = new ArrayBuffer[OutpatientClaim];
  var Carrier_Claims: ArrayBuffer[CarrierClaim] = new ArrayBuffer[CarrierClaim];
  var Prescription_Drug_Events: ArrayBuffer[PrescriptionDrug] = new ArrayBuffer[PrescriptionDrug];
  var HL7Messages: ArrayBuffer[HL7] = new ArrayBuffer[HL7];
  var Medications:ArrayBuffer[Medication] = new ArrayBuffer[Medication]
 }

class CarrierClaimBase extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "Edifecs.CarrierClaimBase"
  def getVersion: String = "00.01.00"
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
  
  var Desynpuf_Id: String = "";
  var Clm_Id: Long = 0;
  var Clm_From_Dt: Int = 0;
  var Clm_Thru_Dt: Int = 0;
  var Icd9_Dgns_Cds: Array[String] = null; // Not really using Foreign Key to Icd9DiagnosisCode
}

class CarrierClaim extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "Edifecs.CarrierClaim"
  def getVersion: String = "00.01.00"
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

  var carrierClaimBase: CarrierClaimBase = new CarrierClaimBase;
  var LineItmsInfo: Array[CarrierClaimLineItem] = null;
}

class CarrierClaimLineItem extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "Edifecs.CarrierClaimLineItem"
  def getVersion: String = "00.01.00"
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
  
  var Desynpuf_Id: String = "";
  var Clm_Id: Long = 0;
  var Prf_Physn_Npi: Long = 0;
  var OrdNo: Int = 0;
  var Tax_Num: Int = 0;
  var Line_Prcsg_Ind_Cd: Int = 0; // Foreign Key to LineProcessingIndicatorTable
  var Line_Icd9_Dgns_Cd: String = ""; // Not really using Foreign Key to Icd9DiagnosisCode
  var Hcpcs_Cd: Int = 0; // Foreign Key to HcpcsCode
  var Line_Nch_Pmt_Amt: Float = 0;
  var Line_Bene_Ptb_Ddctbl_Amt: Float = 0;
  var Line_Bene_Prmry_Pyr_Pd_Amt: Float = 0;
  var Line_Coinsrnc_Amt: Float = 0;
  var Line_Alowd_Chrg_Amt: Float = 0;
}


class HL7 extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "Edifecs.HL7"
  def getVersion: String = "00.01.00"
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
  
  var Bene_Esrd_Ind: Char = ' ';
  var Desynpuf_Id: String = ""
  var Clm_Id: Long = 0
  var Clm_From_Dt: Int = 0
  var Clm_Thru_Dt: Int = 0
  var Bene_Birth_Dt: Int = 0
  var Bene_Death_Dt: Int = 0
  var Bene_Sex_Ident_Cd: Int = 0
  var Bene_Race_Cd: Int = 0
  var Sp_State_Code: Int = 0
  var Bene_County_Cd: Int = 0
  var Bene_Hi_Cvrage_Tot_Mons: Int = 0
  var Bene_Smi_Cvrage_Tot_Mons: Int = 0
  var Bene_Hmo_Cvrage_Tot_Mons: Int = 0
  var Plan_Cvrg_Mos_Num: Int = 0
  var Sp_Alzhdmta: Int = 0
  var Sp_Chf: Int = 0
  var Sp_Chrnkidn: Int = 0
  var Sp_Cncr: Int = 0
  var Sp_Copd: Int = 0
  var Sp_Depressn: Int = 0
  var Sp_Diabetes: Int = 0
  var Sp_Ischmcht: Int = 0
  var Sp_Osteoprs: Int = 0
  var Sp_Ra_Oa: Int = 0
  var Sp_Strketia: Int = 0
  var Age: Int = 0
  var Infectious_Parasitic_Diseases: Int = 0
  var Neoplasms: Int = 0
  var Endocrine_Nutritional_Metabolic_Diseases_Immunity_Disorders: Int = 0
  var Diseases_Blood_Blood_Forming_Organs: Int = 0
  var Mental_Disorders: Int = 0
  var Diseases_Nervous_System_Sense_Organs: Int = 0
  var Diseases_Circulatory_System: Int = 0
  var Diseases_Respiratory_System: Int = 0
  var Diseases_Digestive_System: Int = 0
  var Diseases_Genitourinary_System: Int = 0
  var Complications_Of_Pregnancy_Childbirth_The_Puerperium: Int = 0
  var Diseases_Skin_Subcutaneous_Tissue: Int = 0
  var Diseases_Musculoskeletal_System_Connective_Tissue: Int = 0
  var Congenital_Anomalies: Int = 0
  var Certain_Conditions_Originating_In_The_Perinatal_Period: Int = 0
  var Symptoms_Signs_Ill_Defined_Conditions: Int = 0
  var Injury_Poisoning: Int = 0
  var Factors_Influencing_Health_Status_Contact_With_Health_Services: Int = 0
  var External_Causes_Of_Injury_Poisoning: Int = 0
  var Hypothyroidism: Int = 0
  var Infarction: Int = 0
  var Alzheimer: Int = 0
  var Alzheimer_Related: Int = 0
  var Anemia: Int = 0
  var Asthma: Int = 0
  var Atrial_Fibrillation: Int = 0
  var Hyperplasia: Int = 0
  var Cataract: Int = 0
  var Kidney_Disease: Int = 0
  var Pulmonary_Disease: Int = 0
  var Depression: Int = 0
  var Diabetes: Int = 0
  var Glaucoma: Int = 0
  var Heart_Failure: Int = 0
  var Hip_Pelvic_Fracture: Int = 0
  var Hyperlipidemia: Int = 0
  var Hypertension: Int = 0
  var Ischemic_Heart_Disease: Int = 0
  var Osteoporosis: Int = 0
  var Ra_Oa: Int = 0
  var Stroke: Int = 0
  var Breast_Cancer: Int = 0
  var Colorectal_Cancer: Int = 0
  var Prostate_Cancer: Int = 0
  var Lung_Cancer: Int = 0
  var Endometrial_Cancer: Int = 0
  var Tobacco: Int = 0
  var Shortnessofbreath: Int = 0
  var Height: Double = 0
  var Weight: Double = 0
  var Systolic: Double = 0
  var Diastolic: Double = 0
  var Totalcholesterol: Double = 0
  var Ldl: Double = 0
  var Triglycerides: Double = 0
  var Chestpain: String = ""

  var AATDeficiency: Int = 0
  var ChronicCough: Int = 0
  var ChronicSputum: Int = 0

  var Conditions: Array[Int] = null
}

class InpatientClaim extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "Edifecs.InpatientClaim"
  def getVersion: String = "00.01.00"
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
  
  var Desynpuf_Id: String = "";
  var Clm_Id: Long = 0;
  var At_Physn_Npi: Long = 0;
  var Op_Physn_Npi: Long = 0;
  var Ot_Physn_Npi: Long = 0;
  var Segment: Int = 0;
  var Clm_From_Dt: Int = 0;
  var Clm_Thru_Dt: Int = 0;
  var Prvdr_Num: Int = 0; // Foreign Key to ProviderNumberTable
  var Clm_Admsn_Dt: Int = 0;
  var Admtng_Icd9_Dgns_Cd: String = ""; // Not really using Foreign Key to Icd9DiagnosisCode
  var Clm_Utlztn_Day_Cnt: Int = 0;
  var Nch_Bene_Dschrg_Dt: Int = 0;
  var Clm_Drg_Cd: Int = 0; // Foreign Key to DiagnosisRelatedGroupCode
  var Clm_Pmt_Amt: Float = 0;
  var Nch_Prmry_Pyr_Clm_Pd_Amt: Float = 0;
  var Clm_Pass_Thru_Per_Diem_Amt: Float = 0;
  var Nch_Bene_Ip_Ddctbl_Amt: Float = 0;
  var Nch_Bene_Pta_Coinsrnc_Lblty_Am: Float = 0;
  var Nch_Bene_Blood_Ddctbl_Lblty_Am: Float = 0;
  var Icd9_Dgns_Cds: Array[String] = null; // Not really using Foreign Key to Icd9DiagnosisCode
  var Icd9_Prcdr_Cds: Array[Int] = null; // Foreign Key to Icd9ProcedureCode
  var Hcpcs_Cds: Array[Int] = null; // Foreign Key to HcpcsCode
}

class OutpatientClaim extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "Edifecs.OutpatientClaim"
  def getVersion: String = "00.01.00"
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
  
  var Desynpuf_Id: String = "";
  var Clm_Id: Long = 0;
  var At_Physn_Npi: Long = 0;
  var Op_Physn_Npi: Long = 0;
  var Ot_Physn_Npi: Long = 0;
  var Segment: Int = 0;
  var Clm_From_Dt: Int = 0;
  var Clm_Thru_Dt: Int = 0;
  var Prvdr_Num: Int = 0; // Foreign Key to ProviderNumberTable
  var Admtng_Icd9_Dgns_Cd: String = ""; // Not really using Foreign Key to Icd9DiagnosisCode
  var Clm_Pmt_Amt: Double = 0;
  var Nch_Prmry_Pyr_Clm_Pd_Amt: Float = 0;
  var Nch_Bene_Blood_Ddctbl_Lblty_Am: Float = 0;
  var Nch_Bene_Ptb_Ddctbl_Amt: Float = 0;
  var Nch_Bene_Ptb_Coinsrnc_Amt: Float = 0;
  var Icd9_Dgns_Cds: Array[String] = null; // Not really using Foreign Key to Icd9DiagnosisCode 
  var Icd9_Prcdr_Cds: Array[Int] = null; // Foreign Key to Icd9ProcedureCode
  var Hcpcs_Cds: Array[Int] = null; // Foreign Key to HcpcsCode)
}

class Medication extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "Edifecs.Medication"
  def getVersion: String = "00.01.00"
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
  
  var Desynpuf_Id: String = ""; // Make sure we store only lowercase
  var MedicationIdentifier: String = "" // Make sure we store only lowercase
  var MedicationDetails_Name: String = ""
  var EncounterId: Long = 0 // Same as ClaimId
  var Administration_Dosage_Quantity: Double = 0
}

class PrescriptionDrug extends BaseMsg {
  def IsFixed: Boolean = true
  def IsKv: Boolean = false
  def getMessageName: String = "Edifecs.PrescriptionDrug"
  def getVersion: String = "00.01.00"
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


  var Desynpuf_Id: String = "";
  var Pde_Id: Long = 0;
  var Srvc_Dt: Int = 0;
  var Days_Suply_Num: Int = 0;
  var Ptnt_Pay_Amt: Float = 0;
  var Tot_Rx_Cst_Amt: Float = 0;
  var Qty_Dspnsd_Num: String = "";
  var Prod_Srvc_Id: String = "";
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

