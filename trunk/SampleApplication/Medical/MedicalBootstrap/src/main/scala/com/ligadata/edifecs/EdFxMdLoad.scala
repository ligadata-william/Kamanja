package com.ligadata.edifecs

import scala.collection.mutable.{ Set }
import org.apache.log4j.Logger
import com.ligadata.olep.metadata.MdMgr._
import com.ligadata.olep.metadata.ObjType._
import com.ligadata.olep.metadata._
import com.ligadata.OnLEPBase._
import com.ligadata.BaseTypes._


trait LogTrait {
    val loggerName = this.getClass.getName()
    val logger = Logger.getLogger(loggerName)
}

/** 
 *  FIXME: As an intermediate development, we might load the metadata manager with file content before resurrecting
 *  the cache from a kv store... hence the arguments (currently unused)
 *	
 *  For now, we just call some functions in the object MetadataLoad to load the various kinds of metadata.
 *  The functions used to load metadata depend on metadata that the loaded element needs being present
 *  before hand in the metadata store (e.g., a type of a function arg must exist before the function can
 *  be loaded.
 *  
 *  
 */

class MetadataLoad (val mgr : MdMgr, val typesPath : String, val fcnPath : String, val attrPath : String, msgCtnPath : String) extends LogTrait {
  
	/** construct the loader and call this to complete the cache initialization */
	def initialize { 

		logger.trace("MetadataLoad...loading typedefs")
		InitTypeDefs

		logger.trace("MetadataLoad...loading Edifecs types")
		InitTypesForEdifecs

	    logger.trace("MetadataLoad...loading Edifecs message and container definitions")
	    InitFixedMsgsForEdifecs
	    
		logger.trace("MetadataLoad...loading Edifecs types")
		InitTypesForEdifecs1

		logger.trace("MetadataLoad...loading Beneficiary")
	    InitFixedMsgsForEdifecs1
		
		logger.trace("MetadataLoad...loading Pmml types")
		initTypesFor_com_ligadata_pmml_udfs_Udfs

		logger.trace("MetadataLoad...loading Pmml udfs")
		init_com_ligadata_pmml_udfs_Udfs
		
		logger.trace("MetadataLoad...loading Iterable functions")
		InitFcns
			
	    logger.trace("MetadataLoad...loading function macro definitions")
	    initMacroDefs
	    
	}
	
	// CMS messages + the dimensional data (treated as Containers)
	def InitFixedMsgsForEdifecs : Unit = {
		logger.trace("MetadataLoad...loading Edifecs messages and containers")
		
		
		logger.trace("MetadataLoad...loading BeneficiaryBase")
		mgr.AddFixedMsg(MdMgr.sysNS
					    , "BeneficiaryBase"
					    , "com.ligadata.edifecs.BeneficiaryBase"
					    , List((MdMgr.sysNS, "Desynpuf_Id", MdMgr.sysNS, "String", false)
						, (MdMgr.sysNS, "Bene_Esrd_Ind", MdMgr.sysNS, "Char", false)
						, (MdMgr.sysNS, "Bene_Birth_Dt", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Bene_Death_Dt", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Bene_Sex_Ident_Cd", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Bene_Race_Cd", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Sp_State_Code", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Bene_County_Cd", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Bene_Hi_Cvrage_Tot_Mons", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Bene_Smi_Cvrage_Tot_Mons", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Bene_Hmo_Cvrage_Tot_Mons", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Plan_Cvrg_Mos_Num", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Sp_Alzhdmta", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Sp_Chf", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Sp_Chrnkidn", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Sp_Cncr", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Sp_Copd", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Sp_Depressn", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Sp_Diabetes", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Sp_Ischmcht", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Sp_Osteoprs", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Sp_Ra_Oa", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Sp_Strketia", MdMgr.sysNS, "Int", false)
						, (MdMgr.sysNS, "Medreimb_Ip", MdMgr.sysNS, "Double", false)
						, (MdMgr.sysNS, "Benres_Ip", MdMgr.sysNS, "Double", false)
						, (MdMgr.sysNS, "Pppymt_Ip", MdMgr.sysNS, "Double", false)
						, (MdMgr.sysNS, "Medreimb_Op", MdMgr.sysNS, "Double", false)
						, (MdMgr.sysNS, "Benres_Op", MdMgr.sysNS, "Double", false)
						, (MdMgr.sysNS, "Pppymt_Op", MdMgr.sysNS, "Double", false)
						, (MdMgr.sysNS, "Medreimb_Car", MdMgr.sysNS, "Double", false)
						, (MdMgr.sysNS, "Benres_Car", MdMgr.sysNS, "Double", false)
						, (MdMgr.sysNS, "Pppymt_Car", MdMgr.sysNS, "Double", false)
						));

		logger.trace("MetadataLoad...loading CarrierClaimBase")
		mgr.AddFixedMsg(MdMgr.sysNS
					    , "CarrierClaimBase"
					    , "com.ligadata.edifecs.CarrierClaimBase"
					    , List((MdMgr.sysNS, "Desynpuf_Id", MdMgr.sysNS, "String", false)
					        ,(MdMgr.sysNS, "Clm_Id", MdMgr.sysNS, "Long", false)
					        , (MdMgr.sysNS, "Clm_From_Dt", MdMgr.sysNS, "Int", false)
					        , (MdMgr.sysNS, "Clm_Thru_Dt", MdMgr.sysNS, "Int", false)
					        , (MdMgr.sysNS, "Icd9_Dgns_Cds", MdMgr.sysNS, "ArrayOfString", false)
					        ));

		logger.trace("MetadataLoad...loading CarrierClaimLineItem")
		mgr.AddFixedMsg(MdMgr.sysNS
					    , "CarrierClaimLineItem"
					    , "com.ligadata.edifecs.CarrierClaimLineItem"
					    , List((MdMgr.sysNS, "Desynpuf_Id", MdMgr.sysNS, "String", false)
							, (MdMgr.sysNS, "Clm_Id", MdMgr.sysNS, "Long", false)
							, (MdMgr.sysNS, "Prf_Physn_Npi", MdMgr.sysNS, "Long", false)
							, (MdMgr.sysNS, "OrdNo", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Tax_Num", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Line_Prcsg_Ind_Cd", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Line_Icd9_Dgns_Cd", MdMgr.sysNS, "String", false)
							, (MdMgr.sysNS, "Hcpcs_Cd", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Line_Nch_Pmt_Amt", MdMgr.sysNS, "Float", false)
							, (MdMgr.sysNS, "Line_Bene_Ptb_Ddctbl_Amt", MdMgr.sysNS, "Float", false)
							, (MdMgr.sysNS, "Line_Bene_Prmry_Pyr_Pd_Amt", MdMgr.sysNS, "Float", false)
							, (MdMgr.sysNS, "Line_Coinsrnc_Amt", MdMgr.sysNS, "Float", false)
							, (MdMgr.sysNS, "Line_Alowd_Chrg_Amt", MdMgr.sysNS, "Float", false)
					        ));

		mgr.AddArray(MdMgr.sysNS, "ArrayOfCarrierClaimLineItem", MdMgr.sysNS, "CarrierClaimLineItem", 1, 1)

		logger.trace("MetadataLoad...loading CarrierClaim")
		mgr.AddFixedMsg(MdMgr.sysNS
					    , "CarrierClaim"
					    , "com.ligadata.edifecs.CarrierClaim"
					    , List((MdMgr.sysNS, "carrierClaimBase", MdMgr.sysNS, "CarrierClaimBase", false)
					        ,(MdMgr.sysNS, "LineItmsInfo", MdMgr.sysNS, "ArrayOfCarrierClaimLineItem", false)
					        ));

		logger.trace("MetadataLoad...loading InpatientClaim")
		mgr.AddFixedMsg(MdMgr.sysNS
					    , "InpatientClaim"
					    , "com.ligadata.edifecs.InpatientClaim"
					    , List((MdMgr.sysNS, "Desynpuf_Id", MdMgr.sysNS, "String", false)
							, (MdMgr.sysNS, "Clm_Id", MdMgr.sysNS, "Long", false)
							, (MdMgr.sysNS, "At_Physn_Npi", MdMgr.sysNS, "Long", false)
							, (MdMgr.sysNS, "Op_Physn_Npi", MdMgr.sysNS, "Long", false)
							, (MdMgr.sysNS, "Ot_Physn_Npi", MdMgr.sysNS, "Long", false)
							, (MdMgr.sysNS, "Segment", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Clm_From_Dt", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Clm_Thru_Dt", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Prvdr_Num", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Clm_Admsn_Dt", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Admtng_Icd9_Dgns_Cd", MdMgr.sysNS, "String", false)
							, (MdMgr.sysNS, "Clm_Utlztn_Day_Cnt", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Nch_Bene_Dschrg_Dt", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Clm_Drg_Cd", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Clm_Pmt_Amt", MdMgr.sysNS, "Double", false)
							, (MdMgr.sysNS, "Nch_Prmry_Pyr_Clm_Pd_Amt", MdMgr.sysNS, "Double", false)
							, (MdMgr.sysNS, "Clm_Pass_Thru_Per_Diem_Amt", MdMgr.sysNS, "Double", false)
							, (MdMgr.sysNS, "Nch_Bene_Ip_Ddctbl_Amt", MdMgr.sysNS, "Double", false)
							, (MdMgr.sysNS, "Nch_Bene_Pta_Coinsrnc_Lblty_Am", MdMgr.sysNS, "Double", false)
							, (MdMgr.sysNS, "Nch_Bene_Blood_Ddctbl_Lblty_Am", MdMgr.sysNS, "Double", false)
							, (MdMgr.sysNS, "Icd9_Dgns_Cds", MdMgr.sysNS, "ArrayOfString", false)
							, (MdMgr.sysNS, "Icd9_Prcdr_Cds", MdMgr.sysNS, "ArrayOfInt", false)
							, (MdMgr.sysNS, "Hcpcs_Cds", MdMgr.sysNS, "ArrayOfInt", false)
					        ));

		logger.trace("MetadataLoad...loading OutpatientClaim")
		mgr.AddFixedMsg(MdMgr.sysNS
					    , "OutpatientClaim"
					    , "com.ligadata.edifecs.OutpatientClaim"
					    , List((MdMgr.sysNS, "Desynpuf_Id", MdMgr.sysNS, "String", false)
							, (MdMgr.sysNS, "Clm_Id", MdMgr.sysNS, "Long", false)
							, (MdMgr.sysNS, "At_Physn_Npi", MdMgr.sysNS, "Long", false)
							, (MdMgr.sysNS, "Op_Physn_Npi", MdMgr.sysNS, "Long", false)
							, (MdMgr.sysNS, "Ot_Physn_Npi", MdMgr.sysNS, "Long", false)
							, (MdMgr.sysNS, "Segment", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Clm_From_Dt", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Clm_Thru_Dt", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Prvdr_Num", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Admtng_Icd9_Dgns_Cd", MdMgr.sysNS, "String", false)
							, (MdMgr.sysNS, "Clm_Pmt_Amt", MdMgr.sysNS, "Double", false)
							, (MdMgr.sysNS, "Nch_Prmry_Pyr_Clm_Pd_Amt", MdMgr.sysNS, "Double", false)
							, (MdMgr.sysNS, "Nch_Bene_Blood_Ddctbl_Lblty_Am", MdMgr.sysNS, "Double", false)
							, (MdMgr.sysNS, "Nch_Bene_Ptb_Ddctbl_Amt", MdMgr.sysNS, "Double", false)
							, (MdMgr.sysNS, "Nch_Bene_Ptb_Coinsrnc_Amt", MdMgr.sysNS, "Double", false)
							, (MdMgr.sysNS, "Icd9_Dgns_Cds", MdMgr.sysNS, "ArrayOfString", false)
							, (MdMgr.sysNS, "Icd9_Prcdr_Cds", MdMgr.sysNS, "ArrayOfInt", false)
							, (MdMgr.sysNS, "Hcpcs_Cds", MdMgr.sysNS, "ArrayOfInt", false)
					        ));

		logger.trace("MetadataLoad...loading Medication")
		mgr.AddFixedMsg(MdMgr.sysNS
					    , "Medication"
					    , "com.ligadata.edifecs.Medication"
					    , List((MdMgr.sysNS, "Desynpuf_Id", MdMgr.sysNS, "String", false)
							, (MdMgr.sysNS, "MedicationIdentifier", MdMgr.sysNS, "String", false)
							, (MdMgr.sysNS, "MedicationDetails_Name", MdMgr.sysNS, "String", false)
							, (MdMgr.sysNS, "EncounterId", MdMgr.sysNS, "Long", false)
							, (MdMgr.sysNS, "Administration_Dosage_Quantity", MdMgr.sysNS, "Double", false)
					        ));

		logger.trace("MetadataLoad...loading HL7")
		mgr.AddFixedMsg(MdMgr.sysNS
					    , "HL7"
					    , "com.ligadata.edifecs.HL7"
					    , List((MdMgr.sysNS, "Bene_Esrd_Ind", MdMgr.sysNS, "Char", false)
					    	, (MdMgr.sysNS, "Desynpuf_Id", MdMgr.sysNS, "String", false)
							, (MdMgr.sysNS, "Clm_Id", MdMgr.sysNS, "Long", false)
							, (MdMgr.sysNS, "Clm_From_Dt", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Clm_Thru_Dt", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Bene_Birth_Dt", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Bene_Death_Dt", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Bene_Sex_Ident_Cd", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Bene_Race_Cd", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Sp_State_Code", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Bene_County_Cd", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Bene_Hi_Cvrage_Tot_Mons", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Bene_Smi_Cvrage_Tot_Mons", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Bene_Hmo_Cvrage_Tot_Mons", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Plan_Cvrg_Mos_Num", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Sp_Alzhdmta", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Sp_Chf", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Sp_Chrnkidn", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Sp_Cncr", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Sp_Copd", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Sp_Depressn", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Sp_Diabetes", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Sp_Ischmcht", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Sp_Osteoprs", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Sp_Ra_Oa", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Sp_Strketia", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Age", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Infectious_Parasitic_Diseases", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Neoplasms", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Endocrine_Nutritional_Metabolic_Diseases_Immunity_Disorders", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Diseases_Blood_Blood_Forming_Organs", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Mental_Disorders", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Diseases_Nervous_System_Sense_Organs", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Diseases_Circulatory_System", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Diseases_Respiratory_System", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Diseases_Digestive_System", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Diseases_Genitourinary_System", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Complications_Of_Pregnancy_Childbirth_The_Puerperium", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Diseases_Skin_Subcutaneous_Tissue", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Diseases_Musculoskeletal_System_Connective_Tissue", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Congenital_Anomalies", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Certain_Conditions_Originating_In_The_Perinatal_Period", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Symptoms_Signs_Ill_Defined_Conditions", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Injury_Poisoning", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Factors_Influencing_Health_Status_Contact_With_Health_Services", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "External_Causes_Of_Injury_Poisoning", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Hypothyroidism", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Infarction", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Alzheimer", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Alzheimer_Related", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Anemia", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Asthma", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Atrial_Fibrillation", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Hyperplasia", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Cataract", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Kidney_Disease", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Pulmonary_Disease", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Depression", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Diabetes", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Glaucoma", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Heart_Failure", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Hip_Pelvic_Fracture", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Hyperlipidemia", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Hypertension", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Ischemic_Heart_Disease", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Osteoporosis", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Ra_Oa", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Stroke", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Breast_Cancer", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Colorectal_Cancer", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Prostate_Cancer", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Lung_Cancer", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Endometrial_Cancer", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Tobacco", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Shortnessofbreath", MdMgr.sysNS, "Int", false)
							, (MdMgr.sysNS, "Height", MdMgr.sysNS, "Double", false)
							, (MdMgr.sysNS, "Weight", MdMgr.sysNS, "Double", false)
							, (MdMgr.sysNS, "Systolic", MdMgr.sysNS, "Double", false)
							, (MdMgr.sysNS, "Diastolic", MdMgr.sysNS, "Double", false)
							, (MdMgr.sysNS, "Totalcholesterol", MdMgr.sysNS, "Double", false)
							, (MdMgr.sysNS, "Ldl", MdMgr.sysNS, "Double", false)
							, (MdMgr.sysNS, "Triglycerides", MdMgr.sysNS, "Double", false)
							, (MdMgr.sysNS, "Chestpain", MdMgr.sysNS, "String", false)
							, (MdMgr.sysNS, "Conditions", MdMgr.sysNS, "ArrayOfInt", false)
					        ));

		logger.trace("MetadataLoad...loading PrescriptionDrug")
		mgr.AddFixedMsg(MdMgr.sysNS
					    , "PrescriptionDrug"
					    , "com.ligadata.edifecs.PrescriptionDrug"
					    , List((MdMgr.sysNS, "Desynpuf_Id", MdMgr.sysNS, "String", false)
					        , (MdMgr.sysNS, "Pde_Id", MdMgr.sysNS, "Long", false)
					        , (MdMgr.sysNS, "Srvc_Dt", MdMgr.sysNS, "Int", false)
					        , (MdMgr.sysNS, "Days_Suply_Num", MdMgr.sysNS, "Int", false)
					        , (MdMgr.sysNS, "Ptnt_Pay_Amt", MdMgr.sysNS, "Float", false)
					        , (MdMgr.sysNS, "Tot_Rx_Cst_Amt", MdMgr.sysNS, "Float", false)
					        , (MdMgr.sysNS, "Qty_Dspnsd_Num", MdMgr.sysNS, "String", false)
					        , (MdMgr.sysNS, "Prod_Srvc_Id", MdMgr.sysNS, "String", false)
					        ));
		
		logger.trace("MetadataLoad...loading IdCodeDim")
		mgr.AddFixedContainer(MdMgr.sysNS
					    , "IdCodeDim"
					    , "com.ligadata.edifecs.IdCodeDim"
					    , List((MdMgr.sysNS, "Id", MdMgr.sysNS, "Int", false)
					    	, (MdMgr.sysNS, "Code", MdMgr.sysNS, "String", false)
					    	, (MdMgr.sysNS, "Description", MdMgr.sysNS, "String", false)
					        ));

		logger.trace("MetadataLoad...loading ConflictMedicalCode")
		mgr.AddFixedContainer(MdMgr.sysNS
					    , "ConflictMedicalCode"
					    , "com.ligadata.edifecs.ConflictMedicalCode"
					    , List((MdMgr.sysNS, "Code1", MdMgr.sysNS, "String", false)
							, (MdMgr.sysNS, "Code2", MdMgr.sysNS, "String", false)
							, (MdMgr.sysNS, "Description", MdMgr.sysNS, "String", false)
					        ));

		
		mgr.AddArray(MdMgr.sysNS, "ArrayOfConflictMedicalCode", MdMgr.sysNS, "ConflictMedicalCode", 1, 1)

		
		logger.trace("MetadataLoad...loading EnvContext")		
		mgr.AddFixedContainer(MdMgr.sysNS
			    , "EnvContext"
			    , "com.ligadata.OnLEPBase.EnvContext"
		  		, List()) 
		 		  		
		logger.trace("MetadataLoad...loading BaseMsg")
		 mgr.AddFixedContainer(MdMgr.sysNS
							    , "BaseMsg"
							    , "com.ligadata.OnLEPBase.BaseMsg"
						  		, List()) 
		  		
		logger.trace("MetadataLoad...loading BaseContainer")
		 mgr.AddFixedContainer(MdMgr.sysNS
							    , "BaseContainer"
							    , "com.ligadata.OnLEPBase.BaseContainer"
						  		, List()) 		
				  		
	}

	def InitFixedMsgsForEdifecs1 : Unit = {

		logger.trace("MetadataLoad...loading Beneficiary")
		mgr.AddFixedMsg(MdMgr.sysNS
					    , "Beneficiary"
					    , "com.ligadata.edifecs.Beneficiary"
					    , List((MdMgr.sysNS, "beneficiaryBase", MdMgr.sysNS, "BeneficiaryBase", false)
					        ,(MdMgr.sysNS, "Inpatient_Claims", MdMgr.sysNS, "ArrayBufferOfInpatientClaim", false)
					        , (MdMgr.sysNS, "Outpatient_Claims", MdMgr.sysNS, "ArrayBufferOfOutpatientClaim", false)
					        , (MdMgr.sysNS, "Carrier_Claims", MdMgr.sysNS, "ArrayBufferOfCarrierClaim", false)
					        , (MdMgr.sysNS, "Prescription_Drug_Events", MdMgr.sysNS, "ArrayBufferOfPrescriptionDrug", false)
					        , (MdMgr.sysNS, "HL7Messages", MdMgr.sysNS, "ArrayBufferOfHL7", false)
					        , (MdMgr.sysNS, "Medications", MdMgr.sysNS, "ArrayBufferOfMedication", false)
					        ));
		logger.trace("MetadataLoad..IdStrDimContainer.loading hcpcsCodes")
		mgr.AddFixedContainer(MdMgr.sysNS
					    , "hcpcsCodes"
					    , "com.ligadata.edifecs.hcpcsCodes"
					    , List((MdMgr.sysNS, "idCodeArr", MdMgr.sysNS, "ArrayOfIdCodeDim", false)
					    	, (MdMgr.sysNS, "str2IdMap", MdMgr.sysNS, "HashMapOfStringInt", false)
					        ));

		logger.trace("MetadataLoad.IdStrDimContainer..loading icd9DiagnosisCodes")
		mgr.AddFixedContainer(MdMgr.sysNS
					    , "icd9DiagnosisCodes"
					    , "com.ligadata.edifecs.icd9DiagnosisCodes"
					    , List((MdMgr.sysNS, "idCodeArr", MdMgr.sysNS, "ArrayOfIdCodeDim", false)
					    	, (MdMgr.sysNS, "str2IdMap", MdMgr.sysNS, "HashMapOfStringInt", false)
					        ));

		logger.trace("MetadataLoad..IdStrDimContainer.loading icd9ProcedureCodes")
		mgr.AddFixedContainer(MdMgr.sysNS
					    , "icd9ProcedureCodes"
					    , "com.ligadata.edifecs.icd9ProcedureCodes"
					    , List((MdMgr.sysNS, "idCodeArr", MdMgr.sysNS, "ArrayOfIdCodeDim", false)
					    	, (MdMgr.sysNS, "str2IdMap", MdMgr.sysNS, "HashMapOfStringInt", false)
					        ));

		logger.trace("MetadataLoad.IdStrDimContainer..loading dgRelGrpCodes")
		mgr.AddFixedContainer(MdMgr.sysNS
					    , "dgRelGrpCodes"
					    , "com.ligadata.edifecs.dgRelGrpCodes"
					    , List((MdMgr.sysNS, "idCodeArr", MdMgr.sysNS, "ArrayOfIdCodeDim", false)
					    	, (MdMgr.sysNS, "str2IdMap", MdMgr.sysNS, "HashMapOfStringInt", false)
					        ));

		logger.trace("MetadataLoad.IdStrDimContainer..loading lneProcIndTable")
		mgr.AddFixedContainer(MdMgr.sysNS
					    , "lneProcIndTable"
					    , "com.ligadata.edifecs.lneProcIndTable"
					    , List((MdMgr.sysNS, "idCodeArr", MdMgr.sysNS, "ArrayOfIdCodeDim", false)
					    	, (MdMgr.sysNS, "str2IdMap", MdMgr.sysNS, "HashMapOfStringInt", false)
					        ));

		logger.trace("MetadataLoad.IdStrDimContainer..loading provNumTable")
		mgr.AddFixedContainer(MdMgr.sysNS
					    , "provNumTable"
					    , "com.ligadata.edifecs.provNumTable"
					    , List((MdMgr.sysNS, "idCodeArr", MdMgr.sysNS, "ArrayOfIdCodeDim", false)
					    	, (MdMgr.sysNS, "str2IdMap", MdMgr.sysNS, "HashMapOfStringInt", false)
					        ));

		logger.trace("MetadataLoad...loading conflictMedCds")
		mgr.AddFixedContainer(MdMgr.sysNS
					    , "conflictMedCds"
					    , "com.ligadata.edifecs.ConflictMedCds"
					    , List((MdMgr.sysNS, "codeSet", MdMgr.sysNS, "TreeSetOfString", false)
					        ));


	}


	/** Define any types that may be used in the container, message, fcn, and model metadata */
	def InitTypeDefs = {
	    val baseTypesVer = 100 // Which is 00.01.00

		mgr.AddScalar("System", "Any", tAny, "Any", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.AnyImpl")
		mgr.AddScalar("System", "String", tString, "String", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.StringImpl")
		mgr.AddScalar("System", "Int", tInt, "Int", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.IntImpl")
		mgr.AddScalar("System", "Long", tLong, "Long", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.LongImpl")
		mgr.AddScalar("System", "Boolean", tBoolean, "Boolean", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.BoolImpl")
		mgr.AddScalar("System", "Bool", tBoolean, "Boolean", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.BoolImpl")
		mgr.AddScalar("System", "Double", tDouble, "Double", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.DoubleImpl")
		mgr.AddScalar("System", "Float", tFloat, "Float", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.FloatImpl")
		mgr.AddScalar("System", "Char", tChar, "Char", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.CharImpl")

		
		mgr.AddHashMap("System", "HashMapOfAnyAny", ("System", "Any"), ("System", "Any"), 1)
		mgr.AddMap("System", "MapOfAnyAny", ("System", "Any"), ("System", "Any"), 1)


		
	}
	
	def InitTypesForEdifecs : Unit = {

		mgr.AddArray(MdMgr.sysNS, "ArrayOfString", MdMgr.sysNS, "String", 1, 1)
		mgr.AddArray(MdMgr.sysNS, "ArrayOfCarrierClaimLineItem", MdMgr.sysNS, "CarrierClaimLineItem", 1, 1)
		mgr.AddArray(MdMgr.sysNS, "ArrayOfInt", MdMgr.sysNS, "String", 1, 1)

		mgr.AddHashMap(MdMgr.sysNS, "HashMapOfStringInt", (MdMgr.sysNS, "String"), (MdMgr.sysNS, "Int"), 1)
		mgr.AddTreeSet(MdMgr.sysNS, "TreeSetOfString", MdMgr.sysNS, "String", 1)
	}

	def InitTypesForEdifecs1 : Unit = {

		mgr.AddArray(MdMgr.sysNS, "ArrayOfIdCodeDim", MdMgr.sysNS, "IdCodeDim", 1, 1)
		mgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfCarrierClaim", MdMgr.sysNS, "CarrierClaim", 1, 1)
		mgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfInpatientClaim", MdMgr.sysNS, "InpatientClaim", 1, 1)
		mgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfOutpatientClaim", MdMgr.sysNS, "OutpatientClaim", 1, 1)
		mgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfPrescriptionDrug", MdMgr.sysNS, "PrescriptionDrug", 1, 1)
		mgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfHL7", MdMgr.sysNS, "HL7", 1, 1)
		mgr.AddArrayBuffer(MdMgr.sysNS, "ArrayBufferOfMedication", MdMgr.sysNS, "Medication", 1, 1)

		mgr.AddArrayBuffer(MdMgr.sysNS, "ArrayOfCarrierClaim", MdMgr.sysNS, "CarrierClaim", 1, 1)
		mgr.AddArrayBuffer(MdMgr.sysNS, "ArrayOfInpatientClaim", MdMgr.sysNS, "InpatientClaim", 1, 1)
		mgr.AddArrayBuffer(MdMgr.sysNS, "ArrayOfOutpatientClaim", MdMgr.sysNS, "OutpatientClaim", 1, 1)
		mgr.AddArrayBuffer(MdMgr.sysNS, "ArrayOfPrescriptionDrug", MdMgr.sysNS, "PrescriptionDrug", 1, 1)
		mgr.AddArrayBuffer(MdMgr.sysNS, "ArrayOfHL7", MdMgr.sysNS, "HL7", 1, 1)
		mgr.AddArrayBuffer(MdMgr.sysNS, "ArrayOfMedication", MdMgr.sysNS, "Medication", 1, 1)

		mgr.AddSet(MdMgr.sysNS, "SetOfCarrierClaim", MdMgr.sysNS, "CarrierClaim", 1)
		mgr.AddSet(MdMgr.sysNS, "SetOfInpatientClaim", MdMgr.sysNS, "InpatientClaim", 1)
		mgr.AddSet(MdMgr.sysNS, "SetOfOutpatientClaim", MdMgr.sysNS, "OutpatientClaim", 1)
		mgr.AddSet(MdMgr.sysNS, "SetOfPrescriptionDrug", MdMgr.sysNS, "PrescriptionDrug", 1)
		mgr.AddSet(MdMgr.sysNS, "SetOfHL7", MdMgr.sysNS, "HL7", 1)
		mgr.AddSet(MdMgr.sysNS, "SetOfMedication", MdMgr.sysNS, "Medication", 1)

		mgr.AddTreeSet(MdMgr.sysNS, "TreeSetOfCarrierClaim", MdMgr.sysNS, "CarrierClaim", 1)
		mgr.AddTreeSet(MdMgr.sysNS, "TreeSetOfInpatientClaim", MdMgr.sysNS, "InpatientClaim", 1)
		mgr.AddTreeSet(MdMgr.sysNS, "TreeSetOfOutpatientClaim", MdMgr.sysNS, "OutpatientClaim", 1)
		mgr.AddTreeSet(MdMgr.sysNS, "TreeSetOfPrescriptionDrug", MdMgr.sysNS, "PrescriptionDrug", 1)
		mgr.AddTreeSet(MdMgr.sysNS, "TreeSetOfHL7", MdMgr.sysNS, "HL7", 1)
		mgr.AddTreeSet(MdMgr.sysNS, "TreeSetOfMedication", MdMgr.sysNS, "Medication", 1)

		mgr.AddSortedSet(MdMgr.sysNS, "SortedSetOfCarrierClaim", MdMgr.sysNS, "CarrierClaim", 1)
		mgr.AddSortedSet(MdMgr.sysNS, "SortedSetOfInpatientClaim", MdMgr.sysNS, "InpatientClaim", 1)
		mgr.AddSortedSet(MdMgr.sysNS, "SortedSetOfOutpatientClaim", MdMgr.sysNS, "OutpatientClaim", 1)
		mgr.AddSortedSet(MdMgr.sysNS, "SortedSetOfPrescriptionDrug", MdMgr.sysNS, "PrescriptionDrug", 1)
		mgr.AddSortedSet(MdMgr.sysNS, "SortedSetOfHL7", MdMgr.sysNS, "HL7", 1)
		mgr.AddSortedSet(MdMgr.sysNS, "SortedSetOfMedication", MdMgr.sysNS, "Medication", 1)

	}

	/**
	  
	 
	 */
def initTypesFor_com_ligadata_pmml_udfs_Udfs {
		//MdMgr.MakeScalar(mgr, "System", "Int", tInt)
		//MdMgr.MakeScalar(mgr, "System", "Stack[T]", tNone)
		//MdMgr.MakeScalar(mgr, "System", "Vector[T]", tNone)
		//MdMgr.MakeScalar(mgr, "System", "String", tString)
		//MdMgr.MakeScalar(mgr, "System", "Long", tLong)
		//MdMgr.MakeScalar(mgr, "System", "Boolean", tBoolean)
		//MdMgr.MakeScalar(mgr, "System", "Double", tDouble)
		//MdMgr.MakeScalar(mgr, "System", "Float", tFloat)
		//MdMgr.MakeScalar(mgr, "System", "Any", tAny)
		//MdMgr.MakeScalar(mgr, "System", "EnvContext", tNone)
		//MdMgr.MakeScalar(mgr, "System", "BaseContainer", tNone)
		//MdMgr.MakeHashMap(mgr, "System", "HashMapOfKV", ("System", "K"), ("System", "V"))
		//MdMgr.MakeMap(mgr, "System", "MapOfKV", ("System", "K"), ("System", "V"))
		mgr.AddQueue("System", "QueueOfAny", "System", "Any", 1)
		mgr.AddList("System", "ListOfAny", "System", "Any", 1)
		mgr.AddSortedSet("System", "SortedSetOfAny", "System", "Any", 1)
		mgr.AddTreeSet("System", "TreeSetOfAny", "System", "Any", 1)
		mgr.AddSet("System", "SetOfAny", "System", "Any", 1)
		mgr.AddArrayBuffer("System", "ArrayBufferOfAny", "System", "Any", 1, 1)
		mgr.AddArray("System", "ArrayOfAny", "System", "Any", 1, 1)
		//mgr.AddArray("System", "ArrayOfString", "System", "String", 1, 1)
		mgr.AddTupleType("System", "TupleOfStringString", Array(("System","String"), ("System","String")), 1)
		mgr.AddArray("System", "ArrayOfTupleOfStringString", "System", "TupleOfStringString", 1, 1)
		mgr.AddArrayBuffer("System", "ArrayBufferOfString", "System", "String", 1, 1)
		mgr.AddArray("System", "ArrayOfFloat", "System", "Float", 1, 1)
		mgr.AddArray("System", "ArrayOfDouble", "System", "Double", 1, 1)
		mgr.AddArray("System", "ArrayOfLong", "System", "Long", 1, 1)
		//mgr.AddArray("System", "ArrayOfInt", "System", "Int", 1, 1)
		mgr.AddArrayBuffer("System", "ArrayBufferOfFloat", "System", "Float", 1, 1)
		mgr.AddArrayBuffer("System", "ArrayBufferOfDouble", "System", "Double", 1, 1)
		mgr.AddArrayBuffer("System", "ArrayBufferOfLong", "System", "Long", 1, 1)
		mgr.AddArrayBuffer("System", "ArrayBufferOfInt", "System", "Int", 1, 1)
		mgr.AddHashMap("System", "HashMapOfIntInt", ("System", "Int"), ("System", "Int"), 1)
		mgr.AddHashMap("System", "HashMapOfIntArrayBufferOfInt", ("System", "Int"), ("System", "ArrayBufferOfInt"), 1)
		mgr.AddList("System", "ListOfFloat", "System", "Float", 1)
		mgr.AddList("System", "ListOfDouble", "System", "Double", 1)
		mgr.AddList("System", "ListOfLong", "System", "Long", 1)
		mgr.AddList("System", "ListOfInt", "System", "Int", 1)
		mgr.AddList("System", "ListOfString", "System", "String", 1)
		mgr.AddArrayBuffer("System", "ArrayBufferOfBoolean", "System", "Boolean", 1, 1)
		mgr.AddArray("System", "ArrayOfBaseContainer", "System", "BaseContainer", 1, 1)
	}


		
	
	def init_com_ligadata_pmml_udfs_Udfs {
		mgr.AddFunc("Pmml", "Between", "com.ligadata.pmml.udfs.Udfs.Between", ("System", "ArrayOfAny"), List(("set", "System", "SetOfAny")), null)
		mgr.AddFunc("Pmml", "ToSet", "com.ligadata.pmml.udfs.Udfs.ToSet", ("System", "SetOfAny"), List(("arr", "System", "ArrayOfAny")), null)

		mgr.AddFunc("Pmml", "ToSet", "com.ligadata.pmml.udfs.Udfs.ToSet", ("System", "SetOfAny"), List(("arr", "System", "ArrayBufferOfAny")), null)
		mgr.AddFunc("Pmml", "ToSet", "com.ligadata.pmml.udfs.Udfs.ToSet", ("System", "SetOfAny"), List(("arr", "System", "QueueOfAny")), null)
		mgr.AddFunc("Pmml", "ToSet", "com.ligadata.pmml.udfs.Udfs.ToSet", ("System", "SetOfAny"), List(("arr", "System", "ListOfAny")), null)
		mgr.AddFunc("Pmml", "ToArray", "com.ligadata.pmml.udfs.Udfs.ToArray", ("System", "ArrayOfAny"), List(("arr", "System", "ArrayOfAny")), null)
		mgr.AddFunc("Pmml", "ToArray", "com.ligadata.pmml.udfs.Udfs.ToArray", ("System", "ArrayOfAny"), List(("arr", "System", "ArrayBufferOfAny")), null)
 		mgr.AddFunc("Pmml", "ToArray", "com.ligadata.pmml.udfs.Udfs.ToArray", ("System", "ArrayOfAny"), List(("arr", "System", "SortedSetOfAny")), null)
 		mgr.AddFunc("Pmml", "ToArray", "com.ligadata.pmml.udfs.Udfs.ToArray", ("System", "ArrayOfAny"), List(("arr", "System", "TreeSetOfAny")), null)
 		mgr.AddFunc("Pmml", "ToArray", "com.ligadata.pmml.udfs.Udfs.ToArray", ("System", "ArrayOfAny"), List(("arr", "System", "SetOfAny")), null)
		mgr.AddFunc("Pmml", "ToArray", "com.ligadata.pmml.udfs.Udfs.ToArray", ("System", "ArrayOfAny"), List(("arr", "System", "ListOfAny")), null)
		mgr.AddFunc("Pmml", "ToArray", "com.ligadata.pmml.udfs.Udfs.ToArray", ("System", "ArrayOfAny"), List(("arr", "System", "QueueOfAny")), null)
 		//mgr.AddFunc("Pmml", "ToArray", "com.ligadata.pmml.udfs.Udfs.ToArray", ("System", "ArrayOfAny"), List(("arr", "System", "StackOfAny")), null)
		
		mgr.AddFunc("Pmml", "MakeStrings", "com.ligadata.pmml.udfs.Udfs.MakeStrings", ("System", "ArrayOfString"), List(("arr", "System", "ArrayOfTupleOfStringString"),("separator", "System", "String")), null)
		mgr.AddFunc("Pmml", "MakeOrderedPairs", "com.ligadata.pmml.udfs.Udfs.MakeOrderedPairs", ("System", "ArrayOfTupleOfStringString"), List(("left", "System", "String"),("right", "System", "ArrayBufferOfString")), null)
		mgr.AddFunc("Pmml", "MakeOrderedPairs", "com.ligadata.pmml.udfs.Udfs.MakeOrderedPairs", ("System", "ArrayOfTupleOfStringString"), List(("left", "System", "String"),("right", "System", "ArrayOfString")), null)
		mgr.AddFunc("Pmml", "MakePairs", "com.ligadata.pmml.udfs.Udfs.MakePairs", ("System", "ArrayOfTupleOfStringString"), List(("left", "System", "String"),("right", "System", "ArrayOfString")), null)
		mgr.AddFunc("Pmml", "AgeCalc", "com.ligadata.pmml.udfs.Udfs.AgeCalc", ("System", "Int"), List(("yyyymmdd", "System", "Int")), null)
		mgr.AddFunc("Pmml", "CompressedTimeHHMMSSCC2Secs", "com.ligadata.pmml.udfs.Udfs.CompressedTimeHHMMSSCC2Secs", ("System", "Int"), List(("compressedTime", "System", "Int")), null)
		mgr.AddFunc("Pmml", "AsSeconds", "com.ligadata.pmml.udfs.Udfs.AsSeconds", ("System", "Long"), List(("milliSecs", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Timenow", "com.ligadata.pmml.udfs.Udfs.Timenow", ("System", "Long"), List(), null)
		mgr.AddFunc("Pmml", "dateMilliSecondsSinceMidnight", "com.ligadata.pmml.udfs.Udfs.dateMilliSecondsSinceMidnight", ("System", "Int"), List(), null)
		mgr.AddFunc("Pmml", "dateSecondsSinceMidnight", "com.ligadata.pmml.udfs.Udfs.dateSecondsSinceMidnight", ("System", "Int"), List(), null)
		mgr.AddFunc("Pmml", "dateSecondsSinceYear", "com.ligadata.pmml.udfs.Udfs.dateSecondsSinceYear", ("System", "Int"), List(("yr", "System", "Int")), null)
		mgr.AddFunc("Pmml", "dateDaysSinceYear", "com.ligadata.pmml.udfs.Udfs.dateDaysSinceYear", ("System", "Int"), List(("yr", "System", "Int")), null)
		mgr.AddFunc("Pmml", "trimBlanks", "com.ligadata.pmml.udfs.Udfs.trimBlanks", ("System", "String"), List(("str", "System", "String")), null)
		mgr.AddFunc("Pmml", "endsWith", "com.ligadata.pmml.udfs.Udfs.endsWith", ("System", "Boolean"), List(("inThis", "System", "String"),("findThis", "System", "String")), null)
		mgr.AddFunc("Pmml", "startsWith", "com.ligadata.pmml.udfs.Udfs.startsWith", ("System", "Boolean"), List(("inThis", "System", "String"),("findThis", "System", "String")), null)
		mgr.AddFunc("Pmml", "substring", "com.ligadata.pmml.udfs.Udfs.substring", ("System", "String"), List(("str", "System", "String"),("startidx", "System", "Int")), null)
		mgr.AddFunc("Pmml", "substring", "com.ligadata.pmml.udfs.Udfs.substring", ("System", "String"), List(("str", "System", "String"),("startidx", "System", "Int"),("len", "System", "Int")), null)
		mgr.AddFunc("Pmml", "lowercase", "com.ligadata.pmml.udfs.Udfs.lowercase", ("System", "String"), List(("str", "System", "String")), null)
		mgr.AddFunc("Pmml", "uppercase", "com.ligadata.pmml.udfs.Udfs.uppercase", ("System", "String"), List(("str", "System", "String")), null)
		mgr.AddFunc("Pmml", "round", "com.ligadata.pmml.udfs.Udfs.round", ("System", "Double"), List(("expr", "System", "Double")), null)
		mgr.AddFunc("Pmml", "ceil", "com.ligadata.pmml.udfs.Udfs.ceil", ("System", "Double"), List(("expr", "System", "Double")), null)
		mgr.AddFunc("Pmml", "floor", "com.ligadata.pmml.udfs.Udfs.floor", ("System", "Double"), List(("expr", "System", "Double")), null)
		mgr.AddFunc("Pmml", "threshold", "com.ligadata.pmml.udfs.Udfs.threshold", ("System", "Int"), List(("x", "System", "Double"),("y", "System", "Double")), null)
		mgr.AddFunc("Pmml", "threshold", "com.ligadata.pmml.udfs.Udfs.threshold", ("System", "Int"), List(("x", "System", "Float"),("y", "System", "Float")), null)
		mgr.AddFunc("Pmml", "threshold", "com.ligadata.pmml.udfs.Udfs.threshold", ("System", "Int"), List(("x", "System", "Long"),("y", "System", "Long")), null)
		mgr.AddFunc("Pmml", "threshold", "com.ligadata.pmml.udfs.Udfs.threshold", ("System", "Int"), List(("x", "System", "Int"),("y", "System", "Int")), null)
		mgr.AddFunc("Pmml", "pow", "com.ligadata.pmml.udfs.Udfs.pow", ("System", "Double"), List(("x", "System", "Double"),("y", "System", "Int")), null)
		mgr.AddFunc("Pmml", "exp", "com.ligadata.pmml.udfs.Udfs.exp", ("System", "Double"), List(("expr", "System", "Double")), null)
		mgr.AddFunc("Pmml", "abs", "com.ligadata.pmml.udfs.Udfs.abs", ("System", "Double"), List(("expr", "System", "Double")), null)
		mgr.AddFunc("Pmml", "abs", "com.ligadata.pmml.udfs.Udfs.abs", ("System", "Float"), List(("expr", "System", "Float")), null)
		mgr.AddFunc("Pmml", "abs", "com.ligadata.pmml.udfs.Udfs.abs", ("System", "Long"), List(("expr", "System", "Long")), null)
		mgr.AddFunc("Pmml", "abs", "com.ligadata.pmml.udfs.Udfs.abs", ("System", "Int"), List(("expr", "System", "Int")), null)
		mgr.AddFunc("Pmml", "sqrt", "com.ligadata.pmml.udfs.Udfs.sqrt", ("System", "Double"), List(("expr", "System", "Double")), null)
		mgr.AddFunc("Pmml", "ln", "com.ligadata.pmml.udfs.Udfs.ln", ("System", "Double"), List(("expr", "System", "Double")), null)
		mgr.AddFunc("Pmml", "log10", "com.ligadata.pmml.udfs.Udfs.log10", ("System", "Double"), List(("expr", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Product", "com.ligadata.pmml.udfs.Udfs.Product", ("System", "Float"), List(("exprs", "System", "ArrayOfFloat")), null)
		mgr.AddFunc("Pmml", "Product", "com.ligadata.pmml.udfs.Udfs.Product", ("System", "Double"), List(("exprs", "System", "ArrayOfDouble")), null)
		mgr.AddFunc("Pmml", "Product", "com.ligadata.pmml.udfs.Udfs.Product", ("System", "Long"), List(("exprs", "System", "ArrayOfLong")), null)
		mgr.AddFunc("Pmml", "Product", "com.ligadata.pmml.udfs.Udfs.Product", ("System", "Int"), List(("exprs", "System", "ArrayOfInt")), null)
		mgr.AddFunc("Pmml", "Product", "com.ligadata.pmml.udfs.Udfs.Product", ("System", "Float"), List(("exprs", "System", "ArrayBufferOfFloat")), null)
		mgr.AddFunc("Pmml", "Product", "com.ligadata.pmml.udfs.Udfs.Product", ("System", "Double"), List(("exprs", "System", "ArrayBufferOfDouble")), null)
		mgr.AddFunc("Pmml", "Product", "com.ligadata.pmml.udfs.Udfs.Product", ("System", "Long"), List(("exprs", "System", "ArrayBufferOfLong")), null)
		mgr.AddFunc("Pmml", "Product", "com.ligadata.pmml.udfs.Udfs.Product", ("System", "Int"), List(("exprs", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "Median", "com.ligadata.pmml.udfs.Udfs.Median", ("System", "Float"), List(("exprs", "System", "ArrayOfFloat")), null)
		mgr.AddFunc("Pmml", "Median", "com.ligadata.pmml.udfs.Udfs.Median", ("System", "Double"), List(("exprs", "System", "ArrayOfDouble")), null)
		mgr.AddFunc("Pmml", "Median", "com.ligadata.pmml.udfs.Udfs.Median", ("System", "Long"), List(("exprs", "System", "ArrayOfLong")), null)
		mgr.AddFunc("Pmml", "Median", "com.ligadata.pmml.udfs.Udfs.Median", ("System", "Int"), List(("exprs", "System", "ArrayOfInt")), null)
		mgr.AddFunc("Pmml", "Median", "com.ligadata.pmml.udfs.Udfs.Median", ("System", "Float"), List(("exprs", "System", "ArrayBufferOfFloat")), null)
		mgr.AddFunc("Pmml", "Median", "com.ligadata.pmml.udfs.Udfs.Median", ("System", "Double"), List(("exprs", "System", "ArrayBufferOfDouble")), null)
		mgr.AddFunc("Pmml", "Median", "com.ligadata.pmml.udfs.Udfs.Median", ("System", "Long"), List(("exprs", "System", "ArrayBufferOfLong")), null)
		mgr.AddFunc("Pmml", "Median", "com.ligadata.pmml.udfs.Udfs.Median", ("System", "Int"), List(("exprs", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "HashMapOfIntInt"), List(("exprs", "System", "ArrayBufferOfInt"),("groupByKey", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "HashMapOfIntInt"), List(("exprs", "System", "ArrayBufferOfInt"),("groupByKey", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "Count", "com.ligadata.pmml.udfs.Udfs.Count", ("System", "HashMapOfIntInt"), List(("exprs", "System", "ArrayBufferOfInt"),("groupByKey", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "Avg", "com.ligadata.pmml.udfs.Udfs.Avg", ("System", "HashMapOfIntInt"), List(("exprs", "System", "ArrayBufferOfInt"),("groupByKey", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "Sum", "com.ligadata.pmml.udfs.Udfs.Sum", ("System", "HashMapOfIntInt"), List(("exprs", "System", "ArrayBufferOfInt"),("groupByKey", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "MultiSet", "com.ligadata.pmml.udfs.Udfs.MultiSet", ("System", "HashMapOfIntArrayBufferOfInt"), List(("exprs", "System", "ArrayBufferOfInt"),("groupByKey", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "Count", "com.ligadata.pmml.udfs.Udfs.Count", ("System", "Float"), List(("exprs", "System", "ListOfFloat")), null)
		mgr.AddFunc("Pmml", "Count", "com.ligadata.pmml.udfs.Udfs.Count", ("System", "Double"), List(("exprs", "System", "ListOfDouble")), null)
		mgr.AddFunc("Pmml", "Count", "com.ligadata.pmml.udfs.Udfs.Count", ("System", "Long"), List(("exprs", "System", "ListOfLong")), null)
		mgr.AddFunc("Pmml", "Count", "com.ligadata.pmml.udfs.Udfs.Count", ("System", "Int"), List(("exprs", "System", "ListOfInt")), null)
		mgr.AddFunc("Pmml", "Count", "com.ligadata.pmml.udfs.Udfs.Count", ("System", "Float"), List(("exprs", "System", "ArrayOfFloat")), null)
		mgr.AddFunc("Pmml", "Count", "com.ligadata.pmml.udfs.Udfs.Count", ("System", "Double"), List(("exprs", "System", "ArrayOfDouble")), null)
		mgr.AddFunc("Pmml", "Count", "com.ligadata.pmml.udfs.Udfs.Count", ("System", "Long"), List(("exprs", "System", "ArrayOfLong")), null)
		mgr.AddFunc("Pmml", "Count", "com.ligadata.pmml.udfs.Udfs.Count", ("System", "Int"), List(("exprs", "System", "ArrayOfInt")), null)
		mgr.AddFunc("Pmml", "Count", "com.ligadata.pmml.udfs.Udfs.Count", ("System", "Float"), List(("exprs", "System", "ArrayBufferOfFloat")), null)
		mgr.AddFunc("Pmml", "Count", "com.ligadata.pmml.udfs.Udfs.Count", ("System", "Double"), List(("exprs", "System", "ArrayBufferOfDouble")), null)
		mgr.AddFunc("Pmml", "Count", "com.ligadata.pmml.udfs.Udfs.Count", ("System", "Long"), List(("exprs", "System", "ArrayBufferOfLong")), null)
		mgr.AddFunc("Pmml", "Count", "com.ligadata.pmml.udfs.Udfs.Count", ("System", "Int"), List(("exprs", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "Avg", "com.ligadata.pmml.udfs.Udfs.Avg", ("System", "Float"), List(("exprs", "System", "ListOfFloat")), null)
		mgr.AddFunc("Pmml", "Avg", "com.ligadata.pmml.udfs.Udfs.Avg", ("System", "Double"), List(("exprs", "System", "ListOfDouble")), null)
		mgr.AddFunc("Pmml", "Avg", "com.ligadata.pmml.udfs.Udfs.Avg", ("System", "Long"), List(("exprs", "System", "ListOfLong")), null)
		mgr.AddFunc("Pmml", "Avg", "com.ligadata.pmml.udfs.Udfs.Avg", ("System", "Int"), List(("exprs", "System", "ListOfInt")), null)
		mgr.AddFunc("Pmml", "Avg", "com.ligadata.pmml.udfs.Udfs.Avg", ("System", "Float"), List(("exprs", "System", "ArrayOfFloat")), null)
		mgr.AddFunc("Pmml", "Avg", "com.ligadata.pmml.udfs.Udfs.Avg", ("System", "Double"), List(("exprs", "System", "ArrayOfDouble")), null)
		mgr.AddFunc("Pmml", "Avg", "com.ligadata.pmml.udfs.Udfs.Avg", ("System", "Long"), List(("exprs", "System", "ArrayOfLong")), null)
		mgr.AddFunc("Pmml", "Avg", "com.ligadata.pmml.udfs.Udfs.Avg", ("System", "Int"), List(("exprs", "System", "ArrayOfInt")), null)
		mgr.AddFunc("Pmml", "Avg", "com.ligadata.pmml.udfs.Udfs.Avg", ("System", "Float"), List(("exprs", "System", "ArrayBufferOfFloat")), null)
		mgr.AddFunc("Pmml", "Avg", "com.ligadata.pmml.udfs.Udfs.Avg", ("System", "Double"), List(("exprs", "System", "ArrayBufferOfDouble")), null)
		mgr.AddFunc("Pmml", "Avg", "com.ligadata.pmml.udfs.Udfs.Avg", ("System", "Long"), List(("exprs", "System", "ArrayBufferOfLong")), null)
		mgr.AddFunc("Pmml", "Avg", "com.ligadata.pmml.udfs.Udfs.Avg", ("System", "Int"), List(("exprs", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "Sum", "com.ligadata.pmml.udfs.Udfs.Sum", ("System", "Float"), List(("exprs", "System", "ListOfFloat")), null)
		mgr.AddFunc("Pmml", "Sum", "com.ligadata.pmml.udfs.Udfs.Sum", ("System", "Double"), List(("exprs", "System", "ListOfDouble")), null)
		mgr.AddFunc("Pmml", "Sum", "com.ligadata.pmml.udfs.Udfs.Sum", ("System", "Long"), List(("exprs", "System", "ListOfLong")), null)
		mgr.AddFunc("Pmml", "Sum", "com.ligadata.pmml.udfs.Udfs.Sum", ("System", "Int"), List(("exprs", "System", "ListOfInt")), null)
		mgr.AddFunc("Pmml", "Sum", "com.ligadata.pmml.udfs.Udfs.Sum", ("System", "Float"), List(("exprs", "System", "ArrayOfFloat")), null)
		mgr.AddFunc("Pmml", "Sum", "com.ligadata.pmml.udfs.Udfs.Sum", ("System", "Double"), List(("exprs", "System", "ArrayOfDouble")), null)
		mgr.AddFunc("Pmml", "Sum", "com.ligadata.pmml.udfs.Udfs.Sum", ("System", "Long"), List(("exprs", "System", "ArrayOfLong")), null)
		mgr.AddFunc("Pmml", "Sum", "com.ligadata.pmml.udfs.Udfs.Sum", ("System", "Int"), List(("exprs", "System", "ArrayOfInt")), null)
		mgr.AddFunc("Pmml", "Sum", "com.ligadata.pmml.udfs.Udfs.Sum", ("System", "Float"), List(("exprs", "System", "ArrayBufferOfFloat")), null)
		mgr.AddFunc("Pmml", "Sum", "com.ligadata.pmml.udfs.Udfs.Sum", ("System", "Double"), List(("exprs", "System", "ArrayBufferOfDouble")), null)
		mgr.AddFunc("Pmml", "Sum", "com.ligadata.pmml.udfs.Udfs.Sum", ("System", "Long"), List(("exprs", "System", "ArrayBufferOfLong")), null)
		mgr.AddFunc("Pmml", "Sum", "com.ligadata.pmml.udfs.Udfs.Sum", ("System", "Int"), List(("exprs", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Float"), List(("exprs", "System", "ListOfFloat")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Double"), List(("exprs", "System", "ListOfDouble")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Long"), List(("exprs", "System", "ListOfLong")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Int"), List(("exprs", "System", "ListOfInt")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Float"), List(("exprs", "System", "ArrayOfFloat")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Double"), List(("exprs", "System", "ArrayOfDouble")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Long"), List(("exprs", "System", "ArrayOfLong")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Int"), List(("exprs", "System", "ArrayOfInt")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Float"), List(("exprs", "System", "ArrayBufferOfFloat")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Double"), List(("exprs", "System", "ArrayBufferOfDouble")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Long"), List(("exprs", "System", "ArrayBufferOfLong")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Int"), List(("exprs", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Float"), List(("expr1", "System", "Float"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Double"), List(("expr1", "System", "Float"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Float"), List(("expr1", "System", "Float"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Float"), List(("expr1", "System", "Long"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Double"), List(("expr1", "System", "Long"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Long"), List(("expr1", "System", "Long"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Float"), List(("expr1", "System", "Float"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Float"), List(("expr1", "System", "Int"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Long"), List(("expr1", "System", "Long"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Double"), List(("expr1", "System", "Int"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Double"), List(("expr1", "System", "Int"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Max", "com.ligadata.pmml.udfs.Udfs.Max", ("System", "Int"), List(("expr1", "System", "Int"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Float"), List(("exprs", "System", "ListOfFloat")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Double"), List(("exprs", "System", "ListOfDouble")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Long"), List(("exprs", "System", "ListOfLong")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Int"), List(("exprs", "System", "ListOfInt")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Float"), List(("exprs", "System", "ArrayOfFloat")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Double"), List(("exprs", "System", "ArrayOfDouble")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Long"), List(("exprs", "System", "ArrayOfLong")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Int"), List(("exprs", "System", "ArrayOfInt")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Float"), List(("exprs", "System", "ArrayBufferOfFloat")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Double"), List(("exprs", "System", "ArrayBufferOfDouble")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Long"), List(("exprs", "System", "ArrayBufferOfLong")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Int"), List(("exprs", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Float"), List(("expr1", "System", "Float"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Double"), List(("expr1", "System", "Float"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Float"), List(("expr1", "System", "Float"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Float"), List(("expr1", "System", "Long"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Double"), List(("expr1", "System", "Long"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Long"), List(("expr1", "System", "Long"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Float"), List(("expr1", "System", "Float"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Float"), List(("expr1", "System", "Int"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Long"), List(("expr1", "System", "Long"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Double"), List(("expr1", "System", "Int"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Double"), List(("expr1", "System", "Int"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Min", "com.ligadata.pmml.udfs.Udfs.Min", ("System", "Int"), List(("expr1", "System", "Int"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Float"), List(("exprs", "System", "ArrayBufferOfFloat")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Double"), List(("exprs", "System", "ArrayBufferOfDouble")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Long"), List(("exprs", "System", "ArrayBufferOfLong")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Int"), List(("exprs", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Float"), List(("expr1", "System", "Float"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Double"), List(("expr1", "System", "Float"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Float"), List(("expr1", "System", "Float"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Float"), List(("expr1", "System", "Long"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Double"), List(("expr1", "System", "Long"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Long"), List(("expr1", "System", "Long"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Float"), List(("expr1", "System", "Float"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Float"), List(("expr1", "System", "Int"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Long"), List(("expr1", "System", "Long"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Double"), List(("expr1", "System", "Int"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Double"), List(("expr1", "System", "Int"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Divide", "com.ligadata.pmml.udfs.Udfs.Divide", ("System", "Int"), List(("expr1", "System", "Int"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Float"), List(("exprs", "System", "ArrayOfFloat")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Double"), List(("exprs", "System", "ArrayOfDouble")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Long"), List(("exprs", "System", "ArrayOfLong")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Int"), List(("exprs", "System", "ArrayOfInt")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Float"), List(("exprs", "System", "ArrayBufferOfFloat")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Double"), List(("exprs", "System", "ArrayBufferOfDouble")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Long"), List(("exprs", "System", "ArrayBufferOfLong")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Int"), List(("exprs", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Float"), List(("expr1", "System", "Float"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Double"), List(("expr1", "System", "Float"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Float"), List(("expr1", "System", "Float"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Float"), List(("expr1", "System", "Long"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Double"), List(("expr1", "System", "Long"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Long"), List(("expr1", "System", "Long"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Float"), List(("expr1", "System", "Float"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Float"), List(("expr1", "System", "Int"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Long"), List(("expr1", "System", "Long"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Double"), List(("expr1", "System", "Int"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Double"), List(("expr1", "System", "Int"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Multiply", "com.ligadata.pmml.udfs.Udfs.Multiply", ("System", "Int"), List(("expr1", "System", "Int"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Float"), List(("exprs", "System", "ArrayOfFloat")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Double"), List(("exprs", "System", "ArrayOfDouble")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Long"), List(("exprs", "System", "ArrayOfLong")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Int"), List(("exprs", "System", "ArrayOfInt")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Float"), List(("exprs", "System", "ArrayBufferOfFloat")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Double"), List(("exprs", "System", "ArrayBufferOfDouble")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Long"), List(("exprs", "System", "ArrayBufferOfLong")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Int"), List(("exprs", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Float"), List(("expr1", "System", "Float"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Double"), List(("expr1", "System", "Float"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Float"), List(("expr1", "System", "Float"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Float"), List(("expr1", "System", "Long"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Double"), List(("expr1", "System", "Long"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Long"), List(("expr1", "System", "Long"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Float"), List(("expr1", "System", "Float"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Float"), List(("expr1", "System", "Int"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Long"), List(("expr1", "System", "Long"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Double"), List(("expr1", "System", "Int"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Double"), List(("expr1", "System", "Int"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Minus", "com.ligadata.pmml.udfs.Udfs.Minus", ("System", "Int"), List(("expr1", "System", "Int"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Float"), List(("exprs", "System", "ArrayOfFloat")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Double"), List(("exprs", "System", "ArrayOfDouble")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Long"), List(("exprs", "System", "ArrayOfLong")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Int"), List(("exprs", "System", "ArrayOfInt")), null)
		//mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "String"), List(("exprs", "System", "ArrayOfString")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Float"), List(("exprs", "System", "ArrayBufferOfFloat")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Double"), List(("exprs", "System", "ArrayBufferOfDouble")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Long"), List(("exprs", "System", "ArrayBufferOfLong")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Int"), List(("exprs", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "String"), List(("exprs", "System", "ArrayBufferOfString")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Float"), List(("expr1", "System", "Float"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Double"), List(("expr1", "System", "Float"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Float"), List(("expr1", "System", "Float"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Float"), List(("expr1", "System", "Long"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Double"), List(("expr1", "System", "Long"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Long"), List(("expr1", "System", "Long"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Float"), List(("expr1", "System", "Float"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Float"), List(("expr1", "System", "Int"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Long"), List(("expr1", "System", "Long"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Double"), List(("expr1", "System", "Int"),("expr2", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Double"), List(("expr1", "System", "Int"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Double"),("expr3", "System", "Double"),("expr4", "System", "Double"),("expr5", "System", "Double"),("expr6", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Double"),("expr3", "System", "Double"),("expr4", "System", "Double"),("expr5", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Double"),("expr3", "System", "Double"),("expr4", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Double"),("expr3", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Double"), List(("expr1", "System", "Double"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Long"), List(("expr1", "System", "Int"),("expr2", "System", "Int"),("expr3", "System", "Int"),("expr4", "System", "Int"),("expr5", "System", "Int"),("expr6", "System", "Int"),("expr7", "System", "Int"),("expr8", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Int"), List(("expr1", "System", "Int"),("expr2", "System", "Int"),("expr3", "System", "Int"),("expr4", "System", "Int"),("expr5", "System", "Int"),("expr6", "System", "Int"),("expr7", "System", "Int"),("expr8", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Long"), List(("expr1", "System", "Long"),("expr2", "System", "Long"),("expr3", "System", "Long"),("expr4", "System", "Long"),("expr5", "System", "Long"),("expr6", "System", "Long"),("expr7", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Int"), List(("expr1", "System", "Int"),("expr2", "System", "Int"),("expr3", "System", "Int"),("expr4", "System", "Int"),("expr5", "System", "Int"),("expr6", "System", "Int"),("expr7", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Long"), List(("expr1", "System", "Long"),("expr2", "System", "Long"),("expr3", "System", "Long"),("expr4", "System", "Long"),("expr5", "System", "Long"),("expr6", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Int"), List(("expr1", "System", "Int"),("expr2", "System", "Int"),("expr3", "System", "Int"),("expr4", "System", "Int"),("expr5", "System", "Int"),("expr6", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Long"), List(("expr1", "System", "Long"),("expr2", "System", "Long"),("expr3", "System", "Long"),("expr4", "System", "Long"),("expr5", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Int"), List(("expr1", "System", "Int"),("expr2", "System", "Int"),("expr3", "System", "Int"),("expr4", "System", "Int"),("expr5", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Long"), List(("expr1", "System", "Long"),("expr2", "System", "Long"),("expr3", "System", "Long"),("expr4", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Int"), List(("expr1", "System", "Int"),("expr2", "System", "Int"),("expr3", "System", "Int"),("expr4", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Long"), List(("expr1", "System", "Long"),("expr2", "System", "Long"),("expr3", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Int"), List(("expr1", "System", "Int"),("expr2", "System", "Int"),("expr3", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "Int"), List(("expr1", "System", "Int"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "String"), List(("expr1", "System", "String"),("expr2", "System", "String")), null)
		mgr.AddFunc("Pmml", "NotEqual", "com.ligadata.pmml.udfs.Udfs.NotEqual", ("System", "Boolean"), List(("expr1", "System", "Boolean"),("expr2", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "NotEqual", "com.ligadata.pmml.udfs.Udfs.NotEqual", ("System", "Boolean"), List(("expr1", "System", "Float"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "NotEqual", "com.ligadata.pmml.udfs.Udfs.NotEqual", ("System", "Boolean"), List(("expr1", "System", "Int"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "NotEqual", "com.ligadata.pmml.udfs.Udfs.NotEqual", ("System", "Boolean"), List(("expr1", "System", "Float"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "NotEqual", "com.ligadata.pmml.udfs.Udfs.NotEqual", ("System", "Boolean"), List(("expr1", "System", "Float"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "NotEqual", "com.ligadata.pmml.udfs.Udfs.NotEqual", ("System", "Boolean"), List(("expr1", "System", "Double"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "NotEqual", "com.ligadata.pmml.udfs.Udfs.NotEqual", ("System", "Boolean"), List(("expr1", "System", "Double"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "NotEqual", "com.ligadata.pmml.udfs.Udfs.NotEqual", ("System", "Boolean"), List(("expr1", "System", "Int"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "NotEqual", "com.ligadata.pmml.udfs.Udfs.NotEqual", ("System", "Boolean"), List(("expr1", "System", "Double"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "NotEqual", "com.ligadata.pmml.udfs.Udfs.NotEqual", ("System", "Boolean"), List(("expr1", "System", "Int"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "NotEqual", "com.ligadata.pmml.udfs.Udfs.NotEqual", ("System", "Boolean"), List(("expr1", "System", "String"),("expr2", "System", "String")), null)
		mgr.AddFunc("Pmml", "Equal", "com.ligadata.pmml.udfs.Udfs.Equal", ("System", "Boolean"), List(("expr1", "System", "Boolean"),("expr2", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "Equal", "com.ligadata.pmml.udfs.Udfs.Equal", ("System", "Boolean"), List(("expr1", "System", "Float"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Equal", "com.ligadata.pmml.udfs.Udfs.Equal", ("System", "Boolean"), List(("expr1", "System", "Int"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Equal", "com.ligadata.pmml.udfs.Udfs.Equal", ("System", "Boolean"), List(("expr1", "System", "Float"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Equal", "com.ligadata.pmml.udfs.Udfs.Equal", ("System", "Boolean"), List(("expr1", "System", "Float"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Equal", "com.ligadata.pmml.udfs.Udfs.Equal", ("System", "Boolean"), List(("expr1", "System", "Double"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Equal", "com.ligadata.pmml.udfs.Udfs.Equal", ("System", "Boolean"), List(("expr1", "System", "Double"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Equal", "com.ligadata.pmml.udfs.Udfs.Equal", ("System", "Boolean"), List(("expr1", "System", "Int"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Equal", "com.ligadata.pmml.udfs.Udfs.Equal", ("System", "Boolean"), List(("expr1", "System", "Double"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Equal", "com.ligadata.pmml.udfs.Udfs.Equal", ("System", "Boolean"), List(("expr1", "System", "Int"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Equal", "com.ligadata.pmml.udfs.Udfs.Equal", ("System", "Boolean"), List(("expr1", "System", "String"),("expr2", "System", "String")), null)
		mgr.AddFunc("Pmml", "LessThan", "com.ligadata.pmml.udfs.Udfs.LessThan", ("System", "Boolean"), List(("expr1", "System", "Float"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "LessThan", "com.ligadata.pmml.udfs.Udfs.LessThan", ("System", "Boolean"), List(("expr1", "System", "Int"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "LessThan", "com.ligadata.pmml.udfs.Udfs.LessThan", ("System", "Boolean"), List(("expr1", "System", "Float"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "LessThan", "com.ligadata.pmml.udfs.Udfs.LessThan", ("System", "Boolean"), List(("expr1", "System", "Float"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "LessThan", "com.ligadata.pmml.udfs.Udfs.LessThan", ("System", "Boolean"), List(("expr1", "System", "Double"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "LessThan", "com.ligadata.pmml.udfs.Udfs.LessThan", ("System", "Boolean"), List(("expr1", "System", "Double"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "LessThan", "com.ligadata.pmml.udfs.Udfs.LessThan", ("System", "Boolean"), List(("expr1", "System", "Int"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "LessThan", "com.ligadata.pmml.udfs.Udfs.LessThan", ("System", "Boolean"), List(("expr1", "System", "Double"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "LessThan", "com.ligadata.pmml.udfs.Udfs.LessThan", ("System", "Boolean"), List(("expr1", "System", "Int"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "LessThan", "com.ligadata.pmml.udfs.Udfs.LessThan", ("System", "Boolean"), List(("expr1", "System", "String"),("expr2", "System", "String")), null)
		mgr.AddFunc("Pmml", "LessOrEqual", "com.ligadata.pmml.udfs.Udfs.LessOrEqual", ("System", "Boolean"), List(("expr1", "System", "Float"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "LessOrEqual", "com.ligadata.pmml.udfs.Udfs.LessOrEqual", ("System", "Boolean"), List(("expr1", "System", "Int"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "LessOrEqual", "com.ligadata.pmml.udfs.Udfs.LessOrEqual", ("System", "Boolean"), List(("expr1", "System", "Float"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "LessOrEqual", "com.ligadata.pmml.udfs.Udfs.LessOrEqual", ("System", "Boolean"), List(("expr1", "System", "Float"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "LessOrEqual", "com.ligadata.pmml.udfs.Udfs.LessOrEqual", ("System", "Boolean"), List(("expr1", "System", "Double"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "LessOrEqual", "com.ligadata.pmml.udfs.Udfs.LessOrEqual", ("System", "Boolean"), List(("expr1", "System", "Double"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "LessOrEqual", "com.ligadata.pmml.udfs.Udfs.LessOrEqual", ("System", "Boolean"), List(("expr1", "System", "Int"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "LessOrEqual", "com.ligadata.pmml.udfs.Udfs.LessOrEqual", ("System", "Boolean"), List(("expr1", "System", "Double"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "LessOrEqual", "com.ligadata.pmml.udfs.Udfs.LessOrEqual", ("System", "Boolean"), List(("expr1", "System", "Int"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "LessOrEqual", "com.ligadata.pmml.udfs.Udfs.LessOrEqual", ("System", "Boolean"), List(("expr1", "System", "String"),("expr2", "System", "String")), null)
		mgr.AddFunc("Pmml", "GreaterOrEqual", "com.ligadata.pmml.udfs.Udfs.GreaterOrEqual", ("System", "Boolean"), List(("expr1", "System", "Float"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "GreaterOrEqual", "com.ligadata.pmml.udfs.Udfs.GreaterOrEqual", ("System", "Boolean"), List(("expr1", "System", "Int"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "GreaterOrEqual", "com.ligadata.pmml.udfs.Udfs.GreaterOrEqual", ("System", "Boolean"), List(("expr1", "System", "Float"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "GreaterOrEqual", "com.ligadata.pmml.udfs.Udfs.GreaterOrEqual", ("System", "Boolean"), List(("expr1", "System", "Float"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "GreaterOrEqual", "com.ligadata.pmml.udfs.Udfs.GreaterOrEqual", ("System", "Boolean"), List(("expr1", "System", "Double"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "GreaterOrEqual", "com.ligadata.pmml.udfs.Udfs.GreaterOrEqual", ("System", "Boolean"), List(("expr1", "System", "Double"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "GreaterOrEqual", "com.ligadata.pmml.udfs.Udfs.GreaterOrEqual", ("System", "Boolean"), List(("expr1", "System", "Int"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "GreaterOrEqual", "com.ligadata.pmml.udfs.Udfs.GreaterOrEqual", ("System", "Boolean"), List(("expr1", "System", "Double"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "GreaterOrEqual", "com.ligadata.pmml.udfs.Udfs.GreaterOrEqual", ("System", "Boolean"), List(("expr1", "System", "Int"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "GreaterOrEqual", "com.ligadata.pmml.udfs.Udfs.GreaterOrEqual", ("System", "Boolean"), List(("expr1", "System", "String"),("expr2", "System", "String")), null)
		mgr.AddFunc("Pmml", "GreaterThan", "com.ligadata.pmml.udfs.Udfs.GreaterThan", ("System", "Boolean"), List(("expr1", "System", "Float"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "GreaterThan", "com.ligadata.pmml.udfs.Udfs.GreaterThan", ("System", "Boolean"), List(("expr1", "System", "Int"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "GreaterThan", "com.ligadata.pmml.udfs.Udfs.GreaterThan", ("System", "Boolean"), List(("expr1", "System", "Float"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "GreaterThan", "com.ligadata.pmml.udfs.Udfs.GreaterThan", ("System", "Boolean"), List(("expr1", "System", "Float"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "GreaterThan", "com.ligadata.pmml.udfs.Udfs.GreaterThan", ("System", "Boolean"), List(("expr1", "System", "Double"),("expr2", "System", "Float")), null)
		mgr.AddFunc("Pmml", "GreaterThan", "com.ligadata.pmml.udfs.Udfs.GreaterThan", ("System", "Boolean"), List(("expr1", "System", "Double"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "GreaterThan", "com.ligadata.pmml.udfs.Udfs.GreaterThan", ("System", "Boolean"), List(("expr1", "System", "Int"),("expr2", "System", "Double")), null)
		mgr.AddFunc("Pmml", "GreaterThan", "com.ligadata.pmml.udfs.Udfs.GreaterThan", ("System", "Boolean"), List(("expr1", "System", "Double"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "GreaterThan", "com.ligadata.pmml.udfs.Udfs.GreaterThan", ("System", "Boolean"), List(("expr1", "System", "Int"),("expr2", "System", "Int")), null)
		mgr.AddFunc("Pmml", "GreaterThan", "com.ligadata.pmml.udfs.Udfs.GreaterThan", ("System", "Boolean"), List(("expr1", "System", "String"),("expr2", "System", "String")), null)
		mgr.AddFunc("Pmml", "Between", "com.ligadata.pmml.udfs.Udfs.Between", ("System", "Boolean"), List(("thisOne", "System", "Float"),("leftMargin", "System", "Float"),("rightMargin", "System", "Float"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "Between", "com.ligadata.pmml.udfs.Udfs.Between", ("System", "Boolean"), List(("thisOne", "System", "Int"),("leftMargin", "System", "Int"),("rightMargin", "System", "Float"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "Between", "com.ligadata.pmml.udfs.Udfs.Between", ("System", "Boolean"), List(("thisOne", "System", "Float"),("leftMargin", "System", "Float"),("rightMargin", "System", "Int"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "Between", "com.ligadata.pmml.udfs.Udfs.Between", ("System", "Boolean"), List(("thisOne", "System", "Float"),("leftMargin", "System", "Float"),("rightMargin", "System", "Double"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "Between", "com.ligadata.pmml.udfs.Udfs.Between", ("System", "Boolean"), List(("thisOne", "System", "Double"),("leftMargin", "System", "Double"),("rightMargin", "System", "Float"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "Between", "com.ligadata.pmml.udfs.Udfs.Between", ("System", "Boolean"), List(("thisOne", "System", "Double"),("leftMargin", "System", "Double"),("rightMargin", "System", "Double"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "Between", "com.ligadata.pmml.udfs.Udfs.Between", ("System", "Boolean"), List(("thisOne", "System", "Int"),("leftMargin", "System", "Int"),("rightMargin", "System", "Double"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "Between", "com.ligadata.pmml.udfs.Udfs.Between", ("System", "Boolean"), List(("thisOne", "System", "Double"),("leftMargin", "System", "Double"),("rightMargin", "System", "Int"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "Between", "com.ligadata.pmml.udfs.Udfs.Between", ("System", "Boolean"), List(("thisOne", "System", "Int"),("leftMargin", "System", "Long"),("rightMargin", "System", "Long"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "Between", "com.ligadata.pmml.udfs.Udfs.Between", ("System", "Boolean"), List(("thisOne", "System", "Long"),("leftMargin", "System", "Long"),("rightMargin", "System", "Long"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "Between", "com.ligadata.pmml.udfs.Udfs.Between", ("System", "Boolean"), List(("thisOne", "System", "Int"),("leftMargin", "System", "Int"),("rightMargin", "System", "Int"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "Between", "com.ligadata.pmml.udfs.Udfs.Between", ("System", "Boolean"), List(("thisOne", "System", "String"),("leftMargin", "System", "String"),("rightMargin", "System", "String"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "IsNotIn", "com.ligadata.pmml.udfs.Udfs.IsNotIn", ("System", "Boolean"), List(("fldRefExpr", "System", "Double"),("setExprs", "System", "ListOfDouble")), null)
		mgr.AddFunc("Pmml", "IsNotIn", "com.ligadata.pmml.udfs.Udfs.IsNotIn", ("System", "Boolean"), List(("fldRefExpr", "System", "Float"),("setExprs", "System", "ListOfFloat")), null)
		mgr.AddFunc("Pmml", "IsNotIn", "com.ligadata.pmml.udfs.Udfs.IsNotIn", ("System", "Boolean"), List(("fldRefExpr", "System", "Int"),("setExprs", "System", "ListOfInt")), null)
		mgr.AddFunc("Pmml", "IsNotIn", "com.ligadata.pmml.udfs.Udfs.IsNotIn", ("System", "Boolean"), List(("fldRefExpr", "System", "String"),("setExprs", "System", "ListOfString")), null)
		mgr.AddFunc("Pmml", "IsNotIn", "com.ligadata.pmml.udfs.Udfs.IsNotIn", ("System", "Boolean"), List(("fldRefExpr", "System", "Double"),("setExprs", "System", "ArrayBufferOfDouble")), null)
		mgr.AddFunc("Pmml", "IsNotIn", "com.ligadata.pmml.udfs.Udfs.IsNotIn", ("System", "Boolean"), List(("fldRefExpr", "System", "Float"),("setExprs", "System", "ArrayBufferOfFloat")), null)
		mgr.AddFunc("Pmml", "IsNotIn", "com.ligadata.pmml.udfs.Udfs.IsNotIn", ("System", "Boolean"), List(("fldRefExpr", "System", "Int"),("setExprs", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "IsNotIn", "com.ligadata.pmml.udfs.Udfs.IsNotIn", ("System", "Boolean"), List(("fldRefExpr", "System", "String"),("setExprs", "System", "ArrayBufferOfString")), null)
		mgr.AddFunc("Pmml", "Not", "com.ligadata.pmml.udfs.Udfs.Not", ("System", "Boolean"), List(("boolexpr", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "First", "com.ligadata.pmml.udfs.Udfs.First", ("System", "Any"), List(("coll", "System", "SortedSetOfAny")), null)
		mgr.AddFunc("Pmml", "First", "com.ligadata.pmml.udfs.Udfs.First", ("System", "Any"), List(("coll", "System", "QueueOfAny")), null)
		mgr.AddFunc("Pmml", "First", "com.ligadata.pmml.udfs.Udfs.First", ("System", "Any"), List(("coll", "System", "ArrayBufferOfAny")), null)
		mgr.AddFunc("Pmml", "First", "com.ligadata.pmml.udfsFirstetween", ("System", "Any"), List(("coll", "System", "ArrayOfAny")), null)
		mgr.AddFunc("Pmml", "Last", "com.ligadata.pmml.udfs.Udfs.Last", ("System", "Any"), List(("coll", "System", "SortedSetOfAny")), null)
		mgr.AddFunc("Pmml", "Last", "com.ligadata.pmml.udfs.Udfs.Last", ("System", "Any"), List(("coll", "System", "QueueOfAny")), null)
		mgr.AddFunc("Pmml", "Last", "com.ligadata.pmml.udfs.Udfs.Last", ("System", "Any"), List(("coll", "System", "ArrayBufferOfAny")), null)
		mgr.AddFunc("Pmml", "Last", "com.ligadata.pmml.udfs.Udfs.Last", ("System", "Any"), List(("coll", "System", "ArrayOfAny")), null)
		mgr.AddFunc("Pmml", "Union", "com.ligadata.pmml.udfs.Udfs.Union", ("System", "SetOfAny"), List(("left", "System", "SetOfAny"),("right", "System", "SetOfAny")), null)
		mgr.AddFunc("Pmml", "Union", "com.ligadata.pmml.udfs.Udfs.Union", ("System", "SetOfAny"), List(("left", "System", "SetOfAny"),("right", "System", "ArrayOfAny")), null)
		mgr.AddFunc("Pmml", "Union", "com.ligadata.pmml.udfs.Udfs.Union", ("System", "SetOfAny"), List(("left", "System", "ArrayOfAny"),("right", "System", "SetOfAny")), null)
		mgr.AddFunc("Pmml", "Union", "com.ligadata.pmml.udfs.Udfs.Union", ("System", "SetOfAny"), List(("left", "System", "ArrayOfAny"),("right", "System", "ArrayOfAny")), null)
		mgr.AddFunc("Pmml", "Union", "com.ligadata.pmml.udfs.Udfs.Union", ("System", "SetOfAny"), List(("left", "System", "ArrayBufferOfAny"),("right", "System", "ArrayBufferOfAny")), null)
		mgr.AddFunc("Pmml", "Intersect", "com.ligadata.pmml.udfs.Udfs.Intersect", ("System", "SetOfAny"), List(("left", "System", "TreeSetOfAny"),("right", "System", "SetOfAny")), null)
		mgr.AddFunc("Pmml", "Intersect", "com.ligadata.pmml.udfs.Udfs.Intersect", ("System", "SetOfAny"), List(("left", "System", "SetOfAny"),("right", "System", "TreeSetOfAny")), null)
		mgr.AddFunc("Pmml", "Intersect", "com.ligadata.pmml.udfs.Udfs.Intersect", ("System", "SetOfAny"), List(("left", "System", "TreeSetOfAny"),("right", "System", "TreeSetOfAny")), null)
		mgr.AddFunc("Pmml", "Intersect", "com.ligadata.pmml.udfs.Udfs.Intersect", ("System", "SetOfAny"), List(("left", "System", "TreeSetOfAny"),("right", "System", "ArrayOfAny")), null)
		mgr.AddFunc("Pmml", "Intersect", "com.ligadata.pmml.udfs.Udfs.Intersect", ("System", "SetOfAny"), List(("left", "System", "ArrayOfAny"),("right", "System", "TreeSetOfAny")), null)
		mgr.AddFunc("Pmml", "Intersect", "com.ligadata.pmml.udfs.Udfs.Intersect", ("System", "SetOfAny"), List(("left", "System", "SetOfAny"),("right", "System", "SetOfAny")), null)
		mgr.AddFunc("Pmml", "Intersect", "com.ligadata.pmml.udfs.Udfs.Intersect", ("System", "SetOfAny"), List(("left", "System", "SetOfAny"),("right", "System", "ArrayOfAny")), null)
		mgr.AddFunc("Pmml", "Intersect", "com.ligadata.pmml.udfs.Udfs.Intersect", ("System", "SetOfAny"), List(("left", "System", "ArrayOfAny"),("right", "System", "SetOfAny")), null)
		mgr.AddFunc("Pmml", "Intersect", "com.ligadata.pmml.udfs.Udfs.Intersect", ("System", "SetOfAny"), List(("left", "System", "ArrayOfAny"),("right", "System", "ArrayOfAny")), null)
		mgr.AddFunc("Pmml", "Contains", "com.ligadata.pmml.udfs.Udfs.Contains", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayOfDouble"),("key", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Contains", "com.ligadata.pmml.udfs.Udfs.Contains", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayOfFloat"),("key", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Contains", "com.ligadata.pmml.udfs.Udfs.Contains", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayOfInt"),("key", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Contains", "com.ligadata.pmml.udfs.Udfs.Contains", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayOfLong"),("key", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Contains", "com.ligadata.pmml.udfs.Udfs.Contains", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayOfString"),("key", "System", "String")), null)
		mgr.AddFunc("Pmml", "Contains", "com.ligadata.pmml.udfs.Udfs.Contains", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayBufferOfDouble"),("key", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Contains", "com.ligadata.pmml.udfs.Udfs.Contains", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayBufferOfFloat"),("key", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Contains", "com.ligadata.pmml.udfs.Udfs.Contains", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayBufferOfInt"),("key", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Contains", "com.ligadata.pmml.udfs.Udfs.Contains", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayBufferOfLong"),("key", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Contains", "com.ligadata.pmml.udfs.Udfs.Contains", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayBufferOfString"),("key", "System", "String")), null)
		mgr.AddFunc("Pmml", "NotAnyBetween", "com.ligadata.pmml.udfs.Udfs.NotAnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayOfDouble"),("leftMargin", "System", "Double"),("rightMargin", "System", "Double"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "NotAnyBetween", "com.ligadata.pmml.udfs.Udfs.NotAnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayOfFloat"),("leftMargin", "System", "Float"),("rightMargin", "System", "Float"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "NotAnyBetween", "com.ligadata.pmml.udfs.Udfs.NotAnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayOfInt"),("leftMargin", "System", "Int"),("rightMargin", "System", "Int"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "NotAnyBetween", "com.ligadata.pmml.udfs.Udfs.NotAnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayOfLong"),("leftMargin", "System", "Long"),("rightMargin", "System", "Long"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "NotAnyBetween", "com.ligadata.pmml.udfs.Udfs.NotAnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayOfString"),("leftMargin", "System", "String"),("rightMargin", "System", "String"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "NotAnyBetween", "com.ligadata.pmml.udfs.Udfs.NotAnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayBufferOfDouble"),("leftMargin", "System", "Double"),("rightMargin", "System", "Double"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "NotAnyBetween", "com.ligadata.pmml.udfs.Udfs.NotAnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayBufferOfFloat"),("leftMargin", "System", "Float"),("rightMargin", "System", "Float"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "NotAnyBetween", "com.ligadata.pmml.udfs.Udfs.NotAnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayBufferOfLong"),("leftMargin", "System", "Long"),("rightMargin", "System", "Long"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "NotAnyBetween", "com.ligadata.pmml.udfs.Udfs.NotAnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayBufferOfInt"),("leftMargin", "System", "Int"),("rightMargin", "System", "Int"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "NotAnyBetween", "com.ligadata.pmml.udfs.Udfs.NotAnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayBufferOfString"),("leftMargin", "System", "String"),("rightMargin", "System", "String"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "AnyBetween", "com.ligadata.pmml.udfs.Udfs.AnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayOfDouble"),("leftMargin", "System", "Double"),("rightMargin", "System", "Double"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "AnyBetween", "com.ligadata.pmml.udfs.Udfs.AnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayOfFloat"),("leftMargin", "System", "Float"),("rightMargin", "System", "Float"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "AnyBetween", "com.ligadata.pmml.udfs.Udfs.AnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayOfInt"),("leftMargin", "System", "Int"),("rightMargin", "System", "Int"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "AnyBetween", "com.ligadata.pmml.udfs.Udfs.AnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayOfString"),("leftMargin", "System", "String"),("rightMargin", "System", "String"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "AnyBetween", "com.ligadata.pmml.udfs.Udfs.AnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayBufferOfDouble"),("leftMargin", "System", "Double"),("rightMargin", "System", "Double"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "AnyBetween", "com.ligadata.pmml.udfs.Udfs.AnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayBufferOfFloat"),("leftMargin", "System", "Float"),("rightMargin", "System", "Float"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "AnyBetween", "com.ligadata.pmml.udfs.Udfs.AnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayBufferOfLong"),("leftMargin", "System", "Long"),("rightMargin", "System", "Long"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "AnyBetween", "com.ligadata.pmml.udfs.Udfs.AnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayBufferOfInt"),("leftMargin", "System", "Int"),("rightMargin", "System", "Int"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "AnyBetween", "com.ligadata.pmml.udfs.Udfs.AnyBetween", ("System", "Boolean"), List(("arrayExpr", "System", "ArrayBufferOfString"),("leftMargin", "System", "String"),("rightMargin", "System", "String"),("inclusive", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "IsIn", "com.ligadata.pmml.udfs.Udfs.IsIn", ("System", "Boolean"), List(("fldRefExpr", "System", "Double"),("setExprs", "System", "ListOfDouble")), null)
		mgr.AddFunc("Pmml", "IsIn", "com.ligadata.pmml.udfs.Udfs.IsIn", ("System", "Boolean"), List(("fldRefExpr", "System", "Float"),("setExprs", "System", "ListOfFloat")), null)
		mgr.AddFunc("Pmml", "IsIn", "com.ligadata.pmml.udfs.Udfs.IsIn", ("System", "Boolean"), List(("fldRefExpr", "System", "Int"),("setExprs", "System", "ListOfInt")), null)
		mgr.AddFunc("Pmml", "IsIn", "com.ligadata.pmml.udfs.Udfs.IsIn", ("System", "Boolean"), List(("fldRefExpr", "System", "String"),("setExprs", "System", "ListOfString")), null)
		mgr.AddFunc("Pmml", "IsIn", "com.ligadata.pmml.udfs.Udfs.IsIn", ("System", "Boolean"), List(("fldRefExpr", "System", "Double"),("setExprs", "System", "ArrayOfDouble")), null)
		mgr.AddFunc("Pmml", "IsIn", "com.ligadata.pmml.udfs.Udfs.IsIn", ("System", "Boolean"), List(("fldRefExpr", "System", "Float"),("setExprs", "System", "ArrayOfFloat")), null)
		mgr.AddFunc("Pmml", "IsIn", "com.ligadata.pmml.udfs.Udfs.IsIn", ("System", "Boolean"), List(("fldRefExpr", "System", "Int"),("setExprs", "System", "ArrayOfInt")), null)
		mgr.AddFunc("Pmml", "IsIn", "com.ligadata.pmml.udfs.Udfs.IsIn", ("System", "Boolean"), List(("fldRefExpr", "System", "String"),("setExprs", "System", "ArrayOfString")), null)
		mgr.AddFunc("Pmml", "IsIn", "com.ligadata.pmml.udfs.Udfs.IsIn", ("System", "Boolean"), List(("fldRefExpr", "System", "Double"),("setExprs", "System", "ArrayBufferOfDouble")), null)
		mgr.AddFunc("Pmml", "IsIn", "com.ligadata.pmml.udfs.Udfs.IsIn", ("System", "Boolean"), List(("fldRefExpr", "System", "Float"),("setExprs", "System", "ArrayBufferOfFloat")), null)
		mgr.AddFunc("Pmml", "IsIn", "com.ligadata.pmml.udfs.Udfs.IsIn", ("System", "Boolean"), List(("fldRefExpr", "System", "Int"),("setExprs", "System", "ArrayBufferOfInt")), null)
		mgr.AddFunc("Pmml", "IsIn", "com.ligadata.pmml.udfs.Udfs.IsIn", ("System", "Boolean"), List(("fldRefExpr", "System", "String"),("setExprs", "System", "ArrayBufferOfString")), null)
		mgr.AddFunc("Pmml", "Or", "com.ligadata.pmml.udfs.Udfs.Or", ("System", "Boolean"), List(("boolexprs", "System", "ArrayBufferOfBoolean")), null)
		mgr.AddFunc("Pmml", "Or", "com.ligadata.pmml.udfs.Udfs.Or", ("System", "Boolean"), List(("boolexpr", "System", "Boolean"),("boolexpr1", "System", "Boolean"),("boolexpr2", "System", "Boolean"),("boolexpr3", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "Or", "com.ligadata.pmml.udfs.Udfs.Or", ("System", "Boolean"), List(("boolexpr", "System", "Boolean"),("boolexpr1", "System", "Boolean"),("boolexpr2", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "Or", "com.ligadata.pmml.udfs.Udfs.Or", ("System", "Boolean"), List(("boolexpr", "System", "Boolean"),("boolexpr1", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "And", "com.ligadata.pmml.udfs.Udfs.And", ("System", "Boolean"), List(("boolexpr", "System", "Boolean"),("boolexpr1", "System", "Boolean"),("boolexpr2", "System", "Boolean"),("boolexpr3", "System", "Boolean"),("boolexpr4", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "And", "com.ligadata.pmml.udfs.Udfs.And", ("System", "Boolean"), List(("boolexpr", "System", "Boolean"),("boolexpr1", "System", "Boolean"),("boolexpr2", "System", "Boolean"),("boolexpr3", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "And", "com.ligadata.pmml.udfs.Udfs.And", ("System", "Boolean"), List(("boolexpr", "System", "Boolean"),("boolexpr1", "System", "Boolean"),("boolexpr2", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "And", "com.ligadata.pmml.udfs.Udfs.And", ("System", "Boolean"), List(("boolexpr", "System", "Boolean"),("boolexpr1", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "If", "com.ligadata.pmml.udfs.Udfs.If", ("System", "Boolean"), List(("boolexprs", "System", "ArrayBufferOfBoolean")), null)
		mgr.AddFunc("Pmml", "If", "com.ligadata.pmml.udfs.Udfs.If", ("System", "Boolean"), List(("boolexpr", "System", "Boolean"),("boolexpr1", "System", "Boolean"),("boolexpr2", "System", "Boolean"),("boolexpr3", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "If", "com.ligadata.pmml.udfs.Udfs.If", ("System", "Boolean"), List(("boolexpr", "System", "Boolean"),("boolexpr1", "System", "Boolean"),("boolexpr2", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "If", "com.ligadata.pmml.udfs.Udfs.If", ("System", "Boolean"), List(("boolexpr", "System", "Boolean"),("boolexpr1", "System", "Boolean")), null)
		mgr.AddFunc("Pmml", "If", "com.ligadata.pmml.udfs.Udfs.If", ("System", "Boolean"), List(("boolexpr", "System", "Boolean")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Float"),("value", "System", "BaseContainer")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Double"),("value", "System", "BaseContainer")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Long"),("value", "System", "BaseContainer")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Int"),("value", "System", "BaseContainer")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "String"),("value", "System", "BaseContainer")), null)
		mgr.AddFunc("Pmml", "GetArray", "com.ligadata.pmml.udfs.Udfs.GetArray", ("System", "ArrayOfBaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Float")), null)
		mgr.AddFunc("Pmml", "GetArray", "com.ligadata.pmml.udfs.Udfs.GetArray", ("System", "ArrayOfBaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Double")), null)
		mgr.AddFunc("Pmml", "GetArray", "com.ligadata.pmml.udfs.Udfs.GetArray", ("System", "ArrayOfBaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Long")), null)
		mgr.AddFunc("Pmml", "GetArray", "com.ligadata.pmml.udfs.Udfs.GetArray", ("System", "ArrayOfBaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Int")), null)
		mgr.AddFunc("Pmml", "GetArray", "com.ligadata.pmml.udfs.Udfs.GetArray", ("System", "ArrayOfBaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "String")), null)
		mgr.AddFunc("Pmml", "Get", "com.ligadata.pmml.udfs.Udfs.Get", ("System", "BaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String")), null)
		mgr.AddFunc("Pmml", "Get", "com.ligadata.pmml.udfs.Udfs.Get", ("System", "BaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Get", "com.ligadata.pmml.udfs.Udfs.Get", ("System", "BaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Get", "com.ligadata.pmml.udfs.Udfs.Get", ("System", "BaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Get", "com.ligadata.pmml.udfs.Udfs.Get", ("System", "BaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Get", "com.ligadata.pmml.udfs.Udfs.Get", ("System", "BaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "String")), null)
		//mgr.AddFunc("Pmml", "incrementBy", "com.ligadata.pmml.udfs.Udfs.incrementBy", ("System", "Boolean"), List(("variableName", "System", "String"),("value", "System", "Int")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("variableName", "System", "String"),("value", "System", "Float")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("variableName", "System", "String"),("value", "System", "Boolean")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("variableName", "System", "String"),("value", "System", "Any")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("variableName", "System", "String"),("value", "System", "Double")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("variableName", "System", "String"),("value", "System", "Long")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("variableName", "System", "String"),("value", "System", "Int")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("variableName", "System", "String"),("value", "System", "String")), null)

		//mgr.AddFunc("Pmml", "CollectionLength", "com.ligadata.pmml.udfs.Udfs.CollectionLength", ("System", "Int"), List(("coll", "System", "HashMapOfKV")), null)
		//mgr.AddFunc("Pmml", "CollectionLength", "com.ligadata.pmml.udfs.Udfs.CollectionLength", ("System", "Int"), List(("coll", "System", "MapOfKV")), null)
		//mgr.AddFunc("Pmml", "CollectionLength", "com.ligadata.pmml.udfs.Udfs.CollectionLength", ("System", "Int"), List(("coll", "System", "Stack[T]")), null)
		mgr.AddFunc("Pmml", "CollectionLength", "com.ligadata.pmml.udfs.Udfs.CollectionLength", ("System", "Int"), List(("coll", "System", "QueueOfAny")), null)
		//mgr.AddFunc("Pmml", "CollectionLength", "com.ligadata.pmml.udfs.Udfs.CollectionLength", ("System", "Int"), List(("coll", "System", "Vector[T]")), null)
		mgr.AddFunc("Pmml", "CollectionLength", "com.ligadata.pmml.udfs.Udfs.CollectionLength", ("System", "Int"), List(("coll", "System", "ListOfAny")), null)
		mgr.AddFunc("Pmml", "CollectionLength", "com.ligadata.pmml.udfs.Udfs.CollectionLength", ("System", "Int"), List(("coll", "System", "SortedSetOfAny")), null)
		mgr.AddFunc("Pmml", "CollectionLength", "com.ligadata.pmml.udfs.Udfs.CollectionLength", ("System", "Int"), List(("coll", "System", "TreeSetOfAny")), null)
		mgr.AddFunc("Pmml", "CollectionLength", "com.ligadata.pmml.udfs.Udfs.CollectionLength", ("System", "Int"), List(("coll", "System", "SetOfAny")), null)
		mgr.AddFunc("Pmml", "CollectionLength", "com.ligadata.pmml.udfs.Udfs.CollectionLength", ("System", "Int"), List(("coll", "System", "ArrayBufferOfAny")), null)
		mgr.AddFunc("Pmml", "CollectionLength", "com.ligadata.pmml.udfs.Udfs.CollectionLength", ("System", "Int"), List(("coll", "System", "ArrayOfAny")), null)
		//mgr.AddFunc("Pmml", "CollectionLength", "com.ligadata.pmml.udfs.Udfs.CollectionLength", ("System", "Int"), List(("coll", "System", "HashMapOfAnyAny")), null)
		//mgr.AddFunc("Pmml", "CollectionLength", "com.ligadata.pmml.udfs.Udfs.CollectionLength", ("System", "Int"), List(("coll", "System", "MapOfAnyAny")), null)

	}

	
	def InitFcns = {
		/** 
		    NOTE: These functions are variable in nature, more like macros than
		    actual functions.  They actually deploy two
		    functions (in most cases): the outer container function (e.g., Map or Filter) and the inner
		    function that will operate on the members of the container in some way.
		    
		    Since we only know the outer function that will be used, only it is
		    described.  The inner function is specified in the pmml and the arguments
		    and function lookup are separately done for it. The inner functions will be one of the 
		    be one of the other udfs that are defined in the core udf lib 
		    (e.g., Between(somefield, low, hi, inclusive) 
		    
		    Note too that only the "Any" version of these container types are defined.
		    The code generation will utilize the real item type of the container
		    to cast the object "down" to the right type. 
		    
		    Note that they all have the "isIterable" boolean set to true.
		    
		    nameSpace: String
		      , name: String
		      , physicalName: String
		      , retTypeNsName: (String, String)
		      , args: List[(String, String, String)]
		      , fmfeatures : Set[FcnMacroAttr.Feature]
		 
		 */
		var fcnMacrofeatures : Set[FcnMacroAttr.Feature] = Set[FcnMacroAttr.Feature]()
		fcnMacrofeatures += FcnMacroAttr.ITERABLE
		logger.trace("MetadataLoad...loading container filter functions")
		mgr.AddFunc(MdMgr.sysNS
					, "ContainerFilter"
					, "com.ligadata.pmml.udfs.Udfs.ContainerFilter"
					, (MdMgr.sysNS, "ArrayOfAny")
					, List(("containerId", MdMgr.sysNS, "ArrayOfAny"))
					, fcnMacrofeatures)	  
		mgr.AddFunc(MdMgr.sysNS
					, "ContainerFilter"
					, "com.ligadata.pmml.udfs.Udfs.ContainerFilter"
					, (MdMgr.sysNS, "ArrayBufferOfAny")
					, List(("containerId", MdMgr.sysNS, "ArrayBufferOfAny"))
					, fcnMacrofeatures)	  
		mgr.AddFunc(MdMgr.sysNS
					, "ContainerFilter"
					, "com.ligadata.pmml.udfs.Udfs.ContainerFilter"
					, (MdMgr.sysNS, "ListOfAny")
					, List(("containerId", MdMgr.sysNS, "ListOfAny"))
					, fcnMacrofeatures)	  
		mgr.AddFunc(MdMgr.sysNS
					, "ContainerFilter"
					, "com.ligadata.pmml.udfs.Udfs.ContainerFilter"
					, (MdMgr.sysNS, "SetOfAny")
					, List(("containerId", MdMgr.sysNS, "SetOfAny"))
					, fcnMacrofeatures)	  
		mgr.AddFunc(MdMgr.sysNS
					, "ContainerFilter"
					, "com.ligadata.pmml.udfs.Udfs.ContainerFilter"
					, (MdMgr.sysNS, "TreeSetOfAny")
					, List(("containerId", MdMgr.sysNS, "TreeSetOfAny"))
					, fcnMacrofeatures)	  
		mgr.AddFunc(MdMgr.sysNS
					, "ContainerFilter"
					, "com.ligadata.pmml.udfs.Udfs.ContainerFilter"
					, (MdMgr.sysNS, "MapOfAnyAny")
					, List(("containerId", MdMgr.sysNS, "MapOfAnyAny"))
					, fcnMacrofeatures)	  
		mgr.AddFunc(MdMgr.sysNS
					, "ContainerFilter"
					, "com.ligadata.pmml.udfs.Udfs.ContainerFilter"
					, (MdMgr.sysNS, "HashMapOfAnyAny")
					, List(("containerId", MdMgr.sysNS, "HashMapOfAnyAny"))
					, fcnMacrofeatures)	  


		logger.trace("MetadataLoad...loading container map functions")
		mgr.AddFunc(MdMgr.sysNS
					, "ContainerMap"
					, "com.ligadata.pmml.udfs.Udfs.ContainerMap"
					, (MdMgr.sysNS, "ArrayOfAny")
					, List(("containerId", MdMgr.sysNS, "ArrayOfAny"))
					, fcnMacrofeatures)	  
		mgr.AddFunc(MdMgr.sysNS
					, "ContainerMap"
					, "com.ligadata.pmml.udfs.Udfs.ContainerMap"
					, (MdMgr.sysNS, "ArrayBufferOfAny")
					, List(("containerId", MdMgr.sysNS, "ArrayBufferOfAny"))
					, fcnMacrofeatures)	  
		mgr.AddFunc(MdMgr.sysNS
					, "ContainerMap"
					, "com.ligadata.pmml.udfs.Udfs.ContainerMap"
					, (MdMgr.sysNS, "ListOfAny")
					, List(("containerId", MdMgr.sysNS, "ListOfAny"))
					, fcnMacrofeatures)	  
		mgr.AddFunc(MdMgr.sysNS
					, "ContainerMap"
					, "com.ligadata.pmml.udfs.Udfs.ContainerMap"
					, (MdMgr.sysNS, "SetOfAny")
					, List(("containerId", MdMgr.sysNS, "SetOfAny"))
					, fcnMacrofeatures)	  
		mgr.AddFunc(MdMgr.sysNS
					, "ContainerMap"
					, "com.ligadata.pmml.udfs.Udfs.ContainerMap"
					, (MdMgr.sysNS, "TreeSetOfAny")
					, List(("containerId", MdMgr.sysNS, "TreeSetOfAny"))
					, fcnMacrofeatures)	  
		mgr.AddFunc(MdMgr.sysNS
					, "ContainerMap"
					, "com.ligadata.pmml.udfs.Udfs.ContainerMap"
					, (MdMgr.sysNS, "MapOfAnyAny")
					, List(("containerId", MdMgr.sysNS, "MapOfAnyAny"))
					, fcnMacrofeatures)	  
		mgr.AddFunc(MdMgr.sysNS
					, "ContainerMap"
					, "com.ligadata.pmml.udfs.Udfs.ContainerMap"
					, (MdMgr.sysNS, "HashMapOfAnyAny")
					, List(("containerId", MdMgr.sysNS, "HashMapOfAnyAny"))
					, fcnMacrofeatures)	  
					
		
	  
	}
	
	def initMacroDefs {

		logger.trace("MetadataLoad...loading Macro functions")

		
		
	
		/** catalog the CLASSUPDATE oriented macros: 
		 
	  		"incrementBy(Int,Int)"  
	  		"incrementBy(Double,Double)"  
	  		"incrementBy(Long,Long)"  
		 	"Put(Any,Any,Any)"
		 	"Put(String,String)"
		 	"Put(Int,Int)"
		 	"Put(Long,Long)"
		 	"Put(Double,Double)"
		 	"Put(Boolean,Boolean)"
		 	"Put(Any,Any)"

		 */

		var fcnMacrofeatures : Set[FcnMacroAttr.Feature] = Set[FcnMacroAttr.Feature]()
		fcnMacrofeatures += FcnMacroAttr.CLASSUPDATE
		  
		
		/** Macros Associated with this macro template:
	  		"incrementBy(Any,Int,Int)"  
	  		"incrementBy(Any,Double,Double)"  
	  		"incrementBy(Any,Long,Long)"  
	  		
	  		Something like the following code would cause the macro to be used were
	  		the ClientAlertsToday a FixedField container...
	  		<Apply function="incrementBy">
				<FieldRef field="ClientAlertsToday.OD002Sent"/>
				<Constant dataType="integer">1</Constant> 
			</Apply>
	  		
		 */
		val incrementByMacroStringFixed : String =  """
	class %1%_%2%_incrementBy(var %1% : %1_type%, val %3% : %3_type%)
	{
	  	def incrementBy  : Boolean = { %1%.%2% += %3%; true }
	} """
		
		val incrementByMacroStringMapped : String =  """
	class %1%_%2%_incrementBy(var %1% : %1_type%, val %3% : %3_type%)
	{
	  	def incrementBy  : Boolean = { %1%(%2%) = %1%(%2%) + %3%; true }
	} """
		
		mgr.AddMacro(MdMgr.sysNS
					, "incrementBy"
					, (MdMgr.sysNS, "Boolean")
					, List(("container", MdMgr.sysNS, "Any"), ("containerField", MdMgr.sysNS, "Int"), ("value", MdMgr.sysNS, "Int"))
					, fcnMacrofeatures
					, (incrementByMacroStringFixed,incrementByMacroStringMapped))	  
		  
		mgr.AddMacro(MdMgr.sysNS
					, "incrementBy"
					, (MdMgr.sysNS, "Boolean")
					, List(("container", MdMgr.sysNS, "Any"), ("containerField", MdMgr.sysNS, "Double"), ("value", MdMgr.sysNS, "Double"))
					, fcnMacrofeatures
					, (incrementByMacroStringFixed,incrementByMacroStringMapped))	  
		  
		mgr.AddMacro(MdMgr.sysNS
					, "incrementBy"
					, (MdMgr.sysNS, "Boolean")
					, List(("container", MdMgr.sysNS, "Any"), ("containerField", MdMgr.sysNS, "Long"), ("value", MdMgr.sysNS, "Long"))
					, fcnMacrofeatures
					, (incrementByMacroStringFixed,incrementByMacroStringMapped))	  

					
		/** Macros associated with this macro template:
		 	"Put(Any,Any,Any)"
		 */
		
		val putContainerMacroStringMapped : String =   """
	class %1%_%2%_%3%_Put(var %1% : %1_type%, val %2% : %2_type%, val %3% : %3_type%)
	{
	  	def Put  : Boolean = { %1%.getObject(%2, %3) = %3%; true }
	} """
		
		mgr.AddMacro(MdMgr.sysNS
					, "Put"
					, (MdMgr.sysNS, "Boolean")
					, List(("container", MdMgr.sysNS, "Any"), ("key", MdMgr.sysNS, "Any"), ("value", MdMgr.sysNS, "Any"))
					, fcnMacrofeatures
					, (putContainerMacroStringMapped,putContainerMacroStringMapped))	  
		  				

		val putGlobalContainerMacroStringFixed : String =  """
	class %1%_%2%_%3%_%4%_Put(var %1% : %1_type%, val %2% : %2_type%, val %3% : %3_type%, val %5% : %5_type%)
	{
	  	def Put  : Boolean = { %1%.setObject(%2%, %3%.%4%.toString, %5%); true }
	} """
		
		val putGlobalContainerMacroStringMapped : String =   """
	class %1%_%2%_%3%_%4%_Put(var %1% : %1_type%, val %2% : %2_type%, val %3% : %3_type%, val %5% : %5_type%)
	{
	  	def Put  : Boolean = { %1%.setObject(%2%, %3%(%4%).toString, %5%); true }
	} """
		
		mgr.AddMacro(MdMgr.sysNS
					,"Put"
					, (MdMgr.sysNS, "Boolean")
					, List(("gCtx", MdMgr.sysNS, "EnvContext"), ("containerName", MdMgr.sysNS, "String"), ("elemContainer", MdMgr.sysNS, "BaseMsg"), ("elementkey", MdMgr.sysNS, "String"), ("value", MdMgr.sysNS, "BaseContainer"))
					, fcnMacrofeatures
					, (putGlobalContainerMacroStringFixed,putGlobalContainerMacroStringMapped))	  
		  				
		mgr.AddMacro(MdMgr.sysNS
					, "Put"
					, (MdMgr.sysNS, "Boolean")
					, List(("gCtx", MdMgr.sysNS, "EnvContext"), ("containerName", MdMgr.sysNS, "String"), ("elemContainer", MdMgr.sysNS, "BaseContainer"), ("elementkey", MdMgr.sysNS, "String"), ("value", MdMgr.sysNS, "BaseContainer"))
					, fcnMacrofeatures
					, (putGlobalContainerMacroStringFixed,putGlobalContainerMacroStringMapped))	  
		  				
		mgr.AddMacro(MdMgr.sysNS
					, "Put"
					, (MdMgr.sysNS, "Boolean")
					, List(("gCtx", MdMgr.sysNS, "EnvContext"), ("containerName", MdMgr.sysNS, "String"), ("elemContainer", MdMgr.sysNS, "BaseMsg"), ("elementkey", MdMgr.sysNS, "Long"), ("value", MdMgr.sysNS, "BaseContainer"))
					, fcnMacrofeatures
					, (putGlobalContainerMacroStringFixed,putGlobalContainerMacroStringMapped))	  
		  				
		mgr.AddMacro(MdMgr.sysNS
					, "Put"
					, (MdMgr.sysNS, "Boolean")
					, List(("gCtx", MdMgr.sysNS, "EnvContext"), ("containerName", MdMgr.sysNS, "String"), ("elemContainer", MdMgr.sysNS, "BaseContainer"), ("elementkey", MdMgr.sysNS, "Long"), ("value", MdMgr.sysNS, "BaseContainer"))
					, fcnMacrofeatures
					, (putGlobalContainerMacroStringFixed,putGlobalContainerMacroStringMapped))	  
		  				
		mgr.AddMacro(MdMgr.sysNS
					, "Put"
					, (MdMgr.sysNS, "Boolean")
					, List(("gCtx", MdMgr.sysNS, "EnvContext"), ("containerName", MdMgr.sysNS, "String"), ("elemContainer", MdMgr.sysNS, "BaseMsg"), ("elementkey", MdMgr.sysNS, "Int"), ("value", MdMgr.sysNS, "BaseContainer"))
					, fcnMacrofeatures
					, (putGlobalContainerMacroStringFixed,putGlobalContainerMacroStringMapped))	  
		  				
		mgr.AddMacro(MdMgr.sysNS
					, "Put"
					, (MdMgr.sysNS, "Boolean")
					, List(("gCtx", MdMgr.sysNS, "EnvContext"), ("containerName", MdMgr.sysNS, "String"), ("elemContainer", MdMgr.sysNS, "BaseContainer"), ("elementkey", MdMgr.sysNS, "Int"), ("value", MdMgr.sysNS, "BaseContainer"))
					, fcnMacrofeatures
					, (putGlobalContainerMacroStringFixed,putGlobalContainerMacroStringMapped))	  
		  				
		mgr.AddMacro(MdMgr.sysNS
					, "Put"
					, (MdMgr.sysNS, "Boolean")
					, List(("gCtx", MdMgr.sysNS, "EnvContext"), ("containerName", MdMgr.sysNS, "String"), ("elemContainer", MdMgr.sysNS, "BaseMsg"), ("elementkey", MdMgr.sysNS, "Double"), ("value", MdMgr.sysNS, "BaseContainer"))
					, fcnMacrofeatures
					, (putGlobalContainerMacroStringFixed,putGlobalContainerMacroStringMapped))	  
		  				
		mgr.AddMacro(MdMgr.sysNS
					, "Put"
					, (MdMgr.sysNS, "Boolean")
					, List(("gCtx", MdMgr.sysNS, "EnvContext"), ("containerName", MdMgr.sysNS, "String"), ("elemContainer", MdMgr.sysNS, "BaseContainer"), ("elementkey", MdMgr.sysNS, "Double"), ("value", MdMgr.sysNS, "BaseContainer"))
					, fcnMacrofeatures
					, (putGlobalContainerMacroStringFixed,putGlobalContainerMacroStringMapped))	  
		  				
		mgr.AddMacro(MdMgr.sysNS
					, "Put"
					, (MdMgr.sysNS, "Boolean")
					, List(("gCtx", MdMgr.sysNS, "EnvContext"), ("containerName", MdMgr.sysNS, "String"), ("elemContainer", MdMgr.sysNS, "BaseMsg"), ("elementkey", MdMgr.sysNS, "Any"), ("value", MdMgr.sysNS, "BaseContainer"))
					, fcnMacrofeatures
					, (putGlobalContainerMacroStringFixed,putGlobalContainerMacroStringMapped))	  
		  				
		mgr.AddMacro(MdMgr.sysNS
					, "Put"
					, (MdMgr.sysNS, "Boolean")
					, List(("gCtx", MdMgr.sysNS, "EnvContext"), ("containerName", MdMgr.sysNS, "String"), ("elemContainer", MdMgr.sysNS, "BaseContainer"), ("elementkey", MdMgr.sysNS, "Any"), ("value", MdMgr.sysNS, "BaseContainer"))
					, fcnMacrofeatures
					, (putGlobalContainerMacroStringFixed,putGlobalContainerMacroStringMapped))	  
		  				

		/** Macros associated with this macro template:
		 	"Put(String,String)"
		 	"Put(Int,Int)"
		 	"Put(Long,Long)"
		 	"Put(Double,Double)"
		 	"Put(Boolean,Boolean)"
		 	"Put(Any,Any)"
		 	
		 	Note: No "mapped" version of the template needed for this case.
		 */
		
		val putVariableMacroPmmlDict : String =   """
	class %1%_%2%_Put(var %1% : %1_type%, val %2% : %2_type%)
	{
	  	def Put  : Boolean = { 
	  		val set : Boolean = ctx.valuePut(%1%, %2%)
			set 
		}
	} """

		mgr.AddMacro(MdMgr.sysNS
					, "Put"
					, (MdMgr.sysNS, "Boolean")
					, List(("variableName", MdMgr.sysNS, "String"), ("value", MdMgr.sysNS, "String"))
					, fcnMacrofeatures
					, (putVariableMacroPmmlDict,putVariableMacroPmmlDict)
					,-1)	  
		  
		mgr.AddMacro(MdMgr.sysNS
					, "Put"
					, (MdMgr.sysNS, "Boolean")
					, List(("variableName", MdMgr.sysNS, "String"), ("value", MdMgr.sysNS, "Int"))
					, fcnMacrofeatures
					, (putVariableMacroPmmlDict,putVariableMacroPmmlDict))	  
		  
		mgr.AddMacro(MdMgr.sysNS
					, "Put"
					, (MdMgr.sysNS, "Boolean")
					, List(("variableName", MdMgr.sysNS, "String"), ("value", MdMgr.sysNS, "Long"))
					, fcnMacrofeatures
					, (putVariableMacroPmmlDict,putVariableMacroPmmlDict))	  
		  
		mgr.AddMacro(MdMgr.sysNS
					, "Put"
					, (MdMgr.sysNS, "Boolean")
					, List(("variableName", MdMgr.sysNS, "String"), ("value", MdMgr.sysNS, "Double"))
					, fcnMacrofeatures
					, (putVariableMacroPmmlDict,putVariableMacroPmmlDict))	  
		  
		mgr.AddMacro(MdMgr.sysNS
					, "Put"
					, (MdMgr.sysNS, "Boolean")
					, List(("variableName", MdMgr.sysNS, "String"), ("value", MdMgr.sysNS, "Boolean"))
					, fcnMacrofeatures
					, (putVariableMacroPmmlDict,putVariableMacroPmmlDict))	  
		  
		mgr.AddMacro(MdMgr.sysNS
					, "Put"
					, (MdMgr.sysNS, "Boolean")
					, List(("variableName", MdMgr.sysNS, "String"), ("value", MdMgr.sysNS, "Any"))
					, fcnMacrofeatures
					, (putVariableMacroPmmlDict,putVariableMacroPmmlDict))	  
		  
		  
		  
		/** catalog the ITERABLE macros */
		fcnMacrofeatures.clear
		fcnMacrofeatures += FcnMacroAttr.ITERABLE
	}


}

