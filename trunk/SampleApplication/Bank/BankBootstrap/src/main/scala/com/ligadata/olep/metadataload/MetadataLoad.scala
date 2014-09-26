package com.ligadata.olep.metadataload

import scala.collection.mutable.{ Set }
import org.apache.log4j.Logger
import com.ligadata.olep.metadata.MdMgr._
import com.ligadata.olep.metadata.ObjType._
import com.ligadata.olep.metadata._
import com.ligadata.OnLEPBase._

/** 
 *  FIXME: As an intermediate development, we might load the metadata manager with file content before resurrecting
 *  the cache from a kv store... hence the arguments (currently unused)
 *	
 *  For now, we just call some functions in the object MetadataLoad to load the various kinds of metadata.
 *  The functions used to load metadata depend on metadata that the loaded element needs being present
 *  before hand in the metadata store (e.g., a type of a function arg must exist before the function can
 *  be loaded.
 *  
 *  This is for the bank.  See MetadataLoad1.scala for the medical side.
 *  
 */

class MetadataLoad (val mgr : MdMgr, val logger : Logger, val typesPath : String, val fcnPath : String, val attrPath : String, msgCtnPath : String) {
	val baseTypesVer = 100 // Which is 00.01.00
  
	/** construct the loader and call this to complete the cache initialization */
	def initialize { 

		logger.trace("MetadataLoad...loading typedefs")
		InitTypeDefs

		//logger.trace("MetadataLoad...loading types")
		//mgr.InitTypes(mgr)

	    logger.trace("MetadataLoad...loading type definitions")
	    InitFixedMsgs
		
		logger.trace("MetadataLoad...loading pmml types")
		initTypesFor_com_ligadata_pmml_udfs_Udfs

		logger.trace("MetadataLoad...loading pmml udfs")
		init_com_ligadata_pmml_udfs_Udfs
			
		//logger.trace("MetadataLoad...loading more functions")
		//mgr.InitFunctions(mgr)

	    logger.trace("MetadataLoad...loading model definitions")
	    InitModelDefs
	    
	    logger.trace("MetadataLoad...loading function macro definitions")
	    initMacroDefs
	    
	}
	
	// BankPocMsg + the dimensional data (also treated as Messages)
	def InitFixedMsgs : Unit = {
		logger.trace("MetadataLoad...loading BankPocMsg")
		
/**
    
   def MakeFixedMsg(nameSpace: String
	      , name: String
	      , physicalName: String
	      , args: List[(String, String, String, String, Boolean)] //attribute namespace, attribute name, attribute type namespace, attribute type name, isGlobal
		  , ver: Int = 1
		  , jarNm: String = null
		  , depJars: Array[String] = null): MessageDef = {
		  
		 
		}
		*/
		
		mgr.AddFixedMsg(MdMgr.sysNS
					    , "BankPocMsg"
					    , "com.ligadata.OnLEPBankPoc.BankPocMsg_100"
					    , List(
					        //(MdMgr.sysNS, "SUBMIT_TS", MdMgr.sysNS, "Long", false)
					        (MdMgr.sysNS, "CUST_ID", MdMgr.sysNS, "Long", false)
					        , (MdMgr.sysNS, "ENT_ACC_NUM", MdMgr.sysNS, "Long", false)
					        , (MdMgr.sysNS, "ENT_SEG_TYP", MdMgr.sysNS, "String", false)
					        , (MdMgr.sysNS, "ENT_DTE", MdMgr.sysNS, "Int", false)
					        , (MdMgr.sysNS, "ENT_TME", MdMgr.sysNS, "Int", false)
					        , (MdMgr.sysNS, "ENT_SRC", MdMgr.sysNS, "String", false)
					        , (MdMgr.sysNS, "ENT_AMT_TYP", MdMgr.sysNS, "Int", false)
					        , (MdMgr.sysNS, "ENT_AMT", MdMgr.sysNS, "Double", false)
					        , (MdMgr.sysNS, "ODR_LMT", MdMgr.sysNS, "Double", false)
					        , (MdMgr.sysNS, "ANT_LMT", MdMgr.sysNS, "Double", false)
					        , (MdMgr.sysNS, "UNP_BUFF", MdMgr.sysNS, "Double", false)
					        , (MdMgr.sysNS, "EB_BUFFER", MdMgr.sysNS, "Double", false)
					        , (MdMgr.sysNS, "OD_BUFFER", MdMgr.sysNS, "Double", false)
					        , (MdMgr.sysNS, "LB_LIMIT", MdMgr.sysNS, "Double", false)
					        , (MdMgr.sysNS, "RUN_LDG_XAU", MdMgr.sysNS, "Double", false))
					        /** , (MdMgr.sysNS, "OUTPUT_ALERT_TYPE", MdMgr.sysNS, "Double", false)) */ 
					        , baseTypesVer, "bankmsgsandcontainers_2.10-1.0.jar", Array("basetypes_2.10-0.1.0.jar", "metadata_2.10-1.0.jar", "onlepbase_2.10-1.0.jar"));

		logger.trace("MetadataLoad...loading AlertHistory")
		mgr.AddFixedContainer(MdMgr.sysNS
						    , "AlertHistory"
						    , "com.ligadata.OnLEPBankPoc.AlertHistory_100"
						    , List((MdMgr.sysNS, "CUST_ID", MdMgr.sysNS, "Long", false)
						        , (MdMgr.sysNS, "ENT_ACC_NUM", MdMgr.sysNS, "Long", false)
						        , (MdMgr.sysNS, "LB001Sent", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "OD001Sent", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "OD002Sent", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "OD003Sent", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "NO001Sent", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "EB001Sent", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "EB002Sent", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "UTFSent", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "PTFSent", MdMgr.sysNS, "Int", false))
						        , baseTypesVer, "bankmsgsandcontainers_2.10-1.0.jar", Array("basetypes_2.10-0.1.0.jar", "metadata_2.10-1.0.jar", "onlepbase_2.10-1.0.jar"));
		
		logger.trace("MetadataLoad...loading AlertParameters")
		mgr.AddFixedContainer(MdMgr.sysNS
						    , "AlertParameters"
						    , "com.ligadata.OnLEPBankPoc.AlertParameters_100"
						    , List((MdMgr.sysNS, "ALERT", MdMgr.sysNS, "String", false)
						        , (MdMgr.sysNS, "MAX_ALERTS_PER_DAY", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "ONLINE_START_TIME", MdMgr.sysNS, "Long", false)
						        , (MdMgr.sysNS, "ONLINE_END_TIME", MdMgr.sysNS, "Long", false)
						        , (MdMgr.sysNS, "ALERT_EXPIRY_TIME", MdMgr.sysNS, "Long", false))
						        , baseTypesVer, "bankmsgsandcontainers_2.10-1.0.jar", Array("basetypes_2.10-0.1.0.jar", "metadata_2.10-1.0.jar", "onlepbase_2.10-1.0.jar"));
		
		logger.trace("MetadataLoad...loading CustomerPreferences")
		mgr.AddFixedContainer(MdMgr.sysNS
						    , "CustomerPreferences"
						    , "com.ligadata.OnLEPBankPoc.CustomerPreferences_100"
						    , List((MdMgr.sysNS, "CUST_ID", MdMgr.sysNS, "Long", false)
						        , (MdMgr.sysNS, "SORT_CODE", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "ENT_ACC_NUM", MdMgr.sysNS, "Long", false)
						        , (MdMgr.sysNS, "ACCT_SHORT_NM", MdMgr.sysNS, "String", false)
						        , (MdMgr.sysNS, "RISK_TIER_ID", MdMgr.sysNS, "String", false)
						        , (MdMgr.sysNS, "LB_REG_FLG", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "LB_LIMIT", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "OD_REG_FLG", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "MAX_EB_CNT", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "LAST_UPDATE_TS", MdMgr.sysNS, "String", false)
						        , (MdMgr.sysNS, "CONT_ID", MdMgr.sysNS, "Long", false)
						        , (MdMgr.sysNS, "MOBILE_NUMBER", MdMgr.sysNS, "String", false)
						        , (MdMgr.sysNS, "DECEASED_MARKER", MdMgr.sysNS, "Long", false)
						        , (MdMgr.sysNS, "OD_T2_LIMIT", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "OD_T1_LIMIT", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "NO_FACTOR", MdMgr.sysNS, "Double", false))
						        , baseTypesVer, "bankmsgsandcontainers_2.10-1.0.jar", Array("basetypes_2.10-0.1.0.jar", "metadata_2.10-1.0.jar", "onlepbase_2.10-1.0.jar"));

		logger.trace("MetadataLoad...loading TukTier")
		mgr.AddFixedContainer(MdMgr.sysNS
						    , "TukTier"
						    , "com.ligadata.OnLEPBankPoc.TukTier_100"
						    , List((MdMgr.sysNS, "TIERSET_ID", MdMgr.sysNS, "Long", false)
						        , (MdMgr.sysNS, "T1_START_DATE", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "T1_END_DATE", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "T1_START_AMT", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "T1_END_AMT", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "T1_FEE", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "T1_MAX_PER_PERIOD_CNT", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "T2_START_DATE", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "T2_END_DATE", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "T2_START_AMT", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "T2_END_AMT", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "T2_FEE", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "T2_MAX_PER_PERIOD_CNT", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "T3_START_DATE", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "T3_END_DATE", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "T3_START_AMT", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "T3_END_AMT", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "T3_FEE", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "T3_MAX_PER_PERIOD_CNT", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "T4_START_DATE", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "T4_END_DATE", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "T4_START_AMT", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "T4_END_AMT", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "T4_FEE", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "T4_MAX_PER_PERIOD_CNT", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "T5_START_DATE", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "T5_END_DATE", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "T5_START_AMT", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "T5_END_AMT", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "T5_FEE", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "T5_MAX_PER_PERIOD_CNT", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "T6_START_DATE", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "T6_END_DATE", MdMgr.sysNS, "Int", false)
						        , (MdMgr.sysNS, "T6_START_AMT", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "T6_END_AMT", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "T6_FEE", MdMgr.sysNS, "Double", false)
						        , (MdMgr.sysNS, "T6_MAX_PER_PERIOD_CNT", MdMgr.sysNS, "Int", false)
						        )
					        , baseTypesVer
					        , "bankmsgsandcontainers_2.10-1.0.jar"
					        , Array("basetypes_2.10-0.1.0.jar", "metadata_2.10-1.0.jar", "onlepbase_2.10-1.0.jar"));

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

	def InitModelDefs = {
	  
	  /**
		logger.trace("MetadataLoad...loading model EmergencyBorrowAlert")
		mgr.AddModelDef(MdMgr.sysNS
						, "EmergencyBorrowAlert"
						, "com.ligadata.OnLEPTestModel.EmergencyBorrowAlert_100"
						, "RuleSet"
						, List(("BankPocMsg", "ENT_SEG_TYP", MdMgr.sysNS, "String", false)
						    , ("BankPocMsg", "ENT_DTE", MdMgr.sysNS, "Int", false)
						    , ("BankPocMsg", "ENT_ACC_NUM", MdMgr.sysNS, "Long", false)
						    , ("BankPocMsg", "RUN_LDG_XAU", MdMgr.sysNS, "Double", false)
						    , ("BankPocMsg", "ODR_LMT", MdMgr.sysNS, "Double", false)
						    , ("BankPocMsg", "ENT_AMT", MdMgr.sysNS, "Double", false)
						    , ("BankPocMsg", "ANT_LMT", MdMgr.sysNS, "Double", false)
						    , ("BankPocMsg", "EB_BUFFER", MdMgr.sysNS, "Double", false)
						    , ("BankPocMsg", "UNP_BUFF", MdMgr.sysNS, "Double", false))
						 , List(("SendResult", MdMgr.sysNS, "String")
						     , ("OfflineEvent", MdMgr.sysNS, "String")
						     , ("AlertType", MdMgr.sysNS, "String")
						     , ("Balance", MdMgr.sysNS, "String")
						     , ("AccountShortName", MdMgr.sysNS, "String")
						     , ("ExpiryDate", MdMgr.sysNS, "String")
						     , ("ExpiryTime", MdMgr.sysNS, "String")
						     , ("MobileNumber", MdMgr.sysNS, "String"))
						 , baseTypesVer, "onlepebmodel_2.10-1.0.jar", Array("metadata_2.10-1.0.jar", "bankmsgsandcontainers_2.10-1.0.jar", "onlepbase_2.10-1.0.jar")) */

			
		logger.trace("MetadataLoad...loading AlchemyAlerts model")
		mgr.AddModelDef(
		       MdMgr.sysNS
			, "AlchemyAlerts_000100"
			, "com.Barclay.AlchemyAlerts_000100.pmml.AlchemyAlerts_000100"
			, "RuleSet"
			, List(("ClientPrefs", "OD_T2_LIMIT", "system", "customerpreferences", false)
				,("ClientPrefs", "MAX_EB_CNT", "system", "customerpreferences", false)
				,("ODAlertParms", "ONLINE_END_TIME", "system", "alertparameters", false)
				,("msg", "LB_LIMIT", "system", "bankpocmsg", false)
				,("msg", "OD_BUFFER", "system", "bankpocmsg", false)
				,("msg", "ANT_LMT", "system", "bankpocmsg", false)
				,("msg", "EB_BUFFER", "system", "bankpocmsg", false)
				,("ClientAlertsToday", "OD003Sent", "system", "alerthistory", false)
				,("EBAlertParms", "MAX_ALERTS_PER_DAY", "system", "alertparameters", false)
				,("ClientPrefs", "OD_T1_LIMIT", "system", "customerpreferences", false)
				,("ClientAlertsToday", "UTFSent", "system", "alerthistory", false)
				,("ClientAlertsToday", "LB001Sent", "system", "alerthistory", false)
				,("msg", "ENT_TME", "system", "bankpocmsg", false)
				,("ODAlertParms", "MAX_ALERTS_PER_DAY", "system", "alertparameters", false)
				,("LBAlertParms", "ONLINE_START_TIME", "system", "alertparameters", false)
				,("LBAlertParms", "ONLINE_END_TIME", "system", "alertparameters", false)
				,("msg", "RUN_LDG_XAU", "system", "bankpocmsg", false)
				,("NOAlertParms", "MAX_ALERTS_PER_DAY", "system", "alertparameters", false)
				,("ClientAlertsToday", "NO001Sent", "system", "alerthistory", false)
				,("NOAlertParms", "ONLINE_END_TIME", "system", "alertparameters", false)
				,("ClientPrefs", "NO_FACTOR", "system", "customerpreferences", false)
				,("EBAlertParms", "ONLINE_START_TIME", "system", "alertparameters", false)
				,("msg", "ODR_LMT", "system", "bankpocmsg", false)
				,("ClientAlertsToday", "OD001Sent", "system", "alerthistory", false)
				,("ClientAlertsToday", "PTFSent", "system", "alerthistory", false)
				,("msg", "ENT_AMT", "system", "bankpocmsg", false)
				,("ClientAlertsToday", "EB001Sent", "system", "alerthistory", false)
				,("msg", "UNP_BUFF", "system", "bankpocmsg", false)
				,("msg", "ENT_ACC_NUM", "system", "bankpocmsg", false)
				,("ClientPrefs", "LB_LIMIT", "system", "customerpreferences", false)
				,("ClientAlertsToday", "OD002Sent", "system", "alerthistory", false)
				,("NOAlertParms", "ONLINE_START_TIME", "system", "alertparameters", false)
				,("LBAlertParms", "MAX_ALERTS_PER_DAY", "system", "alertparameters", false)
				,("EBAlertParms", "ONLINE_END_TIME", "system", "alertparameters", false)
				,("ODAlertParms", "ONLINE_START_TIME", "system", "alertparameters", false)
				,("ClientAlertsToday", "EB002Sent", "system", "alerthistory", false))		
			, List(("msg.ENT_DTE", "system", "Int")
				,("AlertType", "system", "String")
				,("msg.RUN_LDG_XAU", "system", "Double")
				,("OfflineEvent", "system", "String")
				,("ClientPrefs.ACCT_SHORT_NM", "system", "String")
				,("ClientPrefs.MOBILE_NUMBER", "system", "String")
				,("EBAlertParms.ALERT_EXPIRY_TIME", "system", "Long")
				,("SendResult", "system", "String"))
			 , baseTypesVer
			 , "alchemy_000100_2.10-1.0.jar"
			 , Array("pmmludfs_2.10-1.0.jar", "pmmlruntime_2.10-1.0.jar", "metadata_2.10-1.0.jar", "bankmsgsandcontainers_2.10-1.0.jar", "onlepbase_2.10-1.0.jar")
		)  
	}


	/** Define any types that may be used in the container, message, fcn, and model metadata */
	def InitTypeDefs = {
		mgr.AddScalar("System", "Any", tAny, "Any", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.AnyImpl")
		mgr.AddScalar("System", "String", tString, "String", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.StringImpl")
		mgr.AddScalar("System", "Int", tInt, "Int", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.IntImpl")
		mgr.AddScalar("System", "Integer", tInt, "Int", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.IntImpl")
		mgr.AddScalar("System", "Long", tLong, "Long", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.LongImpl")
		mgr.AddScalar("System", "Boolean", tBoolean, "Boolean", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.BoolImpl")
		mgr.AddScalar("System", "Bool", tBoolean, "Boolean", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.BoolImpl")
		mgr.AddScalar("System", "Double", tDouble, "Double", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.DoubleImpl")
		mgr.AddScalar("System", "Float", tFloat, "Float", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.FloatImpl")
		mgr.AddScalar("System", "Char", tChar, "Char", baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.CharImpl")

		
		//nameSpace: String, name: String, tpNameSp: String, tpName: String, numDims: Int, ver: Int
		//mgr.AddArray(MdMgr.sysNS, "ArrayOfAny", MdMgr.sysNS, "Any", 1, 1)
		//mgr.AddArray(MdMgr.sysNS, "ArrayBufferOfAny", MdMgr.sysNS, "Any", 1, 1)
		//mgr.AddSet(MdMgr.sysNS, "ListOfAny", MdMgr.sysNS, "Any", 1)
		//mgr.AddSet(MdMgr.sysNS, "SetOfAny", MdMgr.sysNS, "Any", 1)
		//mgr.AddSet(MdMgr.sysNS, "TreeSetOfAny", MdMgr.sysNS, "Any", 1)
		//mgr.AddMap(MdMgr.sysNS, "MapOfAny", (MdMgr.sysNS, "Any"), (MdMgr.sysNS, "Any"), 1)
		//mgr.AddHashMap(MdMgr.sysNS, "HashMapOfAny", (MdMgr.sysNS, "Any"), (MdMgr.sysNS, "Any"), 1)
		
	}
	
	
	def InitFcns = {
		/** 
		    NOTE: These functions are variable in nature, more like templates than
		    actual functions.  They actually deploy two
		    functions: the outer container function (e.g., Map or Filter) and the inner
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
					, (MdMgr.sysNS, "ArrayOfInt")
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
					, (MdMgr.sysNS, "MapOfAny")
					, List(("containerId", MdMgr.sysNS, "MapOfAny"))
					, fcnMacrofeatures)	  
		mgr.AddFunc(MdMgr.sysNS
					, "ContainerFilter"
					, "com.ligadata.pmml.udfs.Udfs.ContainerFilter"
					, (MdMgr.sysNS, "HashMapOfAny")
					, List(("containerId", MdMgr.sysNS, "HashMapOfAny"))
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
					, (MdMgr.sysNS, "ArrayOfInt")
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
					, (MdMgr.sysNS, "MapOfAny")
					, List(("containerId", MdMgr.sysNS, "MapOfAny"))
					, fcnMacrofeatures)	  
		mgr.AddFunc(MdMgr.sysNS
					, "ContainerMap"
					, "com.ligadata.pmml.udfs.Udfs.ContainerMap"
					, (MdMgr.sysNS, "HashMapOfAny")
					, List(("containerId", MdMgr.sysNS, "HashMapOfAny"))
					, fcnMacrofeatures)	  
	  
	}
	
	def initMacroDefs {

		logger.trace("MetadataLoad...loading Macro functions")

		
		/** **************************************************************************************************************/
		
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

		/** **************************************************************************************************************/
					
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
		  				

		/** **************************************************************************************************************/

		/** ***********************************************************************
		 *  Catalog the ITERABLE only macros (no class generation needed for these 
		 **************************************************************************/
		fcnMacrofeatures.clear
		fcnMacrofeatures += FcnMacroAttr.ITERABLE

		/** 
		 	Macros associated with the 'putVariableMacroPmmlDict' macro template:
			 	"Put(String,String)"
			 	"Put(String,Int)"
			 	"Put(String,Long)"
			 	"Put(String,Double)"
			 	"Put(String,Boolean)"
			 	"Put(String,Any)"
		 	
		 	Notes: 
		 		1) No "mapped" version of the template needed for this case.
		 		2) These functions can ONLY be used inside objects that have access to the model's ctx
		 		   (e.g., inside the 'execute(ctx : Context)' function of a derived field)
		 */
		
		val putVariableMacroPmmlDict : String =   """Put(ctx, %1%, %2%)"""

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
		  
	}

	/**
	  
		mgr.AddScalar("System", "Any", tAny, "scala.Any", 100, null, null, "someimplementation")
		
		//nameSpace: String, name: String, tpNameSp: String, tpName: String, numDims: Int, ver: Int
		mgr.AddArray(MdMgr.sysNS, "ArrayOfAny", MdMgr.sysNS, "Any", 1, 1)
	 
	 */
	def initTypesFor_com_ligadata_pmml_udfs_Udfs {
		//mgr.AddScalar("System", "String", tString, "java.lang.String", 100, null, null, "someimplementation")
		//mgr.AddScalar("System", "Int", tInt, "scala.Int", 100, null, null, "someimplementation")
		//mgr.AddScalar("System", "Long", tLong, "scala.Long", 100, null, null, "someimplementation")
		//mgr.AddScalar("System", "Boolean", tBoolean, "scala.Boolean", 100, null, null, "someimplementation")
		//mgr.AddScalar("System", "Double", tDouble, "scala.Double", 100, null, null, "someimplementation")
		//mgr.AddScalar("System", "Float", tFloat, "scala.Float", 100, null, null, "someimplementation")
		//mgr.AddScalar("System", "Any", tAny, "scala.Any", 100, null, null, "someimplementation")
		//mgr.AddScalar("System", "EnvContext", tNone)
		//mgr.AddScalar("System", "BaseContainer", tNone)
		mgr.AddArray("System", "ArrayOfAny", "System", "Any", 1, 1)
		mgr.AddSet("System", "SetOfAny", "System", "Any", 1)
		mgr.AddArray("System", "ArrayOfString", "System", "String", 1, 1)
		mgr.AddTupleType("System", "TupleOfStringString", Array(("System","String"), ("System","String")), 1)
		mgr.AddArray("System", "ArrayOfTupleOfStringString", "System", "TupleOfStringString", 1, 1)
		mgr.AddArrayBuffer("System", "ArrayBufferOfString", "System", "String", 1, 1)
		mgr.AddArray("System", "ArrayOfFloat", "System", "Float", 1, 1 )
		mgr.AddArray("System", "ArrayOfDouble", "System", "Double", 1, 1)
		mgr.AddArray("System", "ArrayOfLong", "System", "Long", 1, 1)
		mgr.AddArray("System", "ArrayOfInt", "System", "Int", 1, 1)
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
		mgr.AddSortedSet("System", "SortedSetOfAny", "System", "Any", 1)
		mgr.AddQueue("System", "QueueOfAny", "System", "Any", 1)
		mgr.AddArrayBuffer("System", "ArrayBufferOfAny", "System", "Any", 1, 1)
		mgr.AddTreeSet("System", "TreeSetOfAny", "System", "Any", 1)
		mgr.AddArrayBuffer("System", "ArrayBufferOfBoolean", "System", "Boolean", 1, 1)
		mgr.AddArray("System", "ArrayOfBaseContainer", "System", "BaseContainer", 1, 1)
	}
		
	
	def init_com_ligadata_pmml_udfs_Udfs {
		mgr.AddFunc("Pmml", "Between", "com.ligadata.pmml.udfs.Udfs.Between", ("System", "ArrayOfAny"), List(("set", "System", "SetOfAny")), null)
		mgr.AddFunc("Pmml", "ToSet", "com.ligadata.pmml.udfs.Udfs.ToSet", ("System", "SetOfAny"), List(("arr", "System", "ArrayOfAny")), null)
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
		mgr.AddFunc("Pmml", "Plus", "com.ligadata.pmml.udfs.Udfs.Plus", ("System", "String"), List(("exprs", "System", "ArrayOfString")), null)
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
		mgr.AddFunc("Pmml", "GetArray", "com.ligadata.pmml.udfs.Udfs.GetArray", ("System", "ArrayOfBaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Float")), null)
		mgr.AddFunc("Pmml", "GetArray", "com.ligadata.pmml.udfs.Udfs.GetArray", ("System", "ArrayOfBaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Double")), null)
		mgr.AddFunc("Pmml", "GetArray", "com.ligadata.pmml.udfs.Udfs.GetArray", ("System", "ArrayOfBaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Long")), null)
		mgr.AddFunc("Pmml", "GetArray", "com.ligadata.pmml.udfs.Udfs.GetArray", ("System", "ArrayOfBaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Int")), null)
		mgr.AddFunc("Pmml", "GetArray", "com.ligadata.pmml.udfs.Udfs.GetArray", ("System", "ArrayOfBaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "String")), null)
		mgr.AddFunc("Pmml", "Get", "com.ligadata.pmml.udfs.Udfs.Get", ("System", "BaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Float")), null)
		mgr.AddFunc("Pmml", "Get", "com.ligadata.pmml.udfs.Udfs.Get", ("System", "BaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Double")), null)
		mgr.AddFunc("Pmml", "Get", "com.ligadata.pmml.udfs.Udfs.Get", ("System", "BaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Long")), null)
		mgr.AddFunc("Pmml", "Get", "com.ligadata.pmml.udfs.Udfs.Get", ("System", "BaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Int")), null)
		mgr.AddFunc("Pmml", "Get", "com.ligadata.pmml.udfs.Udfs.Get", ("System", "BaseContainer"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "String")), null)

		/** These are exposed as macros */
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Float"),("value", "System", "BaseContainer")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Double"),("value", "System", "BaseContainer")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Long"),("value", "System", "BaseContainer")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "Int"),("value", "System", "BaseContainer")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("gCtx", "System", "EnvContext"),("containerId", "System", "String"),("key", "System", "String"),("value", "System", "BaseContainer")), null)
		//mgr.AddFunc("Pmml", "incrementBy", "com.ligadata.pmml.udfs.Udfs.incrementBy", ("System", "Boolean"), List(("variableName", "System", "String"),("value", "System", "Int")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("variableName", "System", "String"),("value", "System", "Float")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("variableName", "System", "String"),("value", "System", "Boolean")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("variableName", "System", "String"),("value", "System", "Any")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("variableName", "System", "String"),("value", "System", "Double")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("variableName", "System", "String"),("value", "System", "Long")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("variableName", "System", "String"),("value", "System", "Int")), null)
		//mgr.AddFunc("Pmml", "Put", "com.ligadata.pmml.udfs.Udfs.Put", ("System", "Boolean"), List(("variableName", "System", "String"),("value", "System", "String")), null)
	}


}

