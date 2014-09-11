package com.Barclay.AlchemyAlerts_000100.pmml

import com.ligadata.OnLEPBase._
import com.ligadata.pmml.udfs._
import com.ligadata.pmml.udfs.Udfs._
import com.ligadata.Pmml.Runtime._
import scala.collection.mutable._
import scala.collection.immutable.{ Set }
import scala.math._
import scala.collection.immutable.StringLike
import scala.util.control.Breaks._

/**
    Application Name         : AlchemyAlerts
    PMML Model Version       : 00.01.00
    Model Name               : AlchemyRules
    Function Name            : classification
    PMML Model Source        : Pmml source supplied as string
    Copyright                : Barclay Bank (2014)
    Description              : Rules that consider bank machine transactions used to determine whether the Alchemy overdraft alerts should be issued for Barclay Bank customers 
*/

object AlchemyAlerts_000100 extends ModelBaseObj {
    def getModelName: String = "com.Barclay.AlchemyAlerts_000100.pmml.AlchemyAlerts_000100"
    def getVersion: String = "000100"
    def getModelVersion: String = getVersion
    val validMessages = Array("com.ligadata.OnLEPBankPoc.BankPocMsg_100")
    def IsValidMessage(msg: BaseMsg): Boolean = { 
        validMessages.filter( m => m == msg.getClass.getName).size > 0
    }

    def CreateNewModel(gCtx : EnvContext, msg : BaseMsg, tenantId: String): ModelBase =
    {
           new AlchemyAlerts_000100(gCtx, msg.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100], getModelName, getVersion, tenantId)
    }

} 
class AlchemyAlerts_000100(val gCtx : com.ligadata.OnLEPBase.EnvContext, val msg : com.ligadata.OnLEPBankPoc.BankPocMsg_100, val modelName:String, val modelVersion:String, val tenantId: String)
   extends ModelBase {
    val ctx : com.ligadata.Pmml.Runtime.Context = new com.ligadata.Pmml.Runtime.Context()
	   /** make the Context avaialble to std udfs to permit state updates via Put, incrementBy, et al */ 
	   com.ligadata.pmml.udfs.Udfs.SetContext(ctx) 
    def GetContext : Context = { ctx }
    override def getModelName : String = AlchemyAlerts_000100.getModelName
    override def getVersion : String = AlchemyAlerts_000100.getVersion
    override def getTenantId : String = tenantId
    var bInitialized : Boolean = false
    var ruleSetModel : RuleSetModel = null
    var simpleRules : ArrayBuffer[SimpleRule] = new ArrayBuffer[SimpleRule]

    /** Initialize the data and transformation dictionaries */
    if (! bInitialized) {
         initialize
         bInitialized = true
    }


    /***********************************************************************/
    ctx.dDict.apply("gCtx").Value(new AnyDataValue(gCtx))
    ctx.dDict.apply("msg").Value(new AnyDataValue(msg))
    /***********************************************************************/
    def initialize : AlchemyAlerts_000100 = {

        ctx.SetRuleSetModel(new RuleSetModel_classification_031("AlchemyRules", "classification", "RuleSet", ""))
        val ruleSetModel : RuleSetModel = ctx.GetRuleSetModel
        /** Initialize the RuleSetModel and SimpleRules array with new instances of respective classes */
        var simpleRuleInstances : ArrayBuffer[SimpleRule] = new ArrayBuffer[SimpleRule]()
        ruleSetModel.AddRule(new SimpleRule_EBRule_025("EBRule", "6", 0.0, 0.0, 0.0, 0.0))
        ruleSetModel.AddRule(new SimpleRule_NORule_026("NORule", "5", 0.0, 0.0, 0.0, 0.0))
        ruleSetModel.AddRule(new SimpleRule_ODRule3_027("ODRule3", "4", 0.0, 0.0, 0.0, 0.0))
        ruleSetModel.AddRule(new SimpleRule_ODRule2_028("ODRule2", "3", 0.0, 0.0, 0.0, 0.0))
        ruleSetModel.AddRule(new SimpleRule_ODRule1_029("ODRule1", "2", 0.0, 0.0, 0.0, 0.0))
        ruleSetModel.AddRule(new SimpleRule_LBRule_030("LBRule", "1", 0.0, 0.0, 0.0, 0.0))
        /* Update the ruleset model with the default score and rule selection methods collected for it */
        ruleSetModel.DefaultScore(new StringDataValue("0"))
        ruleSetModel.AddRuleSelectionMethod(new RuleSelectionMethod("firstHit"))

        /* Update each rules ScoreDistribution if necessary.... */
        /** no rule score distribution for rule1 */
        /** no rule score distribution for rule2 */
        /** no rule score distribution for rule3 */
        /** no rule score distribution for rule4 */
        /** no rule score distribution for rule5 */
        /** no rule score distribution for rule6 */

        /* Update each ruleSetModel's mining schema dict */
        ruleSetModel.AddMiningField("AlertType", new MiningField("AlertType","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("ClientPrefs.ACCT_SHORT_NM", new MiningField("ClientPrefs.ACCT_SHORT_NM","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("ClientPrefs.MOBILE_NUMBER", new MiningField("ClientPrefs.MOBILE_NUMBER","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("SendResult", new MiningField("SendResult","predicted","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("EBAlertParms.ALERT_EXPIRY_TIME", new MiningField("EBAlertParms.ALERT_EXPIRY_TIME","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("OfflineEvent", new MiningField("OfflineEvent","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("msg.RUN_LDG_XAU", new MiningField("msg.RUN_LDG_XAU","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("msg.ENT_DTE", new MiningField("msg.ENT_DTE","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))

        /* For convenience put the mining schema map in the context as well as ruleSetModel */
        ctx.MiningSchemaMap(ruleSetModel.MiningSchemaMap())
        /** initialize the data dictionary */
        var dfoo1 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.dDict += ("AlertType" -> new DataField("AlertType", "String", dfoo1, "", "", ""))
        var dfoo2 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        dfoo2 += ("gCtx" -> "valid")
        dfoo2 += ("msg" -> "valid")

        ctx.dDict += ("parameters" -> new DataField("parameters", "Any", dfoo2, "", "", ""))
        var dfoo3 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.dDict += ("SendResult" -> new DataField("SendResult", "String", dfoo3, "", "", ""))
        var dfoo4 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.dDict += ("gCtx" -> new DataField("gCtx", "Any", dfoo4, "", "", ""))
        var dfoo5 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.dDict += ("msg" -> new DataField("msg", "Any", dfoo5, "", "", ""))
        var dfoo6 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.dDict += ("OfflineEvent" -> new DataField("OfflineEvent", "String", dfoo6, "", "", ""))

        /** initialize the transformation dictionary (derived field part) */
        var xbar1 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("InOD3Range" -> new Derive_InOD3Range("InOD3Range", "Boolean", xbar1, "null", "null", ""))
        var xbar2 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SendLBResultDetermination" -> new Derive_SendLBResultDetermination("SendLBResultDetermination", "Boolean", xbar2, "null", "null", ""))
        var xbar3 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SendEBResultDetermination" -> new Derive_SendEBResultDetermination("SendEBResultDetermination", "Boolean", xbar3, "null", "null", ""))
        var xbar4 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("NOOfflineEvent" -> new Derive_NOOfflineEvent("NOOfflineEvent", "Boolean", xbar4, "null", "null", ""))
        var xbar5 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("InLowBalanceRange" -> new Derive_InLowBalanceRange("InLowBalanceRange", "Boolean", xbar5, "null", "null", ""))
        var xbar6 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("AcctHasLBLimit" -> new Derive_AcctHasLBLimit("AcctHasLBLimit", "Boolean", xbar6, "null", "null", ""))
        var xbar7 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("ShouldOnlineODEventBeIssued" -> new Derive_ShouldOnlineODEventBeIssued("ShouldOnlineODEventBeIssued", "Boolean", xbar7, "null", "null", ""))
        var xbar8 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SendOD3Result" -> new Derive_SendOD3Result("SendOD3Result", "String", xbar8, "null", "null", ""))
        var xbar9 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SumOfAlertsSentToday" -> new Derive_SumOfAlertsSentToday("SumOfAlertsSentToday", "Int", xbar9, "null", "null", ""))
        var xbar10 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("DebitHasCausedOD3Condition" -> new Derive_DebitHasCausedOD3Condition("DebitHasCausedOD3Condition", "Boolean", xbar10, "null", "null", ""))
        var xbar11 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SendNOResultDetermination" -> new Derive_SendNOResultDetermination("SendNOResultDetermination", "Boolean", xbar11, "null", "null", ""))
        var xbar12 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("ShouldEBOfflineEventBeIssued" -> new Derive_ShouldEBOfflineEventBeIssued("ShouldEBOfflineEventBeIssued", "Boolean", xbar12, "null", "null", ""))
        var xbar13 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("EBAlertType" -> new Derive_EBAlertType("EBAlertType", "Boolean", xbar13, "null", "null", ""))
        var xbar14 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("ShouldLBOfflineEventBeIssued" -> new Derive_ShouldLBOfflineEventBeIssued("ShouldLBOfflineEventBeIssued", "Boolean", xbar14, "null", "null", ""))
        var xbar15 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("EBAlertParms" -> new Derive_EBAlertParms("EBAlertParms", "Any", xbar15, "null", "null", ""))
        var xbar16 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("DebitHasCausedLBCondition" -> new Derive_DebitHasCausedLBCondition("DebitHasCausedLBCondition", "Boolean", xbar16, "null", "null", ""))
        var xbar17 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("LBOfflineEvent" -> new Derive_LBOfflineEvent("LBOfflineEvent", "Boolean", xbar17, "null", "null", ""))
        var xbar18 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("ShouldOnlineNOEventBeIssued" -> new Derive_ShouldOnlineNOEventBeIssued("ShouldOnlineNOEventBeIssued", "Boolean", xbar18, "null", "null", ""))
        var xbar19 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("ShouldODOfflineEventBeIssued" -> new Derive_ShouldODOfflineEventBeIssued("ShouldODOfflineEventBeIssued", "Boolean", xbar19, "null", "null", ""))
        var xbar20 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SendLBResult" -> new Derive_SendLBResult("SendLBResult", "String", xbar20, "null", "null", ""))
        var xbar21 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("ODOfflineEvent" -> new Derive_ODOfflineEvent("ODOfflineEvent", "Boolean", xbar21, "null", "null", ""))
        var xbar22 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("ClientAlertsToday" -> new Derive_ClientAlertsToday("ClientAlertsToday", "Any", xbar22, "null", "null", ""))
        var xbar23 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("AcctHasEBLimit" -> new Derive_AcctHasEBLimit("AcctHasEBLimit", "Boolean", xbar23, "null", "null", ""))
        var xbar24 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("DebitHasCausedOD2Condition" -> new Derive_DebitHasCausedOD2Condition("DebitHasCausedOD2Condition", "Boolean", xbar24, "null", "null", ""))
        var xbar25 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SendOD3ResultDetermination" -> new Derive_SendOD3ResultDetermination("SendOD3ResultDetermination", "Boolean", xbar25, "null", "null", ""))
        var xbar26 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("ClientPrefs" -> new Derive_ClientPrefs("ClientPrefs", "Any", xbar26, "null", "null", ""))
        var xbar27 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SendOD2Result" -> new Derive_SendOD2Result("SendOD2Result", "String", xbar27, "null", "null", ""))
        var xbar28 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("DebitHasCausedOD1Condition" -> new Derive_DebitHasCausedOD1Condition("DebitHasCausedOD1Condition", "Boolean", xbar28, "null", "null", ""))
        var xbar29 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("NearOverdraftLimit" -> new Derive_NearOverdraftLimit("NearOverdraftLimit", "Double", xbar29, "null", "null", ""))
        var xbar30 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("ShouldNOOfflineEventBeIssued" -> new Derive_ShouldNOOfflineEventBeIssued("ShouldNOOfflineEventBeIssued", "Boolean", xbar30, "null", "null", ""))
        var xbar31 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("LBAlertParms" -> new Derive_LBAlertParms("LBAlertParms", "Any", xbar31, "null", "null", ""))
        var xbar32 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("ShouldOnlineLBEventBeIssued" -> new Derive_ShouldOnlineLBEventBeIssued("ShouldOnlineLBEventBeIssued", "Boolean", xbar32, "null", "null", ""))
        var xbar33 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("DebitHasCausedNOCondition" -> new Derive_DebitHasCausedNOCondition("DebitHasCausedNOCondition", "Boolean", xbar33, "null", "null", ""))
        var xbar34 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("ODAlertParms" -> new Derive_ODAlertParms("ODAlertParms", "Any", xbar34, "null", "null", ""))
        var xbar35 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SendOD1Result" -> new Derive_SendOD1Result("SendOD1Result", "String", xbar35, "null", "null", ""))
        var xbar36 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SendOD2ResultDetermination" -> new Derive_SendOD2ResultDetermination("SendOD2ResultDetermination", "Boolean", xbar36, "null", "null", ""))
        var xbar37 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("EBOfflineEvent" -> new Derive_EBOfflineEvent("EBOfflineEvent", "Boolean", xbar37, "null", "null", ""))
        var xbar38 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("InOD1Range" -> new Derive_InOD1Range("InOD1Range", "Boolean", xbar38, "null", "null", ""))
        var xbar39 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("AcctHasODLimit" -> new Derive_AcctHasODLimit("AcctHasODLimit", "Boolean", xbar39, "null", "null", ""))
        var xbar40 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("PreviousBalance" -> new Derive_PreviousBalance("PreviousBalance", "Double", xbar40, "null", "null", ""))
        var xbar41 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("InEmergencyBorrowRange" -> new Derive_InEmergencyBorrowRange("InEmergencyBorrowRange", "Boolean", xbar41, "null", "null", ""))
        var xbar42 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("ShouldOnlineEBEventBeIssued" -> new Derive_ShouldOnlineEBEventBeIssued("ShouldOnlineEBEventBeIssued", "Boolean", xbar42, "null", "null", ""))
        var xbar43 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("InNearOverdraftRange" -> new Derive_InNearOverdraftRange("InNearOverdraftRange", "Boolean", xbar43, "null", "null", ""))
        var xbar44 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("InOD2Range" -> new Derive_InOD2Range("InOD2Range", "Boolean", xbar44, "null", "null", ""))
        var xbar45 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SendOD1ResultDetermination" -> new Derive_SendOD1ResultDetermination("SendOD1ResultDetermination", "Boolean", xbar45, "null", "null", ""))
        var xbar46 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("DebitHasCausedEBCondition" -> new Derive_DebitHasCausedEBCondition("DebitHasCausedEBCondition", "Boolean", xbar46, "null", "null", ""))
        var xbar47 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SendEBResult" -> new Derive_SendEBResult("SendEBResult", "Boolean", xbar47, "null", "null", ""))
        var xbar48 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("NOAlertParms" -> new Derive_NOAlertParms("NOAlertParms", "Any", xbar48, "null", "null", ""))
        var xbar49 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SendNOResult" -> new Derive_SendNOResult("SendNOResult", "String", xbar49, "null", "null", ""))

        /** fill the Context's mining field dictionary ...*/
        //val ruleSetModel : RuleSetModel = ctx.GetRuleSetModel
        /** put a reference of the mining schema map in the context for convenience. */
        ctx.MiningSchemaMap(ruleSetModel.MiningSchemaMap())

        /** Build the dictionary of model identifiers 
            Keys are: 
                 ApplicationName , FunctionName, PMML, Version,  
                 Copyright, Description, ModelName, ClassName 
         */
        ctx.pmmlModelIdentifiers("ApplicationName") = Some("AlchemyAlerts")
        ctx.pmmlModelIdentifiers("FunctionName") = Some("classification")
        ctx.pmmlModelIdentifiers("PMML") = Some("Pmml source supplied as string")
        ctx.pmmlModelIdentifiers("Version") = Some("00.01.00")
        ctx.pmmlModelIdentifiers("Copyright") = Some("Barclay Bank (2014)")
        ctx.pmmlModelIdentifiers("Description") = Some("Rules that consider bank machine transactions used to determine whether the Alchemy overdraft alerts should be issued for Barclay Bank customers ")
        ctx.pmmlModelIdentifiers("ModelName") = Some("AlchemyRules")

        ctx.pmmlModelIdentifiers("ClassName") = Some("AlchemyAlerts_000100")

        this
    }   /** end of initialize fcn  */	

    /** provide access to the ruleset model's execute function */
    def execute(outputCurrentModel:Boolean) : ModelResult = {
        ctx.GetRuleSetModel.execute(ctx)
        prepareResults
    }


    /** prepare output results scored by the rules. */
    def prepareResults : ModelResult = {

        val results : Array[Result] = GetContext.GetRuleSetModel.MiningSchemaMap().retain((k,v) => 
    	  		v.usageType == "predicted" || v.usageType == "supplementary").values.toArray.map(mCol => 
    	  		  	{ 

    	  		  	    val someValue : DataValue = ctx.valueFor(mCol.name) 
    	  		  	    val value : Any = someValue match { 
    	  		  	        case d    : DoubleDataValue   => someValue.asInstanceOf[DoubleDataValue].Value 
    	  		  	        case f    : FloatDataValue    => someValue.asInstanceOf[FloatDataValue].Value 
    	  		  	        case l    : LongDataValue     => someValue.asInstanceOf[LongDataValue].Value 
    	  		  	        case i    : IntDataValue      => someValue.asInstanceOf[IntDataValue].Value 
    	  		  	        case b    : BooleanDataValue  => someValue.asInstanceOf[BooleanDataValue].Value 
    	  		  	        case ddv  : DateDataValue     => someValue.asInstanceOf[DateDataValue].Value 
    	  		  	        case dtdv : DateTimeDataValue => someValue.asInstanceOf[DateTimeDataValue].Value 
    	  		  	        case tdv  : TimeDataValue     => someValue.asInstanceOf[TimeDataValue].Value 
    	  		  	        case s    : StringDataValue   => someValue.asInstanceOf[StringDataValue].Value 

    	  		  	        case _ => someValue.asInstanceOf[AnyDataValue].Value 
    	  		  	    } 

    	  		  	    new Result(mCol.name, MinVarType.StrToMinVarType(mCol.usageType), value)  

    	  		  	}) 
        val millisecsSinceMidnight: Long = Builtins.dateMilliSecondsSinceMidnight().toLong 
        val now: org.joda.time.DateTime = new org.joda.time.DateTime() 
        val nowStr: String = now.toString 
        val dateMillis : Long = now.getMillis.toLong - millisecsSinceMidnight 
        new ModelResult(dateMillis, nowStr, AlchemyAlerts_000100.getModelName, AlchemyAlerts_000100.getModelVersion, results) 
    }

}

/*************** Derived Field Class Definitions ***************/

class Derive_ClientAlertsToday (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : AnyDataValue = {
        val ClientAlertsToday = Get(ctx.valueFor("gCtx").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBase.EnvContext], "AlertHistory", ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].ENT_ACC_NUM)
        ctx.xDict.apply("ClientAlertsToday").Value(new AnyDataValue(ClientAlertsToday))
          new AnyDataValue(ClientAlertsToday)
    }

}


class Derive_ClientPrefs (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : AnyDataValue = {
        val ClientPrefs = Get(ctx.valueFor("gCtx").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBase.EnvContext], "CustomerPreferences", ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].ENT_ACC_NUM)
        ctx.xDict.apply("ClientPrefs").Value(new AnyDataValue(ClientPrefs))
          new AnyDataValue(ClientPrefs)
    }

}


class Derive_EBAlertParms (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : AnyDataValue = {
        val EBAlertParms = Get(ctx.valueFor("gCtx").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBase.EnvContext], "AlertParameters", "EB")
        ctx.xDict.apply("EBAlertParms").Value(new AnyDataValue(EBAlertParms))
          new AnyDataValue(EBAlertParms)
    }

}


class Derive_NOAlertParms (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : AnyDataValue = {
        val NOAlertParms = Get(ctx.valueFor("gCtx").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBase.EnvContext], "AlertParameters", "NO")
        ctx.xDict.apply("NOAlertParms").Value(new AnyDataValue(NOAlertParms))
          new AnyDataValue(NOAlertParms)
    }

}


class Derive_ODAlertParms (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : AnyDataValue = {
        val ODAlertParms = Get(ctx.valueFor("gCtx").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBase.EnvContext], "AlertParameters", "OD")
        ctx.xDict.apply("ODAlertParms").Value(new AnyDataValue(ODAlertParms))
          new AnyDataValue(ODAlertParms)
    }

}


class Derive_LBAlertParms (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : AnyDataValue = {
        val LBAlertParms = Get(ctx.valueFor("gCtx").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBase.EnvContext], "AlertParameters", "LB")
        ctx.xDict.apply("LBAlertParms").Value(new AnyDataValue(LBAlertParms))
          new AnyDataValue(LBAlertParms)
    }

}


class Derive_ShouldOnlineEBEventBeIssued (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val ShouldOnlineEBEventBeIssued = Not(Or(GreaterOrEqual(Plus(ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].EB001Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].EB002Sent), ctx.valueFor("EBAlertParms").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertParameters_100].MAX_ALERTS_PER_DAY), GreaterOrEqual(Plus(ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].UTFSent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].PTFSent), 0)))
        ctx.xDict.apply("ShouldOnlineEBEventBeIssued").Value(new BooleanDataValue(ShouldOnlineEBEventBeIssued))
          new BooleanDataValue(ShouldOnlineEBEventBeIssued)
    }

}


class Derive_ShouldOnlineNOEventBeIssued (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val ShouldOnlineNOEventBeIssued = Not(Or(GreaterOrEqual(Plus(ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].EB001Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].EB002Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].NO001Sent), ctx.valueFor("NOAlertParms").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertParameters_100].MAX_ALERTS_PER_DAY), GreaterOrEqual(Plus(ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].UTFSent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].PTFSent), 0)))
        ctx.xDict.apply("ShouldOnlineNOEventBeIssued").Value(new BooleanDataValue(ShouldOnlineNOEventBeIssued))
          new BooleanDataValue(ShouldOnlineNOEventBeIssued)
    }

}


class Derive_ShouldOnlineODEventBeIssued (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val ShouldOnlineODEventBeIssued = Not(Or(GreaterOrEqual(Plus(ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].EB001Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].EB002Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].NO001Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].OD001Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].OD002Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].OD003Sent), ctx.valueFor("ODAlertParms").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertParameters_100].MAX_ALERTS_PER_DAY), GreaterOrEqual(Plus(ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].UTFSent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].PTFSent), 0)))
        ctx.xDict.apply("ShouldOnlineODEventBeIssued").Value(new BooleanDataValue(ShouldOnlineODEventBeIssued))
          new BooleanDataValue(ShouldOnlineODEventBeIssued)
    }

}


class Derive_ShouldOnlineLBEventBeIssued (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val ShouldOnlineLBEventBeIssued = Not(Or(GreaterOrEqual(Plus(ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].EB001Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].EB002Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].NO001Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].OD001Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].OD002Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].OD003Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].LB001Sent), ctx.valueFor("LBAlertParms").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertParameters_100].MAX_ALERTS_PER_DAY), GreaterOrEqual(Plus(ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].UTFSent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].PTFSent), 0)))
        ctx.xDict.apply("ShouldOnlineLBEventBeIssued").Value(new BooleanDataValue(ShouldOnlineLBEventBeIssued))
          new BooleanDataValue(ShouldOnlineLBEventBeIssued)
    }

}


class Derive_EBAlertType (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val EBAlertType = Or(And(Equal(ctx.valueFor("ClientPrefs").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.CustomerPreferences_100].MAX_EB_CNT, 7), new AlertType_value1_Put("AlertType", "EB1").Put), new AlertType_value2_Put("AlertType", "EB2").Put)
        ctx.xDict.apply("EBAlertType").Value(new BooleanDataValue(EBAlertType))
          new BooleanDataValue(EBAlertType)
    }

}

	class AlertType_value1_Put(var AlertType : String, val value1 : String)
	{
	  	def Put  : Boolean = { 
	  		val set : Boolean = ctx.valuePut(AlertType, value1)
			set 
		}
	} 
	class AlertType_value2_Put(var AlertType : String, val value2 : String)
	{
	  	def Put  : Boolean = { 
	  		val set : Boolean = ctx.valuePut(AlertType, value2)
			set 
		}
	} 

class Derive_SumOfAlertsSentToday (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : IntDataValue = {
        val SumOfAlertsSentToday = Plus(ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].LB001Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].OD001Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].OD002Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].OD003Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].NO001Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].EB001Sent, ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100].EB002Sent)
        ctx.xDict.apply("SumOfAlertsSentToday").Value(new IntDataValue(SumOfAlertsSentToday))
          new IntDataValue(SumOfAlertsSentToday)
    }

}


class Derive_EBOfflineEvent (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val EBOfflineEvent = Not(Between(CompressedTimeHHMMSSCC2Secs(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].ENT_TME), ctx.valueFor("EBAlertParms").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertParameters_100].ONLINE_START_TIME, ctx.valueFor("EBAlertParms").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertParameters_100].ONLINE_END_TIME, true))
        ctx.xDict.apply("EBOfflineEvent").Value(new BooleanDataValue(EBOfflineEvent))
          new BooleanDataValue(EBOfflineEvent)
    }

}


class Derive_ShouldEBOfflineEventBeIssued (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val ShouldEBOfflineEventBeIssued = And(Equal(ctx.valueFor("EBOfflineEvent").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("SumOfAlertsSentToday").asInstanceOf[IntDataValue].Value, 0), new OfflineEvent_value3_Put("OfflineEvent", "true").Put)
        ctx.xDict.apply("ShouldEBOfflineEventBeIssued").Value(new BooleanDataValue(ShouldEBOfflineEventBeIssued))
          new BooleanDataValue(ShouldEBOfflineEventBeIssued)
    }

}

	class OfflineEvent_value3_Put(var OfflineEvent : String, val value3 : String)
	{
	  	def Put  : Boolean = { 
	  		val set : Boolean = ctx.valuePut(OfflineEvent, value3)
			set 
		}
	} 

class Derive_NOOfflineEvent (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val NOOfflineEvent = And(Not(Between(CompressedTimeHHMMSSCC2Secs(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].ENT_TME), ctx.valueFor("NOAlertParms").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertParameters_100].ONLINE_START_TIME, ctx.valueFor("NOAlertParms").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertParameters_100].ONLINE_END_TIME, true)), new OfflineEvent_value4_Put("OfflineEvent", "true").Put)
        ctx.xDict.apply("NOOfflineEvent").Value(new BooleanDataValue(NOOfflineEvent))
          new BooleanDataValue(NOOfflineEvent)
    }

}

	class OfflineEvent_value4_Put(var OfflineEvent : String, val value4 : String)
	{
	  	def Put  : Boolean = { 
	  		val set : Boolean = ctx.valuePut(OfflineEvent, value4)
			set 
		}
	} 

class Derive_ShouldNOOfflineEventBeIssued (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val ShouldNOOfflineEventBeIssued = And(Equal(ctx.valueFor("NOOfflineEvent").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("SumOfAlertsSentToday").asInstanceOf[IntDataValue].Value, 0))
        ctx.xDict.apply("ShouldNOOfflineEventBeIssued").Value(new BooleanDataValue(ShouldNOOfflineEventBeIssued))
          new BooleanDataValue(ShouldNOOfflineEventBeIssued)
    }

}


class Derive_ODOfflineEvent (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val ODOfflineEvent = And(Not(Between(CompressedTimeHHMMSSCC2Secs(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].ENT_TME), ctx.valueFor("ODAlertParms").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertParameters_100].ONLINE_START_TIME, ctx.valueFor("ODAlertParms").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertParameters_100].ONLINE_END_TIME, true)), new OfflineEvent_value5_Put("OfflineEvent", "true").Put)
        ctx.xDict.apply("ODOfflineEvent").Value(new BooleanDataValue(ODOfflineEvent))
          new BooleanDataValue(ODOfflineEvent)
    }

}

	class OfflineEvent_value5_Put(var OfflineEvent : String, val value5 : String)
	{
	  	def Put  : Boolean = { 
	  		val set : Boolean = ctx.valuePut(OfflineEvent, value5)
			set 
		}
	} 

class Derive_ShouldODOfflineEventBeIssued (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val ShouldODOfflineEventBeIssued = And(Equal(ctx.valueFor("ODOfflineEvent").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("SumOfAlertsSentToday").asInstanceOf[IntDataValue].Value, 0))
        ctx.xDict.apply("ShouldODOfflineEventBeIssued").Value(new BooleanDataValue(ShouldODOfflineEventBeIssued))
          new BooleanDataValue(ShouldODOfflineEventBeIssued)
    }

}


class Derive_LBOfflineEvent (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val LBOfflineEvent = And(Not(Between(CompressedTimeHHMMSSCC2Secs(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].ENT_TME), ctx.valueFor("LBAlertParms").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertParameters_100].ONLINE_START_TIME, ctx.valueFor("LBAlertParms").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertParameters_100].ONLINE_END_TIME, true)), new OfflineEvent_value6_Put("OfflineEvent", "true").Put)
        ctx.xDict.apply("LBOfflineEvent").Value(new BooleanDataValue(LBOfflineEvent))
          new BooleanDataValue(LBOfflineEvent)
    }

}

	class OfflineEvent_value6_Put(var OfflineEvent : String, val value6 : String)
	{
	  	def Put  : Boolean = { 
	  		val set : Boolean = ctx.valuePut(OfflineEvent, value6)
			set 
		}
	} 

class Derive_ShouldLBOfflineEventBeIssued (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val ShouldLBOfflineEventBeIssued = And(Equal(ctx.valueFor("LBOfflineEvent").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("SumOfAlertsSentToday").asInstanceOf[IntDataValue].Value, 0))
        ctx.xDict.apply("ShouldLBOfflineEventBeIssued").Value(new BooleanDataValue(ShouldLBOfflineEventBeIssued))
          new BooleanDataValue(ShouldLBOfflineEventBeIssued)
    }

}


class Derive_PreviousBalance (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : DoubleDataValue = {
        val PreviousBalance = Minus(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].ENT_AMT)
        ctx.xDict.apply("PreviousBalance").Value(new DoubleDataValue(PreviousBalance))
          new DoubleDataValue(PreviousBalance)
    }

}


class Derive_InEmergencyBorrowRange (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val InEmergencyBorrowRange = And(GreaterThan(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, Plus(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].ODR_LMT, ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].ANT_LMT, ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].UNP_BUFF)), LessThan(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, Plus(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].ODR_LMT, ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].EB_BUFFER)))
        ctx.xDict.apply("InEmergencyBorrowRange").Value(new BooleanDataValue(InEmergencyBorrowRange))
          new BooleanDataValue(InEmergencyBorrowRange)
    }

}


class Derive_DebitHasCausedEBCondition (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val DebitHasCausedEBCondition = And(GreaterOrEqual(ctx.valueFor("PreviousBalance").asInstanceOf[DoubleDataValue].Value, Plus(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].ODR_LMT, ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].EB_BUFFER)), LessThan(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, Plus(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].ODR_LMT, ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].EB_BUFFER)))
        ctx.xDict.apply("DebitHasCausedEBCondition").Value(new BooleanDataValue(DebitHasCausedEBCondition))
          new BooleanDataValue(DebitHasCausedEBCondition)
    }

}


class Derive_NearOverdraftLimit (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : DoubleDataValue = {
        val NearOverdraftLimit = Minus(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].ODR_LMT, Multiply(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].ODR_LMT, Divide(ctx.valueFor("ClientPrefs").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.CustomerPreferences_100].NO_FACTOR, 100)))
        ctx.xDict.apply("NearOverdraftLimit").Value(new DoubleDataValue(NearOverdraftLimit))
          new DoubleDataValue(NearOverdraftLimit)
    }

}


class Derive_InNearOverdraftRange (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val InNearOverdraftRange = And(LessThan(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, ctx.valueFor("NearOverdraftLimit").asInstanceOf[DoubleDataValue].Value), GreaterThan(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, Plus(ctx.valueFor("NearOverdraftLimit").asInstanceOf[DoubleDataValue].Value, ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].EB_BUFFER)))
        ctx.xDict.apply("InNearOverdraftRange").Value(new BooleanDataValue(InNearOverdraftRange))
          new BooleanDataValue(InNearOverdraftRange)
    }

}


class Derive_DebitHasCausedNOCondition (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val DebitHasCausedNOCondition = And(GreaterOrEqual(ctx.valueFor("PreviousBalance").asInstanceOf[DoubleDataValue].Value, ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU), LessThan(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, ctx.valueFor("NearOverdraftLimit").asInstanceOf[DoubleDataValue].Value))
        ctx.xDict.apply("DebitHasCausedNOCondition").Value(new BooleanDataValue(DebitHasCausedNOCondition))
          new BooleanDataValue(DebitHasCausedNOCondition)
    }

}


class Derive_InOD3Range (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val InOD3Range = And(LessThan(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, ctx.valueFor("ClientPrefs").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.CustomerPreferences_100].OD_T2_LIMIT), GreaterThan(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, Plus(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].ODR_LMT, ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].EB_BUFFER)))
        ctx.xDict.apply("InOD3Range").Value(new BooleanDataValue(InOD3Range))
          new BooleanDataValue(InOD3Range)
    }

}


class Derive_DebitHasCausedOD3Condition (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val DebitHasCausedOD3Condition = And(GreaterOrEqual(ctx.valueFor("PreviousBalance").asInstanceOf[DoubleDataValue].Value, ctx.valueFor("ClientPrefs").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.CustomerPreferences_100].OD_T2_LIMIT), LessThan(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, ctx.valueFor("ClientPrefs").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.CustomerPreferences_100].OD_T2_LIMIT))
        ctx.xDict.apply("DebitHasCausedOD3Condition").Value(new BooleanDataValue(DebitHasCausedOD3Condition))
          new BooleanDataValue(DebitHasCausedOD3Condition)
    }

}


class Derive_InOD2Range (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val InOD2Range = And(GreaterOrEqual(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, ctx.valueFor("ClientPrefs").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.CustomerPreferences_100].OD_T2_LIMIT), LessThan(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, ctx.valueFor("ClientPrefs").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.CustomerPreferences_100].OD_T1_LIMIT))
        ctx.xDict.apply("InOD2Range").Value(new BooleanDataValue(InOD2Range))
          new BooleanDataValue(InOD2Range)
    }

}


class Derive_DebitHasCausedOD2Condition (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val DebitHasCausedOD2Condition = And(GreaterOrEqual(ctx.valueFor("PreviousBalance").asInstanceOf[DoubleDataValue].Value, ctx.valueFor("ClientPrefs").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.CustomerPreferences_100].OD_T1_LIMIT), LessThan(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, ctx.valueFor("ClientPrefs").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.CustomerPreferences_100].OD_T1_LIMIT))
        ctx.xDict.apply("DebitHasCausedOD2Condition").Value(new BooleanDataValue(DebitHasCausedOD2Condition))
          new BooleanDataValue(DebitHasCausedOD2Condition)
    }

}


class Derive_InOD1Range (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val InOD1Range = And(GreaterOrEqual(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, Plus(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].ODR_LMT, ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].EB_BUFFER)), GreaterOrEqual(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, ctx.valueFor("ClientPrefs").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.CustomerPreferences_100].OD_T1_LIMIT), LessThan(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].OD_BUFFER))
        ctx.xDict.apply("InOD1Range").Value(new BooleanDataValue(InOD1Range))
          new BooleanDataValue(InOD1Range)
    }

}


class Derive_DebitHasCausedOD1Condition (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val DebitHasCausedOD1Condition = And(GreaterOrEqual(ctx.valueFor("PreviousBalance").asInstanceOf[DoubleDataValue].Value, ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].OD_BUFFER), LessThan(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].OD_BUFFER))
        ctx.xDict.apply("DebitHasCausedOD1Condition").Value(new BooleanDataValue(DebitHasCausedOD1Condition))
          new BooleanDataValue(DebitHasCausedOD1Condition)
    }

}


class Derive_InLowBalanceRange (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val InLowBalanceRange = And(LessThan(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].LB_LIMIT), GreaterOrEqual(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].OD_BUFFER))
        ctx.xDict.apply("InLowBalanceRange").Value(new BooleanDataValue(InLowBalanceRange))
          new BooleanDataValue(InLowBalanceRange)
    }

}


class Derive_DebitHasCausedLBCondition (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val DebitHasCausedLBCondition = And(GreaterOrEqual(ctx.valueFor("PreviousBalance").asInstanceOf[DoubleDataValue].Value, ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].LB_LIMIT), LessThan(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].RUN_LDG_XAU, ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].LB_LIMIT))
        ctx.xDict.apply("DebitHasCausedLBCondition").Value(new BooleanDataValue(DebitHasCausedLBCondition))
          new BooleanDataValue(DebitHasCausedLBCondition)
    }

}


class Derive_AcctHasEBLimit (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val AcctHasEBLimit = LessThan(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].ANT_LMT, 0)
        ctx.xDict.apply("AcctHasEBLimit").Value(new BooleanDataValue(AcctHasEBLimit))
          new BooleanDataValue(AcctHasEBLimit)
    }

}


class Derive_AcctHasODLimit (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val AcctHasODLimit = GreaterThan(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100].ODR_LMT, 0)
        ctx.xDict.apply("AcctHasODLimit").Value(new BooleanDataValue(AcctHasODLimit))
          new BooleanDataValue(AcctHasODLimit)
    }

}


class Derive_AcctHasLBLimit (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val AcctHasLBLimit = GreaterThan(ctx.valueFor("ClientPrefs").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.CustomerPreferences_100].LB_LIMIT, 0)
        ctx.xDict.apply("AcctHasLBLimit").Value(new BooleanDataValue(AcctHasLBLimit))
          new BooleanDataValue(AcctHasLBLimit)
    }

}


class Derive_SendEBResult (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val SendEBResult = And(Equal(ctx.valueFor("AcctHasEBLimit").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("InEmergencyBorrowRange").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("DebitHasCausedEBCondition").asInstanceOf[BooleanDataValue].Value, true), Or(Equal(ctx.valueFor("ShouldEBOfflineEventBeIssued").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("ShouldOnlineEBEventBeIssued").asInstanceOf[BooleanDataValue].Value, true)))
        ctx.xDict.apply("SendEBResult").Value(new BooleanDataValue(SendEBResult))
          new BooleanDataValue(SendEBResult)
    }

}


class Derive_SendNOResult (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : StringDataValue = {
        val SendNOResult = If(And(Equal(ctx.valueFor("InNearOverdraftRange").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("DebitHasCausedNOCondition").asInstanceOf[BooleanDataValue].Value, true), Or(Equal(ctx.valueFor("ShouldNOOfflineEventBeIssued").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("ShouldOnlineNOEventBeIssued").asInstanceOf[BooleanDataValue].Value, true))))
        var result : String = if (SendNOResult) "NO1" else "NOTSET"
        ctx.xDict.apply("SendNOResult").Value(new StringDataValue(result))
        new StringDataValue(result)
    }

}


class Derive_SendOD3Result (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : StringDataValue = {
        val SendOD3Result = If(And(Equal(ctx.valueFor("InOD3Range").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("DebitHasCausedOD3Condition").asInstanceOf[BooleanDataValue].Value, true), Or(Equal(ctx.valueFor("ShouldODOfflineEventBeIssued").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("ShouldOnlineODEventBeIssued").asInstanceOf[BooleanDataValue].Value, true))))
        var result : String = if (SendOD3Result) "OD3" else "NOTSET"
        ctx.xDict.apply("SendOD3Result").Value(new StringDataValue(result))
        new StringDataValue(result)
    }

}


class Derive_SendOD2Result (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : StringDataValue = {
        val SendOD2Result = If(And(Equal(ctx.valueFor("InOD2Range").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("DebitHasCausedOD2Condition").asInstanceOf[BooleanDataValue].Value, true), Or(Equal(ctx.valueFor("ShouldODOfflineEventBeIssued").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("ShouldOnlineODEventBeIssued").asInstanceOf[BooleanDataValue].Value, true))))
        var result : String = if (SendOD2Result) "OD2" else "NOTSET"
        ctx.xDict.apply("SendOD2Result").Value(new StringDataValue(result))
        new StringDataValue(result)
    }

}


class Derive_SendOD1Result (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : StringDataValue = {
        val SendOD1Result = If(And(Equal(ctx.valueFor("InOD1Range").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("DebitHasCausedOD1Condition").asInstanceOf[BooleanDataValue].Value, true), Or(Equal(ctx.valueFor("ShouldODOfflineEventBeIssued").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("ShouldOnlineODEventBeIssued").asInstanceOf[BooleanDataValue].Value, true))))
        var result : String = if (SendOD1Result) "OD1" else "NOTSET"
        ctx.xDict.apply("SendOD1Result").Value(new StringDataValue(result))
        new StringDataValue(result)
    }

}


class Derive_SendLBResult (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : StringDataValue = {
        val SendLBResult = If(And(Equal(ctx.valueFor("AcctHasLBLimit").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("InLowBalanceRange").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("DebitHasCausedLBCondition").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("ShouldLBOfflineEventBeIssued").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("ShouldOnlineLBEventBeIssued").asInstanceOf[BooleanDataValue].Value, true)))
        var result : String = if (SendLBResult) "LB1" else "NOTSET"
        ctx.xDict.apply("SendLBResult").Value(new StringDataValue(result))
        new StringDataValue(result)
    }

}


class Derive_SendEBResultDetermination (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val SendEBResultDetermination = And(Equal(ctx.valueFor("SendEBResult").asInstanceOf[BooleanDataValue].Value, true), Equal(ctx.valueFor("EBAlertType").asInstanceOf[BooleanDataValue].Value, true), startsWith(ctx.valueFor("AlertType").asInstanceOf[StringDataValue].Value, "EB"), Or(And(Equal(ctx.valueFor("AlertType").asInstanceOf[StringDataValue].Value, "EB2"), new ClientAlertsToday_EB002Sent_incrementBy(ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100], 1).incrementBy), And(Equal(ctx.valueFor("AlertType").asInstanceOf[StringDataValue].Value, "EB1"), new ClientAlertsToday_EB001Sent_incrementBy(ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100], 1).incrementBy)), new gCtx_value9_msg_ENT_ACC_NUM_Put(ctx.valueFor("gCtx").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBase.EnvContext], "AlertHistory", ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100], ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100]).Put)
        ctx.xDict.apply("SendEBResultDetermination").Value(new BooleanDataValue(SendEBResultDetermination))
          new BooleanDataValue(SendEBResultDetermination)
    }

}

	class ClientAlertsToday_EB002Sent_incrementBy(var ClientAlertsToday : com.ligadata.OnLEPBankPoc.AlertHistory_100, val value7 : Int)
	{
	  	def incrementBy  : Boolean = { ClientAlertsToday.EB002Sent += value7; true }
	} 
	class ClientAlertsToday_EB001Sent_incrementBy(var ClientAlertsToday : com.ligadata.OnLEPBankPoc.AlertHistory_100, val value8 : Int)
	{
	  	def incrementBy  : Boolean = { ClientAlertsToday.EB001Sent += value8; true }
	} 
	class gCtx_value9_msg_ENT_ACC_NUM_Put(var gCtx : com.ligadata.OnLEPBase.EnvContext, val value9 : String, val msg : com.ligadata.OnLEPBankPoc.BankPocMsg_100, val ClientAlertsToday : com.ligadata.OnLEPBankPoc.AlertHistory_100)
	{
	  	def Put  : Boolean = { gCtx.setObject(value9, msg.ENT_ACC_NUM.toString, ClientAlertsToday); true }
	} 

class Derive_SendNOResultDetermination (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val SendNOResultDetermination = And(Equal(ctx.valueFor("SendNOResult").asInstanceOf[StringDataValue].Value, "NO1"), new AlertType_value10_Put("AlertType", "NO1").Put, new ClientAlertsToday_NO001Sent_incrementBy(ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100], 1).incrementBy, new gCtx_value12_msg_ENT_ACC_NUM_Put(ctx.valueFor("gCtx").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBase.EnvContext], "AlertHistory", ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100], ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100]).Put)
        ctx.xDict.apply("SendNOResultDetermination").Value(new BooleanDataValue(SendNOResultDetermination))
          new BooleanDataValue(SendNOResultDetermination)
    }

}

	class AlertType_value10_Put(var AlertType : String, val value10 : String)
	{
	  	def Put  : Boolean = { 
	  		val set : Boolean = ctx.valuePut(AlertType, value10)
			set 
		}
	} 
	class ClientAlertsToday_NO001Sent_incrementBy(var ClientAlertsToday : com.ligadata.OnLEPBankPoc.AlertHistory_100, val value11 : Int)
	{
	  	def incrementBy  : Boolean = { ClientAlertsToday.NO001Sent += value11; true }
	} 
	class gCtx_value12_msg_ENT_ACC_NUM_Put(var gCtx : com.ligadata.OnLEPBase.EnvContext, val value12 : String, val msg : com.ligadata.OnLEPBankPoc.BankPocMsg_100, val ClientAlertsToday : com.ligadata.OnLEPBankPoc.AlertHistory_100)
	{
	  	def Put  : Boolean = { gCtx.setObject(value12, msg.ENT_ACC_NUM.toString, ClientAlertsToday); true }
	} 

class Derive_SendOD3ResultDetermination (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val SendOD3ResultDetermination = And(Equal(ctx.valueFor("SendOD3Result").asInstanceOf[StringDataValue].Value, "OD3"), new AlertType_value13_Put("AlertType", "OD3").Put, new ClientAlertsToday_OD003Sent_incrementBy(ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100], 1).incrementBy, new gCtx_value15_msg_ENT_ACC_NUM_Put(ctx.valueFor("gCtx").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBase.EnvContext], "AlertHistory", ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100], ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100]).Put)
        ctx.xDict.apply("SendOD3ResultDetermination").Value(new BooleanDataValue(SendOD3ResultDetermination))
          new BooleanDataValue(SendOD3ResultDetermination)
    }

}

	class AlertType_value13_Put(var AlertType : String, val value13 : String)
	{
	  	def Put  : Boolean = { 
	  		val set : Boolean = ctx.valuePut(AlertType, value13)
			set 
		}
	} 
	class ClientAlertsToday_OD003Sent_incrementBy(var ClientAlertsToday : com.ligadata.OnLEPBankPoc.AlertHistory_100, val value14 : Int)
	{
	  	def incrementBy  : Boolean = { ClientAlertsToday.OD003Sent += value14; true }
	} 
	class gCtx_value15_msg_ENT_ACC_NUM_Put(var gCtx : com.ligadata.OnLEPBase.EnvContext, val value15 : String, val msg : com.ligadata.OnLEPBankPoc.BankPocMsg_100, val ClientAlertsToday : com.ligadata.OnLEPBankPoc.AlertHistory_100)
	{
	  	def Put  : Boolean = { gCtx.setObject(value15, msg.ENT_ACC_NUM.toString, ClientAlertsToday); true }
	} 

class Derive_SendOD2ResultDetermination (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val SendOD2ResultDetermination = And(Equal(ctx.valueFor("SendOD2Result").asInstanceOf[StringDataValue].Value, "OD2"), new AlertType_value16_Put("AlertType", "OD2").Put, new ClientAlertsToday_OD002Sent_incrementBy(ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100], 1).incrementBy, new gCtx_value18_msg_ENT_ACC_NUM_Put(ctx.valueFor("gCtx").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBase.EnvContext], "AlertHistory", ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100], ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100]).Put)
        ctx.xDict.apply("SendOD2ResultDetermination").Value(new BooleanDataValue(SendOD2ResultDetermination))
          new BooleanDataValue(SendOD2ResultDetermination)
    }

}

	class AlertType_value16_Put(var AlertType : String, val value16 : String)
	{
	  	def Put  : Boolean = { 
	  		val set : Boolean = ctx.valuePut(AlertType, value16)
			set 
		}
	} 
	class ClientAlertsToday_OD002Sent_incrementBy(var ClientAlertsToday : com.ligadata.OnLEPBankPoc.AlertHistory_100, val value17 : Int)
	{
	  	def incrementBy  : Boolean = { ClientAlertsToday.OD002Sent += value17; true }
	} 
	class gCtx_value18_msg_ENT_ACC_NUM_Put(var gCtx : com.ligadata.OnLEPBase.EnvContext, val value18 : String, val msg : com.ligadata.OnLEPBankPoc.BankPocMsg_100, val ClientAlertsToday : com.ligadata.OnLEPBankPoc.AlertHistory_100)
	{
	  	def Put  : Boolean = { gCtx.setObject(value18, msg.ENT_ACC_NUM.toString, ClientAlertsToday); true }
	} 

class Derive_SendOD1ResultDetermination (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val SendOD1ResultDetermination = And(Equal(ctx.valueFor("SendOD1Result").asInstanceOf[StringDataValue].Value, "OD1"), new AlertType_value19_Put("AlertType", "OD1").Put, new ClientAlertsToday_OD001Sent_incrementBy(ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100], 1).incrementBy, new gCtx_value21_msg_ENT_ACC_NUM_Put(ctx.valueFor("gCtx").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBase.EnvContext], "AlertHistory", ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100], ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100]).Put)
        ctx.xDict.apply("SendOD1ResultDetermination").Value(new BooleanDataValue(SendOD1ResultDetermination))
          new BooleanDataValue(SendOD1ResultDetermination)
    }

}

	class AlertType_value19_Put(var AlertType : String, val value19 : String)
	{
	  	def Put  : Boolean = { 
	  		val set : Boolean = ctx.valuePut(AlertType, value19)
			set 
		}
	} 
	class ClientAlertsToday_OD001Sent_incrementBy(var ClientAlertsToday : com.ligadata.OnLEPBankPoc.AlertHistory_100, val value20 : Int)
	{
	  	def incrementBy  : Boolean = { ClientAlertsToday.OD001Sent += value20; true }
	} 
	class gCtx_value21_msg_ENT_ACC_NUM_Put(var gCtx : com.ligadata.OnLEPBase.EnvContext, val value21 : String, val msg : com.ligadata.OnLEPBankPoc.BankPocMsg_100, val ClientAlertsToday : com.ligadata.OnLEPBankPoc.AlertHistory_100)
	{
	  	def Put  : Boolean = { gCtx.setObject(value21, msg.ENT_ACC_NUM.toString, ClientAlertsToday); true }
	} 

class Derive_SendLBResultDetermination (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val SendLBResultDetermination = And(Equal(ctx.valueFor("SendLBResult").asInstanceOf[StringDataValue].Value, "LB1"), new AlertType_value22_Put("AlertType", "LB1").Put, new ClientAlertsToday_LB001Sent_incrementBy(ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100], 1).incrementBy, new gCtx_value24_msg_ENT_ACC_NUM_Put(ctx.valueFor("gCtx").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBase.EnvContext], "AlertHistory", ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.BankPocMsg_100], ctx.valueFor("ClientAlertsToday").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBankPoc.AlertHistory_100]).Put)
        ctx.xDict.apply("SendLBResultDetermination").Value(new BooleanDataValue(SendLBResultDetermination))
          new BooleanDataValue(SendLBResultDetermination)
    }

}

	class AlertType_value22_Put(var AlertType : String, val value22 : String)
	{
	  	def Put  : Boolean = { 
	  		val set : Boolean = ctx.valuePut(AlertType, value22)
			set 
		}
	} 
	class ClientAlertsToday_LB001Sent_incrementBy(var ClientAlertsToday : com.ligadata.OnLEPBankPoc.AlertHistory_100, val value23 : Int)
	{
	  	def incrementBy  : Boolean = { ClientAlertsToday.LB001Sent += value23; true }
	} 
	class gCtx_value24_msg_ENT_ACC_NUM_Put(var gCtx : com.ligadata.OnLEPBase.EnvContext, val value24 : String, val msg : com.ligadata.OnLEPBankPoc.BankPocMsg_100, val ClientAlertsToday : com.ligadata.OnLEPBankPoc.AlertHistory_100)
	{
	  	def Put  : Boolean = { gCtx.setObject(value24, msg.ENT_ACC_NUM.toString, ClientAlertsToday); true }
	} 


/*************** SimpleRule Class Definitions ***************/

class SimpleRule_EBRule_025 (id : String, score : String, recordCount : Double, nbCorrect : Double, confidence : Double, weight : Double) 
      extends SimpleRule(id, score, recordCount, nbCorrect, confidence, weight) {
    override def execute(ctx : Context, defaultScore : StringDataValue) : String = {
        val answer : Boolean = Equal(ctx.valueFor("SendEBResultDetermination").asInstanceOf[BooleanDataValue].Value,true)
        if (answer == true) score else defaultScore.Value
    }
}
class SimpleRule_NORule_026 (id : String, score : String, recordCount : Double, nbCorrect : Double, confidence : Double, weight : Double) 
      extends SimpleRule(id, score, recordCount, nbCorrect, confidence, weight) {
    override def execute(ctx : Context, defaultScore : StringDataValue) : String = {
        val answer : Boolean = Equal(ctx.valueFor("SendNOResultDetermination").asInstanceOf[BooleanDataValue].Value,true)
        if (answer == true) score else defaultScore.Value
    }
}
class SimpleRule_ODRule3_027 (id : String, score : String, recordCount : Double, nbCorrect : Double, confidence : Double, weight : Double) 
      extends SimpleRule(id, score, recordCount, nbCorrect, confidence, weight) {
    override def execute(ctx : Context, defaultScore : StringDataValue) : String = {
        val answer : Boolean = Equal(ctx.valueFor("SendOD3ResultDetermination").asInstanceOf[BooleanDataValue].Value,true)
        if (answer == true) score else defaultScore.Value
    }
}
class SimpleRule_ODRule2_028 (id : String, score : String, recordCount : Double, nbCorrect : Double, confidence : Double, weight : Double) 
      extends SimpleRule(id, score, recordCount, nbCorrect, confidence, weight) {
    override def execute(ctx : Context, defaultScore : StringDataValue) : String = {
        val answer : Boolean = Equal(ctx.valueFor("SendOD2ResultDetermination").asInstanceOf[BooleanDataValue].Value,true)
        if (answer == true) score else defaultScore.Value
    }
}
class SimpleRule_ODRule1_029 (id : String, score : String, recordCount : Double, nbCorrect : Double, confidence : Double, weight : Double) 
      extends SimpleRule(id, score, recordCount, nbCorrect, confidence, weight) {
    override def execute(ctx : Context, defaultScore : StringDataValue) : String = {
        val answer : Boolean = Equal(ctx.valueFor("SendOD1ResultDetermination").asInstanceOf[BooleanDataValue].Value,true)
        if (answer == true) score else defaultScore.Value
    }
}
class SimpleRule_LBRule_030 (id : String, score : String, recordCount : Double, nbCorrect : Double, confidence : Double, weight : Double) 
      extends SimpleRule(id, score, recordCount, nbCorrect, confidence, weight) {
    override def execute(ctx : Context, defaultScore : StringDataValue) : String = {
        val answer : Boolean = Equal(ctx.valueFor("SendLBResultDetermination").asInstanceOf[BooleanDataValue].Value,true)
        if (answer == true) score else defaultScore.Value
    }
}

/*************** RuleSetModel Class Definition ***************/

class RuleSetModel_classification_031 (modelName : String, functionName : String, algorithmName : String, isScorable : String) 
      extends RuleSetModel(modelName, functionName, algorithmName, isScorable) { 

      override def execute(ctx : Context) {
          var results : ArrayBuffer[String] = ArrayBuffer[String]()
          var res : String = DefaultScore.Value
          breakable {
              RuleSet().foreach(rule => {
                  res = rule.execute(ctx, DefaultScore)
                  if (res != "0") break 
                  /**results += res*/
              })
          }
          results += res
          MakePrediction(ctx, results)
      }
}
