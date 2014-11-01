package com.Edifecs.COPDRiskAssessment_000100.pmml

import com.ligadata.OnLEPBase._
import com.ligadata.pmml.udfs._
import com.ligadata.pmml.udfs.Udfs._
import com.ligadata.Pmml.Runtime._
import scala.collection.mutable._
import scala.collection.immutable.{ Map }
import scala.collection.immutable.{ Set }
import scala.math._
import scala.collection.immutable.StringLike
import scala.util.control.Breaks._

/**
    Application Name         : COPDRiskAssessment
    PMML Model Version       : 00.01.00
    Model Name               : COPDRisk
    Function Name            : classification
    PMML Model Source        : Pmml source supplied as string
    Copyright                : Edifecs Corp. Copyright 2014
    Description              : COPD Risk Assessment
*/

object COPDRiskAssessment_000100 extends ModelBaseObj {
    def getModelName: String = "com.Edifecs.COPDRiskAssessment_000100.pmml.COPDRiskAssessment_000100"
    def getVersion: String = "000100"
    def getModelVersion: String = getVersion
    val validMessages = Array("com.ligadata.edifecs.System_Beneficiary_100")
    def IsValidMessage(msg: BaseMsg): Boolean = { 
        validMessages.filter( m => m == msg.getClass.getName).size > 0
    }

    def CreateNewModel(gCtx : EnvContext, msg : BaseMsg, tenantId: String): ModelBase =
    {
           new COPDRiskAssessment_000100(gCtx, msg.asInstanceOf[com.ligadata.edifecs.System_Beneficiary_100], getModelName, getVersion, tenantId)
    }

} 
class COPDRiskAssessment_000100(val gCtx : com.ligadata.OnLEPBase.EnvContext, val msg : com.ligadata.edifecs.System_Beneficiary_100, val modelName:String, val modelVersion:String, val tenantId: String)
   extends ModelBase {
    val ctx : com.ligadata.Pmml.Runtime.Context = new com.ligadata.Pmml.Runtime.Context()
    def GetContext : Context = { ctx }
    override def getModelName : String = COPDRiskAssessment_000100.getModelName
    override def getVersion : String = COPDRiskAssessment_000100.getVersion
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
    def initialize : COPDRiskAssessment_000100 = {

        ctx.SetRuleSetModel(new RuleSetModel_classification_04("COPDRisk", "classification", "RuleSet", ""))
        val ruleSetModel : RuleSetModel = ctx.GetRuleSetModel
        /** Initialize the RuleSetModel and SimpleRules array with new instances of respective classes */
        var simpleRuleInstances : ArrayBuffer[SimpleRule] = new ArrayBuffer[SimpleRule]()
        ruleSetModel.AddRule(new SimpleRule_CATI_Rule1b_01("CATI_Rule1b", "1b", 0.0, 0.0, 0.0, 0.0))
        ruleSetModel.AddRule(new SimpleRule_CATI_Rule1a_02("CATI_Rule1a", "1a", 0.0, 0.0, 0.0, 0.0))
        ruleSetModel.AddRule(new SimpleRule_CATII_Rule2_03("CATII_Rule2", "II", 0.0, 0.0, 0.0, 0.0))
        /* Update the ruleset model with the default score and rule selection methods collected for it */
        ruleSetModel.DefaultScore(new StringDataValue("0"))
        ruleSetModel.AddRuleSelectionMethod(new RuleSelectionMethod("firstHit"))

        /* Update each rules ScoreDistribution if necessary.... */
        /** no rule score distribution for rule1 */
        /** no rule score distribution for rule2 */
        /** no rule score distribution for rule3 */

        /* Update each ruleSetModel's mining schema dict */
        ruleSetModel.AddMiningField("AATDeficiency", new MiningField("AATDeficiency","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("COPDSymptoms", new MiningField("COPDSymptoms","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("COPDSeverity", new MiningField("COPDSeverity","predicted","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("ChronicSputum", new MiningField("ChronicSputum","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("AYearAgo", new MiningField("AYearAgo","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("inPatientClaimCostsByDate", new MiningField("inPatientClaimCostsByDate","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("Age", new MiningField("Age","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("Dyspnoea", new MiningField("Dyspnoea","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("WithSmokingHistory", new MiningField("WithSmokingHistory","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("outPatientClaimCostsByDate", new MiningField("outPatientClaimCostsByDate","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("Msg_Desynpuf_Id", new MiningField("Msg_Desynpuf_Id","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("FamilyHistory", new MiningField("FamilyHistory","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("ChronicCough", new MiningField("ChronicCough","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("Today", new MiningField("Today","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("WithEnvironmentalExposures", new MiningField("WithEnvironmentalExposures","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))

        /* For convenience put the mining schema map in the context as well as ruleSetModel */
        ctx.MiningSchemaMap(ruleSetModel.MiningSchemaMap())
        /** initialize the data dictionary */
        var dfoo1 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        dfoo1 += ("gCtx" -> "valid")
        dfoo1 += ("msg" -> "valid")

        ctx.dDict += ("parameters" -> new DataField("parameters", "Any", dfoo1, "", "", ""))
        var dfoo2 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.dDict += ("Inp_Clm_Thru_Dt" -> new DataField("Inp_Clm_Thru_Dt", "Int", dfoo2, "", "", ""))
        var dfoo3 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.dDict += ("COPDSeverity" -> new DataField("COPDSeverity", "String", dfoo3, "", "", ""))
        var dfoo4 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.dDict += ("gCtx" -> new DataField("gCtx", "Any", dfoo4, "", "", ""))
        var dfoo5 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.dDict += ("msg" -> new DataField("msg", "Any", dfoo5, "", "", ""))
        var dfoo6 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.dDict += ("Outp_Clm_Thru_Dt" -> new DataField("Outp_Clm_Thru_Dt", "Int", dfoo6, "", "", ""))
        var dfoo7 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.dDict += ("Hl7_Clm_Thru_Dt" -> new DataField("Hl7_Clm_Thru_Dt", "Int", dfoo7, "", "", ""))
        var dfoo8 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.dDict += ("Msg_Desynpuf_Id" -> new DataField("Msg_Desynpuf_Id", "String", dfoo8, "", "", ""))

        /** initialize the transformation dictionary (derived field part) */
        var xbar1 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("CATII_Rule2" -> new Derive_CATII_Rule2("CATII_Rule2", "Boolean", xbar1, "null", "null", ""))
        var xbar2 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("AATDeficiency" -> new Derive_AATDeficiency("AATDeficiency", "Boolean", xbar2, "null", "null", ""))
        var xbar3 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("COPDSymptoms" -> new Derive_COPDSymptoms("COPDSymptoms", "Boolean", xbar3, "null", "null", ""))
        var xbar4 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("DyspnoeaCodeSet" -> new Derive_DyspnoeaCodeSet("DyspnoeaCodeSet", "Any", xbar4, "null", "null", ""))
        var xbar5 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SmokingMessageContainerBases" -> new Derive_SmokingMessageContainerBases("SmokingMessageContainerBases", "Any", xbar5, "null", "null", ""))
        var xbar6 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("CATI_Rule1b" -> new Derive_CATI_Rule1b("CATI_Rule1b", "Boolean", xbar6, "null", "null", ""))
        var xbar7 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("inPatientClaimsByDateKeys" -> new Derive_inPatientClaimsByDateKeys("inPatientClaimsByDateKeys", "Any", xbar7, "null", "null", ""))
        var xbar8 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SputumCodes" -> new Derive_SputumCodes("SputumCodes", "Any", xbar8, "null", "null", ""))
        var xbar9 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("MaterializeOutputs" -> new Derive_MaterializeOutputs("MaterializeOutputs", "Boolean", xbar9, "null", "null", ""))
        var xbar10 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("inPatientClaimTotalCostEachDate" -> new Derive_inPatientClaimTotalCostEachDate("inPatientClaimTotalCostEachDate", "Any", xbar10, "null", "null", ""))
        var xbar11 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("FortyYrsOrOlder" -> new Derive_FortyYrsOrOlder("FortyYrsOrOlder", "Boolean", xbar11, "null", "null", ""))
        var xbar12 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("ChronicSputum" -> new Derive_ChronicSputum("ChronicSputum", "Boolean", xbar12, "null", "null", ""))
        var xbar13 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("CoughCodes" -> new Derive_CoughCodes("CoughCodes", "Any", xbar13, "null", "null", ""))
        var xbar14 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("AYearAgo" -> new Derive_AYearAgo("AYearAgo", "Int", xbar14, "null", "null", ""))
        var xbar15 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("outPatientClaimCostsEachDate" -> new Derive_outPatientClaimCostsEachDate("outPatientClaimCostsEachDate", "Any", xbar15, "null", "null", ""))
        var xbar16 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("outPatientClaimTotalCostEachDate" -> new Derive_outPatientClaimTotalCostEachDate("outPatientClaimTotalCostEachDate", "Any", xbar16, "null", "null", ""))
        var xbar17 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("inPatientClaimCostsByDate" -> new Derive_inPatientClaimCostsByDate("inPatientClaimCostsByDate", "Any", xbar17, "null", "null", ""))
        var xbar18 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SmokingCodeSet" -> new Derive_SmokingCodeSet("SmokingCodeSet", "Any", xbar18, "null", "null", ""))
        var xbar19 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("hl7InfoThisLastYear" -> new Derive_hl7InfoThisLastYear("hl7InfoThisLastYear", "Any", xbar19, "null", "null", ""))
        var xbar20 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("CoughCodeSet" -> new Derive_CoughCodeSet("CoughCodeSet", "Any", xbar20, "null", "null", ""))
        var xbar21 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("Age" -> new Derive_Age("Age", "Int", xbar21, "null", "null", ""))
        var xbar22 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("EnvExposureCodes" -> new Derive_EnvExposureCodes("EnvExposureCodes", "Any", xbar22, "null", "null", ""))
        var xbar23 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("EnvExposureMessageContainerBases" -> new Derive_EnvExposureMessageContainerBases("EnvExposureMessageContainerBases", "Any", xbar23, "null", "null", ""))
        var xbar24 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("outPatientClaimCostTuples" -> new Derive_outPatientClaimCostTuples("outPatientClaimCostTuples", "Any", xbar24, "null", "null", ""))
        var xbar25 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("inPatientClaimCostsEachDate" -> new Derive_inPatientClaimCostsEachDate("inPatientClaimCostsEachDate", "Any", xbar25, "null", "null", ""))
        var xbar26 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("CATI_Rule1a" -> new Derive_CATI_Rule1a("CATI_Rule1a", "Boolean", xbar26, "null", "null", ""))
        var xbar27 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("outpatientInfoThisLastYear" -> new Derive_outpatientInfoThisLastYear("outpatientInfoThisLastYear", "Any", xbar27, "null", "null", ""))
        var xbar28 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("outPatientClaimsByDateKeys" -> new Derive_outPatientClaimsByDateKeys("outPatientClaimsByDateKeys", "Any", xbar28, "null", "null", ""))
        var xbar29 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("Dyspnoea" -> new Derive_Dyspnoea("Dyspnoea", "Boolean", xbar29, "null", "null", ""))
        var xbar30 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("DyspnoeaCodes" -> new Derive_DyspnoeaCodes("DyspnoeaCodes", "Any", xbar30, "null", "null", ""))
        var xbar31 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("inPatientClaimCostTuples" -> new Derive_inPatientClaimCostTuples("inPatientClaimCostTuples", "Any", xbar31, "null", "null", ""))
        var xbar32 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SmokingCodes" -> new Derive_SmokingCodes("SmokingCodes", "Any", xbar32, "null", "null", ""))
        var xbar33 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("outPatientClaimsByDateValues" -> new Derive_outPatientClaimsByDateValues("outPatientClaimsByDateValues", "Any", xbar33, "null", "null", ""))
        var xbar34 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("EnvExposureCodeSet" -> new Derive_EnvExposureCodeSet("EnvExposureCodeSet", "Any", xbar34, "null", "null", ""))
        var xbar35 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("WithSmokingHistory" -> new Derive_WithSmokingHistory("WithSmokingHistory", "Boolean", xbar35, "null", "null", ""))
        var xbar36 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("inPatientClaimsByDateValues" -> new Derive_inPatientClaimsByDateValues("inPatientClaimsByDateValues", "Any", xbar36, "null", "null", ""))
        var xbar37 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("outPatientClaimCostsByDate" -> new Derive_outPatientClaimCostsByDate("outPatientClaimCostsByDate", "Any", xbar37, "null", "null", ""))
        var xbar38 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("inPatientClaimsByDate" -> new Derive_inPatientClaimsByDate("inPatientClaimsByDate", "Any", xbar38, "null", "null", ""))
        var xbar39 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("FamilyHistory" -> new Derive_FamilyHistory("FamilyHistory", "Boolean", xbar39, "null", "null", ""))
        var xbar40 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SputumCodeSet" -> new Derive_SputumCodeSet("SputumCodeSet", "Any", xbar40, "null", "null", ""))
        var xbar41 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("ChronicCough" -> new Derive_ChronicCough("ChronicCough", "Boolean", xbar41, "null", "null", ""))
        var xbar42 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("outPatientClaimsByDate" -> new Derive_outPatientClaimsByDate("outPatientClaimsByDate", "Any", xbar42, "null", "null", ""))
        var xbar43 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("Today" -> new Derive_Today("Today", "Int", xbar43, "null", "null", ""))
        var xbar44 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("DyspnoeaMessageContainerBases" -> new Derive_DyspnoeaMessageContainerBases("DyspnoeaMessageContainerBases", "Any", xbar44, "null", "null", ""))
        var xbar45 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("SputumMessageContainerBases" -> new Derive_SputumMessageContainerBases("SputumMessageContainerBases", "Any", xbar45, "null", "null", ""))
        var xbar46 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("CoughMessageContainerBases" -> new Derive_CoughMessageContainerBases("CoughMessageContainerBases", "Any", xbar46, "null", "null", ""))
        var xbar47 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("WithEnvironmentalExposures" -> new Derive_WithEnvironmentalExposures("WithEnvironmentalExposures", "Boolean", xbar47, "null", "null", ""))
        var xbar48 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("inpatientInfoThisLastYear" -> new Derive_inpatientInfoThisLastYear("inpatientInfoThisLastYear", "Any", xbar48, "null", "null", ""))

        /** fill the Context's mining field dictionary ...*/
        //val ruleSetModel : RuleSetModel = ctx.GetRuleSetModel
        /** put a reference of the mining schema map in the context for convenience. */
        ctx.MiningSchemaMap(ruleSetModel.MiningSchemaMap())

        /** Build the dictionary of model identifiers 
            Keys are: 
                 ApplicationName , FunctionName, PMML, Version,  
                 Copyright, Description, ModelName, ClassName 
         */
        ctx.pmmlModelIdentifiers("ApplicationName") = Some("COPDRiskAssessment")
        ctx.pmmlModelIdentifiers("FunctionName") = Some("classification")
        ctx.pmmlModelIdentifiers("PMML") = Some("Pmml source supplied as string")
        ctx.pmmlModelIdentifiers("Version") = Some("00.01.00")
        ctx.pmmlModelIdentifiers("Copyright") = Some("Edifecs Corp. Copyright 2014")
        ctx.pmmlModelIdentifiers("Description") = Some("COPD Risk Assessment")
        ctx.pmmlModelIdentifiers("ModelName") = Some("COPDRisk")

        ctx.pmmlModelIdentifiers("ClassName") = Some("COPDRiskAssessment_000100")

        this
    }   /** end of initialize fcn  */	

    /** provide access to the ruleset model's execute function */
    def execute(emitAllResults : Boolean) : ModelResult = {
        ctx.GetRuleSetModel.execute(ctx)
        prepareResults(emitAllResults)
    }


    /** prepare output results scored by the rules. */
    def prepareResults(emitAllResults : Boolean) : ModelResult = {

        val defaultScore : String = GetContext.GetRuleSetModel.DefaultScore().Value
        val miningVars : Array[MiningField] = GetContext.GetRuleSetModel.MiningSchemaMap().values.toArray
        val predictionFld : MiningField = miningVars.filter(m => m.usageType == "predicted").head

        /** If supplied flag is true, emit all results, else base decision on whether prediction*/
        /** is a value other than the defaultScore.*/
        val modelProducedResult : Boolean = if (emitAllResults) true else {
            val somePrediction : DataValue = ctx.valueFor(predictionFld.name) 
            val predictedValue : Any = somePrediction match { 
    	  		     case d    : DoubleDataValue   => somePrediction.asInstanceOf[DoubleDataValue].Value 
    	  		     case f    : FloatDataValue    => somePrediction.asInstanceOf[FloatDataValue].Value 
    	  		     case l    : LongDataValue     => somePrediction.asInstanceOf[LongDataValue].Value 
    	  		     case i    : IntDataValue      => somePrediction.asInstanceOf[IntDataValue].Value 
    	  		     case b    : BooleanDataValue  => somePrediction.asInstanceOf[BooleanDataValue].Value 
    	  		     case ddv  : DateDataValue     => somePrediction.asInstanceOf[DateDataValue].Value 
    	  		     case dtdv : DateTimeDataValue => somePrediction.asInstanceOf[DateTimeDataValue].Value 
    	  		     case tdv  : TimeDataValue     => somePrediction.asInstanceOf[TimeDataValue].Value 
    	  		     case s    : StringDataValue   => somePrediction.asInstanceOf[StringDataValue].Value 

    	  		     case _ => somePrediction.asInstanceOf[AnyDataValue].Value 
            } 
            (predictedValue.toString != defaultScore)
        }

        val modelResult : ModelResult = if (modelProducedResult) {
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
            val millisecsSinceMidnight: Long = dateMilliSecondsSinceMidnight().toLong 
            val now: org.joda.time.DateTime = new org.joda.time.DateTime() 
            val nowStr: String = now.toString 
            val dateMillis : Long = now.getMillis.toLong - millisecsSinceMidnight 
            new ModelResult(dateMillis, nowStr, COPDRiskAssessment_000100.getModelName, COPDRiskAssessment_000100.getModelVersion, results) 
        } else { null }

        modelResult
    }

}

/*************** Derived Field Class Definitions ***************/

class Derive_SputumMessageContainerBases (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val SputumMessageContainerBases = GetArray(ctx.valueFor("gCtx").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBase.EnvContext], "FilterArrays", "system.SputumCodes")
        ctx.xDict.apply("SputumMessageContainerBases").Value(new AnyDataValue(SputumMessageContainerBases))
        new AnyDataValue(SputumMessageContainerBases)
    }

}


class Derive_SputumCodes (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val SputumCodes = ctx.valueFor("SputumMessageContainerBases").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.OnLEPBase.MessageContainerBase]].map(itm => itm.asInstanceOf[com.ligadata.edifecs.SputumCodes_100])
        ctx.xDict.apply("SputumCodes").Value(new AnyDataValue(SputumCodes))
        new AnyDataValue(SputumCodes)
    }

}


class Derive_SmokingMessageContainerBases (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val SmokingMessageContainerBases = GetArray(ctx.valueFor("gCtx").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBase.EnvContext], "FilterArrays", "system.SmokeCodes")
        ctx.xDict.apply("SmokingMessageContainerBases").Value(new AnyDataValue(SmokingMessageContainerBases))
        new AnyDataValue(SmokingMessageContainerBases)
    }

}


class Derive_SmokingCodes (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val SmokingCodes = ctx.valueFor("SmokingMessageContainerBases").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.OnLEPBase.MessageContainerBase]].map(itm => itm.asInstanceOf[com.ligadata.edifecs.SmokeCodes_100])
        ctx.xDict.apply("SmokingCodes").Value(new AnyDataValue(SmokingCodes))
        new AnyDataValue(SmokingCodes)
    }

}


class Derive_EnvExposureMessageContainerBases (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val EnvExposureMessageContainerBases = GetArray(ctx.valueFor("gCtx").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBase.EnvContext], "FilterArrays", "system.EnvCodes")
        ctx.xDict.apply("EnvExposureMessageContainerBases").Value(new AnyDataValue(EnvExposureMessageContainerBases))
        new AnyDataValue(EnvExposureMessageContainerBases)
    }

}


class Derive_EnvExposureCodes (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val EnvExposureCodes = ctx.valueFor("EnvExposureMessageContainerBases").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.OnLEPBase.MessageContainerBase]].map(itm => itm.asInstanceOf[com.ligadata.edifecs.EnvCodes_100])
        ctx.xDict.apply("EnvExposureCodes").Value(new AnyDataValue(EnvExposureCodes))
        new AnyDataValue(EnvExposureCodes)
    }

}


class Derive_CoughMessageContainerBases (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val CoughMessageContainerBases = GetArray(ctx.valueFor("gCtx").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBase.EnvContext], "FilterArrays", "system.CoughCodes")
        ctx.xDict.apply("CoughMessageContainerBases").Value(new AnyDataValue(CoughMessageContainerBases))
        new AnyDataValue(CoughMessageContainerBases)
    }

}


class Derive_CoughCodes (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val CoughCodes = ctx.valueFor("CoughMessageContainerBases").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.OnLEPBase.MessageContainerBase]].map(itm => itm.asInstanceOf[com.ligadata.edifecs.CoughCodes_100])
        ctx.xDict.apply("CoughCodes").Value(new AnyDataValue(CoughCodes))
        new AnyDataValue(CoughCodes)
    }

}


class Derive_DyspnoeaMessageContainerBases (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val DyspnoeaMessageContainerBases = GetArray(ctx.valueFor("gCtx").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.OnLEPBase.EnvContext], "FilterArrays", "system.DyspnoeaCodes")
        ctx.xDict.apply("DyspnoeaMessageContainerBases").Value(new AnyDataValue(DyspnoeaMessageContainerBases))
        new AnyDataValue(DyspnoeaMessageContainerBases)
    }

}


class Derive_DyspnoeaCodes (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val DyspnoeaCodes = ctx.valueFor("DyspnoeaMessageContainerBases").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.OnLEPBase.MessageContainerBase]].map(itm => itm.asInstanceOf[com.ligadata.edifecs.DyspnoeaCodes_100])
        ctx.xDict.apply("DyspnoeaCodes").Value(new AnyDataValue(DyspnoeaCodes))
        new AnyDataValue(DyspnoeaCodes)
    }

}


class Derive_SputumCodeSet (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val SputumCodeSet = ToSet(ctx.valueFor("SputumCodes").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.SputumCodes_100]].map( _each => { _each.icd9Code }))
        ctx.xDict.apply("SputumCodeSet").Value(new AnyDataValue(SputumCodeSet))
        new AnyDataValue(SputumCodeSet)
    }

}


class Derive_SmokingCodeSet (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val SmokingCodeSet = ToSet(ctx.valueFor("SmokingCodes").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.SmokeCodes_100]].map( _each => { _each.icd9Code }))
        ctx.xDict.apply("SmokingCodeSet").Value(new AnyDataValue(SmokingCodeSet))
        new AnyDataValue(SmokingCodeSet)
    }

}


class Derive_EnvExposureCodeSet (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val EnvExposureCodeSet = ToSet(ctx.valueFor("EnvExposureCodes").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.EnvCodes_100]].map( _each => { _each.icd9Code }))
        ctx.xDict.apply("EnvExposureCodeSet").Value(new AnyDataValue(EnvExposureCodeSet))
        new AnyDataValue(EnvExposureCodeSet)
    }

}


class Derive_CoughCodeSet (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val CoughCodeSet = ToSet(ctx.valueFor("CoughCodes").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.CoughCodes_100]].map( _each => { _each.icd9Code }))
        ctx.xDict.apply("CoughCodeSet").Value(new AnyDataValue(CoughCodeSet))
        new AnyDataValue(CoughCodeSet)
    }

}


class Derive_DyspnoeaCodeSet (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val DyspnoeaCodeSet = ToSet(ctx.valueFor("DyspnoeaCodes").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.DyspnoeaCodes_100]].map( _each => { _each.icd9Code }))
        ctx.xDict.apply("DyspnoeaCodeSet").Value(new AnyDataValue(DyspnoeaCodeSet))
        new AnyDataValue(DyspnoeaCodeSet)
    }

}


class Derive_Today (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : IntDataValue = {
        val Today = AsCompressedDate(Now())
        ctx.xDict.apply("Today").Value(new IntDataValue(Today))
        new IntDataValue(Today)
    }

}


class Derive_AYearAgo (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : IntDataValue = {
        val AYearAgo = AsCompressedDate(YearsAgo(1))
        ctx.xDict.apply("AYearAgo").Value(new IntDataValue(AYearAgo))
        new IntDataValue(AYearAgo)
    }

}


class Derive_Age (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : IntDataValue = {
        val Age = AgeCalc(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.edifecs.System_Beneficiary_100].Bene_Birth_Dt)
        ctx.xDict.apply("Age").Value(new IntDataValue(Age))
        new IntDataValue(Age)
    }

}


class Derive_FortyYrsOrOlder (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val FortyYrsOrOlder = GreaterOrEqual(ctx.valueFor("Age").asInstanceOf[IntDataValue].Value, 40)
        ctx.xDict.apply("FortyYrsOrOlder").Value(new BooleanDataValue(FortyYrsOrOlder))
        new BooleanDataValue(FortyYrsOrOlder)
    }

}


class Derive_hl7InfoThisLastYear (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val hl7InfoThisLastYear = ToArray(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.edifecs.System_Beneficiary_100].HL7Messages.filter( _each => { com.ligadata.pmml.udfs.Udfs.Between(_each.Clm_Thru_Dt, ctx.valueFor("AYearAgo").asInstanceOf[IntDataValue].Value, ctx.valueFor("Today").asInstanceOf[IntDataValue].Value, true) }))
        ctx.xDict.apply("hl7InfoThisLastYear").Value(new AnyDataValue(hl7InfoThisLastYear))
        new AnyDataValue(hl7InfoThisLastYear)
    }

}


class Derive_WithSmokingHistory (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val WithSmokingHistory = GreaterThan(Plus(CollectionLength(ctx.valueFor("inpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_InpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.IsIn(_each.Admtng_Icd9_Dgns_Cd, ctx.valueFor("SmokingCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]]) })), CollectionLength(ctx.valueFor("inpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_InpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.ContainsAny(ctx.valueFor("SmokingCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]], _each.Icd9_Dgns_Cds) })), CollectionLength(ctx.valueFor("outpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_OutpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.IsIn(_each.Admtng_Icd9_Dgns_Cd, ctx.valueFor("SmokingCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]]) })), CollectionLength(ctx.valueFor("outpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_OutpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.ContainsAny(ctx.valueFor("SmokingCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]], _each.Icd9_Dgns_Cds) }))), 0)
        ctx.xDict.apply("WithSmokingHistory").Value(new BooleanDataValue(WithSmokingHistory))
        new BooleanDataValue(WithSmokingHistory)
    }

}


class Derive_inpatientInfoThisLastYear (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val inpatientInfoThisLastYear = ToArray(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.edifecs.System_Beneficiary_100].Inpatient_Claims.filter( _each => { com.ligadata.pmml.udfs.Udfs.Between(_each.Clm_Thru_Dt, ctx.valueFor("AYearAgo").asInstanceOf[IntDataValue].Value, ctx.valueFor("Today").asInstanceOf[IntDataValue].Value, true) }))
        ctx.xDict.apply("inpatientInfoThisLastYear").Value(new AnyDataValue(inpatientInfoThisLastYear))
        new AnyDataValue(inpatientInfoThisLastYear)
    }

}


class Derive_outpatientInfoThisLastYear (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val outpatientInfoThisLastYear = ToArray(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.edifecs.System_Beneficiary_100].Outpatient_Claims.filter( _each => { com.ligadata.pmml.udfs.Udfs.Between(_each.Clm_Thru_Dt, ctx.valueFor("AYearAgo").asInstanceOf[IntDataValue].Value, ctx.valueFor("Today").asInstanceOf[IntDataValue].Value, true) }))
        ctx.xDict.apply("outpatientInfoThisLastYear").Value(new AnyDataValue(outpatientInfoThisLastYear))
        new AnyDataValue(outpatientInfoThisLastYear)
    }

}


class Derive_WithEnvironmentalExposures (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val WithEnvironmentalExposures = GreaterThan(Plus(CollectionLength(ctx.valueFor("inpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_InpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.IsIn(_each.Admtng_Icd9_Dgns_Cd, ctx.valueFor("EnvExposureCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]]) })), CollectionLength(ctx.valueFor("inpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_InpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.ContainsAny(ctx.valueFor("EnvExposureCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]], _each.Icd9_Dgns_Cds) })), CollectionLength(ctx.valueFor("outpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_OutpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.IsIn(_each.Admtng_Icd9_Dgns_Cd, ctx.valueFor("EnvExposureCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]]) })), CollectionLength(ctx.valueFor("outpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_OutpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.ContainsAny(ctx.valueFor("EnvExposureCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]], _each.Icd9_Dgns_Cds) }))), 0)
        ctx.xDict.apply("WithEnvironmentalExposures").Value(new BooleanDataValue(WithEnvironmentalExposures))
        new BooleanDataValue(WithEnvironmentalExposures)
    }

}


class Derive_AATDeficiency (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val AATDeficiency = GreaterThan(CollectionLength(ctx.valueFor("hl7InfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_HL7_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.Equal(_each.AATDeficiency, 1) })), 0)
        ctx.xDict.apply("AATDeficiency").Value(new BooleanDataValue(AATDeficiency))
        new BooleanDataValue(AATDeficiency)
    }

}


class Derive_Dyspnoea (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val Dyspnoea = GreaterThan(Plus(CollectionLength(ctx.valueFor("inpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_InpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.IsIn(_each.Admtng_Icd9_Dgns_Cd, ctx.valueFor("DyspnoeaCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]]) })), CollectionLength(ctx.valueFor("inpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_InpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.ContainsAny(ctx.valueFor("DyspnoeaCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]], _each.Icd9_Dgns_Cds) })), CollectionLength(ctx.valueFor("outpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_OutpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.IsIn(_each.Admtng_Icd9_Dgns_Cd, ctx.valueFor("DyspnoeaCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]]) })), CollectionLength(ctx.valueFor("outpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_OutpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.ContainsAny(ctx.valueFor("DyspnoeaCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]], _each.Icd9_Dgns_Cds) }))), 0)
        ctx.xDict.apply("Dyspnoea").Value(new BooleanDataValue(Dyspnoea))
        new BooleanDataValue(Dyspnoea)
    }

}


class Derive_ChronicCough (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val ChronicCough = GreaterThan(Plus(CollectionLength(ctx.valueFor("inpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_InpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.IsIn(_each.Admtng_Icd9_Dgns_Cd, ctx.valueFor("CoughCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]]) })), CollectionLength(ctx.valueFor("inpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_InpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.ContainsAny(ctx.valueFor("CoughCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]], _each.Icd9_Dgns_Cds) })), CollectionLength(ctx.valueFor("outpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_OutpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.IsIn(_each.Admtng_Icd9_Dgns_Cd, ctx.valueFor("CoughCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]]) })), CollectionLength(ctx.valueFor("outpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_OutpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.ContainsAny(ctx.valueFor("CoughCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]], _each.Icd9_Dgns_Cds) }))), 0)
        ctx.xDict.apply("ChronicCough").Value(new BooleanDataValue(ChronicCough))
        new BooleanDataValue(ChronicCough)
    }

}


class Derive_ChronicSputum (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val ChronicSputum = GreaterThan(Plus(CollectionLength(ctx.valueFor("inpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_InpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.IsIn(_each.Admtng_Icd9_Dgns_Cd, ctx.valueFor("SputumCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]]) })), CollectionLength(ctx.valueFor("inpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_InpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.ContainsAny(ctx.valueFor("SputumCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]], _each.Icd9_Dgns_Cds) })), CollectionLength(ctx.valueFor("outpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_OutpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.IsIn(_each.Admtng_Icd9_Dgns_Cd, ctx.valueFor("SputumCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]]) })), CollectionLength(ctx.valueFor("outpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_OutpatientClaim_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.ContainsAny(ctx.valueFor("SputumCodeSet").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Set[String]], _each.Icd9_Dgns_Cds) }))), 0)
        ctx.xDict.apply("ChronicSputum").Value(new BooleanDataValue(ChronicSputum))
        new BooleanDataValue(ChronicSputum)
    }

}


class Derive_COPDSymptoms (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val COPDSymptoms = Or(ctx.valueFor("Dyspnoea").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("ChronicCough").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("ChronicSputum").asInstanceOf[BooleanDataValue].Value)
        ctx.xDict.apply("COPDSymptoms").Value(new BooleanDataValue(COPDSymptoms))
        new BooleanDataValue(COPDSymptoms)
    }

}


class Derive_FamilyHistory (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val FamilyHistory = Or(Equal(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.edifecs.System_Beneficiary_100].Sp_Copd, 1), GreaterThan(CollectionLength(ctx.valueFor("hl7InfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_HL7_100]].filter( _each => { com.ligadata.pmml.udfs.Udfs.Or(_each.ChronicCough, _each.Sp_Copd, _each.Shortnessofbreath, _each.ChronicSputum) })), 0))
        ctx.xDict.apply("FamilyHistory").Value(new BooleanDataValue(FamilyHistory))
        new BooleanDataValue(FamilyHistory)
    }

}


class Derive_inPatientClaimsByDate (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val inPatientClaimsByDate = ctx.valueFor("inpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_InpatientClaim_100]].groupBy( _each => { _each.Clm_Thru_Dt })
        ctx.xDict.apply("inPatientClaimsByDate").Value(new AnyDataValue(inPatientClaimsByDate))
        new AnyDataValue(inPatientClaimsByDate)
    }

}


class Derive_inPatientClaimsByDateValues (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val inPatientClaimsByDateValues = ToArray(MapValues(ctx.valueFor("inPatientClaimsByDate").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Map[Int,scala.Array[com.ligadata.edifecs.System_InpatientClaim_100]]]))
        ctx.xDict.apply("inPatientClaimsByDateValues").Value(new AnyDataValue(inPatientClaimsByDateValues))
        new AnyDataValue(inPatientClaimsByDateValues)
    }

}


class Derive_inPatientClaimsByDateKeys (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val inPatientClaimsByDateKeys = ToArray(MapKeys(ctx.valueFor("inPatientClaimsByDate").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Map[Int,scala.Array[com.ligadata.edifecs.System_InpatientClaim_100]]]))
        ctx.xDict.apply("inPatientClaimsByDateKeys").Value(new AnyDataValue(inPatientClaimsByDateKeys))
        new AnyDataValue(inPatientClaimsByDateKeys)
    }

}


class Derive_inPatientClaimCostTuples (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val inPatientClaimCostTuples = ctx.valueFor("inPatientClaimsByDateValues").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[scala.Array[com.ligadata.edifecs.System_InpatientClaim_100]]].map( _each => { _each.map( _each => { (_each.Clm_Pmt_Amt, _each.Nch_Prmry_Pyr_Clm_Pd_Amt, _each.Clm_Pass_Thru_Per_Diem_Amt, _each.Nch_Bene_Ip_Ddctbl_Amt, _each.Nch_Bene_Pta_Coinsrnc_Lblty_Am, _each.Nch_Bene_Blood_Ddctbl_Lblty_Am)}) })
        ctx.xDict.apply("inPatientClaimCostTuples").Value(new AnyDataValue(inPatientClaimCostTuples))
        new AnyDataValue(inPatientClaimCostTuples)
    }

}


class Derive_inPatientClaimCostsEachDate (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val inPatientClaimCostsEachDate = ToArray(ctx.valueFor("inPatientClaimCostTuples").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[scala.Array[scala.Tuple6[Any,Any,Any,Any,Any,Any]]]].map( _each => { com.ligadata.pmml.udfs.Udfs.SumToArrayOfDouble(_each) }))
        ctx.xDict.apply("inPatientClaimCostsEachDate").Value(new AnyDataValue(inPatientClaimCostsEachDate))
        new AnyDataValue(inPatientClaimCostsEachDate)
    }

}


class Derive_inPatientClaimTotalCostEachDate (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val inPatientClaimTotalCostEachDate = ToArray(ctx.valueFor("inPatientClaimCostsEachDate").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[scala.Array[Double]]].map( _each => { com.ligadata.pmml.udfs.Udfs.Sum(_each) }))
        ctx.xDict.apply("inPatientClaimTotalCostEachDate").Value(new AnyDataValue(inPatientClaimTotalCostEachDate))
        new AnyDataValue(inPatientClaimTotalCostEachDate)
    }

}


class Derive_inPatientClaimCostsByDate (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val inPatientClaimCostsByDate = ToMap(Zip(ctx.valueFor("inPatientClaimsByDateKeys").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[Int]], ctx.valueFor("inPatientClaimTotalCostEachDate").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[Double]]))
        ctx.xDict.apply("inPatientClaimCostsByDate").Value(new AnyDataValue(inPatientClaimCostsByDate))
        new AnyDataValue(inPatientClaimCostsByDate)
    }

}


class Derive_outPatientClaimsByDate (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val outPatientClaimsByDate = ctx.valueFor("outpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[com.ligadata.edifecs.System_OutpatientClaim_100]].groupBy( _each => { _each.Clm_Thru_Dt })
        ctx.xDict.apply("outPatientClaimsByDate").Value(new AnyDataValue(outPatientClaimsByDate))
        new AnyDataValue(outPatientClaimsByDate)
    }

}


class Derive_outPatientClaimsByDateValues (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val outPatientClaimsByDateValues = ToArray(MapValues(ctx.valueFor("outPatientClaimsByDate").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Map[Int,scala.Array[com.ligadata.edifecs.System_OutpatientClaim_100]]]))
        ctx.xDict.apply("outPatientClaimsByDateValues").Value(new AnyDataValue(outPatientClaimsByDateValues))
        new AnyDataValue(outPatientClaimsByDateValues)
    }

}


class Derive_outPatientClaimsByDateKeys (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val outPatientClaimsByDateKeys = ToArray(MapKeys(ctx.valueFor("outPatientClaimsByDate").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Map[Int,scala.Array[com.ligadata.edifecs.System_OutpatientClaim_100]]]))
        ctx.xDict.apply("outPatientClaimsByDateKeys").Value(new AnyDataValue(outPatientClaimsByDateKeys))
        new AnyDataValue(outPatientClaimsByDateKeys)
    }

}


class Derive_outPatientClaimCostTuples (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val outPatientClaimCostTuples = ctx.valueFor("outPatientClaimsByDateValues").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[scala.Array[com.ligadata.edifecs.System_OutpatientClaim_100]]].map( _each => { _each.map( _each => { (_each.Clm_Pmt_Amt, _each.Nch_Prmry_Pyr_Clm_Pd_Amt, _each.Nch_Bene_Blood_Ddctbl_Lblty_Am, _each.Nch_Bene_Ptb_Ddctbl_Amt, _each.Nch_Bene_Ptb_Coinsrnc_Amt)}) })
        ctx.xDict.apply("outPatientClaimCostTuples").Value(new AnyDataValue(outPatientClaimCostTuples))
        new AnyDataValue(outPatientClaimCostTuples)
    }

}


class Derive_outPatientClaimCostsEachDate (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val outPatientClaimCostsEachDate = ToArray(ctx.valueFor("outPatientClaimCostTuples").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[scala.Array[scala.Tuple5[Any,Any,Any,Any,Any]]]].map( _each => { com.ligadata.pmml.udfs.Udfs.SumToArrayOfDouble(_each) }))
        ctx.xDict.apply("outPatientClaimCostsEachDate").Value(new AnyDataValue(outPatientClaimCostsEachDate))
        new AnyDataValue(outPatientClaimCostsEachDate)
    }

}


class Derive_outPatientClaimTotalCostEachDate (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val outPatientClaimTotalCostEachDate = ToArray(ctx.valueFor("outPatientClaimCostsEachDate").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[scala.Array[Double]]].map( _each => { com.ligadata.pmml.udfs.Udfs.Sum(_each) }))
        ctx.xDict.apply("outPatientClaimTotalCostEachDate").Value(new AnyDataValue(outPatientClaimTotalCostEachDate))
        new AnyDataValue(outPatientClaimTotalCostEachDate)
    }

}


class Derive_outPatientClaimCostsByDate (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : AnyDataValue = {
        val outPatientClaimCostsByDate = ToMap(Zip(ctx.valueFor("outPatientClaimsByDateKeys").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[Int]], ctx.valueFor("outPatientClaimTotalCostEachDate").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.Array[Double]]))
        ctx.xDict.apply("outPatientClaimCostsByDate").Value(new AnyDataValue(outPatientClaimCostsByDate))
        new AnyDataValue(outPatientClaimCostsByDate)
    }

}


class Derive_MaterializeOutputs (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val MaterializeOutputs = And(Put(ctx, "Msg_Desynpuf_Id", ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.edifecs.System_Beneficiary_100].Desynpuf_Id), GreaterThan(CollectionLength(ctx.valueFor("inPatientClaimCostsByDate").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Map[Int,Double]]), 0), GreaterThan(CollectionLength(ctx.valueFor("outPatientClaimCostsByDate").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.immutable.Map[Int,Double]]), 0))
        ctx.xDict.apply("MaterializeOutputs").Value(new BooleanDataValue(MaterializeOutputs))
        new BooleanDataValue(MaterializeOutputs)
    }

}


class Derive_CATII_Rule2 (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val CATII_Rule2 = If(And(Not(ctx.valueFor("FortyYrsOrOlder").asInstanceOf[BooleanDataValue].Value), Or(ctx.valueFor("COPDSymptoms").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("AATDeficiency").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("FamilyHistory").asInstanceOf[BooleanDataValue].Value)))
        var result : Boolean = if (CATII_Rule2) { And(ctx.valueFor("MaterializeOutputs").asInstanceOf[BooleanDataValue].Value, Put(ctx, "COPDSeverity", "2")) } else { And(ctx.valueFor("MaterializeOutputs").asInstanceOf[BooleanDataValue].Value, Put(ctx, "COPDSeverity", "NotSet"), false) }
        ctx.xDict.apply("CATII_Rule2").Value(new BooleanDataValue(result))
        new BooleanDataValue(result)
    }

}


class Derive_CATI_Rule1b (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val CATI_Rule1b = If(And(ctx.valueFor("FortyYrsOrOlder").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("WithSmokingHistory").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("AATDeficiency").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("WithEnvironmentalExposures").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("COPDSymptoms").asInstanceOf[BooleanDataValue].Value))
        var result : Boolean = if (CATI_Rule1b) { And(ctx.valueFor("MaterializeOutputs").asInstanceOf[BooleanDataValue].Value, Put(ctx, "COPDSeverity", "1b")) } else { And(ctx.valueFor("MaterializeOutputs").asInstanceOf[BooleanDataValue].Value, Put(ctx, "COPDSeverity", "NotSet"), false) }
        ctx.xDict.apply("CATI_Rule1b").Value(new BooleanDataValue(result))
        new BooleanDataValue(result)
    }

}


class Derive_CATI_Rule1a (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val CATI_Rule1a = If(And(ctx.valueFor("FortyYrsOrOlder").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("WithSmokingHistory").asInstanceOf[BooleanDataValue].Value, Or(ctx.valueFor("AATDeficiency").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("WithEnvironmentalExposures").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("COPDSymptoms").asInstanceOf[BooleanDataValue].Value)))
        var result : Boolean = if (CATI_Rule1a) { And(ctx.valueFor("MaterializeOutputs").asInstanceOf[BooleanDataValue].Value, Put(ctx, "COPDSeverity", "1a")) } else { And(ctx.valueFor("MaterializeOutputs").asInstanceOf[BooleanDataValue].Value, Put(ctx, "COPDSeverity", "NotSet"), false) }
        ctx.xDict.apply("CATI_Rule1a").Value(new BooleanDataValue(result))
        new BooleanDataValue(result)
    }

}



/*************** SimpleRule Class Definitions ***************/

class SimpleRule_CATI_Rule1b_01 (id : String, score : String, recordCount : Double, nbCorrect : Double, confidence : Double, weight : Double) 
      extends SimpleRule(id, score, recordCount, nbCorrect, confidence, weight) {
    override def execute(ctx : Context, defaultScore : StringDataValue) : String = {
        val answer : Boolean = Equal(ctx.valueFor("CATI_Rule1b").asInstanceOf[BooleanDataValue].Value,true)
        if (answer == true) score else defaultScore.Value
    }
}
class SimpleRule_CATI_Rule1a_02 (id : String, score : String, recordCount : Double, nbCorrect : Double, confidence : Double, weight : Double) 
      extends SimpleRule(id, score, recordCount, nbCorrect, confidence, weight) {
    override def execute(ctx : Context, defaultScore : StringDataValue) : String = {
        val answer : Boolean = Equal(ctx.valueFor("CATI_Rule1a").asInstanceOf[BooleanDataValue].Value,true)
        if (answer == true) score else defaultScore.Value
    }
}
class SimpleRule_CATII_Rule2_03 (id : String, score : String, recordCount : Double, nbCorrect : Double, confidence : Double, weight : Double) 
      extends SimpleRule(id, score, recordCount, nbCorrect, confidence, weight) {
    override def execute(ctx : Context, defaultScore : StringDataValue) : String = {
        val answer : Boolean = Equal(ctx.valueFor("CATII_Rule2").asInstanceOf[BooleanDataValue].Value,true)
        if (answer == true) score else defaultScore.Value
    }
}

/*************** RuleSetModel Class Definition ***************/

class RuleSetModel_classification_04 (modelName : String, functionName : String, algorithmName : String, isScorable : String) 
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
