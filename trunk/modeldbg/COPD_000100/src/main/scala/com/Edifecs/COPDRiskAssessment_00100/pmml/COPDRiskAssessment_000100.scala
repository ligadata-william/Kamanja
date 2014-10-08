package com.Edifecs.COPDRiskAssessment_000100.pmml

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
        ruleSetModel.AddMiningField("COPDSeverity", new MiningField("COPDSeverity","predicted","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("ChronicSputum", new MiningField("ChronicSputum","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("Dyspnoea", new MiningField("Dyspnoea","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("WithSmokingHistory", new MiningField("WithSmokingHistory","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))
        ruleSetModel.AddMiningField("ChronicCough", new MiningField("ChronicCough","supplementary","",0.0,"",0.0,0.0,new StringDataValue(""),"",""))

        /* For convenience put the mining schema map in the context as well as ruleSetModel */
        ctx.MiningSchemaMap(ruleSetModel.MiningSchemaMap())
        /** initialize the data dictionary */
        var dfoo1 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        dfoo1 += ("gCtx" -> "valid")
        dfoo1 += ("msg" -> "valid")

        ctx.dDict += ("parameters" -> new DataField("parameters", "Any", dfoo1, "", "", ""))
        var dfoo2 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.dDict += ("COPDSeverity" -> new DataField("COPDSeverity", "String", dfoo2, "", "", ""))
        var dfoo3 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.dDict += ("gCtx" -> new DataField("gCtx", "Any", dfoo3, "", "", ""))
        var dfoo4 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.dDict += ("msg" -> new DataField("msg", "Any", dfoo4, "", "", ""))

        /** initialize the transformation dictionary (derived field part) */
        var xbar1 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("CATII_Rule2" -> new Derive_CATII_Rule2("CATII_Rule2", "Boolean", xbar1, "null", "null", ""))
        var xbar2 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("AATDeficiency" -> new Derive_AATDeficiency("AATDeficiency", "Boolean", xbar2, "null", "null", ""))
        var xbar3 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("COPDSymptoms" -> new Derive_COPDSymptoms("COPDSymptoms", "Boolean", xbar3, "null", "null", ""))
        var xbar4 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("CATI_Rule1b" -> new Derive_CATI_Rule1b("CATI_Rule1b", "Boolean", xbar4, "null", "null", ""))
        var xbar5 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("FortyYrsOrOlder" -> new Derive_FortyYrsOrOlder("FortyYrsOrOlder", "Boolean", xbar5, "null", "null", ""))
        var xbar6 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("ChronicSputum" -> new Derive_ChronicSputum("ChronicSputum", "Boolean", xbar6, "null", "null", ""))
        var xbar7 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("AYearAgo" -> new Derive_AYearAgo("AYearAgo", "Any", xbar7, "null", "null", ""))
        var xbar8 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("hl7InfoThisLastYear" -> new Derive_hl7InfoThisLastYear("hl7InfoThisLastYear", "Any", xbar8, "null", "null", ""))
        var xbar9 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("CATI_Rule1a" -> new Derive_CATI_Rule1a("CATI_Rule1a", "Boolean", xbar9, "null", "null", ""))
        var xbar10 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("outpatientInfoThisLastYear" -> new Derive_outpatientInfoThisLastYear("outpatientInfoThisLastYear", "Any", xbar10, "null", "null", ""))
        var xbar11 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("Dyspnoea" -> new Derive_Dyspnoea("Dyspnoea", "Boolean", xbar11, "null", "null", ""))
        var xbar12 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("WithSmokingHistory" -> new Derive_WithSmokingHistory("WithSmokingHistory", "Boolean", xbar12, "null", "null", ""))
        var xbar13 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("FamilyHistory" -> new Derive_FamilyHistory("FamilyHistory", "Boolean", xbar13, "null", "null", ""))
        var xbar14 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("ChronicCough" -> new Derive_ChronicCough("ChronicCough", "Boolean", xbar14, "null", "null", ""))
        var xbar15 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("Today" -> new Derive_Today("Today", "Any", xbar15, "null", "null", ""))
        var xbar16 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("WithEnvironmentalExposures" -> new Derive_WithEnvironmentalExposures("WithEnvironmentalExposures", "Boolean", xbar16, "null", "null", ""))
        var xbar17 : ArrayBuffer[(String,String)] =  new ArrayBuffer[(String,String)]()
        ctx.xDict += ("inpatientInfoThisLastYear" -> new Derive_inpatientInfoThisLastYear("inpatientInfoThisLastYear", "Any", xbar17, "null", "null", ""))

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
    def execute(outputCurrentModel:Boolean) : ModelResult = {
        ctx.GetRuleSetModel.execute(ctx)
        prepareResults
    }


    /** prepare output results scored by the rules. */
    def prepareResults : ModelResult = {

        val defaultScore : String = GetContext.GetRuleSetModel.DefaultScore().Value
        val miningVars : Array[MiningField] = GetContext.GetRuleSetModel.MiningSchemaMap().values.toArray
        val predictionFld : MiningField = miningVars.filter(m => m.usageType == "predicted").head

        /** This piece prevents result generation when model doesn't score by any rule... i.e., is relying on the */
        /** the default score.  This will be optionally executed based upon argument from engine ultimately. */
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
        val modelProducedResult : Boolean = (predictedValue.toString != defaultScore)

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
            val millisecsSinceMidnight: Long = Builtins.dateMilliSecondsSinceMidnight().toLong 
            val now: org.joda.time.DateTime = new org.joda.time.DateTime() 
            val nowStr: String = now.toString 
            val dateMillis : Long = now.getMillis.toLong - millisecsSinceMidnight 
            new ModelResult(dateMillis, nowStr, COPDRiskAssessment_000100.getModelName, COPDRiskAssessment_000100.getModelVersion, results) 
        } else { null }

        modelResult
    }

}

/*************** Derived Field Class Definitions ***************/

class Derive_Today (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : AnyDataValue = {
        val Today = AsSeconds(Now())
        ctx.xDict.apply("Today").Value(new AnyDataValue(Today))
          new AnyDataValue(Today)
    }

}


class Derive_AYearAgo (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : AnyDataValue = {
        val AYearAgo = Minus(ctx.valueFor("Today").asInstanceOf[LongDataValue].Value, Multiply(Multiply(60, 60), Multiply(24, 365)))
        ctx.xDict.apply("AYearAgo").Value(new AnyDataValue(AYearAgo))
          new AnyDataValue(AYearAgo)
    }

}


class Derive_FortyYrsOrOlder (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val FortyYrsOrOlder = GreaterOrEqual(AgeCalc(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.edifecs.System_Beneficiary_100].Bene_Birth_Dt), 40)
        ctx.xDict.apply("FortyYrsOrOlder").Value(new BooleanDataValue(FortyYrsOrOlder))
          new BooleanDataValue(FortyYrsOrOlder)
    }

}


class Derive_hl7InfoThisLastYear (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : AnyDataValue = {
        val hl7InfoThisLastYear = ToArray(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.edifecs.System_Beneficiary_100].HL7Messages.filter( mbr => { com.ligadata.pmml.udfs.Udfs.Between(mbr.Clm_Thru_Dt, ctx.valueFor("AYearAgo").asInstanceOf[LongDataValue].Value, ctx.valueFor("Today").asInstanceOf[LongDataValue].Value, true) }))
        ctx.xDict.apply("hl7InfoThisLastYear").Value(new AnyDataValue(hl7InfoThisLastYear))
          new AnyDataValue(hl7InfoThisLastYear)
    }

}


class Derive_WithSmokingHistory (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val WithSmokingHistory = GreaterThan(CollectionLength(ctx.valueFor("hl7InfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.mutable.ArrayBuffer[com.ligadata.edifecs.System_HL7_100]].filter( mbr => { com.ligadata.pmml.udfs.Udfs.Equal(mbr.Tobacco, 1) })), 0)
        ctx.xDict.apply("WithSmokingHistory").Value(new BooleanDataValue(WithSmokingHistory))
          new BooleanDataValue(WithSmokingHistory)
    }

}


class Derive_inpatientInfoThisLastYear (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : AnyDataValue = {
        val inpatientInfoThisLastYear = ToArray(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.edifecs.System_Beneficiary_100].Inpatient_Claims.filter( mbr => { com.ligadata.pmml.udfs.Udfs.Between(mbr.Clm_Thru_Dt, ctx.valueFor("AYearAgo").asInstanceOf[LongDataValue].Value, ctx.valueFor("Today").asInstanceOf[LongDataValue].Value, true) }))
        ctx.xDict.apply("inpatientInfoThisLastYear").Value(new AnyDataValue(inpatientInfoThisLastYear))
          new AnyDataValue(inpatientInfoThisLastYear)
    }

}


class Derive_outpatientInfoThisLastYear (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : AnyDataValue = {
        val outpatientInfoThisLastYear = ToArray(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.edifecs.System_Beneficiary_100].Outpatient_Claims.filter( mbr => { com.ligadata.pmml.udfs.Udfs.Between(mbr.Clm_Thru_Dt, ctx.valueFor("AYearAgo").asInstanceOf[LongDataValue].Value, ctx.valueFor("Today").asInstanceOf[LongDataValue].Value, true) }))
        ctx.xDict.apply("outpatientInfoThisLastYear").Value(new AnyDataValue(outpatientInfoThisLastYear))
          new AnyDataValue(outpatientInfoThisLastYear)
    }

}


class Derive_WithEnvironmentalExposures (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val WithEnvironmentalExposures = GreaterThan(Plus(CollectionLength(ctx.valueFor("inpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.mutable.ArrayBuffer[com.ligadata.edifecs.System_InpatientClaim_100]].filter( mbr => { com.ligadata.pmml.udfs.Udfs.Between(mbr.Admtng_Icd9_Dgns_Cd, "49300", "49392", true) })), CollectionLength(ctx.valueFor("inpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.mutable.ArrayBuffer[com.ligadata.edifecs.System_InpatientClaim_100]].filter( mbr => { com.ligadata.pmml.udfs.Udfs.AnyBetween(mbr.Icd9_Dgns_Cds, "49300", "49392", true) })), CollectionLength(ctx.valueFor("outpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.mutable.ArrayBuffer[com.ligadata.edifecs.System_OutpatientClaim_100]].filter( mbr => { com.ligadata.pmml.udfs.Udfs.Between(mbr.Admtng_Icd9_Dgns_Cd, "49300", "49392", true) })), CollectionLength(ctx.valueFor("outpatientInfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.mutable.ArrayBuffer[com.ligadata.edifecs.System_OutpatientClaim_100]].filter( mbr => { com.ligadata.pmml.udfs.Udfs.AnyBetween(mbr.Icd9_Dgns_Cds, "49300", "49392", true) }))), 0)
        ctx.xDict.apply("WithEnvironmentalExposures").Value(new BooleanDataValue(WithEnvironmentalExposures))
          new BooleanDataValue(WithEnvironmentalExposures)
    }

}


class Derive_AATDeficiency (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val AATDeficiency = GreaterThan(CollectionLength(ctx.valueFor("hl7InfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.mutable.ArrayBuffer[com.ligadata.edifecs.System_HL7_100]].filter( mbr => { com.ligadata.pmml.udfs.Udfs.Equal(mbr.AATDeficiency, 1) })), 0)
        ctx.xDict.apply("AATDeficiency").Value(new BooleanDataValue(AATDeficiency))
          new BooleanDataValue(AATDeficiency)
    }

}


class Derive_Dyspnoea (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val Dyspnoea = GreaterThan(CollectionLength(ctx.valueFor("hl7InfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.mutable.ArrayBuffer[com.ligadata.edifecs.System_HL7_100]].filter( mbr => { com.ligadata.pmml.udfs.Udfs.Equal(mbr.Shortnessofbreath, 1) })), 0)
        ctx.xDict.apply("Dyspnoea").Value(new BooleanDataValue(Dyspnoea))
          new BooleanDataValue(Dyspnoea)
    }

}


class Derive_ChronicCough (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val ChronicCough = GreaterThan(CollectionLength(ctx.valueFor("hl7InfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.mutable.ArrayBuffer[com.ligadata.edifecs.System_HL7_100]].filter( mbr => { com.ligadata.pmml.udfs.Udfs.Equal(mbr.ChronicCough, 1) })), 0)
        ctx.xDict.apply("ChronicCough").Value(new BooleanDataValue(ChronicCough))
          new BooleanDataValue(ChronicCough)
    }

}


class Derive_ChronicSputum (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val ChronicSputum = GreaterThan(CollectionLength(ctx.valueFor("hl7InfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.mutable.ArrayBuffer[com.ligadata.edifecs.System_HL7_100]].filter( mbr => { com.ligadata.pmml.udfs.Udfs.Equal(mbr.ChronicSputum, 1) })), 0)
        ctx.xDict.apply("ChronicSputum").Value(new BooleanDataValue(ChronicSputum))
          new BooleanDataValue(ChronicSputum)
    }

}


class Derive_COPDSymptoms (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val COPDSymptoms = Or(ctx.valueFor("Dyspnoea").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("ChronicCough").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("ChronicSputum").asInstanceOf[BooleanDataValue].Value)
        ctx.xDict.apply("COPDSymptoms").Value(new BooleanDataValue(COPDSymptoms))
          new BooleanDataValue(COPDSymptoms)
    }

}


class Derive_FamilyHistory (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val FamilyHistory = Or(Equal(ctx.valueFor("msg").asInstanceOf[AnyDataValue].Value.asInstanceOf[com.ligadata.edifecs.System_Beneficiary_100].Sp_Copd, 1), GreaterThan(CollectionLength(ctx.valueFor("hl7InfoThisLastYear").asInstanceOf[AnyDataValue].Value.asInstanceOf[scala.collection.mutable.ArrayBuffer[com.ligadata.edifecs.System_HL7_100]].filter( mbr => { com.ligadata.pmml.udfs.Udfs.Or(mbr.ChronicCough, mbr.Sp_Copd, mbr.Shortnessofbreath, mbr.ChronicSputum) })), 0))
        ctx.xDict.apply("FamilyHistory").Value(new BooleanDataValue(FamilyHistory))
          new BooleanDataValue(FamilyHistory)
    }

}


class Derive_CATII_Rule2 (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val CATII_Rule2 = If(And(Not(ctx.valueFor("FortyYrsOrOlder").asInstanceOf[BooleanDataValue].Value), Or(ctx.valueFor("COPDSymptoms").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("AATDeficiency").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("FamilyHistory").asInstanceOf[BooleanDataValue].Value)))
        var result : Boolean = if (CATII_Rule2) { Put(ctx, "COPDSeverity", "2") } else { Put(ctx, "COPDSeverity", "NotSet") }
        ctx.xDict.apply("CATII_Rule2").Value(new BooleanDataValue(result))
        new BooleanDataValue(result)
    }

}


class Derive_CATI_Rule1b (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val CATI_Rule1b = If(And(ctx.valueFor("FortyYrsOrOlder").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("WithSmokingHistory").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("AATDeficiency").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("WithEnvironmentalExposures").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("COPDSymptoms").asInstanceOf[BooleanDataValue].Value))
        var result : Boolean = if (CATI_Rule1b) { Put(ctx, "COPDSeverity", "1b") } else { Put(ctx, "COPDSeverity", "NotSet") }
        ctx.xDict.apply("CATI_Rule1b").Value(new BooleanDataValue(result))
        new BooleanDataValue(result)
    }

}


class Derive_CATI_Rule1a (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) { 

    override def execute(ctx : Context) : BooleanDataValue = {
        val CATI_Rule1a = If(And(ctx.valueFor("FortyYrsOrOlder").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("WithSmokingHistory").asInstanceOf[BooleanDataValue].Value, Or(ctx.valueFor("AATDeficiency").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("WithEnvironmentalExposures").asInstanceOf[BooleanDataValue].Value, ctx.valueFor("COPDSymptoms").asInstanceOf[BooleanDataValue].Value)))
        var result : Boolean = if (CATI_Rule1a) { Put(ctx, "COPDSeverity", "1b") } else { Put(ctx, "COPDSeverity", "NotSet") }
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
