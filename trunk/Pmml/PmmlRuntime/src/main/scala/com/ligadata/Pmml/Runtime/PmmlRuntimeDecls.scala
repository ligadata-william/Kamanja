package com.ligadata.Pmml.Runtime

import scala.collection.mutable._
import scala.language.implicitConversions
import org.joda.time._
import com.ligadata.olep.metadata._
import com.ligadata.OnLEPBase._
import org.apache.log4j.Logger

trait LogTrait {
    val loggerName = this.getClass.getName()
    val logger = Logger.getLogger(loggerName)
}

/** class Context(mgr : MdMgr) { <== we may want 'mgr' this at runtime ...*/
class Context(val xId : Long) extends LogTrait {
	
	override def equals(another : Any) : Boolean = {
		(this == another)
	}
	
	/** used to generate a unique number for fcn names in same scope during scala code generation */
	var counter : Int = 0
	def Counter() : Int = { 
	  counter += 1 
	  counter 
	}
		
	var xformDict: HashMap[String,DerivedField] = HashMap[String,DerivedField]()
	def xDict : HashMap[String,DerivedField] = xformDict 
	def xDict(dict : HashMap[String,DerivedField]) { xformDict = dict } 

	var dataDict: HashMap[String,DataField] = HashMap[String,DataField]()
	def dDict : HashMap[String,DataField] = dataDict 
	def dDict(dict : HashMap[String,DataField]) { dataDict = dict } 
	
	var rulesetModel : RuleSetModel = new RuleSetModel("someModel", "someFcn", "someAlgo", "false")
	def SetRuleSetModel(model : RuleSetModel) = { rulesetModel = model }
	def GetRuleSetModel : RuleSetModel = { rulesetModel }
	
	var miningSchemaMap : Map[String,MiningField] = Map[String,MiningField]()
	def MiningSchemaMap : Map[String,MiningField] = miningSchemaMap
	def MiningSchemaMap( m : Map[String,MiningField]) { miningSchemaMap = m }

	
	def MissingValueFor (fldName : String) : DataValue = {
		val returnval : DataValue = rulesetModel.MissingValueFor(fldName)
		returnval
	}

	
	/** 
	    Various model identifiers from the model source file header and RuleSetModel:
	 	ApplicationName , FunctionName, PMML, Version, Copyright, Description, ModelName
	 */
	var pmmlModelIdentifiers : HashMap[String,Option[String]] = HashMap[String,Option[String]]()

	def ModelAppName : String = {
		val name : Option[String] = pmmlModelIdentifiers.apply("Application")
		name match {
		  case Some(name) => name
		  case _ => "No application name supplied"
		}
	}
	
	def ModelVersion : String = {
		val version : Option[String] = pmmlModelIdentifiers.apply("Version")
		version match {
		  case Some(version) => version
		  case _ => "No version supplied"
		}
	}
	
	def ModelDescription : String = {
		val descr : Option[String] = pmmlModelIdentifiers.apply("Description")
		descr match {
		  case Some(descr) => descr
		  case _ => "No description supplied"
		}
	}
	
	def ModelSourceFile : String = {
		val srcfile : Option[String] = pmmlModelIdentifiers.apply("PMML")
		srcfile match {
		  case Some(srcfile) => srcfile
		  case _ => "No source file collected..."
		}
	}
	
	/** 
	 *  General data value access to the data found in the data and transaction dictionaries   
	 */ 
	def valueFor(fldName : String) : DataValue = {

		val returnvalue : DataValue = if (dDict.contains(fldName)) {
			dDict.apply(fldName).Value
		} else {
			if (xDict.contains(fldName)) {
				 /** lazy evaluation done for transaction dict derived fields */
				var fld : DerivedField = xDict.apply(fldName)
				if (fld.ValueSet) {
					fld.Value 
				} else {
					val returnValue : DataValue = fld.execute(this)
					fld.Value(returnValue)
					returnValue
				} 
			} else {
				MissingValueFor (fldName)
			}
		}
		returnvalue
	}
	
	
	/** 
	 *  Determine if the DataField or DerivedField has been set  
	 *  
	 *  @param fldName : the name of the DataField or DerivedField of interest
	 *  @return true if the field has a valid value.
	 */ 
	def valueSetFor(fldName : String) : Boolean = {

		val isSet : Boolean = if (dDict.contains(fldName)) {
			val fld : DataField = dDict.apply(fldName)
			fld.ValueSet
		} else {
			if (xDict.contains(fldName)) {
				 /** lazy evaluation done for transaction dict derived fields */
				val fld : DerivedField = xDict.apply(fldName)
				fld.ValueSet 
			} else {
				false
			}
		}
		isSet
	}
	
	/** 
	 *  General data value update (explicit set of value vs executing associated function (for the derived fields)
	 */ 
	def valuePut(fldName : String, value : DataValue) : Boolean = {
		var set : Boolean = false
		if (dDict.contains(fldName)) {
			var fld : DataField = dDict.apply(fldName)
			fld.Value(value)
			set = fld.ValueSet
		} else {
			if (xDict.contains(fldName)) {
				 /** lazy evaluation done for transaction dict derived fields */
				var fld : DerivedField = xDict.apply(fldName)
				fld.Value(value)
				set = fld.ValueSet
			} 
		}
		set
	}

	def valuePut(fldName : String, value : String) : Boolean = { 
		valuePut(fldName, new StringDataValue(value))
	}

	def valuePut(fldName : String, value : Int) : Boolean = { 
		valuePut(fldName, new IntDataValue(value))
	}

	def valuePut(fldName : String, value : Long) : Boolean = { 
		valuePut(fldName, new LongDataValue(value))
	}

	def valuePut(fldName : String, value : Double) : Boolean = { 
		valuePut(fldName, new DoubleDataValue(value))
	}

	def valuePut(fldName : String, value : Float) : Boolean = { 
		valuePut(fldName, new FloatDataValue(value))
	}

	def valuePut(fldName : String, value : Any) : Boolean = { 
		valuePut(fldName, new AnyDataValue(value))
	}
	
	def valuePut(fldName : String, value : Boolean) : Boolean = { 
		valuePut(fldName, new BooleanDataValue(value))
	}
	
	

	/** 
	 *  Increment value any numeric fld by supplied amt (only integers supported for incr value at this time) 
	 */
	def valueIncr(fldName : String, incrAmt : Int) : Boolean = {
		var set : Boolean = false
		if (dDict.contains(fldName)) {
			var fld : DataField = dDict.apply(fldName)
			set = fld.Value match {
			  case i: IntDataValue =>  {
				  val value : Int = fld.asInstanceOf[IntDataValue].Value + incrAmt
				  fld.Value(new IntDataValue(value))
				  true
			  }
			  case l: LongDataValue =>   {
				  val value : Long = fld.asInstanceOf[LongDataValue].Value + incrAmt
				  fld.Value(new LongDataValue(value))
				  true
			  } 
			  case d: DoubleDataValue =>    {
				  val value : Double = fld.asInstanceOf[DoubleDataValue].Value + incrAmt
				  fld.Value(new DoubleDataValue(value))
				  true
			  }
			  case f: FloatDataValue =>    {
				  val value : Float = fld.asInstanceOf[FloatDataValue].Value + incrAmt
				  fld.Value(new FloatDataValue(value))
				  true
			  }
			  case _ => false
			}
		} else {
			if (xDict.contains(fldName)) {
				var fld : DerivedField = xDict.apply(fldName)
				set = fld.Value match {
				  case i: IntDataValue =>  {
					  val value : Int = fld.asInstanceOf[IntDataValue].Value + incrAmt
					  fld.Value(new IntDataValue(value))
					  true
				  }
				  case l: LongDataValue =>   {
					  val value : Long = fld.asInstanceOf[LongDataValue].Value + incrAmt
					  fld.Value(new LongDataValue(value))
					  true
				  } 
				  case d: DoubleDataValue =>    {
					  val value : Double = fld.asInstanceOf[DoubleDataValue].Value + incrAmt
					  fld.Value(new DoubleDataValue(value))
					  true
				  }
				  case f: FloatDataValue =>    {
					  val value : Float = fld.asInstanceOf[FloatDataValue].Value + incrAmt
					  fld.Value(new FloatDataValue(value))
					  true
				  }
				  case _ => false
				}
			}
		}
		set
	}
	
	def isFieldInTransformationDict(fldName : String) : Boolean = {
		xDict.contains(fldName)
	}
	
	def isFieldInDataDict(fldName : String) : Boolean = {
		dDict.contains(fldName)
	}
	
	
}


/** 
 *  data type containers ... each data field and derived data field has one ... This 
 *  is a work around to the generics issue I am having... maybe we can get rid of this and 
 *  use the template approach
 */
class DataValue (val dataType : String) {
	override def toString : String = "a DataValue"
}

object DataValue {
	// DataValue.compare(o1,o2)
	def compare(o1 : Any, o2 : Any) : Boolean = {
		try {
			o1 match {
				case d:Double => o1.asInstanceOf[Double] == o1.asInstanceOf[Double]
				case f:Float => o1.asInstanceOf[Float] == o1.asInstanceOf[Float]
				case l:Long => o1.asInstanceOf[Long] == o1.asInstanceOf[Long]
				case i:Int => o1.asInstanceOf[Int] == o1.asInstanceOf[Int]
				case b:Boolean => o1.asInstanceOf[Boolean] == o1.asInstanceOf[Boolean]
				case ldt:LocalDate => ((o1.asInstanceOf[LocalDate]).compareTo(o1.asInstanceOf[LocalDate]) == 0)
				case ltm:LocalTime => ((o1.asInstanceOf[LocalTime]).compareTo(o1.asInstanceOf[LocalTime]) == 0)
				case dtm:DateTime => ((o1.asInstanceOf[DateTime]).compareTo(o1.asInstanceOf[DateTime]) == 0)
				case s:String => o1.asInstanceOf[String] == o1.asInstanceOf[String]
				case _ => {
					println("Unknown type supplied for result comparison")
					false
				}
			}
		} catch {
			case _ : Throwable => false 
		}
	} 

	def defaultValue(dataType : String) : DataValue = {
		val default : DataValue = dataType match {
		  case "string" => new StringDataValue("NotSet")
		  case "integer" =>  new IntDataValue(0)
		  case "long" =>  new LongDataValue(0)
		  case "float" =>  new FloatDataValue(0) 
		  case "double" =>  new DoubleDataValue(0) 
		  case "date" =>  new DateDataValue(LocalDate.now()) 
		  case "dateTime" =>  new DateTimeDataValue(DateTime.now()) 
		  case "time" =>  new TimeDataValue(LocalTime.now()) 
		  
		  case _ => new AnyDataValue(List[String]())
		}
		default
	}
}

class IntDataValue (var value : Int) extends DataValue("Int") {
	def Value : Int = value
	def Value(valu : Int) { value = valu }
	override def toString : String = value.toString
}

object IntDataValue {
  implicit def asInt(s: String) : Int = augmentString(s).toInt
  def fromString1(i: Int) = i
  def fromString(s : String) : IntDataValue = new IntDataValue(fromString1(s))

  def main(args: Array[String]) {
	  val n: IntDataValue = fromString("1")
  }
}

class LongDataValue (var value : Long) extends DataValue("Long") {
	def Value : Long = value
	def Value(valu : Long) { value = valu }
	override def toString : String = value.toString
}

object LongDataValue {
	private implicit def asLong(s: String) : Long = augmentString(s).toLong
	private def fromString1(l: Long) = l
	def fromString(s : String) : LongDataValue = new LongDataValue(fromString1(s))
	def main(args: Array[String]) {
		val n: LongDataValue = fromString("1")
	}
}

class AnyDataValue(var value : Any) extends DataValue("Any") {
	def Value : Any = value
	def Value(valu : Any) { value = valu }  
	override def toString : String = ValueString(value)

	def ValueString(v: Any): String = {
	    if (v.isInstanceOf[scala.collection.immutable.Set[_]]) {
	      return v.asInstanceOf[scala.collection.immutable.Set[_]].mkString(",")
	    }
	    if (v.isInstanceOf[scala.collection.mutable.Set[_]]) {
	      return v.asInstanceOf[scala.collection.mutable.Set[_]].mkString(",")
	    }
	    if (v.isInstanceOf[List[_]]) {
	      return v.asInstanceOf[List[_]].mkString(",")
	    }
	    if (v.isInstanceOf[Array[_]]) {
	      return v.asInstanceOf[Array[_]].mkString(",")
	    }
	    v.toString
	}
}

object AnyDataValue {
	def fromString(s: String) = new AnyDataValue(s)
}

class StringDataValue(var value : String) extends DataValue("String") {
	def Value : String = value
	def Value(valu : String) { value = valu }  
	override def toString : String = value
}

object StringDataValue {
	def fromString(s: String) = new StringDataValue(s)
}

class FloatDataValue(var value : Float) extends DataValue("Float") {
	def Value : Float = value
	def Value(valu : Float) { value = valu }  
	override def toString : String = value.toString
}

object FloatDataValue {
	private implicit def asFloat(s: String) : Float = augmentString(s).toFloat
	private def fromString1(l: Float) = l
	def fromString(s : String) : FloatDataValue = new FloatDataValue(fromString1(s))
	def main(args: Array[String]) {
		val n: FloatDataValue = fromString("1")
	}
}

class DoubleDataValue(var value : Double) extends DataValue("Double") {
	def Value : Double = value
	def Value(valu : Double) { value = valu }  
	override def toString : String = value.toString
}

object DoubleDataValue {
	private implicit def asDouble(s: String) : Double = augmentString(s).toDouble
	private def fromString1(l: Double) = l
	def fromString(s : String) : DoubleDataValue = new DoubleDataValue(fromString1(s))
	def main(args: Array[String]) {
		val n: DoubleDataValue = fromString("1")
	}
}

class DateDataValue(var value : LocalDate) extends DataValue("LocalDate") {
	def Value : LocalDate = value
	def Value(valu : LocalDate) { value = valu }  
	override def toString : String = {
		val locDate : org.joda.time.LocalDate = value.asInstanceOf[DateDataValue].Value
		val mo = locDate.getMonthOfYear()
		val day = locDate.getDayOfMonth()
		val yr = locDate.getYear()
		val formatter : org.joda.time.format.DateTimeFormatter = org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd");
		val formattedDate : String  = formatter.print(locDate);
		s"${'"'}$formattedDate${'"'}"
	}
}

object DateDataValue {
	def fromString(dateTimeStr : String) : DateDataValue =  {
		val dateTime : org.joda.time.LocalDate = LocalDate.parse(dateTimeStr)
		new DateDataValue(dateTime)
	}
}

class DateTimeDataValue(var value : DateTime) extends DataValue("DateTime") {
	def Value : DateTime = value
	def Value(valu : DateTime) { value = valu }  
	override def toString : String = {
		val locDate : org.joda.time.DateTime = value.asInstanceOf[DateTimeDataValue].Value
		val mo = locDate.getMonthOfYear()
		val day = locDate.getDayOfMonth()
		val yr = locDate.getYear()
		val formatter : org.joda.time.format.DateTimeFormatter = org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd");
		val formattedDate : String  = formatter.print(locDate);
		s"${'"'}$formattedDate${'"'}"
	}
}

object DateTimeDataValue {
	def fromString(dateTimeStr : String)  : DateTimeDataValue = {
		val dateTime : org.joda.time.DateTime = DateTime.parse(dateTimeStr)
		new DateTimeDataValue(dateTime)
	}
}

class TimeDataValue(var value : LocalTime) extends DataValue("LocalTime") {
	def Value : LocalTime = value
	def Value(valu : LocalTime) { value = valu }  
	override def toString : String =  {
		val locTime : org.joda.time.LocalTime = value.asInstanceOf[TimeDataValue].Value
		val hrs = locTime.getHourOfDay()
		val mins = locTime.getMinuteOfHour()
		val secs = locTime.getSecondOfMinute()
		val ms = locTime.getMillisOfSecond()
		val formatter : org.joda.time.format.DateTimeFormatter = org.joda.time.format.DateTimeFormat.forPattern("k:m:s:S");
		val formattedTime : String  = formatter.print(locTime);
		s"${'"'}$formattedTime${'"'}"
	}
}

object TimeDataValue {
	def fromString(locTimeStr : String) : TimeDataValue = {
		var locTime : org.joda.time.LocalTime = LocalTime.parse(locTimeStr)
		new TimeDataValue(locTime)
	}
}

class BooleanDataValue(var value : Boolean) extends DataValue("Boolean") {
	def Value : Boolean = value
	def Value(valu : Boolean) { value = valu }  
	override def toString : String = {
		val locDateTime : org.joda.time.LocalDate = value.asInstanceOf[DateDataValue].Value
		val formatter : org.joda.time.format.DateTimeFormatter = org.joda.time.format.ISODateTimeFormat.dateTime();
		val formattedDateTime : String  = formatter.print(locDateTime);
		s"${'"'}$formattedDateTime${'"'}"
	}
}

object BooleanDataValue {
	def fromString(boolStr : String) : BooleanDataValue = {
		val bStr = boolStr.toLowerCase()
		var boolObj : BooleanDataValue = bStr match {
			case "true" | "t" | "1" => { 
				new BooleanDataValue(true)
			}
			case "false" | "f" | "0" => {
				new BooleanDataValue(false)
			}
		}
		boolObj
	}
}


/** 
 *  The DataField is the runtime representation of the DataFields found in the DataDictionary
 *  in the PMML file.   
 */
class DataField(val name : String, val dataType : String, values : ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) {
	
	var valueHasBeenSet : Boolean = false
	var valueRef : DataValue = null
	def Value : DataValue = { if (valueRef == null) DataValue.defaultValue(dataType) else valueRef }
	def Value(valu : DataValue) { 
	  valueRef = valu 
	  valueHasBeenSet = true
	}
	
	def ValueSet : Boolean = { valueHasBeenSet }
}

/** 
 *  The DerivedField is the runtime representation of the DataFields found in the DataDictionary
 *  in the PMML file.  A function named Fcn_name is called to compute the value that is stored in the value string
 */
class DerivedField(val name : String, val dataType : String, values : ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) {
	var valueHasBeenSet : Boolean = false
	var valueRef : DataValue = null
	def Value : DataValue =  { if (valueRef == null) DataValue.defaultValue(dataType) else valueRef }
	def Value(valu : DataValue) { 
	  	valueRef = valu 
	  	valueHasBeenSet = true
	}
	
	def ValueSet : Boolean = { valueHasBeenSet }
	
	/** generated model will override this with the apply function implementation */
	def execute(ctx : Context) : DataValue = { new StringDataValue("bogus value") }
}

/** 
 *  The MiningField indicates the actual fields that the model uses in its Model.  It indicates which of the fields are 
 *  designated as model output via the opType field... any{"active","predicted","supplementary","group","order","frequencyWeight","analysisWeight"}
 *  
 *
 */
class MiningField(val name : String
				, val usageType : String
				, val opType : String
				, val importance : Double
				, val outliers : String
				, val lowValue : Double
				, val highValue : Double
				, val missingValueReplacement : DataValue
				, val missingValueTreatment : String
				, val invalidValueTreatment : String) {
	
}

class SimpleRule(val id : String
			    , val score : String
			    , var recordCount : Double
			    , var nbCorrect : Double
			    , var confidence : Double
			    , var weight : Double)  {
  
	var scoreDistributions : ArrayBuffer[ScoreDistribution] = ArrayBuffer[ScoreDistribution]()
	
	def Name(ctx : Context) : String = { "RuleId_" + id + ctx.Counter() }

	def addScoreDistribution(sd : ScoreDistribution) {
		scoreDistributions += sd
	}

	def RecordCount(rCount : Double) {
		recordCount = rCount
	}

	def CorrectCount(cCount : Double) {
		nbCorrect = cCount
	}

	def Confidence(conf : Double) {
		confidence = conf
	}

	def Weight(wt : Double) {
		weight = wt
	}

	/** Subclasses will implement this one */
	def execute(ctx : Context, defaultScore : StringDataValue) : String = { "" }
}

class ScoreDistribution(var value : String
			    , var recordCount : Double
			    , var confidence : Double
			    , var probability : Double) {

	def RecordCount(rc : Double) { recordCount = rc }
	def Confidence(conf : Double) { confidence = conf }
	def Probability(prob : Double) { probability = prob }

}

class RuleSelectionMethod(criterion : String) {

}


class RuleSetModel(val modelName : String
				, val functionName : String
				, val algorithmName : String
				, val isScorable : String) {
  
	var miningSchemaMap : Map[String,MiningField] = Map[String,MiningField]()
	var ruleSet : ArrayBuffer[SimpleRule] = ArrayBuffer[SimpleRule]()
	var ruleSelectionMethods : ArrayBuffer[RuleSelectionMethod] = ArrayBuffer[RuleSelectionMethod]()
	var defaultScore : StringDataValue = null /** this attribute picked up from RuleSet */
	
	def AddMiningField(fieldName : String, fieldDetails: MiningField) {
		miningSchemaMap(fieldName) = fieldDetails
	}

	def AddRule(r : SimpleRule) {
		ruleSet += r
	}
	
	def AddRuleSelectionMethod(method : RuleSelectionMethod) {
		ruleSelectionMethods += method
	}

	def DefaultScore(dflt : StringDataValue) { defaultScore = dflt }
	def DefaultScore() : StringDataValue = { if (defaultScore != null) defaultScore else new StringDataValue("0") }
	

	def MiningSchemaMap() : Map[String,MiningField] = { miningSchemaMap } 
	def RuleSet() : ArrayBuffer[SimpleRule] = { ruleSet } 
	def RuleSelectionMethods() : ArrayBuffer[RuleSelectionMethod] = { ruleSelectionMethods } 
	
	
	def OutputFieldNames() : Array[String] = {
		val outputs = miningSchemaMap.filter((m) => m._2.opType == "predicted" || m._2.opType == "supplementary").keys.toArray
		outputs
	}
	
	def OutputFieldValues(ctx : Context) : Array[Any] = {
		OutputFieldNames().map(outfldName => ctx.valueFor(outfldName))
	}
	
/**	def OutputFieldValues(ctx : Context) : Array[Option[DataValue]] = {
		OutputFieldNames().map(outfldName => ctx.valueFor(outfldName))
	} 
 */
	
	def MissingValueFor (field : String) : DataValue = {
		miningSchemaMap.apply(field).missingValueReplacement
	}
	
	/** 
	 *  FIXME: The current mechanism only supports "FirstHit".  For models with selection methods
	 *  "weightedSum" and/or "weightedMax" with no defaultScore, a more elaborate mechanism is needed
	 *  that uses the model developer's rule confidence, weight, and score distribution values to 
	 *  choose among the rules that completed
	 */
	def MakePrediction(ctx : Context, results : ArrayBuffer[String]) = {
		val predictedField = miningSchemaMap.filter((m) => m._2.usageType == "predicted").keys.toArray.apply(0)
		
		/** if the field in question is not in the data dictionary (i.e., the transformation dictionary)
		 *  nothing more needs to be done.  It only needs to be transferred to the data dictionary 
		 *  .. at least in this version.  FIXME: When local transformation dictionary is supported, we need to
		 *  revisit this situation. I think the mining fields for report to the OLE should be in 
		 *  either of the global dictionaries ... the data or transformation dictionaries.
		 */
		if (ctx.isFieldInDataDict(predictedField)) {
			if (results.length > 0) {
				val prediction : String = results.apply(0)	
				var value : StringDataValue = new StringDataValue(prediction)
				value.Value(prediction)
				ctx.dataDict.apply(predictedField).Value(value) 
			} else {			
				ctx.dataDict.apply(predictedField).Value(DefaultScore()) 			
			}
		} else {
			val prediction : String = results.apply(0)	
			var value : StringDataValue = new StringDataValue(prediction)
			value.Value(prediction)
			ctx.xDict.apply(predictedField).Value(value) 
		}
	}

	/** Subclasses will implement this one */
	def execute(ctx : Context) { }
}

/** 
 *  PmmlRuntimeDecls contains those data types that will house the 
 *  data dictionary and transformation dictionary items, among others.
 */

