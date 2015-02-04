package com.ligadata.Compiler

import scala.collection.mutable._
import scala.math._
import scala.collection.immutable.StringLike
import scala.collection.immutable.List
import scala.collection.immutable.Map

import org.joda.time.base
import org.joda.time.chrono
import org.joda.time.convert
import org.joda.time.field
import org.joda.time.format
import org.joda.time.tz
import org.joda.time.LocalDate
import org.joda.time.Years

import com.ligadata.Pmml.Runtime._

object PmmlError extends LogTrait {
  
	/** 
	 *  Process the supplied error, first printing the geo location info in the element stack.
	 *  Leave the stack intact so other error or warning messages that may follow also have this information.
	 *  PmmlError.logError
	 *  
	 *  @param ctx the execution context for the compiler
	 *  @param errorMsg the error message to print after printing the location information
	 */
	def logError(ctx : PmmlContext, errorMsg : String) : Unit = {
		
		if (ctx.elementStack.nonEmpty) { 
			val buffer : StringBuilder = new StringBuilder
			
			val sizeofStack : Int = ctx.elementStack.size
			var i : Int = 0
			ctx.elementStack.foreach( elem => {
				val elemStr : String = elem.toString
				if (elemStr != null && elemStr.size > 0) {
					buffer.append(elem.toString)
					i += 1
					if (i < sizeofStack) {
						buffer.append(" found in ")
					} 
				}
			})
			val errorVicinity : String = buffer.toString
			logger.error(s"While processing $errorVicinity...")
			logger.error(errorMsg)
		}
	  
	}

	/** 
	 *  Process the supplied warning, first printing the geo location info in the element stack.
	 *  Leave the stack intact so other warning or error messages that may follow also have this information.
	 *  PmmlError.logError
	 *  
	 *  @param ctx the execution context for the compiler
	 *  @param warningMsg the warning message to print after printing the location information
	 */
	def logWarning(ctx : PmmlContext, warningMsg : String) : Unit = {
		
		if (ctx.elementStack.nonEmpty) { 
			val buffer : StringBuilder = new StringBuilder
			
			val sizeofStack : Int = ctx.elementStack.size
			var i : Int = 0
			ctx.elementStack.foreach( elem => {
				val elemStr : String = elem.toString
				if (elemStr != null && elemStr.size > 0) {
					buffer.append(elem.toString)
					i += 1
					if (i < sizeofStack) {
						buffer.append(" found in ")
					} 
				}
			})
			val warnVicinity : String = buffer.toString
			logger.warn(s"While processing $warnVicinity...")
			logger.warn(warningMsg)
		}
	  
	}
}


object PmmlTypes extends LogTrait {
  
  	def scalaDataType(dataType : String) : String = {
	  		val typ = dataType match {
		      case "string" => "String"
		      case "integer" => "Int" 
		      case "float" => "Float" 
		      case "double" => "Double" 
		      case "real" => "Double" 
		      case "boolean" => "Boolean" 
		      case "date" => "LocalDate" 
		      case "time" => "LocalTime" 
		      case "dateTime" => "LocalDate" 
		      case "dateDaysSince[0]" => "Long" 
		      case "dateDaysSince[1960]" => "Long" 
		      case "dateDaysSince[1970]" => "Long" 
		      case "dateDaysSince[1980]" => "Long" 
		      case "timeSeconds" => "Long" 
		      case "dateTimeSecondsSince[0]" => "Long" 
		      case "dateTimeSecondsSince[1960]" => "Long" 
		      case "dateTimeSecondsSince[1970]" => "Long" 
		      case "dateTimeSecondsSince[1980]" => "Long" 
		  
		      case _ => {
		    	  //println(s"scalaDataType($dataType)... unknown type... Any returned")
		    	  "Any"
		      }
			} 
	  		typ
	}
  	
  	/** Wrap quotes if needed around supplied string acorrding to the type */ 
  	def QuoteString(fcnNameStr : String, dataType : String) : String = {
  		val str = dataType match {
	      case "string" | "date" | "time" | "dateTime" => s"${'"'}$fcnNameStr${'"'}" 
	      case _  => fcnNameStr
		} 
  		str
	}

  	def scalaDerivedDataType(dataType : String) : String = {
  			//println(s"scalaDerivedDataType arg = $dataType")
	  		val typ = dataType match {
		      case "String" => "StringDataValue"
		      case "Int" => "IntDataValue" 
		      case "Integer" => "IntDataValue" 
		      case "Long" => "LongDataValue" 
		      case "Float" => "FloatDataValue" 
		      case "Double" => "DoubleDataValue" 
		      case "Boolean" => "BooleanDataValue" 
		      case "LocalDate" => "DateDataValue" 
		      case "LocalTime" => "TimeDataValue" 
		      case "DateTime" => "DateTimeDataValue"
		  
		      case _ => {
		    	  //println(s"scalaDerivedDataType($dataType)... unknown type... Any returned")
		    	  "Any"
		      }
			} 
	  		typ
	}

 	def scalaDerivedDataTypeToScalaType(derivedDataType : String) : String = {
	  		val typ = derivedDataType match {
		      case "StringDataValue" => "String"
		      case "IntDataValue" => "Int" 
		      case "LongDataValue" => "Long" 
		      case "FloatDataValue" => "Float" 
		      case "DoubleDataValue" => "Double" 
		      case "BooleanDataValue" => "Boolean" 
		      case "DateDataValue" => "LocalDate" 
		      case "TimeDataValue" => "LocalTime" 
		      case "DateTimeDataValue" => "DateTime"
		  
		      case _ =>  "Any" 
			} 
	  		typ
	}

 	def scalaTypeToDataValueType(scalaType : String) : String = {
	  		val typ = scalaType match {
		      case "String" => "StringDataValue"
		      case "Int" => "IntDataValue" 
		      case "Integer" => "IntDataValue" 
		      case "Long" =>  "LongDataValue"
		      case "Float" => "FloatDataValue" 
		      case "Double" => "DoubleDataValue" 
		      case "Boolean" => "BooleanDataValue" 
		      case "LocalDate" => "DateDataValue" 
		      case "LocalTime" => "TimeDataValue" 
		      case "DateTime" => "DateTimeDataValue"
		  
		      case _ =>  "AnyDataValue" 
			} 
	  		typ
	}

  	def dataValueFromString(dataType : String, strRep : String) : DataValue = {
	  		val obj : DataValue = dataType match {
		      case "String" => StringDataValue.fromString(strRep)
		      case "Int" => IntDataValue.fromString(strRep)
		      case "Integer" => IntDataValue.fromString(strRep)
		      case "Long" => LongDataValue.fromString(strRep)
		      case "Float" => FloatDataValue.fromString(strRep)
		      case "Double" => DoubleDataValue.fromString(strRep)
		      case "Boolean" => BooleanDataValue.fromString(strRep)
		      case "LocalDate" => DateDataValue.fromString(strRep)
		      case "LocalTime" => TimeDataValue.fromString(strRep)
		      case "DateTime" => DateTimeDataValue.fromString(strRep)
		  
		      case _ => {
		    	  //println(s"dataValueFromString($dataType, $strRep)... unknown type ... coercion to String")
		    	  StringDataValue.fromString(strRep)
		      }
			} 
	  		obj
	}

  	
  	def scalaBuiltinNameFcnSelector(opname : String) : String = {
		val fcnName = opname match {
		      case "if" => "If"
		      case "or" => "Or"
		      case "and" => "And"
		      case "xor" => "Xor"
		      case "equal" => "Equal"
		      case "notEqual" => "NotEqual"
		      case "-" => "Minus"
		      case "+" => "Plus"
		      case "*" => "Multiply"
		      case "/" => "Divide"
		      case "not" => "Not"
		      case "lessThan" => "LessThan"
		      case "lessOrEqual" => "LessOrEqual"
		      case "greaterThan" => "GreaterThan"
		      case "greaterOrEqual" => "GreaterOrEqual"
		      case "isMissing" => "IsMissing"
		      case "isNotMissing" => "IsNotMissing"
		      case "isIn" => "IsIn"
		      case "isNotIn" => "IsNotIn"
		      case _  => "Unknown Operator"
		}
		fcnName
  	}
  	
  	def translateBuiltinNameIfNeeded(fcnName : String) : String = {
  		val fcnNameX : String = scalaBuiltinNameFcnSelector(fcnName)
  		if (fcnNameX != "Unknown Operator") fcnNameX else fcnName
  	}
  	
   	def scalaNameForIterableFcnName(iterableFcnName : String) : String = {
		val fcnName = iterableFcnName.toLowerCase() match {
		      case "containermap" => "map"
		      case "containerfilter" => "filter"
		      case "groupby" => "groupBy"
		      case _  => null
		}
		fcnName
  	}
 
}

/** 
 *  This enumeration describes the type of traversal to perform for 
 *  PmmlExecNode tree traversal.  By default (since we are doing functional language,
 *  the order is PREORDER
 */
object Traversal extends Enumeration {
	type Order = Value 
	val INORDER, PREORDER, POSTORDER = Value
}

/** 
 *  CodeFragment enumeration describes which kind of string to generate for a given
 *  PmmlExecNode.  This enumeration can be used to control if desired which aspect
 *  of a given PMML tree to print.  For example, the TransformationDictionary DerivedField
 *  has both a declaration (as part of the dictionary) and a declaration of a class that has
 *  the apply functions for computing the DerivedField's value.  
 */
object CodeFragment extends Enumeration {
	type Kind = Value
	val VARDECL, VALDECL, FUNCCALL, DERIVEDCLASS, RULECLASS, RULESETCLASS , MININGFIELD, MAPVALUE, AGGREGATE, USERFUNCTION = Value
}


