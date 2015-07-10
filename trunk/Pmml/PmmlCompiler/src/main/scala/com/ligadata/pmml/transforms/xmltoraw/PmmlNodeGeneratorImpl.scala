/** ApplicationHeaderNodes.scala */

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import org.xml.sax.Attributes

class ApplicationPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {

		val ofInterest : ArrayBuffer[String] = ArrayBuffer("name", "version")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val name : String = selectedValues.apply(0).asInstanceOf[String]
		val version : String = selectedValues.apply(1).asInstanceOf[String]
		new PmmlApplication(namespaceURI, localName , qName, lineNumber, columnNumber, name, version)
	}
}




/** DataDictionaryNodes.scala */

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import org.xml.sax.Attributes

class DataDictionaryPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {

		val ofInterest : ArrayBuffer[String] = ArrayBuffer("numberOfFields")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val numberOfFields : String = selectedValues.apply(0).asInstanceOf[String]
		new PmmlDataDictionary(namespaceURI, localName , qName, lineNumber, columnNumber, numberOfFields)
	}
}

class DataFieldPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {

		val ofInterest : ArrayBuffer[String] = ArrayBuffer("name", "displayName", "optype", "dataType", "taxonomy", "isCyclic")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val name : String = selectedValues.apply(0).asInstanceOf[String]
		val displayName : String = selectedValues.apply(1).asInstanceOf[String]
		val optype : String = selectedValues.apply(2).asInstanceOf[String]
		val dataType : String = selectedValues.apply(3).asInstanceOf[String]
		val taxonomy : String = selectedValues.apply(4).asInstanceOf[String]
		val isCyclic : String = selectedValues.apply(5).asInstanceOf[String]
		new PmmlDataField(namespaceURI, localName , qName, lineNumber, columnNumber, name, displayName, optype, dataType, taxonomy, isCyclic)
	}
}

class ValuePmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {

		val ofInterest : ArrayBuffer[String] = ArrayBuffer("value", "displayValue", "property")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val value : String = selectedValues.apply(0).asInstanceOf[String]
		val displayValue : String = selectedValues.apply(1).asInstanceOf[String]
		val property : String = selectedValues.apply(2).asInstanceOf[String]
		new PmmlValue(namespaceURI, localName , qName, lineNumber, columnNumber, value, displayValue, property)
	}
}


/** TransformationDictionaryNodes.scala */


package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import org.xml.sax.Attributes

class TransformationDictionaryPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {

		new PmmlTransformationDictionary(namespaceURI, localName , qName, lineNumber, columnNumber)
	}
}

class DerivedFieldPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("name", "displayName", "optype", "dataType")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val name : String = selectedValues.apply(0).asInstanceOf[String]
		val displayName : String = selectedValues.apply(1).asInstanceOf[String]
		val optype : String = selectedValues.apply(2).asInstanceOf[String]
		val dataType : String = selectedValues.apply(3).asInstanceOf[String]
		new PmmlDerivedField(namespaceURI, localName , qName, lineNumber, columnNumber, name, displayName, optype, dataType)
	}
}


class ApplyPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("function", "mapMissingTo", "invalidValueTreatment")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val function : String = selectedValues.apply(0).asInstanceOf[String]
		val mapMissingTo : String = selectedValues.apply(1).asInstanceOf[String]
		val invalidValueTreatment : String = selectedValues.apply(2).asInstanceOf[String]
		new PmmlApply(namespaceURI, localName , qName, lineNumber, columnNumber, function, mapMissingTo, invalidValueTreatment)
	}
}

class FieldRefPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("field", "mapMissingTo")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val field : String = selectedValues.apply(0).asInstanceOf[String]
		val mapMissingTo : String = selectedValues.apply(1).asInstanceOf[String]
		new PmmlFieldRef(namespaceURI, localName , qName, lineNumber, columnNumber, field, mapMissingTo)
	}
}


class ConstantPmmlNodeGenerator() extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode = {

		val ofInterest : ArrayBuffer[String] = ArrayBuffer("dataType")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		var dataType : String = selectedValues.apply(0).asInstanceOf[String]
		if (dataType == "") dataType = "string"
		new PmmlConstant(namespaceURI, localName , qName, lineNumber, columnNumber, dataType)
	}
}

class ArrayPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("n","type")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val n : String = selectedValues.apply(0).asInstanceOf[String]
		val arrayType = selectedValues.apply(1).asInstanceOf[String]
		new PmmlArray(namespaceURI, localName , qName, lineNumber, columnNumber, n, arrayType)
	}
}
	

/** MapValuesTableNodes.scala */

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import org.xml.sax.Attributes

class MapValuesPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("mapMissingTo", "defaultValue", "outputColumn", "dataType")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val mapMissingTo : String = selectedValues.apply(0).asInstanceOf[String]
		val defaultValue : String = selectedValues.apply(1).asInstanceOf[String]
		val outputColumn : String = selectedValues.apply(2).asInstanceOf[String]
		val dataType : String = selectedValues.apply(3).asInstanceOf[String]
		new PmmlMapValues(namespaceURI, localName , qName, lineNumber, columnNumber, mapMissingTo, defaultValue, outputColumn, dataType)
	}
}

class FieldColumnPairPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("field", "column")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val field : String = selectedValues.apply(0).asInstanceOf[String]
		val column : String = selectedValues.apply(1).asInstanceOf[String]
		new PmmlFieldColumnPair(namespaceURI, localName , qName, lineNumber, columnNumber, field, column)
	}
}

lass InlineTablePmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		new PmmlInlineTable(namespaceURI, localName , qName, lineNumber, columnNumber)
	}
}

class RowPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		new PmmlRow(namespaceURI, localName , qName, lineNumber, columnNumber)
	}
}

class RowTuplePmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		new PmmlRowTuple(namespaceURI, localName , qName, lineNumber, columnNumber)
	}
}

class TableLocatorPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		new PmmlTableLocator(namespaceURI, localName , qName, lineNumber, columnNumber)
	}
}


/** RuleSetNodes.scala */

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import org.xml.sax.Attributes

class RuleSetModelPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("modelName", "functionName", "algorithmName", "isScorable")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val modelName : String = selectedValues.apply(0).asInstanceOf[String]
		val functionName : String = selectedValues.apply(1).asInstanceOf[String]
		val algorithmName : String = selectedValues.apply(2).asInstanceOf[String]
		val isScorable : String = selectedValues.apply(3).asInstanceOf[String]
		new PmmlRuleSetModel(namespaceURI, localName , qName, lineNumber, columnNumber, modelName, functionName, algorithmName, isScorable)
	}
}

class SimpleRulePmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("id", "score", "recordCount", "nbCorrect", "confidence", "weight")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val id : String = selectedValues.apply(0).asInstanceOf[String]
		val score : String = selectedValues.apply(1).asInstanceOf[String]
		val recordCount : String = selectedValues.apply(2).asInstanceOf[String]
		val nbCorrect : String = selectedValues.apply(3).asInstanceOf[String]
		val confidence : String = selectedValues.apply(4).asInstanceOf[String]
		val weight : String = selectedValues.apply(5).asInstanceOf[String]
		new PmmlSimpleRule(namespaceURI, localName , qName, lineNumber, columnNumber, id, score, recordCount, nbCorrect, confidence, weight)
	}
}


class RuleSetPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("recordCount", "nbCorrect", "defaultScore", "defaultConfidence")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val recordCount : String = selectedValues.apply(0).asInstanceOf[String]
		val nbCorrect : String = selectedValues.apply(1).asInstanceOf[String]
		val defaultScore : String = selectedValues.apply(2).asInstanceOf[String]
		val defaultConfidence : String = selectedValues.apply(3).asInstanceOf[String]
		new PmmlRuleSet(namespaceURI, localName , qName, lineNumber, columnNumber, recordCount, nbCorrect, defaultScore, defaultConfidence)
	}
}
	
class RuleSelectionMethodPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("criterion")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val criterion : String = selectedValues.apply(0).asInstanceOf[String]
		new PmmlRuleSelectionMethod(namespaceURI, localName , qName, lineNumber, columnNumber, criterion)
	}
}
	




/** PredicateNodes.scala */

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import org.xml.sax.Attributes

class CompoundPredicatePmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("booleanOperator")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val booleanOperator : String = selectedValues.apply(0).asInstanceOf[String]
		new PmmlCompoundPredicate(namespaceURI, localName , qName, lineNumber, columnNumber, booleanOperator)
	}
}

class SimplePredicatePmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("field", "operator", "value")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val field : String = selectedValues.apply(0).asInstanceOf[String]
		val operator : String = selectedValues.apply(1).asInstanceOf[String]
		val value : String = selectedValues.apply(2).asInstanceOf[String]
		new PmmlSimplePredicate(namespaceURI, localName , qName, lineNumber, columnNumber, field, operator, value)
	}
}

class SimpleSetPredicatePmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("field", "booleanOperator")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val field : String = selectedValues.apply(0).asInstanceOf[String]
		val booleanOperator : String = selectedValues.apply(1).asInstanceOf[String]
		val value : String = selectedValues.apply(2).asInstanceOf[String]
		new PmmlSimpleSetPredicate(namespaceURI, localName, qName, lineNumber, columnNumber, field, booleanOperator)	
	}
}



/** MiningSchemaNodes.scala */


package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import org.xml.sax.Attributes

class MiningSchemaPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		new PmmlMiningSchema(namespaceURI, localName , qName, lineNumber, columnNumber)
	}
}

class MiningFieldPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("name"
													    , "usageType"
													    , "optype"
													    , "importance"
													    , "outliers"
													    , "lowValue"
													    , "highValue"
													    , "missingValueReplacement"
													    , "missingValueTreatment"
													    , "invalidValueTreatment")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val name : String = selectedValues.apply(0).asInstanceOf[String]
		val usageType : String = selectedValues.apply(1).asInstanceOf[String]
		val optype : String = selectedValues.apply(2).asInstanceOf[String]
		val importance : String = selectedValues.apply(3).asInstanceOf[String]
		val outliers : String = selectedValues.apply(4).asInstanceOf[String]
		val lowValue : String = selectedValues.apply(5).asInstanceOf[String]
		val highValue : String = selectedValues.apply(6).asInstanceOf[String]
		val missingValueReplacement : String = selectedValues.apply(7).asInstanceOf[String]
		val missingValueTreatment : String = selectedValues.apply(8).asInstanceOf[String]
		val invalidValueTreatment : String = selectedValues.apply(9).asInstanceOf[String]
		new PmmlMiningField(namespaceURI
						, localName 
						, qName
						, lineNumber
						, columnNumber
						, name
						, usageType
						, optype
						, importance
						, outliers
						, lowValue
						, highValue
						, missingValueReplacement
						, missingValueTreatment
						, invalidValueTreatment)
	}
}


/** FunctionNodes.scala */


package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import org.xml.sax.Attributes


class DefineFunctionPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("name", "optype", "dataType")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val name : String = selectedValues.apply(0).asInstanceOf[String]
		val optype : String = selectedValues.apply(1).asInstanceOf[String]
		val dataType : String = selectedValues.apply(2).asInstanceOf[String]
		new PmmlDefineFunction(namespaceURI, localName , qName, lineNumber, columnNumber, name, optype, dataType)
	}
}


class ParameterFieldPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("name", "optype", "dataType")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val name : String = selectedValues.apply(0).asInstanceOf[String]
		val optype : String = selectedValues.apply(1).asInstanceOf[String]
		val dataType : String = selectedValues.apply(2).asInstanceOf[String]
		new PmmlParameterField(namespaceURI, localName , qName, lineNumber, columnNumber, name, optype, dataType)
	}
}



/** StatisticalNodes.scala */


package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import org.xml.sax.Attributes

class ScoreDistributionPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("value", "recordCount", "confidence", "probability")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val value : String = selectedValues.apply(0).asInstanceOf[String]
		val recordCount : String = selectedValues.apply(1).asInstanceOf[String]
		val confidence : String = selectedValues.apply(2).asInstanceOf[String]
		val probability : String = selectedValues.apply(3).asInstanceOf[String]
		new PmmlScoreDistribution(namespaceURI, localName , qName, lineNumber, columnNumber, value, recordCount, confidence, probability)
	}
}

class IntervalPmmlNodeGenerator extends PmmlNodeGenerator {

	/**
	    With the supplied xml arguments build a PmmlNode and return it to the dispatcher that is calling.
 	    @param namespaceURI: String 
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return a PmmlNode
	 */
	 
	def make(namespaceURI: String
			, localName: String 
			, qName:String 
			, atts: Attributes
			, lineNumber : Int
			, columnNumber : Int) : PmmlNode =  {

		val ofInterest : ArrayBuffer[String] = ArrayBuffer("closure", "leftMargin", "rightMargin")
		val selectedValues = PmmlNode.hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val closure : String = selectedValues.apply(0).asInstanceOf[String]
		val leftMargin : String = selectedValues.apply(0).asInstanceOf[String]
		val rightMargin : String = selectedValues.apply(1).asInstanceOf[String]
		new PmmlInterval(namespaceURI, localName , qName, lineNumber, columnNumber, closure, leftMargin, rightMargin)
	}
}
