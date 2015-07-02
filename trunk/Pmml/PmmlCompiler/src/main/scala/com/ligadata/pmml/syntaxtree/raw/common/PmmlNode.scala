package com.ligadata.pmml.compiler

import scala.collection.immutable.{List}
import scala.collection.mutable._
import org.xml.sax.Attributes

class PmmlNodeBase(val namespaceURI: String, val localName: String , val qName:String
					, val lineNumber : Int, val columnNumber : Int) {} 
class PmmlNode(namespaceURI: String, localName: String , qName:String
					, lineNumber : Int, columnNumber : Int) 
		extends PmmlNodeBase(namespaceURI, localName, qName, lineNumber, columnNumber) 
{	 
	var children : ArrayBuffer[PmmlNodeBase] = ArrayBuffer[PmmlNodeBase]()
	def Children : ArrayBuffer[PmmlNodeBase] = children
	
	def addChild(child : PmmlNodeBase) = {
		children += child
	}
	
	/** By default only element names found in the map are collected.  There are cases where 
	 *  the children are self defined and at the collection time not understood by the 
	 *  element collection tools.  See the Pmmlrow for an example.  The row tuples are 
	 *  blindly collected when the following function answers true
	 */
	def CollectChildTuples : Boolean = { false }
}

class PmmlConstant(namespaceURI: String
			    , localName: String 
			    , qName:String
				, lineNumber : Int
				, columnNumber : Int
			    , val dataType : String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
	var value : String = ""
	def Value(v : String) { value = v }
	def Value() : String = value
}

class PmmlHeader(namespaceURI: String
			    , localName: String 
			    , qName:String
				, lineNumber : Int
				, columnNumber : Int
			    , val copyright : String
			    , val description : String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
	
}

class PmmlApplication(namespaceURI: String
			    , localName: String 
			    , qName:String
				, lineNumber : Int
				, columnNumber : Int
			    , val name : String
			    , val version : String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
}

class PmmlDataDictionary(namespaceURI: String
			    , localName: String 
			    , qName:String
				, lineNumber : Int
				, columnNumber : Int
			    , numberOfFields : String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
}	 


class PmmlDataField(namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int
				, val name : String
				, val displayName : String
				, val optype : String
				, val dataType : String
				, val taxonomy : String
				, val isCyclic : String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
}

class PmmlInterval(namespaceURI: String
			    , localName: String 
			    , qName:String
				, lineNumber : Int
				, columnNumber : Int
			    , closure : String
			    , leftMargin : String
			    , rightMargin : String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
}

class PmmlValue(  namespaceURI: String
			    , localName: String 
			    , qName:String
				, lineNumber : Int
				, columnNumber : Int
			    , val value : String
			    , val displayValue : String
			    , val property : String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
}

class PmmlTransformationDictionary (namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
		 
}

class PmmlDerivedField(namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int
				, val name : String
				, val displayName : String
				, val optype : String
				, val dataType : String)  extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
}	 

class PmmlDefineFunction(namespaceURI: String
						, localName: String 
						, qName:String
						, lineNumber : Int
						, columnNumber : Int
						, val name : String
						, val optype : String
						, val dataType : String)  extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {	
}

class PmmlParameterField(namespaceURI: String
						, localName: String 
						, qName:String
						, lineNumber : Int
						, columnNumber : Int
						, val name : String
						, val optype : String
						, val dataType : String)  extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {	
}

class PmmlApply(namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int
				, val function : String
				, val mapMissingTo : String
				, val invalidValueTreatment : String)  extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
  
	var categorizedValues : ArrayBuffer[Tuple2[String,String]] = ArrayBuffer[Tuple2[String,String]]()
	def CategorizedValue(idx : Int) : Tuple2[String,String] = categorizedValues.apply(idx)
	def addCategorizedValue(one : String, two : String)  : ArrayBuffer[Tuple2[String,String]] = { categorizedValues += (one -> two) }
	
}

class PmmlFieldRef(namespaceURI: String
				, localName : String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int
				, val field : String
				, val mapMissingTo : String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
}

class PmmlMapValues(namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int
				, val mapMissingTo : String
				, val defaultValue : String 
				, val outputColumn :String 
				, val dataType :String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
	var containerStyle : String = "map" /** By default "map", an xMapValuesMap is created; else "array" for an xMapValuesVector */
	var dataSource : String = "inline" /** By default "inline", the table/map content from InlineTable; else "locator" for a TableLocator */
	  
	def ContainerStyle(style : String) { containerStyle = style }
	def DataSource(src : String) { dataSource = src }
}    

class PmmlFieldColumnPair (namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int
				, val field : String
				, val column : String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
} 
/**
 
	<xs:element name="TableLocator">
	  <xs:complexType>
	    <xs:sequence>
	      <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded"/>
	    </xs:sequence>
	  </xs:complexType>
	</xs:element> 
	
    <TableLocator>
      <Extension name="dbname" value="myDB"/>
    </TableLocator>
    
  <xs:element name="Extension">
    <xs:complexType>
      <xs:complexContent mixed="true">
        <xs:restriction base="xs:anyType">
          <xs:sequence>
            <xs:any processContents="skip" minOccurs="0" maxOccurs="unbounded"/>
          </xs:sequence>
          <xs:attribute name="extender" type="xs:string" use="optional"/>
          <xs:attribute name="name" type="xs:string" use="optional"/>
          <xs:attribute name="value" type="xs:string" use="optional"/>
        </xs:restriction>
      </xs:complexContent>
    </xs:complexType>
  </xs:element>
 
*/

class PmmlTableLocator(namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int)  extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
} 

class PmmlExtension(namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int
				, val extender : String
				, val name : String
				, val value : String)  extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
} 

class PmmlInlineTable (namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int)  extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
} 

class Pmmlrow(namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int
				, val collectRowTuples : Boolean = true) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
	override def CollectChildTuples : Boolean = { collectRowTuples }
}

class PmmlRowTuple(namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int)  extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
	var value : String = ""
	def Value(v : String) {
		value = v
	}
	def Value() : String ={
		value
	}
} 

class PmmlRuleSetModel(namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int
				, val modelName: String
				, val functionName : String
				, val algorithmName : String
				, val isScorable : String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {

}

class PmmlSimpleRule(namespaceURI: String
				, localName: String 
				, qName:String 
				, lineNumber : Int
				, columnNumber : Int
				, val id: String
				, val score : String
				, val recordCount : String
				, val nbCorrect : String
				, val confidence : String
				, val weight : String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
}

class PmmlScoreDistribution(namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int
				, val value: String
				, val recordCount : String
				, val confidence : String
				, val probability : String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
}


class PmmlCompoundPredicate (namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int
				, val booleanOperator : String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
}


class PmmlSimplePredicate(namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int
				, val field : String
				, val operator : String
				, val value : String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
}

/** 
 *  <Array n="3" type="string">ab  "a b"   "with \"quotes\" "</Array>
 */
class PmmlArray(namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int
				, val n : String
				, val arrayType : String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
	var valueString : String = ""
	
	def SetArrayValue(value : String) {
	  	valueString = value
	}
}

class PmmlSimpleSetPredicate(namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int
				, val field : String
				, val booleanOperator : String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
}

class PmmlMiningSchema(namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
	
}

class PmmlMiningField(namespaceURI: String, localName: String , qName:String
				, lineNumber : Int
				, columnNumber : Int
				, val name : String
			    , val usageType : String
			    , val optype : String = ""
			    , val importance : String = ""
			    , val outliers : String = "asIs"
				, val lowValue : String = "-4294967295"
				, val highValue : String = "4294967296"
				, val missingValueReplacement : String = "0"
				, val missingValueTreatment : String = "asIs"
				, val invalidValueTreatment : String = "asIs") extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
}

class PmmlRuleSet (namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int
				, val recordCount : String
				, val nbCorrect : String
				, val defaultScore : String
				, val defaultConfidence : String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
}

class PmmlRuleSelectionMethod (namespaceURI: String
				, localName: String 
				, qName:String
				, lineNumber : Int
				, columnNumber : Int
				, val criterion : String) extends PmmlNode(namespaceURI, localName, qName, lineNumber, columnNumber) {
	
 }

object PmmlNode {
  
	/** 
	 *  hlpOrganizeAttributes is called from each of the mkPmml* 'make' fcns in order to get 
	 *  the attributes from the PMML in the order of the constructor.  If the attribute, especially
	 *  common on the optional attributes, is not present, a value of 'None' is returned in its place.
	 *  
	 *  At some point, we can dress this up by utilizing the defaults from the xsd instead (meaning that 
	 *  another ArrayBuffer would be supplied here with the appropriate defaults for each attribute).
	 */
	def hlpOrganizeAttributes(atts: Attributes, ofInterest : ArrayBuffer[String]) : Any = {

		var attributes:ArrayBuffer[String] = ArrayBuffer()
		var values:ArrayBuffer[String] = ArrayBuffer()
		var idx : Int = 0
		for (i <- 0 to atts.getLength()) { 
			attributes+=atts.getQName(idx)
			values+=atts.getValue(idx)
			idx = idx + 1
		}
		val attrValPairs:ArrayBuffer[(String,String)] = attributes.zip(values)
		val selectedValues : ArrayBuffer[_] = for (ctorArg <- ofInterest) yield {
			val matches = attrValPairs.filter(_._1 == ctorArg)
			if (matches.length > 0) matches.apply(0)._2 else "" /** take the first (should be ONLY) one */
		}
		
		selectedValues
	}
	
	def mkPmmlConstant(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlConstant = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("dataType")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		var dataType : String = selectedValues.apply(0).asInstanceOf[String]
		if (dataType == None) dataType = "string"
		new PmmlConstant(namespaceURI, localName , qName, lineNumber, columnNumber, dataType)
	}

	def  mkPmmlHeader(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlHeader = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("copyright", "description")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val copyright : String = selectedValues.apply(0).asInstanceOf[String]
		val description : String = selectedValues.apply(1).asInstanceOf[String]
		new PmmlHeader(namespaceURI, localName , qName, lineNumber, columnNumber, copyright, description)
	}
	
	def mkPmmlApplication(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlApplication = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("name", "version")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val name : String = selectedValues.apply(0).asInstanceOf[String]
		val version : String = selectedValues.apply(1).asInstanceOf[String]
		new PmmlApplication(namespaceURI, localName , qName, lineNumber, columnNumber, name, version)
	}
	
	def mkPmmlDataDictionary(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlDataDictionary = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("numberOfFields")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val numberOfFields : String = selectedValues.apply(0).asInstanceOf[String]
		new PmmlDataDictionary(namespaceURI, localName , qName, lineNumber, columnNumber, numberOfFields)
	}
	
	def mkPmmlDataField(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlDataField = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("name", "displayName", "optype", "dataType", "taxonomy", "isCyclic")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val name : String = selectedValues.apply(0).asInstanceOf[String]
		val displayName : String = selectedValues.apply(1).asInstanceOf[String]
		val optype : String = selectedValues.apply(2).asInstanceOf[String]
		val dataType : String = selectedValues.apply(3).asInstanceOf[String]
		val taxonomy : String = selectedValues.apply(4).asInstanceOf[String]
		val isCyclic : String = selectedValues.apply(5).asInstanceOf[String]
		new PmmlDataField(namespaceURI, localName , qName, lineNumber, columnNumber, name, displayName, optype, dataType, taxonomy, isCyclic)
	}
	
	def mkPmmlInterval(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlInterval = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("closure", "leftMargin", "rightMargin")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val closure : String = selectedValues.apply(0).asInstanceOf[String]
		val leftMargin : String = selectedValues.apply(0).asInstanceOf[String]
		val rightMargin : String = selectedValues.apply(1).asInstanceOf[String]
		new PmmlInterval(namespaceURI, localName , qName, lineNumber, columnNumber, closure, leftMargin, rightMargin)
	}
	
	def mkPmmlValue(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlValue = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("value", "displayValue", "property")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val value : String = selectedValues.apply(0).asInstanceOf[String]
		val displayValue : String = selectedValues.apply(1).asInstanceOf[String]
		val property : String = selectedValues.apply(2).asInstanceOf[String]
		new PmmlValue(namespaceURI, localName , qName, lineNumber, columnNumber, value, displayValue, property)
	}
	
	def mkPmmlTransformationDictionary(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlTransformationDictionary = {
		new PmmlTransformationDictionary(namespaceURI, localName , qName, lineNumber, columnNumber)
	}
	
	def mkPmmlDerivedField(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlDerivedField = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("name", "displayName", "optype", "dataType")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val name : String = selectedValues.apply(0).asInstanceOf[String]
		val displayName : String = selectedValues.apply(1).asInstanceOf[String]
		val optype : String = selectedValues.apply(2).asInstanceOf[String]
		val dataType : String = selectedValues.apply(3).asInstanceOf[String]
		new PmmlDerivedField(namespaceURI, localName , qName, lineNumber, columnNumber, name, displayName, optype, dataType)
	}
	
	def mkPmmlDefineFunction(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlDefineFunction = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("name", "optype", "dataType")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val name : String = selectedValues.apply(0).asInstanceOf[String]
		val optype : String = selectedValues.apply(1).asInstanceOf[String]
		val dataType : String = selectedValues.apply(2).asInstanceOf[String]
		new PmmlDefineFunction(namespaceURI, localName , qName, lineNumber, columnNumber, name, optype, dataType)
	}
	
	def mkPmmlParameterField(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlParameterField = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("name", "optype", "dataType")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val name : String = selectedValues.apply(0).asInstanceOf[String]
		val optype : String = selectedValues.apply(1).asInstanceOf[String]
		val dataType : String = selectedValues.apply(2).asInstanceOf[String]
		new PmmlParameterField(namespaceURI, localName , qName, lineNumber, columnNumber, name, optype, dataType)
	}
	
	def mkPmmlApply(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlApply = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("function", "mapMissingTo", "invalidValueTreatment")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val function : String = selectedValues.apply(0).asInstanceOf[String]
		val mapMissingTo : String = selectedValues.apply(1).asInstanceOf[String]
		val invalidValueTreatment : String = selectedValues.apply(2).asInstanceOf[String]
		new PmmlApply(namespaceURI, localName , qName, lineNumber, columnNumber, function, mapMissingTo, invalidValueTreatment)
	}
	
	def mkPmmlFieldRef(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlFieldRef = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("field", "mapMissingTo")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val field : String = selectedValues.apply(0).asInstanceOf[String]
		val mapMissingTo : String = selectedValues.apply(1).asInstanceOf[String]
		new PmmlFieldRef(namespaceURI, localName , qName, lineNumber, columnNumber, field, mapMissingTo)
	}
	
	def mkPmmlMapValues(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlMapValues = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("mapMissingTo", "defaultValue", "outputColumn", "dataType")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val mapMissingTo : String = selectedValues.apply(0).asInstanceOf[String]
		val defaultValue : String = selectedValues.apply(1).asInstanceOf[String]
		val outputColumn : String = selectedValues.apply(2).asInstanceOf[String]
		val dataType : String = selectedValues.apply(3).asInstanceOf[String]
		new PmmlMapValues(namespaceURI, localName , qName, lineNumber, columnNumber, mapMissingTo, defaultValue, outputColumn, dataType)
	}
	
	def mkPmmlFieldColumnPair(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlFieldColumnPair = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("field", "column")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val field : String = selectedValues.apply(0).asInstanceOf[String]
		val column : String = selectedValues.apply(1).asInstanceOf[String]
		new PmmlFieldColumnPair(namespaceURI, localName , qName, lineNumber, columnNumber, field, column)
	}

	def mkPmmlInlineTable(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlInlineTable = {
		new PmmlInlineTable(namespaceURI, localName , qName, lineNumber, columnNumber)
	}
	
	def mkPmmlrow(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : Pmmlrow = {
		new Pmmlrow(namespaceURI, localName , qName, lineNumber, columnNumber)
	}

	def mkPmmlRowTuple(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlRowTuple = {
		new PmmlRowTuple(namespaceURI, localName , qName, lineNumber, columnNumber)
	}
	

	def mkPmmlRuleSetModel(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlRuleSetModel = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("modelName", "functionName", "algorithmName", "isScorable")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val modelName : String = selectedValues.apply(0).asInstanceOf[String]
		val functionName : String = selectedValues.apply(1).asInstanceOf[String]
		val algorithmName : String = selectedValues.apply(2).asInstanceOf[String]
		val isScorable : String = selectedValues.apply(3).asInstanceOf[String]
		new PmmlRuleSetModel(namespaceURI, localName , qName, lineNumber, columnNumber, modelName, functionName, algorithmName, isScorable)
	}

	def mkPmmlSimpleRule(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlSimpleRule = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("id", "score", "recordCount", "nbCorrect", "confidence", "weight")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val id : String = selectedValues.apply(0).asInstanceOf[String]
		val score : String = selectedValues.apply(1).asInstanceOf[String]
		val recordCount : String = selectedValues.apply(2).asInstanceOf[String]
		val nbCorrect : String = selectedValues.apply(3).asInstanceOf[String]
		val confidence : String = selectedValues.apply(4).asInstanceOf[String]
		val weight : String = selectedValues.apply(5).asInstanceOf[String]
		new PmmlSimpleRule(namespaceURI, localName , qName, lineNumber, columnNumber, id, score, recordCount, nbCorrect, confidence, weight)
	}

	def mkPmmlScoreDistribution(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlScoreDistribution = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("value", "recordCount", "confidence", "probability")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val value : String = selectedValues.apply(0).asInstanceOf[String]
		val recordCount : String = selectedValues.apply(1).asInstanceOf[String]
		val confidence : String = selectedValues.apply(2).asInstanceOf[String]
		val probability : String = selectedValues.apply(3).asInstanceOf[String]
		new PmmlScoreDistribution(namespaceURI, localName , qName, lineNumber, columnNumber, value, recordCount, confidence, probability)
	}

	def mkPmmlCompoundPredicate(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlCompoundPredicate = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("booleanOperator")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val booleanOperator : String = selectedValues.apply(0).asInstanceOf[String]
		new PmmlCompoundPredicate(namespaceURI, localName , qName, lineNumber, columnNumber, booleanOperator)
	}

	def mkPmmlSimplePredicate(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlSimplePredicate = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("field", "operator", "value")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val field : String = selectedValues.apply(0).asInstanceOf[String]
		val operator : String = selectedValues.apply(1).asInstanceOf[String]
		val value : String = selectedValues.apply(2).asInstanceOf[String]
		new PmmlSimplePredicate(namespaceURI, localName , qName, lineNumber, columnNumber, field, operator, value)
	}
	
	def mkPmmlSimpleSetPredicate(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlSimpleSetPredicate = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("field", "booleanOperator")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val field : String = selectedValues.apply(0).asInstanceOf[String]
		val booleanOperator : String = selectedValues.apply(1).asInstanceOf[String]
		val value : String = selectedValues.apply(2).asInstanceOf[String]
		new PmmlSimpleSetPredicate(namespaceURI, localName, qName, lineNumber, columnNumber, field, booleanOperator)	
	}

	def mkPmmlMiningSchema(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlMiningSchema = {
		new PmmlMiningSchema(namespaceURI, localName , qName, lineNumber, columnNumber)
	}
	

	def mkPmmlMiningField(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlMiningField = {
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
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
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

	
	def mkPmmlRuleSet(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlRuleSet = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("recordCount", "nbCorrect", "defaultScore", "defaultConfidence")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val recordCount : String = selectedValues.apply(0).asInstanceOf[String]
		val nbCorrect : String = selectedValues.apply(1).asInstanceOf[String]
		val defaultScore : String = selectedValues.apply(2).asInstanceOf[String]
		val defaultConfidence : String = selectedValues.apply(3).asInstanceOf[String]
		new PmmlRuleSet(namespaceURI, localName , qName, lineNumber, columnNumber, recordCount, nbCorrect, defaultScore, defaultConfidence)
	}
  
  def mkPmmlRuleSelectionMethod(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlRuleSelectionMethod = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("criterion")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val criterion : String = selectedValues.apply(0).asInstanceOf[String]
		new PmmlRuleSelectionMethod(namespaceURI, localName , qName, lineNumber, columnNumber, criterion)
	}
  
  def mkPmmlArray(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlArray = {
		val ofInterest : ArrayBuffer[String] = ArrayBuffer("n","type")
		val selectedValues = hlpOrganizeAttributes(atts, ofInterest).asInstanceOf[ArrayBuffer[_]]
		val n : String = selectedValues.apply(0).asInstanceOf[String]
		val arrayType = selectedValues.apply(1).asInstanceOf[String]
		new PmmlArray(namespaceURI, localName , qName, lineNumber, columnNumber, n, arrayType)
	}

	def mkPmmlTableLocator(namespaceURI: String, localName: String , qName:String , atts: Attributes
					, lineNumber : Int, columnNumber : Int) : PmmlTableLocator = {
		new PmmlTableLocator(namespaceURI, localName , qName, lineNumber, columnNumber)
	}

}

