
package com.ligadata.pmml.transforms.xmltoraw.common

import scala.collection.mutable.ArrayBuffer
import org.xml.sax.Attributes
import com.ligadata.pmml.compiler._
import com.ligadata.pmml.traits._
import com.ligadata.pmml.support._
import com.ligadata.pmml.syntaxtree.raw.common._
import com.ligadata.pmml.transforms.xmltoraw.common._


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


