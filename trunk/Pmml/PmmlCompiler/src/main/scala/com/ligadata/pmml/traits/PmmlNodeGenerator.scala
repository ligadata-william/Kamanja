package com.ligadata.pmml.traits

import org.xml.sax.Attributes
import com.ligadata.pmml.syntaxtree.raw.common._


/**
 * The PmmlNodeGeneratorDispatch interface accepts the PMML element values collected by the Sax parse.
 * Based upon the xml "qName" supplied, the appropriate PmmlNodeGenerator is dispatched.  The returned node is added
 * to the syntax tree.
 */
trait PmmlNodeGeneratorDispatch {

	/** 
		Select the appropriate PmmlNode generator for the supplied xml values, locate its
	    PmmlNodeGenerator (the 'qName' is the key), and dispatch it. The returned node is added to the 
	    syntax tree owned by (or addressable by) the PmmlNodeGeneratorDispatch implementation.
 	    
 	    @param namespaceURI: String
 	    @param localName: String 
 	    @param qName:String
 	    @param atts: Attributes
 	    @param lineNumber : Int
 	    @param columnNumber : Int
 	    @return Unit
	 */

	def dispatch(namespaceURI: String, localName: String , qName:String , atts: Attributes, lineNumber : Int, columnNumber : Int) 
	
}

/**
 * The PmmlNodeGenerator is an an interface that consumes the xml content supplied in the parameter list
 * of the generator itself 
 * 
 */
trait PmmlNodeGenerator {

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
			, columnNumber : Int) : PmmlNode
	
}

