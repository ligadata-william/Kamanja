package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._


class ConstantPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val c : PmmlConstant =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlConstant]) {
				pmmlnode.asInstanceOf[PmmlConstant] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlConstant... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val cNode : Option[PmmlExecNode] = if (c != null) {
			Some(new xConstant(c.lineNumber, c.columnNumber, c.dataType, PmmlTypes.dataValueFromString(c.dataType, c.Value())))
		} else {
			None
		}
		cNode
	}
	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class HeaderPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val headerNode : PmmlHeader =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlHeader]) {
				pmmlnode.asInstanceOf[PmmlHeader] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlHeader... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val node : Option[PmmlExecNode] = if (headerNode != null) {
			/** Collect the copyright and description if present for later use during code generation */
			ctx.pmmlTerms("Copyright") = Some(headerNode.copyright)
			ctx.pmmlTerms("Description") = Some(headerNode.description)
		  
			Some(new xHeader(headerNode.lineNumber, headerNode.columnNumber, headerNode.copyright, headerNode.description))
		} else {
			None
		}
		node
	}
	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class ApplicationPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val headerNode : PmmlApplication =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlApplication]) {
				pmmlnode.asInstanceOf[PmmlApplication] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlApplication... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val node : Option[PmmlExecNode] = if (headerNode != null) {
			/** Collect the Application name and the version if it is present */
			ctx.pmmlTerms("ApplicationName") = Some(headerNode.name)
			ctx.pmmlTerms("Version") = Some(MdMgr.FormatVersion(if (headerNode.version == null || headerNode.version.trim.isEmpty) "1.1" else headerNode.version))

			/** update the header parent node with the application name and version, don't create node*/
			
			val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
			top match {
			  case Some(top) => {
				  var header : xHeader = top.asInstanceOf[xHeader]
				  header.ApplicationNameVersion(headerNode.name, headerNode.version)
			  }
			  case _ => None
			}
			
			None 
		} else {
			None
		}
		node
	}
	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class DataDictionaryPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val dictNode : PmmlDataDictionary =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlDataDictionary]) {
				pmmlnode.asInstanceOf[PmmlDataDictionary] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlDataDictionary... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val node : Option[PmmlExecNode] = if (dictNode != null) {
			Some(new xDataDictionary(dictNode.lineNumber, dictNode.columnNumber)) 
		} else {
			None
		}
		node
	}
	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class DataFieldPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val dfld : PmmlDataField =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlDataField]) {
				pmmlnode.asInstanceOf[PmmlDataField] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlDataField... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val node : Option[PmmlExecNode] = if (dfld != null) {
			val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
			var datafld : xDataField = null
			top match {
			  case Some(top) => {
				  var dict : xDataDictionary = top.asInstanceOf[xDataDictionary]
				  datafld = new xDataField(dfld.lineNumber, dfld.columnNumber, dfld.name, dfld.displayName, dfld.optype, dfld.dataType, dfld.taxonomy, dfld.isCyclic)
				  dict.add(datafld)
				  ctx.dDict(datafld.name) = datafld
				  if (dfld.Children.size > 0) {
					  /** pick up the values for DataFields */
					  dfld.Children.foreach( aNode => {
						  if (aNode.isInstanceOf[PmmlValue]) {
						  val vNode : PmmlValue = aNode.asInstanceOf[PmmlValue]
							  datafld.addValuePropertyPair(vNode.value, vNode.property)
						  }
					  })
				  }
			  }
			  case _ => None
			}
			None
		} else {
			None
		}
		node
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class ValuePmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlValue =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlValue]) {
				pmmlnode.asInstanceOf[PmmlValue] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlValue... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
			top match {
			  case Some(top) => {
				  if (top.isInstanceOf[xDataField]) {
					  var d : xDataField = top.asInstanceOf[xDataField]
					  d.addValuePropertyPair(node.value, node.property)
				  } else {
					  var df : xDerivedField = top.asInstanceOf[xDerivedField]
					  df.addValuePropertyPair(node.value, node.property)
				  }
			  }
			  case _ => None
			}
			None
		} else {
			None
		}
		xnode
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class IntervalPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlInterval =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlInterval]) {
				pmmlnode.asInstanceOf[PmmlInterval] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlInterval... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
			top match {
				case Some(top) => {
					if (top.isInstanceOf[xDataField]) {
						  var d : xDataField = top.asInstanceOf[xDataField]
						  d.continuousConstraints(node.leftMargin, node.rightMargin, node.closure)
					} else {
						  var df : xDerivedField = top.asInstanceOf[xDerivedField]
						  df.continuousConstraints(node.leftMargin, node.rightMargin, node.closure)
					}
				}
				case _ => None
			}
			None
		} else {
			None
		}
		xnode
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class TransformationDictionaryPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlTransformationDictionary =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlTransformationDictionary]) {
				pmmlnode.asInstanceOf[PmmlTransformationDictionary] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlTransformationDictionary... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			Some(new xTransformationDictionary(node.lineNumber, node.columnNumber))
		} else {
			None
		}
		xnode
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class DerivedFieldPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlDerivedField =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlDerivedField]) {
				pmmlnode.asInstanceOf[PmmlDerivedField] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlDerivedField... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			/** update the data dictionary parent node with the datafield; don't create node*/
			val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
			var fld : xDerivedField = null
			top match {
			  case Some(top) => {
				  	var dict : xTransformationDictionary = top.asInstanceOf[xTransformationDictionary]
				  	fld = new xDerivedField(node.lineNumber, node.columnNumber, node.name, node.displayName, node.optype, node.dataType)
					dict.add(fld)
					ctx.xDict(fld.name) = fld
			  }
			  case _ => None
			}
			if (fld == null) {
				None
			} else {
			  Some(fld)
			}
		} else {
			None
		}
		xnode
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class DefineFunctionPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlDefineFunction =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlDefineFunction]) {
				pmmlnode.asInstanceOf[PmmlDefineFunction] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlDefineFunction... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			Some(new xDefineFunction(node.lineNumber, node.columnNumber, node.name, node.optype, node.dataType))
		} else {
			None
		}
		xnode
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class ParameterFieldPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlParameterField =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlParameterField]) {
				pmmlnode.asInstanceOf[PmmlParameterField] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlParameterField... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			Some(new xParameterField(node.lineNumber, node.columnNumber, node.name, node.optype, node.dataType))
		} else {
			None
		}
		xnode
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class ApplyPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlApply =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlApply]) {
				pmmlnode.asInstanceOf[PmmlApply] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlApply... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			Some(new xApply(node.lineNumber, node.columnNumber, node.function, node.mapMissingTo, node.invalidValueTreatment))
		} else {
			None
		}
		xnode
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class FieldRefPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlFieldRef =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlFieldRef]) {
				pmmlnode.asInstanceOf[PmmlFieldRef] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlFieldRef... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			Some(new xFieldRef(node.lineNumber, node.columnNumber, node.field, node.mapMissingTo))
		} else {
			None
		}
		xnode
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class MapValuesPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlMapValues =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlMapValues]) {
				pmmlnode.asInstanceOf[PmmlMapValues] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlMapValues... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			val typ = node.dataType
			val containerStyle = node.containerStyle
			val dataSrc = node.dataSource
			
			var mapvalues = if (containerStyle == "map") {
				if (dataSrc == "inline") {
					new xMapValuesMapInline(node.lineNumber, node.columnNumber, node.mapMissingTo, node.defaultValue, node.outputColumn, node.dataType, containerStyle, dataSrc)
				  
				} else { /** locator */
					new xMapValuesMapExternal(node.lineNumber, node.columnNumber, node.mapMissingTo, node.defaultValue, node.outputColumn, node.dataType, containerStyle, dataSrc) 
				}
			} else { /** array */
				if (dataSrc == "inline") {
					new xMapValuesArrayInline(node.lineNumber, node.columnNumber, node.mapMissingTo, node.defaultValue, node.outputColumn, node.dataType, containerStyle, dataSrc)
				} else { /** locator */
					new xMapValuesArrayExternal(node.lineNumber, node.columnNumber, node.mapMissingTo, node.defaultValue, node.outputColumn, node.dataType, containerStyle, dataSrc) 
				}
			}
			Some(mapvalues)
		} else {
			None
		}
		xnode
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class FieldColumnPairPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlFieldColumnPair =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlFieldColumnPair]) {
				pmmlnode.asInstanceOf[PmmlFieldColumnPair] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlFieldColumnPair... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			/** update the value map node with the field col pair; don't create node*/
			val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
			top match {
			  case Some(top) => {
				  	var mv :xMapValuesMap = top.asInstanceOf[xMapValuesMap]
					mv.addFieldColumnPair (node.field , node.column) 
			  }
			  case _ => None
			}
			None
		} else {
			None
		}
		xnode
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class RowColumnPairPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlRow =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlRow]) {
				pmmlnode.asInstanceOf[PmmlRow] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlRow... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
			top match {
			  case Some(top) => {
				  	var mv :xMapValuesMap = top.asInstanceOf[xMapValuesMap]

					/** Each PmmlRow has children (in this implementation we expect two values...
					 *  a key and a value.  The value maps to the MapValues output and the 
					 *  there is but one key sine we represent the inline table as a Map.
					 *  NOTE that more than one key can be present according to the spec. 
					 *  For this revision, this is not supported.  Tuple2 only.
					 */
					val tuples : ArrayBuffer[Option[PmmlRowTuple]] = node.children.filter(_.isInstanceOf[PmmlRowTuple]).asInstanceOf[ArrayBuffer[Option[PmmlRowTuple]]]
			
					val valuelen = tuples.length
					if (valuelen == 2) {
						val t0 = tuples(0)
						val k : String = t0 match {
						  case Some(t0) => t0.Value()
						  case _ => ""
						}
						val t1 = tuples(1)
						t1 match {
						  case Some(t1) => {
							  if (k.length() > 0)
								  mv.add(k,t1.Value())
						  }
						  case _ => None
						}
					} else {
						PmmlError.logError(ctx, s"Currently only 2 tuple rows (simple maps) are supported for InlineTables... row has $valuelen elements")
					}
			  }
			  case _ => None
			}
			None
		} else {
			None
		}
		xnode
	}	
}


package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class TableLocatorPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlTableLocator =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlTableLocator]) {
				pmmlnode.asInstanceOf[PmmlTableLocator] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlTableLocator... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			val pmmlxtensions : Queue[Option[PmmlExtension]] = node.children.filter(_.isInstanceOf[PmmlExtension]).asInstanceOf[Queue[Option[PmmlExtension]]]

			if (pmmlxtensions.length > 0) {
				val extens  = pmmlxtensions.map(x => {
					x match {
					  	case Some(x) => new xExtension(node.lineNumber, node.columnNumber, x.extender, x.name, x.value)
					  	case _ => new xExtension(node.lineNumber, node.columnNumber, "", "", "")
					}
				}).toArray
			  
				val extensions = extens.filter(_.name.length > 0)
				if (! ctx.pmmlExecNodeStack.isEmpty) {
					val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
					top match {
						case Some(top) => {
							top match {
							  	case xMap : xMapValuesMap => xMap.addExtensions(extensions) 
							  	case xArr : xMapValuesArray => xArr.addExtensions(extensions) 
							  	case _ => PmmlError.logError(ctx, "Parent of TableLocator is not a kind MapValues...discarded")
							}
						}
						case _ => PmmlError.logError(ctx, "TableLocator cannot be added")
					}
				}
			}
		  
			None
		} else {
			None
		}
		xnode
	}	
}

	
package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class InlineTablePmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlInlineTable =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlInlineTable]) {
				pmmlnode.asInstanceOf[PmmlInlineTable] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlInlineTable... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			val pmmlxtensions : Queue[Option[PmmlExtension]] = node.children.filter(_.isInstanceOf[PmmlExtension]).asInstanceOf[Queue[Option[PmmlExtension]]]

			if (pmmlxtensions.length > 0) {
				val extenss  = pmmlxtensions.map(x => {
					x match {
					  	case Some(x) => new xExtension(node.lineNumber, node.columnNumber, x.extender, x.name, x.value)
					  	case _ => new xExtension(node.lineNumber, node.columnNumber, "", "", "")
					}
				}).toArray
				val extensions = extenss.filter(_.name.length > 0)
				if (! ctx.pmmlExecNodeStack.isEmpty) {
					val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
					top match {
						case Some(top) => {
							top match {
							  	case xMap : xMapValuesMap => xMap.addExtensions(extensions) 
							  	case xArr : xMapValuesArray => xArr.addExtensions(extensions) 
							  	case _ => PmmlError.logError(ctx, "Parent of InlineTable is not a kind MapValues...discarded")
							}
						}
						case _ => PmmlError.logError(ctx, "inline table cannot be added")
					}
				}
			}
			None
		} else {
			None
		}
		xnode
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class RuleSetModelPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlRuleSetModel =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlRuleSetModel]) {
				pmmlnode.asInstanceOf[PmmlRuleSetModel] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlRuleSetModel... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			/** Gather content from RuleSetModel attributes for later use by the Scala code generator */
			ctx.pmmlTerms("ModelName") = Some(node.modelName)
			ctx.pmmlTerms("FunctionName") = Some(node.functionName)
			
			Some(new xRuleSetModel(node.lineNumber, node.columnNumber, node.modelName, node.functionName, node.algorithmName, node.isScorable))
		} else {
			None
		}
		xnode
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class MiningSchemaPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlMiningSchema =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlMiningSchema]) {
				pmmlnode.asInstanceOf[PmmlMiningSchema] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlMiningSchema... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			node.Children.foreach((child) => {
				val ch = child.asInstanceOf[PmmlMiningField]
				dispatcher.dispatch(ch.qName, ch)
			})
			None
		} else {
			None
		}
		xnode
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class MiningFieldPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlMiningField =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlMiningField]) {
				pmmlnode.asInstanceOf[PmmlMiningField] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlMiningField... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
			top match {
			  case Some(top) => {
				  	var mf : xRuleSetModel = top.asInstanceOf[xRuleSetModel]
					mf.addMiningField (node.name , hlpMkMiningField(ctx, node)) 
			  }
			  case _ => None
			}
			None
		} else {
			None
		}
		xnode
	}

	def hlpMkMiningField(ctx : PmmlContext, d : PmmlMiningField) : xMiningField = {
		val name : String = d.name
		var fld : xMiningField = new xMiningField(d.lineNumber, d.columnNumber, d.name
											    , d.usageType
											    , d.optype
											    , 0.0
											    , d.outliers
											    , 0.0
											    , 0.0
											    , d.missingValueReplacement
											    , d.missingValueTreatment
											    , d.invalidValueTreatment)
		try {
			fld.Importance(d.importance.toDouble)
			fld.LowValue(d.lowValue.toDouble)
			fld.HighValue(d.highValue.toDouble)
		} catch {
			case _ : Throwable => ctx.logger.debug (s"Unable to coerce one or more of the mining field doubles... name = $name")
		}
	  	fld
	}
	

}


package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class SimpleRulePmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlSimpleRule =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlSimpleRule]) {
				pmmlnode.asInstanceOf[PmmlSimpleRule] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlSimpleRule... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			var rsm : Option[xRuleSetModel] = ctx.pmmlExecNodeStack.apply(1).asInstanceOf[Option[xRuleSetModel]]
			
			val id : Option[String] = Some(node.id)
			var rule : xSimpleRule = new xSimpleRule( node.lineNumber, node.columnNumber, id
														    , node.score
														    , 0.0 /** recordCount */
														    , 0.0 /** nbCorrect */
														    , 0.0 /** confidence */
														    , 0.0) /** weight */
			rsm match {
				case Some(rsm) => {		
					try {
						rule.RecordCount(node.recordCount.toDouble)
						rule.CorrectCount(node.nbCorrect.toDouble)
						rule.Confidence(node.confidence.toDouble)
						rule.Weight(node.weight.toDouble)
					} catch {
						case _ : Throwable => ctx.logger.debug (s"Unable to coerce one or more mining 'double' fields... name = $id")
					}
				
					rsm.addRule (rule) 
				}
				case _ => None
			}
			Some(rule)
		} else {
			None
		}
		xnode
	}	
}

	
package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class ScoreDistributionPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlScoreDistribution =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlScoreDistribution]) {
				pmmlnode.asInstanceOf[PmmlScoreDistribution] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlScoreDistribution... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
			top match {
			  case Some(top) => {
				  	var mf : xSimpleRule = ctx.pmmlExecNodeStack.top.asInstanceOf[xSimpleRule]
					var sd : xScoreDistribution = new xScoreDistribution(node.lineNumber, node.columnNumber, node.value
																, 0.0 /** recordCount */
															    , 0.0 /** confidence */
															    , 0.0) /** probability */
					try {
						sd.RecordCount(node.recordCount.toDouble)
						sd.Confidence(node.confidence.toDouble)
						sd.Probability(node.probability.toDouble)
					} catch {
					  case _ : Throwable => ctx.logger.debug ("Unable to coerce one or more score probablity Double values")
					}
						
					mf.addScoreDistribution(sd)
			  }
			  case _ => None
			}
			None
		} else {
			None
		}
		xnode
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class RuleSelectionMethodPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlRuleSelectionMethod =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlRuleSelectionMethod]) {
				pmmlnode.asInstanceOf[PmmlRuleSelectionMethod] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlRuleSelectionMethod... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.apply(1)   // xRuleSetModel is the grandparent
			top match {
			  case Some(top) => {
				  	var mf : xRuleSetModel = top.asInstanceOf[xRuleSetModel]
				  	var rsm : xRuleSelectionMethod = new xRuleSelectionMethod(node.lineNumber, node.columnNumber, node.criterion)
					mf.addRuleSetSelectionMethod(rsm)
			  }
			  case _ => None
			}
			None
		} else {
			None
		}
		xnode
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class SimplePredicatePmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlSimplePredicate =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlSimplePredicate]) {
				pmmlnode.asInstanceOf[PmmlSimplePredicate] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlSimplePredicate... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			Some(new xSimplePredicate(node.lineNumber, node.columnNumber, node.field, node.operator, node.value))
		} else {
			None
		}
		xnode
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class SimpleSetPredicatePmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlSimpleSetPredicate =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlSimpleSetPredicate]) {
				pmmlnode.asInstanceOf[PmmlSimpleSetPredicate] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlSimpleSetPredicate... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			Some(new xSimpleSetPredicate(node.lineNumber, node.columnNumber, node.field, node.booleanOperator))
		} else {
			None
		}
		xnode
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class CompoundPredicatePmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val node : PmmlCompoundPredicate =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlCompoundPredicate]) {
				pmmlnode.asInstanceOf[PmmlCompoundPredicate] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlCompoundPredicate... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (node != null) {
			Some(new xCompoundPredicate(node.lineNumber, node.columnNumber, node.booleanOperator))
		} else {
			None
		}
		xnode
	}	
}

package com.ligadata.pmml.compiler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.fatafat.metadata._

class ArrayPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with LogTrait {

	/**
	    Construct a PmmlExecNode appropriate for the PmmlNode supplied  In some cases no node is returned
	    (i.e., None).  This can happen when the PmmlNode content is subsumed by the parent node.  See DataField
	    handling for an example where the DataNode content is added to the parent DataDictionary.

 	    @param dispatcher: PmmlExecNodeGeneratorDispatch
 	    @param qName: String (the original element name from the PMML)
 	    @param pmmlnode:PmmlNode
 	    @return optionally an appropriate PmmlExecNode or None
	 */
	 
	def make(dispatcher : PmmlExecNodeGeneratorDispatch, qName : String, pmmlnode : PmmlNode) : Option[PmmlExecNode] = {
		val a : PmmlArray =  if (pmmlnode != null && pmmlnode.isInstanceOf[PmmlArray]) {
				pmmlnode.asInstanceOf[PmmlArray] 
			} else {
				if (pmmlnode != null) {
					PmmlError.logError(ctx, s"For $qName, expecting a PmmlArray... got a ${pmmlnode.getClass.getName}... check PmmlExecNode generator initialization")
				}
				null
			}
		val xnode : Option[PmmlExecNode] = if (a != null) {
			val valStr : String = a.valueString
			val elements = valStr.split(' ').toArray
			var trimmedElems  = elements.map (e => e.trim)
			val arrayPattern = """\"(.*)\"""".r
			
			val sanQuotesTrimmedElems = trimmedElems.map { x =>
				val matched = arrayPattern.findFirstMatchIn(x)
				matched match {
				  case Some(m) => m.group(1)
				  case _ => x
				}
			}
	 		
			var arr : xArray = new xArray(a.lineNumber, a.columnNumber, a.n, a.arrayType, sanQuotesTrimmedElems)
			Some(arr)
		} else {
			None
		}
		xnode
	}	
}

