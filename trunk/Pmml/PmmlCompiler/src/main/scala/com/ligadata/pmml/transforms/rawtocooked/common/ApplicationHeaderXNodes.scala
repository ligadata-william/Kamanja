package com.ligadata.pmml.transforms.rawtocooked.common

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import com.ligadata.kamanja.metadata.MdMgr._
import com.ligadata.kamanja.metadata._
import com.ligadata.pmml.compiler._
import com.ligadata.pmml.support._
import com.ligadata.pmml.traits._
import com.ligadata.pmml.syntaxtree.raw.common._
import com.ligadata.pmml.syntaxtree.cooked.common._


class HeaderPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with com.ligadata.pmml.compiler.LogTrait {

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

class ApplicationPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with com.ligadata.pmml.compiler.LogTrait {

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

