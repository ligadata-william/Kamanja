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

class ScoreDistributionPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with com.ligadata.pmml.compiler.LogTrait {

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

class IntervalPmmlExecNodeGenerator(val ctx : PmmlContext) extends PmmlExecNodeGenerator with com.ligadata.pmml.compiler.LogTrait {

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
					} else if (top.isInstanceOf[xDerivedField]) {
						  var df : xDerivedField = top.asInstanceOf[xDerivedField]
						  df.continuousConstraints(node.leftMargin, node.rightMargin, node.closure)
					} else {
						PmmlError.logError(ctx, s"For $qName... unable to subsume the interval into parent... parent is not known to contain an interval... parent = ${top.getClass.getName}")
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



