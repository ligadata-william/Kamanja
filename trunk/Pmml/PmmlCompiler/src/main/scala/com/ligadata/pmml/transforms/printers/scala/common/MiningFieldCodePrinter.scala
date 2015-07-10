package com.ligadata.pmml.compiler

import scala.collection.mutable._
import scala.math._
import scala.collection.immutable.StringLike
import scala.util.control.Breaks._
import com.ligadata.pmml.runtime._
import org.apache.log4j.Logger
import com.ligadata.fatafat.metadata._

class MiningFieldCodePrinter(ctx : PmmlContext) {

	/**
	 *  Answer a string (code representation) for the supplied node.
	 *  @param node the PmmlExecNode
	 *  @param the CodePrinterDispatch to use should recursion to child nodes be required.
 	    @param the kind of code fragment to generate...any 
 	    	{VARDECL, VALDECL, FUNCCALL, DERIVEDCLASS, RULECLASS, RULESETCLASS , MININGFIELD, MAPVALUE, AGGREGATE, USERFUNCTION}
	 *  @order the traversalOrder to traverse this node...any {INORDER, PREORDER, POSTORDER} 
	 *  
	 *  @return some string representation of this node
	 */
	def print(node : Option[PmmlExecNode]
			, generator : CodePrinterDispatch
			, kind : CodeFragment.Kind
			, traversalOrder : Traversal.Order) : String = {

		val xnode : xMiningField = node match {
			case Some(node) => {
				if (node.isInstanceOf[xMiningField]) node.asInstanceOf[xMiningField] else null
			}
			case _ => null
		}

		val printThis = if (xnode != null) {
			codeGenerator(xnode, generator, kind, traversalOrder)
		} else {
			if (node != null) {
				PmmlError.logError(ctx, s"For ${node.qName}, expecting an xMiningField... got a ${node.getClass.getName}... check CodePrinter dispatch map initialization")
			}
			""
		}
		printThis
	}
	

	private def codeGenerator(node : xMiningField
							, generator : CodePrinterDispatch
							, kind : CodeFragment.Kind
							, traversalOrder : Traversal.Order) : String = 	{

		val fld : String = traversalOrder match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => { /** arguments for the add mining field function in RuleSetModel */
			    kind match {
			      case CodeFragment.MININGFIELD => {
			    	  s"ruleSet.AddMiningField(${'"'}${node.name}${'"'}, new MiningField(${'"'}${node.name}${'"'},${'"'}${node.usageType}${'"'},${'"'}${node.opType}${'"'},${'"'}${node.importance}${'"'},${'"'}${node.outliers}${'"'},${'"'}${node.lowValue}${'"'},${'"'}${node.highValue}{'"'},${'"'}${node.missingValueReplacement}${'"'},${'"'}${node.missingValueTreatment}${'"'},${'"'}${node.invalidValueTreatment}${'"'}))\n"
			      }
			      case _ => {
			    	  PmmlError.logError(ctx, "MiningField .. Only CodeFragment.MININGFIELD is supported")
			    	  ""
			      }
			    }
			}
		}
		fld
	}
}

