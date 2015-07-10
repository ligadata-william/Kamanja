package com.ligadata.pmml.compiler

import scala.collection.mutable._
import scala.math._
import scala.collection.immutable.StringLike
import scala.util.control.Breaks._
import com.ligadata.pmml.runtime._
import org.apache.log4j.Logger
import com.ligadata.fatafat.metadata._

class CompoundPredicateCodePrinter(ctx : PmmlContext) {

	/**
	 *  Answer a string (code representation) for the supplied node.
	 *  @param node the PmmlExecNode
	 *  @param the CodePrinterDispatch to use should recursion to child nodes be required.
 	 *  @param the kind of code fragment to generate...any 
 	 *   	{VARDECL, VALDECL, FUNCCALL, DERIVEDCLASS, RULECLASS, RULESETCLASS , MININGFIELD, MAPVALUE, AGGREGATE, USERFUNCTION}
	 *  @order the traversalOrder to traverse this node...any {INORDER, PREORDER, POSTORDER} 
	 *  
	 *  @return some string representation of this node
	 */
	def print(node : Option[PmmlExecNode]
			, generator : CodePrinterDispatch
			, kind : CodeFragment.Kind
			, traversalOrder : Traversal.Order) : String = {

		val xnode : xCompoundPredicate = node match {
			case Some(node) => {
				if (node.isInstanceOf[xCompoundPredicate]) node.asInstanceOf[xCompoundPredicate] else null
			}
			case _ => null
		}

		val printThis = if (xnode != null) {
			codeGenerator(xnode, generator, kind, traversalOrder)
		} else {
			if (node != null) {
				PmmlError.logError(ctx, s"For ${node.qName}, expecting an xCompoundPredicate... got a ${node.getClass.getName}... check CodePrinter dispatch map initialization")
			}
			""
		}
		printThis
	}
	

	private def codeGenerator(node : xCompoundPredicate
							, generator : CodePrinterDispatch
							, kind : CodeFragment.Kind
							, traversalOrder : Traversal.Order) : String = 	{

	  	val fcnBuffer : StringBuilder = new StringBuilder()
		val compoundPredStr : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				val boolOpFcn : String = PmmlTypes.scalaBuiltinNameFcnSelector(booleanOperator)
				val isShortCircuitOp = (boolOpFcn == "or" || boolOpFcn == "and")
				if (isShortCircuitOp) {
					val op : String = if (boolOpFcn == "or") "||" else "&&"
			  		fcnBuffer.append("(")
					NodePrinterHelpers.andOrFcnPrint(op
													, node
												    , ctx
												    , generator
												    , fragmentKind
												    , Traversal.INORDER
												    , fcnBuffer
												    , null)		    
			  		fcnBuffer.append(")")
			  		fcnBuffer.toString
				} else {
					val cPred = s"$boolOpFcn("
					fcnBuffer.append(cPred)
					var i : Int = 0
	
			  		Children.foreach((child) => {
			  			i += 1
				  		generator.generate(Some(child), fcnBuffer, CodeFragment.FUNCCALL)
				  		if (i < Children.length) fcnBuffer.append(", ")
			  		})
	
			  		val closingParen : String = s")"
			  		fcnBuffer.append(closingParen)
			  		fcnBuffer.toString
				}
			}
		}
		compoundPredStr
	}
}

