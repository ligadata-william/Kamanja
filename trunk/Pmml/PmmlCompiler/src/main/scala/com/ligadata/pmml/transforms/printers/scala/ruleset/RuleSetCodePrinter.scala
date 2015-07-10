package com.ligadata.pmml.compiler

import scala.collection.mutable._
import scala.math._
import scala.collection.immutable.StringLike
import scala.util.control.Breaks._
import com.ligadata.pmml.runtime._
import org.apache.log4j.Logger
import com.ligadata.fatafat.metadata._

class RuleSetCodePrinter(ctx : PmmlContext) {

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

		val xnode : xRuleSet = node match {
			case Some(node) => {
				if (node.isInstanceOf[xRuleSet]) node.asInstanceOf[xRuleSet] else null
			}
			case _ => null
		}

		val printThis = if (xnode != null) {
			codeGenerator(xnode, generator, kind, traversalOrder)
		} else {
			if (node != null) {
				PmmlError.logError(ctx, s"For ${node.qName}, expecting an xRuleSet... got a ${node.getClass.getName}... check CodePrinter dispatch map initialization")
			}
			""
		}
		printThis
	}
	

	private def codeGenerator(node : xRuleSet
							, generator : CodePrinterDispatch
							, kind : CodeFragment.Kind
							, traversalOrder : Traversal.Order) : String = 	{

		val rs : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				ctx.logger.trace("xRuleSet is not used for code generation... the important information it possesses has been given to its parent xRuleSetModel")
				ctx.logger.trace("xRuleSet children are important, however.  They are available through an instance of this class.")

				val clsBuffer : StringBuilder = new StringBuilder()
				node.Children.foreach((child) => {
					generator.generateCode1(Some(child), clsBuffer, generator, kind)
				})
			   	clsBuffer.toString
			}
		}
		rs
	}

}

