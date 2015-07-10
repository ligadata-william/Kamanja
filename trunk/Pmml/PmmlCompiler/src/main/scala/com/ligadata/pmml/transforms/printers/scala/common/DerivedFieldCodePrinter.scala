package com.ligadata.pmml.compiler

import scala.collection.mutable._
import scala.math._
import scala.collection.immutable.StringLike
import scala.util.control.Breaks._
import com.ligadata.pmml.runtime._
import org.apache.log4j.Logger
import com.ligadata.fatafat.metadata._

class DerivedFieldCodePrinter(ctx : PmmlContext) {

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

		val xnode : xDerivedField = node match {
			case Some(node) => {
				if (node.isInstanceOf[xDerivedField]) node.asInstanceOf[xDerivedField] else null
			}
			case _ => null
		}

		val printThis = if (xnode != null) {
			codeGenerator(xnode, generator, kind, traversalOrder)
		} else {
			if (node != null) {
				PmmlError.logError(ctx, s"For ${node.qName}, expecting an xDerivedField... got a ${node.getClass.getName}... check CodePrinter dispatch map initialization")
			}
			""
		}
		printThis
	}
	

	private def codeGenerator(node : DerivedField
							, generator : CodePrinterDispatch
							, kind : CodeFragment.Kind
							, traversalOrder : Traversal.Order) : String = 	{
		ctx.elementStack.push(node) /** track the element as it is processed */
		
		val typeStr : String = PmmlTypes.scalaDataType(node.dataType)
		
		val fieldDecl : String = traversalOrder match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				kind match {
					case CodeFragment.DERIVEDCLASS => {
						ctx.fcnTypeInfoStack.clear /** if there is cruft, kill it now as we prep for dive into derived field and its function hierarchy*/
						NodePrinterHelpers.derivedFieldFcnHelper(node, ctx, generator, kind, order)
					}
					case _ => { 
						PmmlError.logError(ctx, "DerivedField node - unsupported CodeFragment.Kind") 
						""
					}
				}
			}
		}
		ctx.elementStack.pop 	/** done discard the current element */
		
		fieldDecl
	}
}

