package com.ligadata.pmml.compiler

import scala.collection.mutable._
import scala.math._
import scala.collection.immutable.StringLike
import scala.util.control.Breaks._
import com.ligadata.pmml.runtime._
import org.apache.log4j.Logger
import com.ligadata.fatafat.metadata._

class SimplePredicateCodePrinter(ctx : PmmlContext) {

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

		val xnode : xSimplePredicate = node match {
			case Some(node) => {
				if (node.isInstanceOf[xSimplePredicate]) node.asInstanceOf[xSimplePredicate] else null
			}
			case _ => null
		}

		val printThis = if (xnode != null) {
			codeGenerator(xnode, generator, kind, traversalOrder)
		} else {
			if (node != null) {
				PmmlError.logError(ctx, s"For ${node.qName}, expecting an xSimplePredicate... got a ${node.getClass.getName}... check CodePrinter dispatch map initialization")
			}
			""
		}
		printThis
	}
	

	private def codeGenerator(node : xSimplePredicate
							, generator : CodePrinterDispatch
							, kind : CodeFragment.Kind
							, traversalOrder : Traversal.Order) : String = 	{

	  	val fcnBuffer : StringBuilder = new StringBuilder()
		val simplePredStr : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				val opFcn : String = PmmlTypes.scalaBuiltinNameFcnSelector(node.operator)
				val sPred = s"$opFcn("
				fcnBuffer.append(sPred)
				
				val fldRef : String = PmmlExecNode.prepareFieldReference(ctx, node.field)
				
				/** getFieldType answers an array of the field types for the given field and a boolean indicating if it is a container type.*/
				val scalaTypes : Array[(String,Boolean, BaseTypeDef)] = ctx.getFieldType(node.field, false) 
				val scalaType : String = scalaTypes(0)._1
				val quotes : String = scalaType match {
					case "String" | "LocalDate" | "LocalTime" | "DateTime" => s"${'"'}"
					case _ => ""
				} 
				val fieldRefConstPair : String = s"$fldRef,$quotes$value$quotes"
				fcnBuffer.append(fieldRefConstPair)
		  		val closingParen : String = s")"
		  		fcnBuffer.append(closingParen)
		  		fcnBuffer.toString
			}
		}
		simplePredStr
	}
}

