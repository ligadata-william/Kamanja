package com.ligadata.pmml.compiler

import scala.collection.mutable._
import scala.math._
import scala.collection.immutable.StringLike
import scala.util.control.Breaks._
import com.ligadata.pmml.runtime._
import org.apache.log4j.Logger
import com.ligadata.fatafat.metadata._

class ArrayCodePrinter(ctx : PmmlContext) {

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

		val xnode : xArray = node match {
			case Some(node) => {
				if (node.isInstanceOf[xArray]) node.asInstanceOf[xArray] else null
			}
			case _ => null
		}

		val printThis = if (xnode != null) {
			codeGenerator(xnode, generator, kind, traversalOrder)
		} else {
			if (node != null) {
				PmmlError.logError(ctx, s"For ${node.qName}, expecting an xArray... got a ${node.getClass.getName}... check CodePrinter dispatch map initialization")
			}
			""
		}
		printThis
	}
	

	private def codeGenerator(node : xArray
							, generator : CodePrinterDispatch
							, kind : CodeFragment.Kind
							, traversalOrder : Traversal.Order) : String = 	{

		val arrayStr : String = order match {
			case Traversal.INORDER => { "INORDER Not Used" }
			case Traversal.POSTORDER => { "POSTORDER Not Used" }
			case Traversal.PREORDER => {			  
				val aBuffer : StringBuilder = new StringBuilder()
				val arrTyp : String = PmmlTypes.scalaDataType(node.arrayType)
				val quotes : String = arrTyp match {
					case "String" | "LocalDate" | "LocalTime" | "DateTime" => s"${'"'}"
					case _ => ""
				} 
				var i = 0
				for (itm <- node.array) { 
					i += 1
					val comma : String = if (i < node.array.length) ", " else ""
					aBuffer.append(s"$quotes$itm$quotes$comma")
				}
				
				kind match {
				  case CodeFragment.FUNCCALL =>	"List(" + aBuffer.toString +")"		   
				  case CodeFragment.VARDECL | CodeFragment.VALDECL => s"ArrayBuffer[$arrTyp]($aBuffer.toString)"			  
				  case _ => "unsupported code fragment kind requested"				  
				}
			}
		}
		arrayStr
	}
}

