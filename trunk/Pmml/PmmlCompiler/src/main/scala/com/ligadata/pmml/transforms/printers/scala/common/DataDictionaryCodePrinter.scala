package com.ligadata.pmml.compiler

import scala.collection.mutable._
import scala.math._
import scala.collection.immutable.StringLike
import scala.util.control.Breaks._
import com.ligadata.pmml.runtime._
import org.apache.log4j.Logger
import com.ligadata.fatafat.metadata._

class DataDictionaryCodePrinter(ctx : PmmlContext) {

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

		val xnode : xDataDictionary = node match {
			case Some(node) => {
				if (node.isInstanceOf[xDataDictionary]) node.asInstanceOf[xDataDictionary] else null
			}
			case _ => null
		}

		val printThis = if (xnode != null) {
			codeGenerator(xnode, generator, kind, traversalOrder)
		} else {
			if (node != null) {
				PmmlError.logError(ctx, s"For ${node.qName}, expecting an xDataDictionary... got a ${node.getClass.getName}... check CodePrinter dispatch map initialization")
			}
			""
		}
		printThis
	}
	
	private def asCode(xnode : xDataDictionary) : String = {
		val dictBuffer : StringBuilder = new StringBuilder()
		
		var serNo : Int = 0
		xnode.map.foreach {
		  	case ( _,dfld) => { 
		  		val dName = dfld.name
				val dNameFixer = "[-.]+".r
				val className : String = dNameFixer.replaceAllIn(dName,"_")

				val typ = PmmlTypes.scalaDataType(dfld.dataType)
		  		val lm : String = dfld.leftMargin
		  		val rm : String = dfld.rightMargin
		  		val cls : String = dfld.closure
		  		serNo += 1
		  		val valnm = s"dfoo$serNo"
		  		var valuesStr = NodePrinterHelpers.valuesHlp(dfld.values, valnm, "        ")
		  		dictBuffer.append(s"        var $valnm : ArrayBuffer[(String,String)] = $valuesStr\n")
		  		var dataFieldStr : String = s"        ctx.dDict += (${'"'}$dName${'"'} -> new DataField(${'"'}$dName${'"'}, ${'"'}$typ${'"'}, $valnm, ${'"'}$lm${'"'}, ${'"'}$rm${'"'}, ${'"'}$cls${'"'}))\n"
	    		dictBuffer.append(dataFieldStr)
		  	}
		}
		dictBuffer.toString
	}

	private def codeGenerator(node : xDataDictionary
							, generator : CodePrinterDispatch
							, kind : CodeFragment.Kind
							, traversalOrder : Traversal.Order) : String =
	{
		val dictDecl : String = order match {
			case Traversal.PREORDER => asCode(node)
			case _ => {
				PmmlError.logError(ctx, s"DataDictionary only supports Traversal.PREORDER")
				""
			}
		}
		dictDecl
	}



}