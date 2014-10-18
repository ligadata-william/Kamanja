package com.ligadata.Compiler

import scala.collection.mutable._
import scala.math._
import scala.collection.immutable.StringLike
import scala.util.control.Breaks._
import org.apache.log4j.Logger
import com.ligadata.olep.metadata._

/** 
 *	Print an iterable function.  General print form is 
 * 
 * 		collectionFromArg1.<fcnName>( mbr => {
 *   		mbrFunction(arg1.arg2,...)
 *   	})
 *    
 *  except all on one line.
 *  
 *  For those iterable functions without an iterable function ...
 *  
 *  	collectionFromArg1.<fcnName>( mbr => {
 *   		(arg1.arg2,...)
 *   	})
 *    
 *  If only one argument, the parentheses are omitted
 *  
 *  @param fcnName - the top level function name (before possible translation)
 *  @param node - the original syntax tree node
 *  @param ctx - the PmmlContext for this compilation
 *  @param generator - the print navigation control for printing syntax trees
 *  @param order - the child navigation order supplied by the generator responsible for orchestration of the print
 *  @param iterableFcn - the metadata that describes the selected iterable function (this is the outer function)
 *  @param iterableArgTypes - the type information collected for the iterableFcn (and its first arg, the collection)
 *  @param mbrFcn - the member function metadata for the function that will operate on the collection's members (this may be null)
 *  @param mbrArgTypes - the member function's argument types
 *  @param collectionType - the type of the first argument of the apply function - some kind of iterable
 *  @param collectionsElementTypes - an array of base types that describe the types used to describe the collection content
 *  	(e.g., the element type for a List, the key and value for a map, etc.)
 */
class IterableFcnPrinter(val fcnName : String
						, val node : xApply
					    , val ctx : PmmlContext
					    , val generator : PmmlModelGenerator
					    , val generate : CodeFragment.Kind
					    , val order : Traversal.Order
					    , val fcnTypeInfo :  FcnTypeInfo) extends LogTrait {
					    
	/** 
	 *  Iterable functions are comprised of three pieces:
	 *  
	 *  1) The receiver Iterable collection and the scala function that corresponds to the pmml apply name along with
	 *  	the mbr variable to used by the member function or projection. For example,
	 *  	
	 *   		coll.map(itm =>
	 *   
	 *  2) The member function that uses the member variable.  For example a "Between" function:
	 *  
	 *  		{ Pmml.Between(itm.intfield, 2012, 2014, true) }
	 *    
	 *  3) The coercion or casting of the result to the appropriate type.. say from an Array[Any] that executed
	 *  	to the array of the elements that are returned by the Between function variant selected:
	 *   
	 *   		).asInstanceOf[Array[Int]]
	 */
	def print(fcnBuffer : StringBuilder) : Unit = {
  
	  
		val iterablePart : String = iterablePrint
		val mbrFcnPart : String = mbrFunctionPrint
		val castPrint : String = castResult
		
		//fcnBuffer.append(s"$iterablePart${'{'} $mbrFcnPart ${'}'}$castPrint")
		fcnBuffer.append(s"$iterablePart${'{'} $mbrFcnPart ${'}'})")
		
		val fcnUse : String = fcnBuffer.toString
		logger.trace(s"IterableFcnPrint... fcn use : $fcnUse")
		val huh : String = "huh" // debugging rest stop 
	}

	/**
	 * 	Print the iterable argument (the receiver) and its function with the mbr variable expression 
	 *  
	 *  e.g., "coll.map(itm =>"
	 */
	private def iterablePrint : String = {
		var scalaFcnName : String = PmmlTypes.scalaNameForIterableFcnName(fcnName)
		if (scalaFcnName == null) {
			scalaFcnName = node.function
		}
		val iterArgBuffer : StringBuilder = new StringBuilder
		val iterChild : PmmlExecNode = node.Children.head
		generator.generateCode1(Some(iterChild), iterArgBuffer, generator, CodeFragment.FUNCCALL)
		iterArgBuffer.append(s".$scalaFcnName( mbr => ")
		
		iterArgBuffer.toString
	}
	
	/**
	 * 	Print the member function that uses the member variable.  For example a "Between" function:
	 *  
	 *  		{ Pmml.Between(itm.intfield, 2012, 2014, true) }
	 *  
	 *  It is also possible that there is no member function, only field references or constant expressions
	 *  This is common with the 'map' function when a simple projection is done on one or more fields.
	 *  If more than one field is found in this situation, a tuple is returned with for the mentioned fields
	 *  and constants. 
	 *  			between(Admtng_Icd9_Dgns_Cd"49300""49392"true)
	 *  @return string representation of the member function or projection of the variables
	 */
	private def mbrFunctionPrint : String =  {
		val mbrFcnBuffer : StringBuilder = new StringBuilder
		val (firstElemArgIdx, lastElemArgIdx) : (Int,Int) = fcnTypeInfo.elemFcnArgRange
		val hasMbrFcn : Boolean = (fcnTypeInfo.mbrFcn != null)
		if (hasMbrFcn) {
			mbrFcnBuffer.append(s"${fcnTypeInfo.mbrFcn.physicalName}(")
			if ((lastElemArgIdx - firstElemArgIdx) >= 0) { 
				for (i <- firstElemArgIdx to lastElemArgIdx) {
					val child : PmmlExecNode = node.Children.apply(i)
					generator.generateCode1(Some(child), mbrFcnBuffer, generator, CodeFragment.FUNCCALL)
					if (i < lastElemArgIdx) {
						mbrFcnBuffer.append(", ")
					}
				}
			} else {
				logger.warn(s"There are no arguments for this function ... ${fcnTypeInfo.mbrFcn.Name}")
			}
			mbrFcnBuffer.append(")")
		} else {
			val mbrFcnNm : String = if (fcnTypeInfo.mbrFcn != null) fcnTypeInfo.mbrFcn.Name else ""
			if ((lastElemArgIdx - firstElemArgIdx) >= 0) {
				val howManyInProjection = (lastElemArgIdx - firstElemArgIdx) + 1

				if (howManyInProjection > 1) {
					mbrFcnBuffer.append("(")
				}
				
				for (i <- firstElemArgIdx to lastElemArgIdx) {
					val child : PmmlExecNode = node.Children.apply(i)
					generator.generateCode1(Some(child), mbrFcnBuffer, generator, CodeFragment.FUNCCALL)
					if (i < lastElemArgIdx) {
						mbrFcnBuffer.append(", ")
					}
				}
				
				if (howManyInProjection > 1) {
					mbrFcnBuffer.append(")")
				}
				
			}  else {
				logger.error(s"Not only are there There are no arguments.. is there no function too? ... '$mbrFcnNm'... investigate this.")
			}
		}
		mbrFcnBuffer.toString
	}
	
	private def castResult : String = {
	  
		val tc : TypeCollector = new TypeCollector(fcnTypeInfo)
		val fcnReturnType : String = tc.determineReturnType(node)
		val buffer : StringBuilder = new StringBuilder
		buffer.append(s").asInstanceOf[$fcnReturnType]")
		val castStr : String = buffer.toString
		castStr
	}
		

}

