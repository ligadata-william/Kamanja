package com.ligadata.udf.extract

import scala.reflect.runtime.universe._
import scala.collection.mutable._
import scala.Symbol
import sys.process._
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.File
import java.io.PrintWriter
import java.net.URL
import java.net.URLClassLoader
import org.apache.log4j.Logger
import com.ligadata.pmml.udfs._


/**
	MethodExtract accepts an fully qualifed scala object name 
	and a namespace that its methods will have in the metadata manager.
	The method metadata for the methods in this object are extracted and 
	MetadataManager FunctionDef catalog method invocations are 
	formed.
	
	As part of the process, the types that are needed in order to catalog
	the methods in the object are also noted and commands are built for them
	as well.

	The current procedure is to save these invocations in a file
	that are then compiled and executed.

	Better approach is needed ordered worst to best...

	1) Use the MetadataAPI command line interface
	and send each FunctionDef spec (in the form it expects)
	to the API.
	2) dynamically compile a program that adds this information 
	directly to the MetadataApi
	3) The MetadataAPI implements the previous item in its 
	implementation.  The API would accept the fully qualified
	object path and possibly the jar in which it appears.  It would 
	dynamically load the class and pass the path name to the
	MethodExtract that would then generate the source code.
	The API would compile the returned source code and 
	execute it.	

 */



trait LogTrait {
    val loggerName = this.getClass.getName()
    val logger = Logger.getLogger(loggerName)
}


object MethodExtract extends App with LogTrait{ 

	def companion[T](name : String)(implicit man: Manifest[T]) : T = {
		Class.forName(name + "$").getField("MODULE$").get(man.runtimeClass).asInstanceOf[T] 
	}

	override def main (args : Array[String]) {
 	  
		var typeMap : Map[String,String] = Map[String,String]()
		var typeArray : ArrayBuffer[String] = ArrayBuffer[String]()
		
		val arglist = args.toList
		type OptionMap = scala.collection.mutable.Map[Symbol, String]
		def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
			list match {
		    	case Nil => map
		    	case "--object" :: value :: tail =>
		    						nextOption(map ++ Map('object -> value), tail)
		    	case "--namespace" :: value :: tail =>
		    						nextOption(map ++ Map('namespace -> value), tail)
		    	case "--exclude" :: value :: tail =>
		    						nextOption(map ++ Map('excludeList -> value), tail)
		    	case option :: tail => {
		    		logger.error("Unknown option " + option)
					val usageMsg : String = usage
					logger.error(s"$usageMsg")
		    		sys.exit(1)
		    	}
		        	 
		  }
		}
		
		val options = nextOption(Map(),arglist)
		val clsName = if (options.contains('object)) options.apply('object) else null 
		val namespace = if (options.contains('namespace)) options.apply('namespace) else null
		val excludeListStr = if (options.contains('excludeList)) options.apply('excludeList) else null
		var excludeList : Array[String] = null
		
		if (clsName == null || namespace == null) {
			val usageStr = usage
			logger.error("There must be a fully qualified class name supplied as an argument.")
			logger.error(usageStr)
			sys.exit(1)
		}
		val justObjectsFcns : String = clsName.split('.').last.trim
		logger.trace(s"Just catalog the functions found in $clsName")

		if (excludeListStr != null) {
			excludeList = excludeListStr.split(',').map(fn => fn.trim)
		}
		/** get the members */
		val mbrs = companion[com.ligadata.pmml.udfs.UdfBase](clsName).members
		
	  	val initFcnNameBuffer : StringBuilder = new StringBuilder()
		val initFcnNameNodes : Array[String] = clsName.split('.').map(node => node.trim)
	  	val initFcnBuffer : StringBuilder = new StringBuilder()
		initFcnNameNodes.addString(initFcnBuffer,"_")
		val initFcnName : String = initFcnBuffer.toString
		initFcnBuffer.append(s"\ndef init_$initFcnName {\n")

		/** filter out the methods and then only utilize those that are in the object ... ignore inherited trait methods */ 
		mbrs.filter(_.toString.startsWith("method")).foreach{ fcnMethod => 
			val fcnMethodObj = fcnMethod.asInstanceOf[MethodSymbol]
			val name = fcnMethodObj.name
			val returnType = fcnMethodObj.returnType
			val fullName = fcnMethodObj.fullName
			val typeSig = fcnMethodObj.typeSignature
			if (fullName.contains(justObjectsFcns)) {
				val nm : String = name.toString
				val fnm : String = fullName.toString
				val rt : String = returnType.toString
				val ts : String = typeSig.toString
				val notExcluded : Boolean = (excludeList.filter( exclnm => nm.contains(exclnm) || rt.contains(exclnm)).length == 0)
				if (notExcluded && ! nm.contains("$")) {		  
					val cmd : MethodCmd = new MethodCmd(logger, namespace, typeMap, typeArray, nm, fnm, rt, ts)
					if (cmd != null) {
						val cmdStr : String = cmd.toString
						initFcnBuffer.append(s"\t$cmdStr")
					}
				} else {
					logger.trace(s"Method $fullName returning $rt excluded")
				}
			}
		}
		initFcnBuffer.append(s"}\n")
		val fcnStr = initFcnBuffer.toString
		logger.info(s"$fcnStr")

		/** similarly, dump the type creation function */
	  	val initTypeBuffer : StringBuilder = new StringBuilder()
		initTypeBuffer.append(s"\ndef initTypesFor_$initFcnName {\n")
		/** print the type commands out in the order encountered s.t. types that refer to other (sub) types can find them in mgr */
		/** print out the scalars first ... then the others */
		typeArray.filter(_.contains("MakeScalar")).foreach( typeCmd => {
			initTypeBuffer.append(s"\t$typeCmd\n")
		})
		typeArray.filterNot(_.contains("MakeScalar")).foreach( typeCmd => {
			initTypeBuffer.append(s"\t$typeCmd\n")
		})
		initTypeBuffer.append(s"}\n")
		val typeCmdsStr = initTypeBuffer.toString
		logger.info(s"$typeCmdsStr")
		
		logger.trace("Complete!")
	}

	def usage : String = {
"""	
Usage: scala com.ligadata.udf.extract.MethodExtract --object <fully qualifed scala object name> 
											  --namespace <the onlep namespace> 
											  --exclude <a list of functions to ignore>
         where 	<fully qualifed scala object name> (required) is the scala object name that contains the 
					functions to be cataloged
				<the onlep namespace> in which these UDFs should be cataloged
				<a list of functions to ignore> is a comma delimited list of functions to ignore (OPTIONAL)
       NOTE: The jar containing this scala object must be on the class path.  Both arguments are mandatory.

"""
	}
}



 