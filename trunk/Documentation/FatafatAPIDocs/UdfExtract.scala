package com.ligadata.udf.extract

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{ universe => ru }
import scala.collection.mutable._
import scala.collection.immutable.{Set, TreeMap}
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
import com.ligadata.olep.metadata._
import scala.util.parsing.json.JSON
import scala.util.parsing.json.{JSONObject, JSONArray}
import org.json4s._
import org.json4s.JsonDSL._
import com.ligadata.Serialize._
import java.net.URLClassLoader


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
 	  
		var typeMap : Map[String,BaseElemDef] = Map[String,BaseElemDef]()
		var typeArray : ArrayBuffer[BaseElemDef] = ArrayBuffer[BaseElemDef]()
		var funcDefArgs : ArrayBuffer[FuncDefArgs] = ArrayBuffer[FuncDefArgs]()
		
		val arglist = args.toList
		type OptionMap = scala.collection.mutable.Map[Symbol, String]
		def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
			list match {
		    	case Nil => map
		    	case "--object" :: value :: tail =>
		    						nextOption(map ++ Map('object -> value), tail)
		    	case "--cp" :: value :: tail =>
		    						nextOption(map ++ Map('cp -> value), tail)
		    	case "--namespace" :: value :: tail =>
		    						nextOption(map ++ Map('namespace -> value), tail)
		    	case "--exclude" :: value :: tail =>
		    						nextOption(map ++ Map('excludeList -> value), tail)
		    	case "--versionNumber" :: value :: tail =>
		    						nextOption(map ++ Map('versionNumber -> value), tail)
		    	case "--deps" :: value :: tail =>
		    						nextOption(map ++ Map('deps -> value), tail)
		    	case "--typeDefsPath" :: value :: tail =>
		    						nextOption(map ++ Map('typeDefsPath -> value), tail)
		    	case "--fcnDefsPath" :: value :: tail =>
		    						nextOption(map ++ Map('fcnDefsPath -> value), tail)
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
		val classPath = if (options.contains('cp)) options.apply('cp) else null
		val namespace = if (options.contains('namespace)) options.apply('namespace) else null
		val excludeListStr = if (options.contains('excludeList)) options.apply('excludeList) else null
		var excludeList : Array[String] = null
		val versionNumberStr = if (options.contains('versionNumber)) options.apply('versionNumber) else null
		var versionNumber : Int = 100
		try {
			if (versionNumberStr != null) versionNumber = versionNumberStr.toInt
		} catch {
		  case _:Throwable => versionNumber = 100
		}
		val depsIn = if (options.contains('deps)) options.apply('deps) else null
		val typedefPath = if (options.contains('typeDefsPath)) options.apply('typeDefsPath) else null
		val fcndefPath = if (options.contains('fcnDefsPath)) options.apply('fcnDefsPath) else null
		
		if (clsName == null || classPath == null || namespace == null || depsIn == null || typedefPath == null || fcndefPath == null) {
			val usageStr = usage
			logger.error("Missing arguments...")
			logger.error(usageStr)
			sys.exit(1)
		}
		/**  
		 *  Split the deps ... the first element and the rest... the head element has the
		 *  jar name where this lib lives (assuming that the sbtProjDependencies.scala script was used
		 *  to prepare the dependencies).  For the jar, we only need the jar's name... strip the path
		 */
		val depsArr : Array[String] = depsIn.split(',').map(_.trim)
		val jarName = depsArr.head.split('/').last
		val deps : Array[String] = depsArr.tail
		
		/** prepare the class path array so that the udf can be loaded (and its udfs introspected) */
		val cp : Array[String] = classPath.split(':').map(_.trim)
		val udfLoaderInfo = new UdfExtractLoaderInfo
		LoadJarIfNeeded(cp, udfLoaderInfo.loadedJars, udfLoaderInfo.loader)
		
		val justObjectsFcns : String = clsName.split('.').last.trim
		//logger.trace(s"Just catalog the functions found in $clsName")

		if (excludeListStr != null) {
			excludeList = excludeListStr.split(',').map(fn => fn.trim)
		} else {
			excludeList = Array[String]()
		}
		/** get the members */
		val mbrs = companion[com.ligadata.pmml.udfs.UdfBase](clsName).members
		
	  	val initFcnNameBuffer : StringBuilder = new StringBuilder()
		val initFcnNameNodes : Array[String] = clsName.split('.').map(node => node.trim)
	  	val initFcnBuffer : StringBuilder = new StringBuilder()
		initFcnNameNodes.addString(initFcnBuffer,"_")
		val initFcnName : String = initFcnBuffer.toString
		initFcnBuffer.append(s"\ndef init_$initFcnName {\n")
				
		val mgr : MdMgr = InitializeMdMgr

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
					val cmd : MethodCmd = new MethodCmd(mgr, versionNumber, namespace, typeMap, typeArray, nm, fnm, rt, ts)
					if (cmd != null) {
						val (funcInfo,cmdStr) : (FuncDefArgs,String) = cmd.makeFuncDef
						if (funcInfo != null) {
							funcDefArgs += funcInfo
						}
						initFcnBuffer.append(s"\t$cmdStr")
					}
				} else {
					logger.trace(s"Method $fullName returning $rt excluded")
				}
			}
		}
		initFcnBuffer.append(s"}\n")
		val fcnStr = initFcnBuffer.toString
		//logger.info(s"$fcnStr")

		/** Serialize the types that were generated during the UDF lib introspection and print them to stdout */
		val sortedTypeMap : LinkedHashMap[String, BaseElemDef] = LinkedHashMap(typeMap.toSeq.sortBy(_._1):_*)
		//sortedTypeMap.keys.toArray.foreach( typ => println(typ))
		//println
		
		
		/**
		 * What is above (the sorted map) is fine for understanding what types are actually needed by the 
		 * UDF lib supplied, however it cannot be emitted that way for intake by the MetadataAPI.  The types build
		 * upon themselves with the inner types of an array of array of tupleN having the tupleN emitted first.
		 * 
		 * Therefore the typeArray is iterated.  To avoid duplicate emissions, a set is used to track what has been 
		 * emitted.  Only one type with a given name should be emitted.  These are collected in the emitTheseTypes array.
		 */
		var trackEmission : scala.collection.mutable.Set[String] = scala.collection.mutable.Set[String]()
		var emitTheseTypes : ArrayBuffer[BaseElemDef] = ArrayBuffer[BaseElemDef]()
		var i : Int = 0
		typeArray.foreach( typ => 
		  	if (! trackEmission.contains(typ.Name)) {
		  		i += 1
		  		emitTheseTypes += typ
  				trackEmission += typ.Name
		  	})
		/** println(s"There are $i unique types with ${typeArray.size} instances") */
		
		/** Serialize and write json type definitions to file */
		val typesAsJson : String = JsonSerializer.SerializeObjectListToJson("Types",emitTheseTypes.toArray)	
		writeFile(typesAsJson, typedefPath)

		/** Create the FunctionDef objects to be serialized by combining the FuncDefArgs collected with the version and deps info */
		val features: scala.collection.mutable.Set[FcnMacroAttr.Feature] = null
		val funcDefs : Array[FunctionDef] = funcDefArgs.toArray.map( fArgs => {
			
			mgr.MakeFunc(fArgs.namespace
						, fArgs.fcnName
						, fArgs.physicalName
						, (fArgs.returnNmSpc, fArgs.returnTypeName)
						, fArgs.argTriples.toList
						, features
						, fArgs.versionNo
						, jarName
						, deps)
		})
		
		/** Serialize and write json function definitions to file */
		val functionsAsJson : String = JsonSerializer.SerializeObjectListToJson("Functions",funcDefs.toArray)		
		writeFile(functionsAsJson, fcndefPath)
		
		//logger.trace("Complete!")
	}

	def usage : String = {
"""	
Collect the function definitions from the supplied object that is found in the supplied class path.  The classpath contains
the object and any supporting libraries it might require.  The supplied namespace and version number will be used for the namespace 
and version values respectively for all function definitions produced.  The deps string contains the same jars as the classpath argument
but without paths.  These are used to create the deps values for the function definitions.  The results for the types and function
definitions are written to the supplied typeDefsPath and fcnDefsPath respectively.
	  
Usage: scala com.ligadata.udf.extract.MethodExtract --object <fully qualifed scala object name> 
													--cp <classpath>
                                                    --namespace <the onlep namespace> 
                                                    --exclude <a list of functions to ignore>
                                                    --versionNumber <N>
                                                    --deps <jar dependencies comma delimited list>
													--typeDefsPath <types file path>
													--fcnDefsPath <function definition file path>
         where 	<fully qualifed scala object name> (required) is the scala object name that contains the 
					functions to be cataloged
				<the onlep namespace> in which these UDFs should be cataloged
				<a list of functions to ignore> is a comma delimited list of functions to ignore (OPTIONAL)
				<N> is the version number to be assigned to all functions in the UDF lib.  It should be greater than
					the prior versions that may have been for a prior version of the UDFs.
				<jar dependencies comma delimited list> this is the list of jars that this UDF lib (jar) depends upon.
					A complete dependency list can be obtained by running the sbtProjDependencies.scala script.
				<types file path> the file path that will receive any type definitions that may be needed to catalog the functions
					being collected
				<function definition file path> the file path that will receive the function definitions
				
	  
       NOTE: The jar containing this scala object and jars upon which it depends should be on the class path.  Except for
	   the exclusion list, all arguments are mandatory.

"""
	}
	
	/** 
	 *  Retrieve a fresh and empty MdMgr from MdMgr object.  Seed it with some essential scalars (and essential system containers) 
	 *  to start the ball rolling.
	 *  
	 *  @return nearly empty MdMgr... seeded with essential scalars
	 */
	def InitializeMdMgr : MdMgr = {
		val versionNumber : Int = 1
		val mgr : MdMgr = MdMgr.GetMdMgr

		/** seed essential types */
		mgr.AddScalar(MdMgr.sysNS, "Any", ObjType.tAny, "Any", versionNumber, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.AnyImpl")
		mgr.AddScalar(MdMgr.sysNS, "String", ObjType.tString, "String", versionNumber, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.StringImpl")
		mgr.AddScalar(MdMgr.sysNS, "Int", ObjType.tInt, "Int", versionNumber, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.IntImpl")
		mgr.AddScalar(MdMgr.sysNS, "Integer", ObjType.tInt, "Int", versionNumber, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.IntImpl")
		mgr.AddScalar(MdMgr.sysNS, "Long", ObjType.tLong, "Long", versionNumber, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.LongImpl")
		mgr.AddScalar(MdMgr.sysNS, "Boolean", ObjType.tBoolean, "Boolean", versionNumber, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.BoolImpl")
		mgr.AddScalar(MdMgr.sysNS, "Bool", ObjType.tBoolean, "Boolean", versionNumber, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.BoolImpl")
		mgr.AddScalar(MdMgr.sysNS, "Double", ObjType.tDouble, "Double", versionNumber, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.DoubleImpl")
		mgr.AddScalar(MdMgr.sysNS, "Float", ObjType.tFloat, "Float", versionNumber, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.FloatImpl")
		mgr.AddScalar(MdMgr.sysNS, "Char", ObjType.tChar, "Char", versionNumber, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.CharImpl")

		mgr.AddFixedContainer(MdMgr.sysNS
						    , "Context"
						    , "com.ligadata.Pmml.Runtime.Context"
					  		, List()) 
		 		  		
		mgr.AddFixedContainer(MdMgr.sysNS
						    , "EnvContext"
						    , "com.ligadata.OnLEPBase.EnvContext"
					  		, List()) 
		 		  		
		mgr.AddFixedContainer(MdMgr.sysNS
							    , "BaseMsg"
							    , "com.ligadata.OnLEPBase.BaseMsg"
						  		, List()) 
		  		
		mgr.AddFixedContainer(MdMgr.sysNS
							    , "BaseContainer"
							    , "com.ligadata.OnLEPBase.BaseContainer"
						  		, List()) 		
				  		
		mgr.AddFixedContainer(MdMgr.sysNS
							    , "MessageContainerBase"
							    , "com.ligadata.OnLEPBase.MessageContainerBase"
						  		, List()) 		

		mgr
	}
	
	/**
	 * 	Write the supplied text to the supplied path.
	 *  
	 *   @param text a string with either type or function definition declarations as its content
	 *   @param targetPath the file path that will receive the text. 
	 */
	
	private def writeFile(text : String, targetPath : String) {
		val file = new File(targetPath);
		val bufferedWriter = new BufferedWriter(new FileWriter(file))
		bufferedWriter.write(text)
		bufferedWriter.close
	}
	

	/**
	 * 	Load any jars that were supplied on the command line in the --cp argument
	 *  
	 *  @param jars the --cp values supplied on command line
	 *  @loadedJars a TreeSet for avoiding multiple loads of same jar 
	 *  @param loader the class loader to use 
	 *  
	 *  @return true if loading was successful, else false
	 */

	private def LoadJarIfNeeded(jars : Array[String], loadedJars: TreeSet[String], loader: UDFClassLoader): Boolean = {

	    // Loading all jars
	    for (j <- jars) {
	      //logger.info("Processing Jar " + j.trim)
	      val fl = new File(j.trim)
	      if (fl.exists) {
	        try {
	          if (loadedJars(fl.getPath())) {
	            //logger.info("Jar " + j.trim + " already loaded to class path.")
	          } else {
	            loader.addURL(fl.toURI().toURL())
	            //logger.info("Jar " + j.trim + " added to class path.")
	            loadedJars += fl.getPath()
	          }
	        } catch {
	          case e: Exception => {
	            logger.error("Jar " + j.trim + " failed added to class path. Message: " + e.getMessage)
	            return false
	          }
	        }
	      } else {
	        logger.error("Jar " + j.trim + " not found")
	        return false
	      }
	    }
	
	    true
	}
	

}


class UDFClassLoader(urls: Array[URL], parent: ClassLoader) extends URLClassLoader(urls, parent) {
	override def addURL(url: URL) {
		super.addURL(url)
	}
} 


class UdfExtractLoaderInfo {
	// Class loader
	val loader: UDFClassLoader = new UDFClassLoader(ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader].getURLs(), getClass().getClassLoader())
	// Loaded jars
	val loadedJars: TreeSet[String] = new TreeSet[String];
	// Get a mirror for reflection
	val mirror: scala.reflect.runtime.universe.Mirror = ru.runtimeMirror(loader)
}




 