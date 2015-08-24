package com.ligadata.MetadataAPI

import com.ligadata.Utils._

/**
 * This is an object that lists all the dependent jars that Kamanja depends upon to compile.
 * @author danielkozin
 */
object CompileDependencies {
  
  val jars: List[String] = List("guava-18.0.jar",
                            "log4j-1.2.17.jar",
                            "scala-library-2.10.4.jar",
                            "scala-reflect-2.10.4.jar",
                            "json4s-jackson_2.10-3.2.9.jar",
                            "jackson-annotations-2.3.0.jar",
                            "jackson-databind-2.3.1.jar",
                            "json4s-ast_2.10-3.2.9.jar",
                            "json4s-core_2.10-3.2.9.jar",
                            "json4s-native_2.10-3.2.9.jar",
                            "jackson-core-2.3.1.jar",
                            "pmmlruntime_2.10-1.0.jar",
                            "pmmludfs_2.10-1.0.jar",
                            "pmmlcompiler_2.10-1.0.jar",
                            "basetypes_2.10-0.1.0.jar",
                            "joda-convert-1.6.jar",
                            "joda-time-2.8.2.jar",
                            "bootstrap_2.10-1.0.jar",
                            "kamanjabase_2.10-1.0.jar",
                            "methodextractor_2.10-1.0.jar",
                            "messagedef_2.10-1.0.jar",
                            "basefunctions_2.10-0.1.0.jar",
                            "metadata_2.10-1.0.jar",
                            "exceptions_2.10-1.0.jar") 
    
                            
  /**
    *  Create a CLASSPATH based on the jars LIST.  Will Look for each jar file in the list in one of the JAR_PATHS 
    *  directory
    *  @return String 
    */
  def makeClassPathString: String = {
    
    // We can have multiple JAR_PATHS, so wee need to look for each .jar file in each of the specified directories
    // Take the first found jar file.
    val jarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet
    var cp = jars.map(jar => Utils.GetValidJarFile(jarPaths, jar)).mkString(":")
    
    println("CLASSPATH ====> \n"+ cp)
    
    cp
  }
}