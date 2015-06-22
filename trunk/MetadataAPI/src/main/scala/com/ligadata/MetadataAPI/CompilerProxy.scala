package com.ligadata.MetadataAPI


import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader
import javax.xml.parsers.SAXParserFactory
import org.xml.sax.InputSource
import org.xml.sax.XMLReader
import scala.collection.mutable._
import java.io.BufferedWriter
import java.io.FileWriter
import sys.process._
import java.io.PrintWriter
import org.apache.log4j._
import com.ligadata.fatafat.metadata._
import com.ligadata._
import com.ligadata.messagedef._
import com.ligadata.Compiler._
import com.ligadata.fatafat.metadata.ObjFormatType._
import com.ligadata.Serialize._
import com.ligadata.Exceptions._

object JarPathsUtils{
  def GetValidJarFile(jarPaths: collection.immutable.Set[String], jarName: String): String = {
    if (jarPaths == null) return jarName // Returning base jarName if no jarpaths found
    jarPaths.foreach(jPath => {
      val fl = new File(jPath + "/" + jarName)
      if (fl.exists) {
        return fl.getPath
      }
    })
    return jarName // Returning base jarName if not found in jar paths
  }
}

// CompilerProxy has utility functions to:
// Call MessageDefinitionCompiler, 
// Call PmmlCompiler, 
// Generate jar files out of output of above compilers
// Persist model definitions and corresponding jar files in Metadata Mgr
// Persist message definitions, and corresponding jar files in Metadata Mgr
class CompilerProxy{

  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)

  lazy val compiler_work_dir = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("COMPILER_WORK_DIR")
  println(compiler_work_dir)

  def setLoggerLevel(level: Level){
    logger.setLevel(level);
  }

  def dumpStrTextToFile(strText : String, filePath : String) {
    val file = new File(filePath);
    val bufferedWriter = new BufferedWriter(new FileWriter(file))
    bufferedWriter.write(strText)
    bufferedWriter.close
  }

  def writeSrcFile(scalaGeneratedCode : String, scalaSrcTargetPath : String) {
    val file = new File(scalaSrcTargetPath);
    val bufferedWriter = new BufferedWriter(new FileWriter(file))
    bufferedWriter.write(scalaGeneratedCode)
    bufferedWriter.close
  }

  def createScalaFile(targPath : String, moduleSrcName : String, scalaGeneratedCode : String) {
    val scalaTargetPath = s"$targPath/$moduleSrcName"
    writeSrcFile(scalaGeneratedCode, scalaTargetPath)
  }

  /* 
   * Compile the supplied generated code and jar it, the originating pmml model, and the class output from the 
   * compile.  Add a registration module as well.  Note the classpath dependencies in the manifest.mf file
   * that is also included in the jar.
   */

  def compile (jarBuildDir : String
	     , scalahome : String
	     , moduleName : String
	     , classpath : String
	     , sourceCode : String
	     , clientName : String
	     , sourceLanguage : String = "scala") : Int = 
  { 
    var srcFileName: String = ""
    var compileCommand: scala.collection.mutable.Seq[String] = null
    if (sourceLanguage.equals("java")) {
      srcFileName = s"$moduleName.java"
      compileCommand = Seq("sh", "-c", s"$scalahome/bin/javac -cp $classpath $jarBuildDir/$srcFileName")
    }
    else {
      srcFileName = s"$moduleName.scala" 
      compileCommand = Seq("sh", "-c", s"$scalahome/bin/scalac -cp $classpath $jarBuildDir/$srcFileName")
    }
 println("--> Creating SOURCEFILE AGAIN ->" +jarBuildDir+" "+srcFileName)
    createScalaFile(s"$jarBuildDir", srcFileName, sourceCode)

    logger.debug(s"compile cmd used: $compileCommand")
    
    val compileRc = Process(compileCommand).!
    if (compileRc != 0) {
      logger.error(s"Compile for $srcFileName has failed...rc = $compileRc")
      logger.error(s"Command used: $compileCommand")
      compileRc
    } else {
      //The compiled class files are found in com/$client/pmml of the current folder.. mv them to $jarBuildDir
      val mvCmd : String = s"mv com $compiler_work_dir/$moduleName/"
      val mvCmdRc : Int = Process(mvCmd).!
      if (mvCmdRc != 0) {
	      logger.warn(s"unable to move classes to build directory, $jarBuildDir ... rc = $mvCmdRc")
	      logger.warn(s"cmd used : $mvCmd")
      }	
      println("compile command RC is " + mvCmdRc)
      mvCmdRc
    }
  }

  def jarCode ( moduleNamespace: String
		, moduleName: String
		, moduleVersion: String
		, sourceCode : String
		, classpath : String
		, jarTargetDir : String
		, clientName : String
		, pmmlFilePath : String
		, scalahome : String
		, javahome : String
    , isLocalOnly: Boolean = false
    , sourceLanguage: String = "scala") : (Int, String) =
  {   
 println("Creating/removing directories")
    
    var currentWorkFolder: String = moduleName
    if (isLocalOnly) {
      currentWorkFolder = currentWorkFolder + "_local"
    }  
 
    // Remove this directory with all the junk if it already exists
    val killDir = s"rm -Rf $compiler_work_dir/"+currentWorkFolder
    
    println(s"-> KILLING "+killDir)
    val killDirRc = Process(killDir).! /** remove any work space that may be present from prior failed run  */
    if (killDirRc != 0) {
      logger.error(s"Unable to rm $compiler_work_dir/$moduleName ... rc = $killDirRc")
      return (killDirRc, "")
    }
    
    // Create a new clean directory
    val buildDir = s"mkdir $compiler_work_dir/"+currentWorkFolder
    println(s"-> BUILDING "+buildDir)
    val tmpdirRc = Process(buildDir).! /** create a clean space to work in */
    if (tmpdirRc != 0) {
      logger.error(s"The compilation of the generated source has failed because $buildDir could not be created ... rc = $tmpdirRc")
      return (tmpdirRc, "")
    }   
    
    /** create a copy of the pmml source in the work directory */
    val cpRc = Process(s"cp $pmmlFilePath $compiler_work_dir/"+currentWorkFolder).!
    
    println(s"-> COPYING cp $pmmlFilePath $compiler_work_dir/"+currentWorkFolder)
    if (cpRc != 0) {
      logger.error(s"Unable to create a copy of the pmml source xml for inclusion in jar ... rc = $cpRc")
      return (cpRc, "")
    }

println("Creating JAR / Compiling =====> "+ moduleName )

    /** compile the generated code if its a local copy, make sure we save off the  postfixed _local in the working directory*/
    var sourceName: String = moduleName
    var cHome: String = scalahome
    if (sourceLanguage.equals("java")) 
       cHome = javahome
    else {
      if (isLocalOnly) sourceName = moduleName+"_local" else sourceName = moduleName
    }
    
    val rc = compile(s"$compiler_work_dir/$currentWorkFolder", cHome, sourceName, classpath, sourceCode, clientName, sourceLanguage) 
    

    // Bail if compilation filed.
    if (rc != 0) {
      return (rc, "")
    }
    
    /** create the jar */
    var  moduleNameJar: String = ""
    if (!isLocalOnly) {
      var d = new java.util.Date()
      var epochTime = d.getTime
      moduleNameJar = moduleNamespace + "_" + moduleName + "_" + moduleVersion + "_" + epochTime + ".jar"
    } else {
      moduleNameJar = moduleNamespace + "_" + moduleName + ".jar"
    }

    val jarPath = compiler_work_dir + "/" + moduleNameJar      
    val jarCmd : String = s"$javahome/bin/jar cvf $jarPath -C $compiler_work_dir/"+currentWorkFolder +"/ ."
    logger.debug(s"jar cmd used: $jarCmd")
    logger.debug(s"Jar $moduleNameJar produced.  Its contents:")

    val jarRc : Int = Process(jarCmd).!
    if (jarRc != 0) {
       println(s"unable to create jar $moduleNameJar ... rc = $jarRc")
      logger.error(s"unable to create jar $moduleNameJar ... rc = $jarRc")
      return (jarRc, "")
    }
	
    
    /** move the new jar to the target dir where it is to live */
    val mvCmd : String = s"mv $jarPath $jarTargetDir/"
    logger.debug(s"mv cmd used: $mvCmd")
    val mvCmdRc : Int = Process(mvCmd).!
    if (mvCmdRc != 0) {
      logger.error(s"unable to move new jar $moduleNameJar to target directory, $jarTargetDir ... rc = $mvCmdRc")
      logger.error(s"cmd used : $mvCmd")
       println(s"unable to move new jar $moduleNameJar to target directory, $jarTargetDir ... rc = $mvCmdRc")
        println(s"cmd used : $mvCmd")
    }
    (0, s"$moduleNameJar")
  }
  
  
  def compileModelFromSource (sourceCode: String, metaProps: java.util.Properties , sourceLang: String = "scala"): ModelDef = {
    try {
      var injectLoggingStmts : Boolean = false   
      val modDef : ModelDef = MdMgr.GetMdMgr.MakeModelDef(metaProps.getProperty("namespace"),
                                                          metaProps.getProperty("name"),
                                                          metaProps.getProperty("objName"),
                                                          "RuleSet",
                                                          List[(String, String, String, String, Boolean, String)](),
                                                          List[(String, String, String)]() ,
                                                          1,
                                                          "LowBalanceAlertModel.jar",
                                                          metaProps.getProperty("dep").asInstanceOf[Array[String]],
                                                          false
                                                 )
 
 println("compileJavaModel ====> START")  
 
 
       //Adding the package name
       val packageName = "com.ligadata.models."+metaProps.getProperty("namespace") +".V"+ MdMgr.FormatVersion(metaProps.getProperty("version")).substring(0,6)
       
       println("compileJavaModel ====> " + packageName)  
       var firstImport = sourceCode.indexOf("import")
       var packagedSource = ""
       packagedSource = "package " +packageName + "; " + sourceCode.substring(firstImport)
       println("compileJavaModel ====> " + packagedSource.substring(0,packageName.length+10))  
       
       
       
  
       // Save off the source Code in the working directories.
      // val msgDefFilePath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR") + "/" + modDef.name + ".txt"
       val msgDefFilePath = compiler_work_dir + "/" + modDef.name + ".txt"
      // dumpStrTextToFile(packagedSource,msgDefFilePath)
     //  val msgDefClassFilePath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR") + "/" + modDef.name + "."+sourceLang
       val msgDefClassFilePath = compiler_work_dir + "/" + modDef.name + "."+sourceLang
       dumpStrTextToFile(packagedSource,msgDefClassFilePath)

println("compileJavaModel ====> ..." + msgDefClassFilePath)  
     
       // Get classpath and jarpath ready
       var classPath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("CLASSPATH").trim
       if (classPath.size == 0) classPath = "."  
       val jarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet
       
       // Handle the Dependency Jar stuff
       if (modDef.DependencyJarNames != null) {
          val depJars = modDef.DependencyJarNames.map(j => JarPathsUtils.GetValidJarFile(jarPaths, j)).mkString(":")
          if (classPath != null && classPath.size > 0) {
            classPath = classPath + ":" + depJars 
          } else {
            classPath = depJars 
          }
       }

       var (status2,jarFileName) = jarCode(metaProps.getProperty("namespace"),
                                                metaProps.getProperty("name"),
                                                metaProps.getProperty("version"),
                                                packagedSource,
                                                classPath,
                                                MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR"),
                                                "TestClient",
                                                msgDefClassFilePath,
                                                MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SCALA_HOME"),
                                                MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAVA_HOME"),
                                                false,
                                                sourceLang)
                    
       /* The following check require cleanup at some point */
       if (status2 > 1 ){
         throw new ModelCompilationFailedException("Failed to produce the jar file")
       }

       // Return the ModDef - SUCCESS
       modDef.jarName = jarFileName
       modDef.PhysicalName(packageName+"."+ metaProps.getProperty("objName"))
       modDef
    }  catch {
      case e:AlreadyExistsException =>{
        logger.error("Failed to compile the model definition " + e.toString)
        throw new ModelCompilationFailedException(e.getMessage())
      }
      case e:Exception =>{
        logger.error("Failed to compile the model definition " + e.toString)
        throw new ModelCompilationFailedException(e.getMessage())
      }
    }  
    
  }
  
  

  def compilePmml(pmmlStr: String, recompile: Boolean = false) : (String,ModelDef) = {
    try{
      /** Ramana, if you set this to true, you will cause the generation of logger.info (...) stmts in generated model */
      var injectLoggingStmts : Boolean = false 

      val model_exec_log = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("MODEL_EXEC_LOG")
      if(model_exec_log.equalsIgnoreCase("true")){
	injectLoggingStmts = true
      }

      val compiler  = new PmmlCompiler(MdMgr.GetMdMgr, "ligadata", logger, injectLoggingStmts, 
				       MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(","))
      val (classStr,modDef) = compiler.compile(pmmlStr,compiler_work_dir,recompile)
      
      /** if errors were encountered... the model definition is not manufactured.
       *  Avoid Scala compilation of the broken src.  Src file MAY be available 
       *  in classStr.  However, if there were simple syntactic issues or simple semantic
       *  issues, it may not be generated.
       */
      if (modDef != null) { 

	      var pmmlScalaFile = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR") + "/" + modDef.name + ".pmml"    
	
	      var classPath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("CLASSPATH").trim
	      
	      if (classPath.size == 0)
	    	  classPath = "."

    	  val jarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet

    	  if (modDef.DependencyJarNames != null) {
			  val depJars = modDef.DependencyJarNames.map(j => JarPathsUtils.GetValidJarFile(jarPaths, j)).mkString(":")
			  if (classPath != null && classPath.size > 0) {
				  classPath = classPath + ":" + depJars 
			  } else {
				  classPath = depJars 
			  }
	      }

	      var (jarFile,depJars) = 
				compiler.createJar(classStr,
						   classPath,
						   pmmlScalaFile,
						   MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR"),
						   MetadataAPIImpl.GetMetadataAPIConfig.getProperty("MANIFEST_PATH"),
						   MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SCALA_HOME"),
						   MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAVA_HOME"),
						   false,
						   compiler_work_dir)

		  /* The following check require cleanup at some point */
		  if(jarFile.compareToIgnoreCase("Not Set") == 0 ){
			  throw new ModelCompilationFailedException("Failed to produce the jar file")
		  }
	
	      modDef.jarName = jarFile
		  modDef.dependencyJarNames = depJars.map(f => {(new java.io.File(f)).getName})
	      if( modDef.ver == 0 ) {
	    	  modDef.ver = 1
	      }
	      if( modDef.modelType == null) {
	    	  modDef.modelType = "RuleSet"
	      }

	      modDef.objectDefinition = pmmlStr
	      modDef.objectFormat = fXML
 
      } /** end of (modDef != null) */

      (classStr,modDef)
    } catch {
    	case e:Exception =>{
    		logger.error("Failed to compile the model definition " + e.toString)
    		throw new ModelCompilationFailedException(e.getMessage())
    	}
    	case e:AlreadyExistsException => {
    		logger.error("Failed to compile the model definition " + e.toString)
    		throw new ModelCompilationFailedException(e.getMessage())
    	}
    }
  }


  @throws(classOf[MsgCompilationFailedException])
  def compileMessageDef(msgDefStr: String,recompile:Boolean = false) : (String,ContainerDef,String) = {
    try{
      val mgr = MdMgr.GetMdMgr
      val msg = new MessageDefImpl()
      logger.debug("Call Message Compiler ....")
      val(classStrVer, msgDef, classStrNoVer) = msg.processMsgDef(msgDefStr, "JSON",mgr,recompile)    
      logger.debug("Message Compilation done ...." + JsonSerializer.SerializeObjectToJson(msgDef))

  println("*****************"+compiler_work_dir+"********************")    
    //  val msgDefFilePath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR") + "/" + msgDef.name + ".txt"
      val msgDefFilePath = compiler_work_dir + "/" + msgDef.name + ".txt"
      dumpStrTextToFile(msgDefStr,msgDefFilePath)
    //  val msgDefClassFilePath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR") + "/" + msgDef.name + ".scala"
      val msgDefClassFilePath = compiler_work_dir + "/" + msgDef.name + ".scala"
      dumpStrTextToFile(classStrVer,msgDefClassFilePath)
     // val msgDefFilePathLocal = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR") + "/" + msgDef.name + "_local.txt"
      val msgDefFilePathLocal = compiler_work_dir + "/" + msgDef.name + "_local.txt"
      dumpStrTextToFile(msgDefStr,msgDefFilePathLocal)     
//      val msgDefClassFilePathLocal = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR") + "/" + msgDef.name + "_local.scala"
      val msgDefClassFilePathLocal = compiler_work_dir + "/" + msgDef.name + "_local.scala"
      dumpStrTextToFile(classStrNoVer,msgDefClassFilePathLocal)

     var classPath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("CLASSPATH").trim
     
     

      if (msgDef.DependencyJarNames != null) {
        val jarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet
        val depJars = msgDef.DependencyJarNames.map(j => JarPathsUtils.GetValidJarFile(jarPaths, j)).mkString(":")
        if (classPath != null && classPath.size > 0) {
          classPath = classPath + ":" + depJars 
        } else {
          classPath = depJars 
        }
      }

      // Call JarCode 2x, first call will generate a versioned Jar File. Second will generate
      // the not versioned one.
      logger.debug("Generating Versioned JarFile for "+msgDefClassFilePath)
      var(status,jarFileVersion) = jarCode(msgDef.nameSpace,
				    msgDef.name,
				    msgDef.ver.toString,
				    classStrVer,
				    classPath,
				    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR"),
				    "Test Client",
				    msgDefFilePath,
				    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SCALA_HOME"),
				    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAVA_HOME"))
      logger.debug("Status => " + status)    
      
      logger.debug("Generating No Versioned JarFile for "+msgDefClassFilePath)
      var(status2,jarFileNoVersion) = jarCode(msgDef.nameSpace,
            msgDef.name,
            msgDef.ver.toString,
            classStrNoVer,
            classPath,
            MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR"),
            "Test Client",
            msgDefFilePathLocal,
            MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SCALA_HOME"),
            MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAVA_HOME"),
            true)
      logger.debug("Status => " + status2)     

      if( status != 0 ){
	      logger.error("Compilation of MessgeDef scala file has failed, Message is not added")
	      throw new MsgCompilationFailedException(msgDefStr)
      }

      logger.debug("Jar Files => " + jarFileVersion + ", "+jarFileNoVersion)

      if ( msgDef.nameSpace == null ){
	      msgDef.nameSpace = MetadataAPIImpl.sysNS
      }

      msgDef.jarName = jarFileVersion
      if (msgDef.containerType.isInstanceOf[ContainerTypeDef])
        msgDef.containerType.asInstanceOf[ContainerTypeDef].jarName = jarFileVersion

      msgDef.objectDefinition = msgDefStr
      msgDef.objectFormat = fJSON
      
      (classStrVer,msgDef,classStrNoVer)
    }
    catch{
      case e:Exception =>{
	logger.debug("Failed to compile the message definition " + e.toString)
	e.printStackTrace
	throw new MsgCompilationFailedException(e.getMessage())
      }
      case e:AlreadyExistsException =>{
	logger.debug("Failed to compile the message definition " + e.toString)
	//e.printStackTrace
	throw new MsgCompilationFailedException(e.getMessage())
      }
    }
  }
}

