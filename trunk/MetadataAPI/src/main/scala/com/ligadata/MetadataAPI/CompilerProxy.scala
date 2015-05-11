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

case class MsgCompilationFailedException(e: String) extends Exception(e)
case class ModelCompilationFailedException(e: String) extends Exception(e)

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
	     , scalaGeneratedCode : String
	     , clientName : String
	     ) : Int = 
  {  
    val scalaSrcFileName : String = s"$moduleName.scala"
    createScalaFile(s"$jarBuildDir", scalaSrcFileName, scalaGeneratedCode)

    val scalacCmd = Seq("sh", "-c", s"$scalahome/bin/scalac -cp $classpath $jarBuildDir/$scalaSrcFileName")
    logger.debug(s"scalac cmd used: $scalacCmd")
    val scalaCompileRc = Process(scalacCmd).!
    if (scalaCompileRc != 0) {
      logger.error(s"Compile for $scalaSrcFileName has failed...rc = $scalaCompileRc")
      logger.error(s"Command used: $scalacCmd")
      scalaCompileRc
    }
    else{
      //The compiled class files are found in com/$client/pmml of the current folder.. mv them to $jarBuildDir
      val mvCmd : String = s"mv com $compiler_work_dir/$moduleName/"
      val mvCmdRc : Int = Process(mvCmd).!
      if (mvCmdRc != 0) {
	logger.error(s"unable to move classes to build directory, $jarBuildDir ... rc = $mvCmdRc")
	logger.error(s"cmd used : $mvCmd")
      }		
      mvCmdRc
    }
  }

  def jarCode ( moduleNamespace: String
		, moduleName: String
		, moduleVersion: String
		, scalaGeneratedCode : String
		, classpath : String
		, jarTargetDir : String
		, clientName : String
		, pmmlFilePath : String
		, scalahome : String
		, javahome : String) : (Int, String) =
  {
    //val (prop,envVars) = MetadataAPIImpl.readProperties
    /** prep the workspace and go there*/
    val killDir = s"rm -Rf $compiler_work_dir/$moduleName"
    val killDirRc = Process(killDir).! /** remove any work space that may be present from prior failed run  */
    if (killDirRc != 0) {
      logger.error(s"Unable to rm $compiler_work_dir/$moduleName ... rc = $killDirRc")
      return (killDirRc, "")
    }
    val buildDir = s"mkdir $compiler_work_dir/$moduleName"
    val tmpdirRc = Process(buildDir).! /** create a clean space to work in */
    if (tmpdirRc != 0) {
      logger.error(s"The compilation of the generated source has failed because $buildDir could not be created ... rc = $tmpdirRc")
      return (tmpdirRc, "")
    }
    /** create a copy of the pmml source in the work directory */
    val cpRc = Process(s"cp $pmmlFilePath $compiler_work_dir/$moduleName/").!
    if (cpRc != 0) {
      logger.error(s"Unable to create a copy of the pmml source xml for inclusion in jar ... rc = $cpRc")
      return (cpRc, "")
    }
    /** compile the generated code */
    val rc : Int = compile(s"$compiler_work_dir/$moduleName", scalahome, moduleName, classpath, scalaGeneratedCode, clientName)
    if (rc != 0) {
      return (rc, "")
    }

    /** create the jar */
    var d = new java.util.Date()
    var epochTime = d.getTime
    // insert epochTime into jar file
    val moduleNameJar : String = moduleNamespace + "_" + moduleName + "_" + moduleVersion + "_" + epochTime + ".jar"
    val jarPath = compiler_work_dir + "/" + moduleNameJar
    val jarCmd : String = s"$javahome/bin/jar cvf $jarPath -C $compiler_work_dir/$moduleName/ ."
    logger.debug(s"jar cmd used: $jarCmd")
    logger.debug(s"Jar $moduleNameJar produced.  Its contents:")
    val jarRc : Int = Process(jarCmd).!
    if (jarRc != 0) {
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
    }
		
    (0, s"$moduleNameJar")
  }

  /**
   * compileScala - same as compilePmml, but we dont need to generate scala code from the PMML xml string
   * @param String scala source code
   * @return ModelDef
   */
  def compileScala(scalaStr: String) : ModelDef = {
    try {
      var injectLoggingStmts : Boolean = false 
      val modDef : ModelDef = MdMgr.GetMdMgr.MakeModelDef("System",
                                                          "SimpleModel",
                                                          "System.SipmleModel",
                                                          "RuleSet",
                                                          List[(String, String, String, String, Boolean, String)](),
                                                          List[(String, String, String)]() ,
                                                          1,
                                                          "simpleModel.jar",
                                                          Array[String]("fatafatbase_2.10-1.0.jar"),
                                                          false
                                                 )
    
       // Need to have a java specific compiler ???
       val compiler  = new PmmlCompiler(MdMgr.GetMdMgr, "ligadata", logger, injectLoggingStmts, 
                                      MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(","))
       
       // Get classpath and jarpath ready
       var classPath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("CLASSPATH").trim
       if (classPath.size == 0) classPath = "."  
       val jarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet
     
       if (modDef.DependencyJarNames != null) {
          val depJars = modDef.DependencyJarNames.map(j => JarPathsUtils.GetValidJarFile(jarPaths, j)).mkString(":")
          if (classPath != null && classPath.size > 0) {
            classPath = classPath + ":" + depJars 
          } else {
            classPath = depJars 
          }
       }
 println("===>  About to call createJar")   
       var (jarFile,depJars) = compiler.createJar(scalaStr,
                    classPath,
                    null,
                    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR"),
                    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("MANIFEST_PATH"),
                    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SCALA_HOME"),
                    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAVA_HOME"),
                    false,
                    compiler_work_dir)

       /* The following check require cleanup at some point */
       if (jarFile.compareToIgnoreCase("Not Set") == 0 ){
         throw new ModelCompilationFailedException("Failed to produce the jar file")
       }
       
       
       modDef
     } catch {
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

      var pmmlScalaFile = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR") + "/" + modDef.name + ".pmml"    

      var classPath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("CLASSPATH").trim
      
      if (classPath.size == 0) classPath = "."

      val jarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet

      if (modDef.DependencyJarNames != null) {
	      val depJars = modDef.DependencyJarNames.map(j => JarPathsUtils.GetValidJarFile(jarPaths, j)).mkString(":")
	      if (classPath != null && classPath.size > 0) {
          classPath = classPath + ":" + depJars 
	      } else {
          classPath = depJars 
	      }
      }

      var (jarFile,depJars) = compiler.createJar(classStr,
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
      if( modDef.ver == 0 ){
	      modDef.ver     = 1
      }
      if( modDef.modelType == null){
	      modDef.modelType = "RuleSet"
      }

      modDef.objectDefinition = pmmlStr
      modDef.objectFormat = fXML

      (classStr,modDef)
    } catch{
      case e:Exception =>{

	logger.error("Failed to compile the model definition " + e.toString)
	throw new ModelCompilationFailedException(e.getMessage())
      }
      case e:AlreadyExistsException =>{
	logger.error("Failed to compile the model definition " + e.toString)

	throw new ModelCompilationFailedException(e.getMessage())
      }
    }
  }


  @throws(classOf[MsgCompilationFailedException])
  def compileMessageDef(msgDefStr: String,recompile:Boolean = false) : (String,ContainerDef) = {
    try{
      val mgr = MdMgr.GetMdMgr
      val msg = new MessageDefImpl()
      logger.debug("Call Message Compiler ....")
      val(classStr, msgDef) = msg.processMsgDef(msgDefStr, "JSON",mgr,recompile)
      logger.debug("Message Compilation done ...." + JsonSerializer.SerializeObjectToJson(msgDef))

      
      val msgDefFilePath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR") + "/" + msgDef.name + ".txt"
      dumpStrTextToFile(msgDefStr,msgDefFilePath)
      val msgDefClassFilePath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR") + "/" + msgDef.name + ".scala"
      dumpStrTextToFile(classStr,msgDefClassFilePath)

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

      var(status,jarFile) = jarCode(msgDef.nameSpace,
				    msgDef.name,
				    msgDef.ver.toString,
				    classStr,
				    classPath,
				    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR"),
				    "Test Client",
				    msgDefFilePath,
				    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SCALA_HOME"),
				    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAVA_HOME"))


      logger.debug("Status => " + status)

      if( status != 0 ){
	logger.error("Compilation of MessgeDef scala file has failed, Message is not added")
	throw new MsgCompilationFailedException(msgDefStr)
      }

      logger.debug("Jar File => " + jarFile)

      if ( msgDef.nameSpace == null ){
	msgDef.nameSpace = MetadataAPIImpl.sysNS
      }

      msgDef.jarName = jarFile
      if (msgDef.containerType.isInstanceOf[ContainerTypeDef])
        msgDef.containerType.asInstanceOf[ContainerTypeDef].jarName = jarFile

      msgDef.objectDefinition = msgDefStr
      msgDef.objectFormat = fJSON
      
      (classStr,msgDef)
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

