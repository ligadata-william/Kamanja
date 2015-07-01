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

import java.net.URL
import java.net.URLClassLoader

/**
 *  MetadataClassLoader - contains the classes that need to be dynamically resolved the the
 *  reflective calls
 */
class CompilerProxyClassLoader(urls: Array[URL], parent: ClassLoader) extends URLClassLoader(urls, parent) {
  override def addURL(url: URL) {
    super.addURL(url)
  }
}

/**
 * MetadataLoaderInfo
 */
class CompilerProxyLoader {
  // class loader
  val loader: MetadataClassLoader = new MetadataClassLoader(ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader].getURLs(), getClass().getClassLoader())
  // Loaded jars
  val loadedJars: TreeSet[String] = new TreeSet[String]
  // Get a mirror for reflection
  //val mirror: scala.reflect.runtime.universe.Mirror = ru.runtimeMirror(loader)
}

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
       , targetClassFolder: String
	     , sourceLanguage : String = "scala") : Int = 
  { 
    
  
    
    var srcFileName: String = ""
    var compileCommand: scala.collection.mutable.Seq[String] = null
    if (sourceLanguage.equals("java")) {
        println ("ABOUT TO COMPILE "+moduleName+".java")
      
      srcFileName = s"$moduleName.java"
      // If the target folder is provided, then the Package Directory is already created, and we
      // need to add the -d option to the JAVAC
      if (targetClassFolder == null) {
          compileCommand = Seq("sh", "-c", s"$scalahome/bin/javac -cp $classpath $jarBuildDir/$srcFileName")
      } else {
          compileCommand = Seq("sh", "-c", s"$scalahome/bin/javac -d $compiler_work_dir/$targetClassFolder/ -cp $classpath $jarBuildDir/$srcFileName")      
      }   
    }
    else {
      println ("ABOUT TO COMPILE "+moduleName+".scala "+ "   in "+ jarBuildDir)
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
      var targetForMv: String = moduleName
      
      println(targetClassFolder)
      
      if (targetClassFolder != null) {
          return compileRc 
      } 
      val mvCmd : String = s"mv com $compiler_work_dir/$moduleName/"
      println(mvCmd)
      val mvCmdRc : Int = Process(mvCmd).!
      if (mvCmdRc != 0) {
	      logger.warn(s"unable to move classes to build directory, $jarBuildDir ... rc = $mvCmdRc")
	      logger.warn(s"cmd used : $mvCmd")
      }	
      println("compile command RC is " + mvCmdRc)
      mvCmdRc

    }
  }

  
  /**
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   */
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
    , sourceLanguage: String = "scala"
    , helperJavaSource: String = null
    , helperJavaSourcePath: String = null) : (Int, String) =
  {   
 println("Creating/removing directories for "+pmmlFilePath)
    
    var currentWorkFolder: String = moduleName
    if (isLocalOnly) {
      currentWorkFolder = currentWorkFolder + "_local"
    }  
 
 println("WORK FOLDER IS GOINGTO BE "+ compiler_work_dir+"/"+currentWorkFolder)
 
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

    // Compile 
    val rc = compile(s"$compiler_work_dir/$currentWorkFolder", cHome, sourceName, classpath, sourceCode, clientName, null, sourceLanguage) 

    // Bail if compilation filed.
    if (rc != 0) {
      return (rc, "")
    }
    
    // May need to compile additional java files (for message helpers)
    if (helperJavaSource != null) {
      // Compile 
      val tempClassPath = classpath+":"+s"$compiler_work_dir/$currentWorkFolder"
      val rc = compile(s"$compiler_work_dir/$currentWorkFolder", javahome, moduleName+"Factory", tempClassPath, helperJavaSource, clientName, moduleName, "java")  
      // Bail if compilation filed.
      if (rc != 0) {
        return (rc, "")
      }
    }
    

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

    println("JarName is "+ moduleNameJar)
    
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
  
  
  
  /**
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   */
  
  private def parseSourceForMetadata (sourceCode: String,
                                      modelConfigName: String,
                                      sourceLang: String,
                                      msgDefClassFilePath: String,
                                      classPath: String,
                                      elements: Set[BaseElemDef]): (String, String, String, String) = {
    
    println("Parsing MODEL CONFIG METADATA")
    
    
   // dumpStrTextToFile(sourceCode,msgDefClassFilePath)
    
    // Pull the Namespace, for now its just the package name, in the future, this can be explicitely defined in the 
    // Model Obj....
    val packageExpression = "\\s*package\\s*".r
    val endPackageEpression = "[\t\n\r\f;]".r
    var indx1: Integer = -1
    var indx2: Integer = -1

    var packageName: String = ""
    var codeBeginIndx = sourceCode.indexOf("import")
    var sMatchResult = packageExpression.findFirstMatchIn(sourceCode).getOrElse(null)
    if (sMatchResult != null) indx1 = sMatchResult.end
    var eMatchResult = endPackageEpression.findFirstMatchIn(sourceCode.substring(indx1)).getOrElse(null)
    if (eMatchResult != null) indx2 = eMatchResult.end

    if (indx1 != -1 && indx2 > indx1)
      packageName = sourceCode.substring(indx1,indx2).trim
    else
      ("","","")
 
    // Augment the code with the new package name  
    var repackagedCode = "package "+ packageName +".V0;\n" + sourceCode.substring(codeBeginIndx)   

    var typeNamespace: Array[String] = null
    var typeImports: String = ""
    
    // Create the IMPORT satements for all the dependend Types
    elements.foreach (elem =>{
      typeImports = typeImports + "\nimport "+ elem.physicalName +";"
    })
    
    // Remove all the existing reference to the dependent types code 
    elements.foreach(elem => {
      println("DEP ELEM: "+elem.PhysicalName + " "+elem.Version+" "+elem.NameSpace+"    Resolving ->" )
      var eName: Array[String] = elem.PhysicalName.split('.').map(_.trim)

      if ((eName.length - 1) > 0) {
        typeNamespace = new Array[String](eName.length - 1)      
        for (i <- 0 until typeNamespace.length ) {
          typeNamespace(i) = eName(i)
        }
        var typeClassName: String = eName(eName.length - 1)
        // Replace the "import com...ClassName" import statement
        repackagedCode = repackagedCode.replaceAll(("\\s*import\\s*"+typeNamespace.mkString(".")+"[.*]"+typeClassName + "\\;*"), "")            
      }
    })
    //Replace the "import com....*;" statement - JAVA STYLE IMPORT ALL
    repackagedCode = repackagedCode.replaceAll(("\\s*import\\s*"+typeNamespace.mkString(".")+"[.*]"+"\\*\\;*"), "")
    // Replace the "import com...._;" type of statement  - SCALA STYLE IMPORT ALL
    repackagedCode = repackagedCode.replaceAll(("\\s*import\\s*"+typeNamespace.mkString(".")+"[.*]"+"_\\;*"), "")
     // Replace the "import com....{xxx};" type of statement  - SCALA STYLE IMPORT SPECIFIC CLASSES IN BATCH
    repackagedCode = repackagedCode.replaceAll( "\\s*import\\s*"+typeNamespace.mkString(".")+"\\.\\{.*?\\}", "")
  
    // Add all the needed imports - have to recalculate the beginning of the imports in the original source code, since a bunch of imports were
    // removed.
    var finalSourceCode = "package "+ packageName +".V0;\n" + typeImports +"\n" + repackagedCode.substring(repackagedCode.indexOf("import"))
    
    println("$$$$$$$$$$$$$")
    println(finalSourceCode)
    
    dumpStrTextToFile(finalSourceCode,msgDefClassFilePath)    
     
    // Need to determine the name of the class file in case of Java  
    var tempClassName: String = modelConfigName
      
    // Create a temporary jarFile file so that we can figure out what the metadata info for this class is. 
    var (status,jarFileName) = jarCode(packageName+".V0",
                                       tempClassName,
                                       "V0",
                                       finalSourceCode,
                                       classPath,
                                       MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR"),
                                       "TestClient",
                                       msgDefClassFilePath,
                                       MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SCALA_HOME"),
                                       MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAVA_HOME"),
                                       false,
                                       sourceLang)    
                                       
     if (status != 0)
       ("","","")
 
     println("JARRING SUCCESS! "+jarFileName)    
     
     
     val classLoader: CompilerProxyLoader = new CompilerProxyLoader      
     val jarPaths0 = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet
     val jarName0 = JarPathsUtils.GetValidJarFile(jarPaths0, jarFileName)
     val fl = new File(jarName0)  
     if (fl.exists) {
       try {
         classLoader.loader.addURL(fl.toURI().toURL())
         println("Jar " + jarName0.trim + " added to class path.")
         classLoader.loadedJars += fl.getPath()
       } catch {
         case e: Exception => {
           logger.error("Failed to add "+jarName0 + " due to internal exception " + e.printStackTrace)
           return null
         }
       }
     } else {
       logger.error("Unable to locate Jar '"+jarName0+"'")
       ("","","")
     }  
    
     var fullClassName = packageName+".V0" + "." + tempClassName   
     val objectName = fullClassName + "$"
     val cons = Class.forName(objectName, true, classLoader.loader).getDeclaredConstructors()       
     cons(0).setAccessible(true);
     val baseModelTrait: com.ligadata.FatafatBase.ModelBaseObj = cons(0).newInstance().asInstanceOf[com.ligadata.FatafatBase.ModelBaseObj]  
     
     
     (packageName, baseModelTrait.ModelName, baseModelTrait.Version, finalSourceCode)
  }
  
  
  /**
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   */
  def compileModelFromSource (sourceCode: String, modelConfigName: String , sourceLang: String = "scala"): ModelDef = {

      var injectLoggingStmts : Boolean = false  
      var versionString: String = ""
      var trueModelName: String = ""

      try {  
       val (classPath, elements) =  getClassPath(modelConfigName) 
       val msgDefClassFilePath = compiler_work_dir + "/" + modelConfigName + "."+sourceLang   
       val (modelNamespace, modelName, modelVersion, repackagedCode) = parseSourceForMetadata(sourceCode, modelConfigName,sourceLang,msgDefClassFilePath,classPath,elements)
       
       println("=-=-=-=-=-=-=-=-=>>>> "+modelNamespace +"."+ modelName +"."+ modelVersion+ "     ver is "+ MdMgr.ConvertVersionToLong(MdMgr.FormatVersion(modelVersion)))
                             
       
       // Now, we need to create a real jar file - Need to add an actual Package name with a real Napespace and Version numbers.
       val packageName = modelNamespace +".V"+ MdMgr.ConvertVersionToLong(MdMgr.FormatVersion(modelVersion))    
       var packagedSource  = "package "+packageName + ";\n " + repackagedCode.substring(repackagedCode.indexOf("import"))
       dumpStrTextToFile(packagedSource,msgDefClassFilePath)

      
       //Classpath is set by now.
       var (status2,jarFileName) = jarCode(modelNamespace ++".V"+ MdMgr.ConvertVersionToLong(MdMgr.FormatVersion(modelVersion)),
                                           modelName,
                                           modelVersion,
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

      // Create the ModelDef object
       
      // TODO... for now keep modelNapespace hardcoded to System... since nothing in this product can process a real namespace name 
      var modelNamespaceTemp = "System"
      val modDef : ModelDef = MdMgr.GetMdMgr.MakeModelDef(modelNamespaceTemp, modelName,"","RuleSet",
                                                          List[(String, String, String, String, Boolean, String)](),
                                                          List[(String, String, String)]() ,
                                                          MdMgr.ConvertVersionToLong(MdMgr.FormatVersion(modelVersion)),"",
                                                          MetadataAPIImpl.getModelDependencies(modelName).toArray[String],
                                                          false
                                                 )     
      
       // Physical Name for a model depends if its a Scala or a Java model.  For scala it is just
       // packageName.ObjectName,  for java it is packageName.ClassName.ClassObj
       // Return the ModDef - SUCCESS
       var modelPName: String = packageName
       if (sourceLang.equalsIgnoreCase("scala")) {
         modelPName = modelPName + "." + modelName
       } else {
         modelPName = modelPName + "." + modelName + "." +  modelName + "Obj"  
       }
                                                 
       modDef.jarName = jarFileName
       modDef.physicalName = modelPName

       println (modDef.physicalName)
       
       
       modDef
    } catch {
      case e:AlreadyExistsException =>{
        logger.error("Failed to compile the model definition " + e.toString)
        throw new ModelCompilationFailedException(e.getMessage())
      }
      case e:Exception =>{
        throw e
        logger.error("Failed to compile the model definition " + e.toString)
        throw new ModelCompilationFailedException(e.getMessage())
      }
    } 
    
  }
  
  
  /**
   * getClassPath - 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   */
  private def getClassPath(modelName: String): (String,Set[BaseElemDef]) = {
    
       var inDeps = MetadataAPIImpl.getModelDependencies(modelName)
       var inMC = MetadataAPIImpl.getModelMessagesContainers(modelName)
       var depElems: Set[BaseElemDef] = Set[BaseElemDef]()
       
      // Get classpath and jarpath ready
       var classPath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("CLASSPATH").trim
       if (classPath.size == 0) classPath = "."  
       val jarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet
       
       jarPaths.foreach (x => println(x))
       
       
       println("compileJavaModel ====> yyyy")
       var msgContDepSet: Set[String] = Set[String]()
       var msgJars: String = ""
       if (inMC == null)  
         logger.warn("Dependant message/containers were not provided into Model Compiler")
       else {  
         inMC.foreach (dep => {
           val elem: BaseElemDef = getDependencyElement(dep).getOrElse(null)
           if (elem == null)
             logger.warn("Unknown dependency "+dep)
           else {
             depElems += elem
             logger.warn("Resolved dependency "+dep+ " to "+ elem.jarName)
             msgContDepSet = msgContDepSet + elem.jarName
           }            
         })
         msgJars = msgContDepSet.map(j => JarPathsUtils.GetValidJarFile(jarPaths, j)).mkString(":")    
         println("*****************")
         println(msgJars)
       }  
       
       // Handle the Dependency Jar stuff
       if (inDeps != null) {
         println("compileJavaModel ====> xxxx")
          var depJars = inDeps.map(j => JarPathsUtils.GetValidJarFile(jarPaths, j)).mkString(":")
          
          if (depJars != null && depJars.length > 0) depJars = depJars + ":" + msgJars else depJars = msgJars
   println("---")
         println(depJars)
   println("---")
         println(msgJars)
    println("---")       
         
          if (classPath != null && classPath.size > 0) {
            classPath = classPath + ":" + depJars 
          } else {
            classPath = depJars 
          }
       }
       (classPath,depElems)
  }
  
  /**
   * getDependencyElement - return a BaseElemDef of the element represented by the key.
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   */
  private def getDependencyElement (key: String) : Option[BaseElemDef] = {
     var elem: Option[BaseElemDef] = None
     
     // is it a container?
     var contElemSet =  MdMgr.GetMdMgr.Containers(key, true, true).getOrElse(scala.collection.immutable.Set[ContainerDef]())
     if (contElemSet.size > 0) {
       elem = Some(contElemSet.last)
       return elem
     }
     
     // is it a message
     var msgElemSet =  MdMgr.GetMdMgr.Messages(key, true, true).getOrElse(scala.collection.immutable.Set[MessageDef]())
     if (msgElemSet.size > 0) {
       elem = Some(msgElemSet.last)
       return elem
     }
     // Return None if nothing found
     None
  }
  

  /**
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   */
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
      val((classStrVer, classStrVerJava), msgDef, (classStrNoVer, classStrNoVerJava)) = msg.processMsgDef(msgDefStr, "JSON",mgr,recompile)    
      logger.debug("Message Compilation done ...." + JsonSerializer.SerializeObjectToJson(msgDef))

      val nameArray = msgDef.PhysicalName.split('.')
      var realClassName: String = ""
      if (nameArray.length > 0) {
        realClassName = nameArray(nameArray.length-1)
      }
      
      val msgDefFilePath = compiler_work_dir + "/" +realClassName + ".txt"
      dumpStrTextToFile(msgDefStr,msgDefFilePath)

      val msgDefClassFilePath = compiler_work_dir + "/" + realClassName + ".scala"
      dumpStrTextToFile(classStrVer,msgDefClassFilePath)
      
      // This is a java file
      val msgDefHelperClassFilePath = compiler_work_dir + "/" + realClassName + "Factory.java"
      dumpStrTextToFile(classStrVerJava,msgDefHelperClassFilePath)

      val msgDefFilePathLocal = compiler_work_dir + "/" + realClassName + "_local.txt"
      dumpStrTextToFile(msgDefStr,msgDefFilePathLocal)     

      val msgDefClassFilePathLocal = compiler_work_dir + "/" + realClassName + "_local.scala"
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
				    realClassName,
				    msgDef.ver.toString,
				    classStrVer,
				    classPath,
				    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR"),
				    "Test Client",
				    msgDefFilePath,
				    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SCALA_HOME"),
				    MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAVA_HOME"),
            false,"scala",
            classStrVerJava,
            msgDefHelperClassFilePath)
      logger.debug("Status => " + status)  
      
      // This is a java file
      val msgDefHelperClassFilePathLocal = compiler_work_dir + "/" + realClassName + "Factory.java"
      dumpStrTextToFile(classStrNoVerJava,msgDefHelperClassFilePathLocal)
      
      println
      println
      println
      println(msgDefHelperClassFilePathLocal)
      println
      println
      println    
      
      logger.debug("Generating No Versioned JarFile for "+msgDefClassFilePath)
      var(status2,jarFileNoVersion) = jarCode(msgDef.nameSpace,
            realClassName,
            msgDef.ver.toString,
            classStrNoVer,
            classPath,
            MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR"),
            "Test Client",
            msgDefFilePathLocal,
            MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SCALA_HOME"),
            MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAVA_HOME"),
            true,"scala",
            classStrNoVerJava,
            msgDefHelperClassFilePathLocal)
            
            
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

