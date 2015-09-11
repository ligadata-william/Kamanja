package com.ligadata.KamanjaShell

import com.ligadata.MetadataAPI.MetadataAPIImpl
import scala.io.Source
import java.util.logging.Logger

/**
 * @author danielkozin
 */
object MetadataProxy {
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  
  /**
   * Initialize Metadata with the config file.
   */
  def initMetadata(ic: InstanceContext, config: String) {
      // Initialize and start the Metadata Service if the global port is not 0.
      // else, just initialize a local metadataAPI object.
      if (ic.getIPort.toInt > 0) {
        // Remote Service to be implemented later
      } else {
        MetadataAPIImpl.InitMdMgrFromBootStrap(config, false)
      }
  }
  
  def shutdown(ic: InstanceContext): Unit = {
    
    if (ic.getIPort.toInt > 0) {
        println("Shutting down MetadataAPI Service....")
       // Remote Service to be implemented later
    } else {
      println("Shutting down local MetadataAPI.....")
      MetadataAPIImpl.shutdown
      //MetadataAPIImpl.CloseZKSession
    }   
  }

  /**
   * Upload the cluster configuration to be used by the runtime.
   */
  def addClusterConfig(ic: InstanceContext, configFile: String) {
    var cfgStr: String = ""
    if (ic.getIPort.toInt > 0) {
       // Remote Service to be implemented later
    } else {
      println("ADDING config "+ configFile)
        try {
          cfgStr = Source.fromFile(configFile).mkString
           println(MetadataAPIImpl.UploadConfig(cfgStr, Some(ic.getUserid), "testConf"))
        } catch {
          case fnfe: java.io.FileNotFoundException => logger.info("File "+ configFile +" does not exit")
          case npe: Exception => npe.printStackTrace
        }
       
    }   
  }
  
  /**
   *  Add container.
   */
  def addContainer(ic: InstanceContext, file: String, appPath: String = "") {
    var containerText: String = ""
    if (ic.getIPort.toInt > 0) {
       // Remote Service to be implemented later
    } else {
      containerText = Source.fromFile(file).mkString
      MetadataAPIImpl.AddContainer(containerText, "JSON", Some(ic.getUserid))
    }  
  }
  
  /**
   * Get containers on the system.
   */
  def getContainer (ic: InstanceContext, key: String = "") {
    if (ic.getIPort.toInt > 0) {
       // Remote Service to be implemented later
    } else {
      if (key.length > 0) {
        val(namespace, name, version) = com.ligadata.kamanja.metadata.Utils.parseNameToken(key)
        println(MetadataAPIImpl.GetContainerDefFromCache(namespace, name, "JSON", version, Some(ic.getUserid))   ) 
      }
      else {
        println(MetadataAPIImpl.GetAllContainersFromCache(true, Some(ic.getUserid)).foreach(x=>println(x))  )        
      }  
    }     
  }
  
  /**
   * Get containers on the system.
   */
  def getMessage (ic: InstanceContext, key: String = "") {
    if (ic.getIPort.toInt > 0) {
       // Remote Service to be implemented later
    } else {
      if (key.length > 0) {
        val(namespace, name, version) = com.ligadata.kamanja.metadata.Utils.parseNameToken(key)
        println(MetadataAPIImpl.GetMessageDefFromCache(namespace, name, "JSON", version, Some(ic.getUserid))   ) 
      }
      else {
        println(MetadataAPIImpl.GetAllMessagesFromCache(true, Some(ic.getUserid)).foreach(x=>println(x))  )        
      }  
    }     
  }
  
  /**
   * Get models on the system
   */
  def getModel(ic: InstanceContext, key: String = "") {
    if (ic.getIPort.toInt > 0) {
       // Remote Service to be implemented later
    } else {
      if (key.length > 0) {
        val(namespace, name, version) = com.ligadata.kamanja.metadata.Utils.parseNameToken(key)
        println(MetadataAPIImpl.GetMessageDefFromCache(namespace, name, "JSON", version, Some(ic.getUserid))   ) 
      }
      else {
        println(MetadataAPIImpl.GetAllMessagesFromCache(true, Some(ic.getUserid)).foreach(x=>println(x))  )        
      }  
    }     
  }
  
  /**
   * Remove container
   */
  def removeContainer(ic: InstanceContext, containerKey: String) {
    if (ic.getIPort.toInt > 0) {
       // Remote Service to be implemented later
    } else {
       val(namespace, name, version) = com.ligadata.kamanja.metadata.Utils.parseNameToken(containerKey)
      MetadataAPIImpl.RemoveContainer(namespace, name, version.toLong,  Some(ic.getUserid))
    }     
  }
  
  
  /**
   *  Add message.
   */
  def addMessage(ic: InstanceContext, file: String, appPath: String = "") {
    var containerText: String = ""
    if (ic.getIPort.toInt > 0) {
       // Remote Service to be implemented later
    } else {
      containerText = Source.fromFile(file).mkString
      MetadataAPIImpl.AddMessage(containerText, "JSON", Some(ic.getUserid))
    }  
  }
  
  /**
   *  Remove message
   */
  def removeMessage(ic: InstanceContext, messageKey: String) {
    if (ic.getIPort.toInt > 0) {
       // Remote Service to be implemented later
    } else {
      val(namespace, name, version) = com.ligadata.kamanja.metadata.Utils.parseNameToken(messageKey)
      MetadataAPIImpl.RemoveMessage(namespace, name, version.toLong,  Some(ic.getUserid))
    }     
  }
  
  def addNativeModel (ic: InstanceContext, modelPath: String, makefilePath: String, mType: String) {
     if (ic.getIPort.toInt > 0) {
       // Remote Service to be implemented later
    } else {
       val pmmlStr = Source.fromFile(modelPath).mkString
       var configName = ""
       
       // Native models need to have a model configuration, i.e. a makefile.
       // A user can pass in a path to local file containing 1 or more model configurations in JSON format.
       // or he can just pass the name of a single JSON configuration.   If the only a name was passed, 
       // we assume that the config file was already added to the metadata.  If the entire path was added, then
       // we also add the entire model config to the metadata, then we continue with the model deployment.
       println(makefilePath)
       
       val tokMakefile = makefilePath.split("/")
       if (tokMakefile.size > 1) {
         var tempPath = ""
         for (i <- 0 until tokMakefile.size - 1 )  {
           //make sure that paths starting with a / are paresed correctly..
           if (tokMakefile(i).size > 0)
             tempPath = tempPath + "/"+ tokMakefile(i)
         } 
         configName = tokMakefile(tokMakefile.size - 1)
         
         // Upload the Model Config file
         println("Uploading Model Configurations from "+tempPath)
         val cfgStr = Source.fromFile(tempPath).mkString
        println( MetadataAPIImpl.UploadModelsConfig(cfgStr,  Some(ic.getUserid), "testConf"))
         
       } else if (tokMakefile.size == 1) {
         configName = makefilePath
       } else {
         println("Cannot determine Configuration Nmae")
         return
       }
       
       
      MetadataAPIImpl.AddModelFromSource(pmmlStr, mType, ic.getUserid+"."+configName, Some(ic.getUserid))
    }       
  }
  
  def addPmmlModel (ic: InstanceContext, modelPath: String) {
     if (ic.getIPort.toInt > 0) {
       // Remote Service to be implemented later
    } else {
       val pmmlStr = Source.fromFile(modelPath).mkString
       MetadataAPIImpl.AddModel(pmmlStr, Some(ic.getUserid))
    }       
  }
  
  
  
  
  
  
  
  
  
  
}