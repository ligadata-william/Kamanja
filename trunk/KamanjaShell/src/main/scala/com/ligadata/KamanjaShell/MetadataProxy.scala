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
       // Remote Service to be implemented later
    } else {
      MetadataAPIImpl.shutdown
      MetadataAPIImpl.CloseZKSession
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
  def addContainer(ic: InstanceContext, path: String, appPath: String = "") {
    var containerText: String = ""
    if (ic.getIPort.toInt > 0) {
       // Remote Service to be implemented later
    } else {
      var file: String = path
      if (appPath.length > 0)
        file = appPath + "/" + file
   
      containerText = Source.fromFile(file).mkString
      MetadataAPIImpl.AddContainer(containerText, "JSON", Some(ic.getUserid))
    }  
  }
  
  /**
   * 
   */
  def removeContainer(ic: InstanceContext, containerKey: String) {
    if (ic.getIPort.toInt > 0) {
       // Remote Service to be implemented later
    } else {
      val(contNameSpace, contName, contVersion) = com.ligadata.kamanja.metadata.Utils.parseNameToken(containerKey)
      MetadataAPIImpl.RemoveContainer(contNameSpace, contName, contVersion.toLong,  Some(ic.getUserid))
    }     
  }
  
  
  
}