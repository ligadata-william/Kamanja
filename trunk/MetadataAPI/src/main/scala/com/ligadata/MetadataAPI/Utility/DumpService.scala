package main.scala.com.ligadata.MetadataAPI.Utility

import com.ligadata.MetadataAPI.MetadataAPIImpl
import org.apache.log4j._
import com.ligadata.kamanja.metadata.MdMgr
/**
 * Created by dhaval on 8/13/15.
 */
object DumpService {
  private val userid: Option[String] = Some("metadataapi")
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)

  def dumpMetadata: String ={
    var response=""
    try{
      MdMgr.GetMdMgr.dump
      response="Metadata dumped in DEBUG mode"
    }catch{
      case e: Exception => {
        response=e.getStackTrace.toString
      }
    }
    response
  }
  def dumpAllNodes: String ={
    var response=""
    try{
      response=MetadataAPIImpl.GetAllNodes("JSON", userid)
    }
    catch {
      case e: Exception => {
        response=e.getStackTrace.toString
      }
    }
    response
  }
  def dumpAllClusters: String ={
    var response=""
    try{
      response=MetadataAPIImpl.GetAllClusters("JSON", userid)
    }
    catch {
      case e: Exception => {
        response=e.getStackTrace.toString
      }
    }
    response
  }
  def dumpAllClusterCfgs: String ={
    var response=""
    try{
      response=MetadataAPIImpl.GetAllClusterCfgs("JSON", userid)
    }
    catch {
      case e: Exception => {
        response=e.getStackTrace.toString
      }
    }
    response
  }
  def dumpAllAdapters: String ={
    var response=""
    try{
      response=MetadataAPIImpl.GetAllAdapters("JSON", userid)
    }
    catch {
      case e: Exception => {
        response=e.getStackTrace.toString
      }
    }
    response
  }
}
