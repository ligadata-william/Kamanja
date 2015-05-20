package com.ligadata.MetadataAPI

import com.ligadata.fatafat.metadata.ObjType._
import com.ligadata.fatafat.metadata._
import com.ligadata.fatafat.metadata.MdMgr._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import com.ligadata.Serialize._
import com.ligadata.Utils._
import util.control.Breaks._

object MetadataAPIOutputMsg {

  def AddOutputMessage(outputMsgText: String, format: String): String = {
    try {
      if (format != "JSON") {
        var apiResult = new ApiResult(ErrorCodeConstants.Not_Implemented_Yet, "AddOutputMsg", null, ErrorCodeConstants.Not_Implemented_Yet_Msg + ":" + outputMsgText)
        apiResult.toString()
      } else {
        var outputMsg = com.ligadata.outputmsgdef.OutputMsgDefImpl.parseOutputMessageDef(outputMsgText, "JSON")
        MetadataAPIImpl.SaveObject(outputMsg, MdMgr.GetMdMgr)
        MetadataAPIImpl.AddObjectToCache(outputMsg, MdMgr.GetMdMgr)
        var objectsAdded = new Array[BaseElemDef](0)
        objectsAdded = objectsAdded :+ outputMsg
        MetadataAPIImpl.SaveOutputMsObjectList(objectsAdded)
        val operations = for (op <- objectsAdded) yield "Add"
        logger.trace("Notify engine via zookeeper")
        MetadataAPIImpl.NotifyEngine(objectsAdded, operations)
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddOutputMsg", null, ErrorCodeConstants.Add_OutputMessage_Successful + ":" + outputMsgText)
        apiResult.toString()

      }
    } catch {
      case e: MappingException => {
        logger.trace("Failed to parse the output message, json => " + outputMsgText + ",Error => " + e.getMessage())
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddOutputMsg", null, "Error :" + ErrorCodeConstants.Add_OutputMessage_Failed + ":" + outputMsgText)
        apiResult.toString()
      }
      case e: AlreadyExistsException => {
        logger.trace("Failed to add the output message, json => " + outputMsgText + ",Error => " + e.getMessage())
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddOutputMsg", null, "Error :" + e.toString() + ErrorCodeConstants.Add_OutputMessage_Failed + ":" + outputMsgText)
        apiResult.toString()
      }
      case e: Exception => {
        logger.trace("Failed to up the output message, json => " + outputMsgText + ",Error => " + e.getMessage())
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddOutputMsg", null, "Error :" + e.toString() + ErrorCodeConstants.Add_OutputMessage_Failed + ":" + outputMsgText)
        apiResult.toString()
      }
    }
  }

  def UpdateOutputMsg(outputMsgText: String): String = {
    try {
      var outputMsgDef = com.ligadata.outputmsgdef.OutputMsgDefImpl.parseOutputMessageDef(outputMsgText, "JSON")
      val key = MdMgr.MkFullNameWithVersion(outputMsgDef.nameSpace, outputMsgDef.name, outputMsgDef.ver)
      val latestVersion = GetLatestOutputMsg(outputMsgDef)
      var isValid = true
      if (latestVersion != None) {
        isValid = MetadataAPIImpl.IsValidVersion(latestVersion.get, outputMsgDef)
        println("isValid  " + isValid)
      }
      if (isValid) {
        RemoveOutputMsg(latestVersion.get.nameSpace, latestVersion.get.name, latestVersion.get.ver)
        val result = AddOutputMsg(outputMsgDef)
        var objectsUpdated = new Array[BaseElemDef](0)
        var operations = new Array[String](0)
        objectsUpdated = objectsUpdated :+ latestVersion.get
        operations = operations :+ "Remove"
        objectsUpdated = objectsUpdated :+ outputMsgDef
        operations = operations :+ "Add"
        MetadataAPIImpl.NotifyEngine(objectsUpdated, operations)
        result
      } else {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateOutputMessage", null, "Error :" + null + ErrorCodeConstants.Update_OutputMessage_Failed + ":" + outputMsgText)
        apiResult.toString()
      }
    } catch {
      case e: ObjectNotFoundException => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateOutputMessage", null, "Error :" + e.toString() + ErrorCodeConstants.Update_OutputMessage_Failed + ":" + outputMsgText)
        apiResult.toString()
      }
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateOutputMessage", null, "Error :" + e.toString() + ErrorCodeConstants.Update_OutputMessage_Failed + ":" + outputMsgText)
        apiResult.toString()
      }
    }
  }

  // Get the latest output message for a given FullName
  def GetLatestOutputMsg(outputMsgDef: OutputMsgDef): Option[OutputMsgDef] = {
    try {
      var key = outputMsgDef.nameSpace + "." + outputMsgDef.name + "." + outputMsgDef.ver
      val o = MdMgr.GetMdMgr.OutputMessages(outputMsgDef.nameSpace.toLowerCase,
        outputMsgDef.name.toLowerCase,
        false,
        true)
      o match {
        case None =>
          None
          logger.trace("Output msg not in the cache => " + key)
          None
        case Some(o) =>
          if (o.size > 0) {
            logger.trace("output msg found => " + o.head.asInstanceOf[OutputMsgDef].FullNameWithVer)
            Some(o.head.asInstanceOf[OutputMsgDef])

          } else
            None
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }

  // Add Output (outputmsg def)
  def AddOutputMsg(outputMsgDef: OutputMsgDef): String = {
    try {
      var key = outputMsgDef.FullNameWithVer
      MetadataAPIImpl.SaveObject(outputMsgDef, MdMgr.GetMdMgr)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddOutputMsg", null, ErrorCodeConstants.Add_OutputMessage_Successful + ":" + key)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddOutputMsg", null, "Error :" + e.toString() + ErrorCodeConstants.Add_OutputMessage_Failed + ":" + outputMsgDef.Name)
        apiResult.toString()
      }
    }
  }

  def GetAllOutputMsgsFromCache(active: Boolean): Array[String] = {
    var outputMsgList: Array[String] = new Array[String](0)
    try {
      val outputMsgDefs = MdMgr.GetMdMgr.OutputMessages(active, true)
      outputMsgDefs match {
        case None =>
          None
          logger.debug("No Output Msgs found ")
          outputMsgList
        case Some(o) =>
          val os = o.toArray
          val oCount = os.length
          outputMsgList = new Array[String](oCount)
          for (i <- 0 to oCount - 1) {
            outputMsgList(i) = os(i).FullNameWithVer
          }
          outputMsgList
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException("Failed to fetch all the OutputMsgs:" + e.toString)
      }
    }
  }

  // Remove output message with OutputMsg Name and Version Number
  def RemoveOutputMsg(nameSpace: String, name: String, version: Long): String = {
    var key = nameSpace + "." + name + "." + version
    try {
      val om = MdMgr.GetMdMgr.OutputMessage(nameSpace.toLowerCase, name.toLowerCase, version, true)
      om match {
        case None =>
          None
          logger.debug("output message not found => " + key)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveOutputMsg", null, ErrorCodeConstants.Remove_OutputMessage_Failed + ":" + key)
          apiResult.toString()
        case Some(o) =>
          logger.debug("output message found => " + o.asInstanceOf[OutputMsgDef].FullNameWithVer)
          MetadataAPIImpl.DeleteObject(o.asInstanceOf[OutputMsgDef])
          var objectsUpdated = new Array[BaseElemDef](0)
          objectsUpdated = objectsUpdated :+ o.asInstanceOf[OutputMsgDef]
          var operations = for (op <- objectsUpdated) yield "Remove"
          MetadataAPIImpl.NotifyEngine(objectsUpdated, operations)
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveOutputMsg", null, ErrorCodeConstants.Remove_OutputMessage_Successful + ":" + key)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveOutputMsg", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_OutputMessage_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

}