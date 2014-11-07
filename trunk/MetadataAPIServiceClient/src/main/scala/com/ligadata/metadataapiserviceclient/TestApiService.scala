package com.ligadata.metadataapiserviceclient

import uk.co.bigbeeconsultants.http._
import uk.co.bigbeeconsultants.http.response._
import uk.co.bigbeeconsultants.http.request._
import uk.co.bigbeeconsultants.http.header.MediaType._

import java.net.URL
import org.apache.log4j._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.io._
case class APIResultInfo(statusCode:Int, statusDescription: String, resultData: String)
case class APIResultJsonProxy(APIResults: APIResultInfo)

case class ApiResultParsingException(e: String) extends Throwable(e)
case class Json4sParsingException(e: String) extends Throwable(e)

object GetModel {
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)

  @throws(classOf[Json4sParsingException])
  @throws(classOf[ApiResultParsingException])
  def getApiResult(apiResultJson: String): (Int,String) = {
    // parse using Json4s
    try{
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(apiResultJson)
      logger.trace("Parsed the json : " + apiResultJson)
      val apiResultInfo = json.extract[APIResultJsonProxy]
      (apiResultInfo.APIResults.statusCode,apiResultInfo.APIResults.resultData)
    } catch {
      case e:MappingException =>{
	e.printStackTrace()
	throw Json4sParsingException(e.getMessage())
      }
      case e:Exception => {
	e.printStackTrace()
	throw new ApiResultParsingException(e.getMessage())
      }
    }
  }

  def GetResponse(url:String,body: Option[String]): String = {
    val httpClient = new HttpClient
    var response:Response = null
    if( body == None){
      response = httpClient.get(new URL(url))
    }
    else{
      val requestBody = new StringRequestBody(body.get,TEXT_XML)
      response = httpClient.put(new URL(url),requestBody)
    }
    logger.trace("response.status => " + response.status)
    logger.trace("response.body   => " + response.body.asString)
    val(statusCode,resultData) = getApiResult(response.body.asString)
    logger.trace(resultData)
    resultData
  }


  def main(args: Array[String]) {
    logger.setLevel(Level.TRACE);
    val httpClient = new HttpClient
    // GetModel
    logger.trace(GetResponse("http://127.0.0.1:8080/api/GetModel/system/copdriskassessment_000100/100",None))
    // GetType
    logger.trace(GetResponse("http://127.0.0.1:8080/api/GetType/arrayoflong",None))
    // AddModel
    val dirName = "/home/vmandava/github/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Models"
    val pmmlFilePath = dirName + "/COPD_V200.xml"
    val pmmlStr = Source.fromFile(pmmlFilePath).mkString    
    logger.trace(GetResponse("http://127.0.0.1:8080/api/AddModel",Some(pmmlStr)))
  }
}
