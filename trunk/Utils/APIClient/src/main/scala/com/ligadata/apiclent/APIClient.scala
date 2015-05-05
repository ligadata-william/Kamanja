package com.ligadata.apiutils

import util.control.Breaks._

import scala.collection.mutable.{ArrayBuffer}
import uk.co.bigbeeconsultants.http._
import uk.co.bigbeeconsultants.http.response._
import uk.co.bigbeeconsultants.http.request._
import uk.co.bigbeeconsultants.http.header._
import uk.co.bigbeeconsultants.http.header.MediaType._
import request.Request
import header.Headers
import header.HeaderName._

import java.net.URL
import org.apache.log4j._
import java.io._
import java.util.Properties
import scala.io._

import com.netaporter.uri._
import com.netaporter.uri.dsl._
import com.netaporter.uri.Uri._

object APIClient {

  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  val config = Config(connectTimeout = 1200000,readTimeout = 1200000).allowInsecureSSL

  val usage = """
  Usage: APIClient [-l log4j_level] [-n nodeList] [-t http_req_type ] [-i userId] [-p password] [-r rolename] [-f file_name] [-u url] ...
  Where: 
  -t [GET|PUT|POST|DELETE]
  -i userId
  -p password
  -r roleName
  -f fileName(applicable to certain api functions)
  -u url
  -n nodeList
  -l [INFO|DEBUG|TRACE]
  """
  var logLevel: String = null
  var httpReqType: String = null
  var userId: String = null
  var password: String = null
  var roleName: String = null
  var fileName: String = null
  var url: String = null
  var nodeList: String = null
  var help: Boolean = false
  val unknown = "(^-[^\\s])".r

  def SetLoggerLevel(level: Level) {
    logger.setLevel(level);
  }

  def GetJarAsArrayOfBytes(jarName: String): Array[Byte] = {
    try {
      val iFile = new File(jarName)
      if (!iFile.exists) {
        throw new FileNotFoundException("Jar file (" + jarName + ") is not found: ")
      }

      val bis = new BufferedInputStream(new FileInputStream(iFile));
      val baos = new ByteArrayOutputStream();
      var readBuf = new Array[Byte](1024) // buffer size

      // read until a single byte is available
      while (bis.available() > 0) {
        val c = bis.read();
        baos.write(c)
      }
      bis.close();
      baos.toByteArray()
    } catch {
      case e: IOException => {
        throw new Exception("Failed to Convert the Jar (" + jarName + ") to array of bytes: " + e.getMessage())
      }
      case e: Exception => {
        throw new Exception("Failed to Convert the Jar (" + jarName + ") to array of bytes: " + e.getMessage())
      }
    }
  }

  def MakeHttpRequest(reqType: String, url:String, userId:String, 
		      password:String, roleName:String, fileName:String, fileType:String) :String = {
    try{
      var body:String = null
      var strBody: StringRequestBody = null
      var binaryBody: BinaryRequestBody = null
      var response:Response = null
      var httpClient = new HttpClient(config)
      
      val headers = Headers(
	new Header("Content-Type","application/json"),
	new Header("userid",userId),
	new Header("password",password),
	new Header("role",roleName)
      )

      if( reqType.toUpperCase == "GET"){
	  response = httpClient.get(new URL(url),headers)
      }
      else if( reqType.toUpperCase == "DELETE"){
	  response = httpClient.delete(new URL(url),headers)
      }
      else{
	if( fileName != null ){
	  if( fileType == "TEXT" ){
	    body = Source.fromFile(fileName).mkString	
	    strBody = new StringRequestBody(body,TEXT_PLAIN)
	    reqType.toUpperCase match {
	      case "PUT" => 
		response = httpClient.put(new URL(url),strBody,headers)
	      case "POST" => 
		response = httpClient.post(new URL(url),Some(strBody),headers)
	    }
	  }
	  else if ( fileType == "BINARY" ){
	    val ba = GetJarAsArrayOfBytes(fileName)
	    val binaryBody = new BinaryRequestBody(ba,APPLICATION_OCTET_STREAM)
	    reqType.toUpperCase match {
	      case "PUT" =>
		response = httpClient.put(new URL(url),binaryBody,headers)
	      case "POST" =>
		response = httpClient.post(new URL(url),Some(binaryBody),headers)
	    }
	  }
	  else{
	    throw new Exception("Unknown fileType " + fileType)
	  }
	}
      }
      return response.body.asString
    } catch {
      case e: Exception =>
	    throw new Exception(e.getMessage())
    }
  }

  val pf: PartialFunction[List[String], List[String]] = {
    case "-h" :: tail => help = true; tail
    case "-l" :: (arg: String) :: tail => logLevel = arg; tail
    case "-t" :: (arg: String) :: tail => httpReqType = arg; tail
    case "-i" :: (arg: String) :: tail => userId = arg; tail
    case "-p" :: (arg: String) :: tail => password = arg; tail
    case "-r" :: (arg: String) :: tail => roleName = arg; tail
    case "-f" :: (arg: String) :: tail => fileName = arg; tail
    case "-u" :: (arg: String) :: tail => url = arg; tail
    case "-n" :: (arg: String) :: tail => nodeList = arg; tail
    case unknown(bad) :: tail => die("unknown argument " + bad + "\n" + usage)
  }

  def parseArgs(args: List[String], pf: PartialFunction[List[String], List[String]]): List[String] = args match {
    case Nil => Nil
    case _ => if (pf isDefinedAt args) parseArgs(pf(args),pf) else args.head :: parseArgs(args.tail,pf)
  }

  def die(msg: String = usage) = {
    println(msg)
    sys.exit(1)
  }

  def main(args: Array[String]) {
    var fileType:String = null
    // if there are required args:
    if (args.length == 0) die()
    val arglist = args.toList
    val remainingopts = parseArgs(arglist,pf)

    if( logLevel != null ){
      logLevel match {
	case "DEBUG"  => SetLoggerLevel(Level.DEBUG)
	case "TRACE"  => SetLoggerLevel(Level.TRACE)
	case "INFO"   => SetLoggerLevel(Level.INFO)
	case "WARN"   => SetLoggerLevel(Level.WARN)
	case "ERROR"  => SetLoggerLevel(Level.ERROR)
	case _        => logger.error("Unknown value " + logLevel + " for log4j LogLevel, Should be one of DEBUG/TRACE/INFO/WARN/ERROR")
			 SetLoggerLevel(Level.DEBUG)
      }
    }
    
    if( help ){
      die()
    }

    logger.info("logLevel=" + logLevel)
    logger.info("userId=" + userId)
    logger.info("roleName=" + roleName)
    logger.info("httpReqType=" + httpReqType)
    logger.info("fileName=" + fileName)
    logger.info("url=" + url)
    logger.info("nodeList=" + nodeList)
    logger.info("remainingopts=" + remainingopts)

    // Validate arguments
    // Some arguments are required
    if( userId == null || password == null || httpReqType == null || url == null || nodeList == null ){
      die()
    }

    // httpReqType has to be one of get/put/post/delete
    val reqType = httpReqType.toLowerCase
    if( reqType != "put" && reqType != "post" && reqType != "get" && reqType != "delete" ){
      logger.error("Unknown value " + httpReqType + " for httpReqType,should be one of get/put/post/delete")
      return
    }
    
    // if httpReqType is "put" or "post", fileName must be supplied.
    // fileName must have the suffix .json or .xml or .jar
    if( httpReqType.toLowerCase == "put" || httpReqType.toLowerCase == "post"){
      if ( fileName == null ){
	logger.error("fileName can't be null for put/post requestsl")
	return
      } 
      else{
	// Make sure the file exists
	val fl = new File(fileName)
	if( fl.exists == false ){
	  logger.error("File " + fileName + " Couldn't be found")
	  return
	}
	if( fileName.toLowerCase.endsWith(".jar") ){
	  fileType = "BINARY"
	}
	else if( fileName.toLowerCase.endsWith(".json") || fileName.toLowerCase.endsWith(".xml") ){
	  fileType = "TEXT"
	}
	else{
	  logger.error("Unknown suffix for the given file " + fileName + ",should end with .jar/.json/.xml")
	  return
	}
      }
    }


      
    val nodes = nodeList.split(",")
    var leaderNode:String = null
    breakable{
      nodes.foreach( node => {
	val leaderUrl = "https://" + node + "/api/leader"
	try{
	  val res = MakeHttpRequest("get",leaderUrl,userId,password,roleName,null,null)
	  leaderNode = res
	  logger.info("Leader => " + leaderNode)
	  break
	} catch {
	  case e: Exception => {
	    logger.error("Failed to connect to " + leaderUrl)
	  }
	}
      })
    }
    
    if( leaderNode == null ){
      logger.error("Failed to find the leader node..")
    }

    // verify the input URL
    var uri:com.netaporter.uri.Uri = null
    try{
      uri = parse(url)
      logger.info("host => " + uri.host)
      logger.info("port => " + uri.port)
      logger.info("path => " + uri.path)
    } catch {
      case e:Exception => {
	logger.error("Failed to parse the input url " + url + ",error => " + e.getMessage())
	return
      }
    }
    
    // construct new url if the leader is different
    val currentNode = uri.host.get + ":" + uri.port.get
    var newUrl = url
    if( leaderNode != currentNode ){
      newUrl = "https://" + leaderNode + uri.path
      logger.info("Redirecting the request to new leader: New URL =>" + newUrl)
    }

    val res = MakeHttpRequest(httpReqType,newUrl,userId,password,roleName,fileName,fileType)
    logger.info("response => " + res)
  }
}
