package main.scala.com.ligadata.MetadataAPI.Utility

import java.io.File

import com.ligadata.MetadataAPI.MetadataAPIImpl

import scala.io.Source

import org.apache.log4j._

/**
 * Created by dhaval on 8/12/15.
 */
object TypeService {
  private val userid: Option[String] = Some("metadataapi")
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)

  def addType(input: String): String ={
    var response = ""
    var typeFileDir: String = ""
    //val gitMsgFile = "https://raw.githubusercontent.com/ligadata-dhaval/Kamanja/master/HelloWorld_Msg_Def.json"
    if (input == "") {
      typeFileDir = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("TYPE_FILES_DIR")
      if (typeFileDir == null) {
        response = "TYPE_FILES_DIR property missing in the metadata API configuration"
      } else {
        //verify the directory where messages can be present
        IsValidDir(typeFileDir) match {
          case true => {
            //get all files with json extension
            val types: Array[File] = new java.io.File(typeFileDir).listFiles.filter(_.getName.endsWith(".json"))
            types.length match {
              case 0 => {
                println("Types not found at " + typeFileDir)
                "Types not found at " + typeFileDir
              }
              case option => {
                val typeDefs = getUserInputFromMainMenu(types)
                for (typeDef <- typeDefs) {
                  response += MetadataAPIImpl.AddTypes(typeDef.toString, "JSON", userid)
                }
              }
            }
          }
          case false => {
            //println("Message directory is invalid.")
            response = "Message directory is invalid."
          }
        }
      }
    } else {
      //input provided
      var message = new File(input.toString)
      val typeDef = Source.fromFile(message).mkString
      response = MetadataAPIImpl.AddTypes(typeDef.toString, "JSON", userid)
    }
    response
  }

  def getType: String ={
    var response=""
    try {
      val typeKeys = MetadataAPIImpl.GetAllKeys("TypeDef", None)
      if (typeKeys.length == 0) {
        val errorMsg="Sorry, No types available, in the Metadata, to display!"
        response=errorMsg
      }
      else{
        println("\nPick the type to be displayed from the following list: ")
        var srno = 0
        for(typeKey <- typeKeys){
          srno+=1
          println("["+srno+"] "+typeKey)
        }
        println("Enter your choice: ")
        val choice: Int = readInt()

        if (choice < 1 || choice > typeKeys.length) {
          val errormsg="Invalid choice " + choice + ". Start with the main menu."
          response=errormsg
        }
        val typeKey = typeKeys(choice - 1)
        val typeKeyTokens = typeKey.split("\\.")
        val typeNameSpace = typeKeyTokens(0)
        val typeName = typeKeyTokens(1)
        val typeVersion = typeKeyTokens(2)
        response = MetadataAPIImpl.GetType(typeNameSpace, typeName,"JSON",typeVersion, userid).toString

      }

    } catch {
      case e: Exception => {
        response=e.getStackTrace.toString
      }
    }
    response
  }
  def getAllTypes: String ={
     MetadataAPIImpl.GetAllTypes("JSON", userid)
  }

  def removeType: String ={
    var response=""
    try {
      val typeKeys =MetadataAPIImpl.GetAllKeys("TypeDef", None)

      if (typeKeys.length == 0) {
        val errorMsg="Sorry, No models available, in the Metadata, to delete!"
        //println(errorMsg)
        response=errorMsg
      }
      else{
        println("\nPick the model to be deleted from the following list: ")
        var srno = 0
        for(modelKey <- typeKeys){
          srno+=1
          println("["+srno+"] "+modelKey)
        }
        println("Enter your choice: ")
        val choice: Int = readInt()

        if (choice < 1 || choice > typeKeys.length) {
          val errormsg="Invalid choice " + choice + ". Start with the main menu."
          //println(errormsg)
          response=errormsg
        }
        val typeKey = typeKeys(choice - 1)
        val typeKeyTokens = typeKey.split("\\.")
        val typeNameSpace = typeKeyTokens(0)
        val typeName = typeKeyTokens(1)
        val typeVersion = typeKeyTokens(2)
        response = MetadataAPIImpl.RemoveModel(typeNameSpace, typeName, typeVersion.toLong, userid).toString
      }

    } catch {
      case e: Exception => {
        //e.printStackTrace
        response=e.getStackTrace.toString
      }
    }
    response
  }
  //NOT REQUIRED
  def loadTypesFromAFile: String ={
    var response="NOT REQUIRED. Please use the ADD TYPE option."
    response
  }

  def dumpAllTypesByObjTypeAsJson: String ={
    var response=""
    response
  }

  //utility
  def IsValidDir(dirName: String): Boolean = {
    val iFile = new File(dirName)
    if (!iFile.exists) {
      println("The File Path (" + dirName + ") is not found: ")
      false
    } else if (!iFile.isDirectory) {
      println("The File Path (" + dirName + ") is not a directory: ")
      false
    } else
      true
  }

  def   getUserInputFromMainMenu(messages: Array[File]): Array[String] = {
    var listOfMsgDef: Array[String] = Array[String]()
    var srNo = 0
    println("\nPick a Type Definition file(s) from below choices\n")
    for (message <- messages) {
      srNo += 1
      println("[" + srNo + "]" + message)
    }
    print("\nEnter your choice(If more than 1 choice, please use commas to seperate them): \n")
    val userOptions: List[Int] = Console.readLine().filter(_ != '\n').split(',').filter(ch => (ch != null && ch != "")).map(_.trim.toInt).toList
    //check if user input valid. If not exit
    for (userOption <- userOptions) {
      userOption match {
        case userOption if (1 to srNo).contains(userOption) => {
          //find the file location corresponding to the message
          var message = messages(userOption - 1)
          //process message
          val messageDef = Source.fromFile(message).mkString
          listOfMsgDef = listOfMsgDef :+ messageDef
        }
        case _ => {
          println("Unknown option: ")
        }
      }
    }
    listOfMsgDef
  }
}
