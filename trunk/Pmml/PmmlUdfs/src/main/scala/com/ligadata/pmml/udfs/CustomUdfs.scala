package com.ligadata.pmml.udfs

import java.util.UUID
import org.joda.time.{LocalDate, DateTime}
import com.ligadata.pmml.udfs._
import com.ligadata.pmml.udfs.Udfs._
import scala.reflect.runtime.universe._
import org.joda.time.LocalDate
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.apache.log4j.Logger
import com.ligadata.Pmml.Runtime._

/**
  * Sample udfs .. com.ligadata.pmml.udfs.SomeCustomUdfs
    Add this file to the trunk/Pmml/PmmlUdfs/src/main/scala/com/ligadata/udfs folder 
    so it is a sibling to the PmmlUdfs.scala file.  It will be built when run
    the easy installer from source.
    Notice the full package qualifed name is com.ligadata.pmml.udfs.SomeCustomUdfs
    This will be needed later when you want to use it in your model.
    Once it is built, you can run the extractUDFLibMetadata.scala script on it that
    will produce the metadata from it.
    Assuming you have the sbtProjDependencies.scala and extractUDFLibMetadata.scala
    scripts in a folder on your PATH, you can use this command from 'trunk' to compile it:
    extractUdfLibMetadata.scala --sbtProject PmmlUdfs 
                                --fullObjectPath com.ligadata.pmml.udfs.SomeCustomUdfs
                                --namespace Pmml
                                --versionNumber 100
                                --typeDefsPath /tmp/typesforSomeCustomUdfs.json
                                --fcnDefsPath /tmp/fcnsforSomCustomUdfs.json
    The last two arguments are paths to the json that is produced by the script looking
    into the udf jar for PmmlUdfs at the functions on the 'fullObjectPath'.
    As written, the types do not really need to be loaded into the MetadataAPI, as they 
    have all been defined in the bootstrap.  The udfs json file should be loaded however.
    To use this udf library in one of your models, you need to reference it in the model
    itself.  In the DataDictionary section, put the following:
        <DataField name="UDFSearchPath" displayName="UDFSearchPath" dataType="container">
          <Value value="com.ligadata.pmml.udfs.SomeCustomUdfs" property="valid"/>
        </DataField>
    That's about it for now.  Eventually we will support udfs in other package spaces and
    not included in the pmml project.  For now this is what is done so other issues more
    pressing can be addressed.
  */
object CustomUdfs extends UdfBase with LogTrait  {
  
    /**
     * Returns a random UUID string
     */
    def ID_GEN() : String = {
        UUID.randomUUID().toString;
    }

   /**
       Convert the supplied iso8601 date integers according to these format codes:
      
     Symbol  Meaning                      Presentation  Examples
     ------  -------                      ------------  -------
     G       era                          text          AD
     C       century of era (>=0)         number        20
     Y       year of era (>=0)            year          1996
    
     x       weekyear                     year          1996
     w       week of weekyear             number        27
     e       day of week                  number        2
     E       day of week                  text          Tuesday; Tue
    
     y       year                         year          1996
     D       day of year                  number        189
     M       month of year                month         July; Jul; 07
     d       day of month                 number        10
    
     a       halfday of day               text          PM
     K       hour of halfday (0~11)       number        0
     h       clockhour of halfday (1~12)  number        12
    
     H       hour of day (0~23)           number        0
     k       clockhour of day (1~24)      number        24
     m       minute of hour               number        30
     s       second of minute             number        55
     S       fraction of second           number        978
    
     z       time zone                    text          Pacific Standard Time; PST
     Z       time zone offset/id          zone          -0800; -08:00; America/Los_Angeles
    
     '       escape for text              delimiter
     ''      single quote                 literal    
     
     @param fmtStr: a String specifying the desired format.
     @param yyyymmdds: one or more iso8601 dates... only the last will print
     @return string rep of this date
     */
    def dateBlock(fmtStr : String, yyyymmdds : Any*): String = {
        val dateTime : DateTime = toDateTime(yyyymmdds.toList.last.asInstanceOf[Int])
        val fmt : DateTimeFormatter  = DateTimeFormat.forPattern(fmtStr);
        val str : String = fmt.print(dateTime);
        str
    }

    /** 
     *  Print the two strings to the log.  The first is some location or context information.  The second
     *  is the event description.
     *  
     *  @param severity  a string describing log severity level ... any {error, warn, info, debug, trace}
     *  @param contextMsg  a string describing the context of why this message is to be logged (or anything else for that matter)
     *  @param eventMsg a string that describes what has actually happened
     *  @param bool a Boolean that is returned as this function's result (to play into the pmml logic as desired)
     *  @return bool
     */
    def LogMsg(severity : String, contextMsg : String, eventMsg : String, bool : Boolean) : Boolean = {
        if (severity != null && contextMsg != null && eventMsg != null) {
            val sev : String = severity.toLowerCase
            sev match {
                case "error" => logger.error(s"$contextMsg...$eventMsg")
                case "warn" => logger.warn(s"$contextMsg...$eventMsg")
                case "info" => logger.info(s"$contextMsg...$eventMsg")
                case "debug" => logger.debug(s"$contextMsg...$eventMsg")
                case "trace" => logger.trace(s"$contextMsg...$eventMsg")
                case _ => logger.trace(s"$contextMsg...$eventMsg")
            }
        } else {
          logger.error("LogMsg called with bogus arguments")
        }
        bool
    }
    
    /** 
     *  Accept an indefinite number of objects and concatenate their string representations 
     *  @param args : arguments whose string representations will be concatenated   
     *  @return concatenation of args' string representations
     */
    def Concat(args : Any*) : String = {
      val argList : List[Any] = args.toList
      val buffer : StringBuilder = new StringBuilder
      argList.foreach ( arg => if (arg != null) buffer.append(arg.toString) else "" )
      val concatenation : String = buffer.toString
      concatenation
     }
  
    /** 
     *  Accept a parent variable, a child variable and a replacement variable.  Replace all instances of the child  inside the parent with the replacement.  The function will return a string
     *  @param relacewithin : The parent variable (can be any type) within which the replacement will be searched for
     *  @param inWord : The child string which will be searched for within the "replacewithin" variable
     *  @param replacewith : The string with which all instances of the child will be replaced in "replacewithin"
     *  @return outString : string where all "inwords" have been replaced by "replacewith"
     */
    
    def replace (replacewithin: Any, inWord: Any, replacewith: Any): String = {
      var  replacewithinA:String=""
      var inWordA:String=""
      var replacewithA=""
      if (replacewithin != null){
          replacewithinA = replacewithin.toString
      }
      else{
        throw new IllegalArgumentException("replacewithin should not be null value")
      } 
      if(inWord !=null){
         inWordA = inWord.toString
      }
      else
      {
        throw new  IllegalArgumentException("inWord should not be null value")
      }
      if(replacewith !=null){
         replacewithA = replacewith.toString
      }
      else
      {
        return replacewithin.toString()
      }
      var outString = replacewithinA.replaceAll(inWordA, replacewithA)
      outString
    }
  
    /** 
     *  Accept a parent variable and a child variable.  Return a boolean value which is true if an instance of the child lies within the parent and false otherwise
     *  @param matchwithin : The parent variable (can be any type) within which the matching variable will be searched for
     *  @param matchwith : The child that will be searched for within "matchwithin"
     *  @return OutBool : Boolean value evaluating whether an instance of "matchwith" exists within "matchwithin"
     */
    def matches (matchwithin: Any, matchwith: Any): Boolean = {
      if (matchwithin ==null) return false
      if (matchwith==null) return false
      var matchwithinA = matchwithin.toString
      var matchwithA = matchwith.toString
      var outString = matchwithinA.replaceAll(matchwithA, matchwithA + matchwithA)
      var outBool = if (outString == matchwithinA) false; else true;
      outBool
    }

    /** 
     *  Generate a random Double between 0 and 1
     *  @return ranDouble : random Double between 0 and 1
     */

    def random(): Double = {
      val r = scala.util.Random
      var randouble = r.nextDouble
      randouble
    }

    /** 
     *  Accept a number of any type and format it in a specified manner
     *  @param num : The number which is to be formatted
     *  @param formatting : The format which the number is to take.  This should be given in standard form, e.g. %.2f for a 2 decimal place float
     *  @return formattedStr : A string version of the number formatted in the required way
     */
    def formatNumber[T]( num : T, formatting : String) : String = {
      if(!num.isInstanceOf[java.lang.Number]){
        throw new IllegalArgumentException("num should be instence of java.lang.Number")
      }
      val formattedStr : String = (formatting.format(num)).toString
      formattedStr
    }
  
    /**
     * matchTermsetCount - Method will analyze a given string for the presence of specified tokens and return the Array of integers where
     *                      each element corresponds to the number of times a token in that position of the context array appears in the 
     *                      inputString.  This method is CASE INSENSITIVE 
     *                      
     *  @param String: String to analyze
     *  @param Array[String]: The list of tokens to compare the inputString to
     *  @return Array[Integer]
     */
    def matchTermsetCount (inputString: String, context: Array[String]): Array[Integer] = {           
      var pos = 0
      var lcIS = inputString.toLowerCase
      var outArray: Array[Integer] = new Array[Integer](context.size) 
      context.foreach(word => {  

        // Tricky thing here... this will not pick up a word if it is a first or the 
        // last word in an inputString. so we need to do some fancy sting checking to
        // handle these 2 cases.
        // General case... chech for a middle word in the inputString   
         outArray(pos) =  (" "+word+" ").toLowerCase.r.findAllIn(lcIS).length
 
        // Check for the last word in an input string
        if (lcIS.endsWith(" "+word.toLowerCase)) outArray(pos) += 1
        // Check for the first word in an input string
        if (lcIS.startsWith(word.toLowerCase+" ")) outArray(pos) += 1
		if (lcIS.equalsIgnoreCase(word)) outArray(pos) += 1
        println("   ==>"+outArray(pos))  
        pos = pos + 1
      })
      outArray
    }
	
	def getMatchingTokens (inputString: String, context: Array[String]): String = {
      if(inputString==null) return ""
      if(context==null)return ""
      var pos = 0
      var lcIS:String = inputString.toLowerCase
      var outArray:String = ""
      context.foreach(word => {  

        // Tricky thing here... this will not pick up a word if it is a first or the 
        // last word in an inputString. so we need to do some fancy sting checking to
        // handle these 2 cases.
        // General case... chech for a middle word in the inputString   
        var num =  (" "+word+" ").toLowerCase.r.findAllIn(lcIS).length
 
        // Check for the last word in an input string
        if (lcIS.endsWith(" "+word.toLowerCase)) num += 1
        // Check for the first word in an input string
        if (lcIS.startsWith(word.toLowerCase+" ")) num += 1    
		if (lcIS.equalsIgnoreCase(word)) num += 1
        if (num > 0) {
          outArray = outArray + "." + word
        }
      })
      outArray
    }
  
    /**
     * matchTermsetBoolean - Method will analyze a given string for the presence of tokens specified in the context parameters.  If the number of
     *                      present tokens exceeds the degree parameter, return TRUE, else return FALSE.  This method is CASE INSENSITIVE 
     *                        
     *                      
     *  @param String: String to analyze
     *  @param Array[String]: The list of tokens to compare the inputString to
     *  @param Integer: the threshold. 
     *  @return Boolean
     */
    def matchTermsetBoolean (inputString: String, context: Array[String], degree: Integer): Boolean = {
      var total: Int = 0
      if(inputString==null) return false
      if(context==null)return false
      matchTermsetCount(inputString, context).foreach(v => {total = total + v})
      if (total >= degree) true else false
    }
	def Length(str : String)  : Int = {
       val len : Int = if (str != null) str.size else 0
       len
	   }
}