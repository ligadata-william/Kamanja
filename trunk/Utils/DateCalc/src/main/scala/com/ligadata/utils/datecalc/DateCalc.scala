package com.ligadata.utils.datecalc

import scala.collection.mutable._
import scala.collection.immutable.Seq
import org.joda.time.base
import org.joda.time.chrono
import org.joda.time.convert
import org.joda.time.field
import org.joda.time.format
import org.joda.time.tz
import org.joda.time.LocalDate
import org.joda.time.DateTime
import org.joda.time.Years
import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.chrono.JulianChronology

object DateCalc extends App{

	def usage : String = {
"""	
Simple date calculator.  Specify date expression, an arithmetic operator, a second expression's unit type and value, and an output format.
	  
Usage: scala com.ligadata.utils.DateCalc --expr1 <some date time string that can be interpreted by joda> 
                                         --expr1Format
                                         --op <classpath>
                                         --expr2Type <the type of the expr2>
                                         --expr2 <some integer expression> 
                                         --format <the joda output format to be used to print the answer>
      where expr1's value is a parsable string that represents some date that can be parsed by the joda formatter
            expr1Format is the format the parser will use to interpret expr1
            op is the arithmetic operator to use upon the two expressions... currently any in{+,-}
            expr2Type describes the unit measure of expr2... currently any in {second,minute,hour,day})
            expr2's value... a string that forms a date or an integer value of one of the unit types
            format describes the output format using the joda format codes (see below). The default 
                    chronology is assumed; no support for alternate chronologies (e.g., JulianChronology) at this time
	  
	  Example (subtract 10 minutes from expr1; emit the resulting date in the supplied format parameter): 

	  DateCalc-1.0 --expr1 "2015-04-16 15:50:50"  --expr1Format "yyyy-MM-dd HH:mm:ss" 
					--op "-" 
					--expr2Type "minute" --expr2 "10" 
					--format "yyyy-MM-dd HH:mm"
	  
	  joda Format controls:
	  
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

	  
"""
	}
	  
    override def main (args : Array[String]) {
 	  
		val arglist = args.toList
		type OptionMap = scala.collection.mutable.Map[Symbol, String]
		def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
			list match {
		    	case Nil => map
		    	case "--op" :: value :: tail =>
		    						nextOption(map ++ Map('op -> value), tail)
		    	case "--expr1Format" :: value :: tail =>
		    						nextOption(map ++ Map('expr1Format -> value), tail)
		    	case "--expr1" :: value :: tail =>
		    						nextOption(map ++ Map('expr1 -> value), tail)
		    	case "--expr2" :: value :: tail =>
		    						nextOption(map ++ Map('expr2 -> value), tail)
		    	case "--expr2Type" :: value :: tail =>
		    						nextOption(map ++ Map('expr2Type -> value), tail)
		    	case "--expr2Format" :: value :: tail =>
		    						nextOption(map ++ Map('expr2Format -> value), tail)
		    	case "--format" :: value :: tail =>
		    						nextOption(map ++ Map('format -> value), tail)
		    	case option :: tail => {
		    		println("Unknown option " + option)
					val usageMsg : String = usage
					println(s"$usageMsg")
		    		sys.exit(1)
		    	}
		        	 
		  }
		}
		
		val options = nextOption(Map(),arglist)
		val op = if (options.contains('op)) options.apply('op) else null 
		val expr1 = if (options.contains('expr1)) options.apply('expr1) else null
		val expr1Format = if (options.contains('expr1Format)) options.apply('expr1Format) else null
		val expr2Format : String = null
		val expr2 = if (options.contains('expr2)) options.apply('expr2) else null
		val expr2Type = if (options.contains('expr2Type)) options.apply('expr2Type) else null
		val format = if (options.contains('format)) options.apply('format) else null
		
		val reasonable : Boolean = (op != null && expr1 != null && expr1Format != null && expr2 != null && expr2Type != null && format != null) 
		val proceed : Boolean = if (reasonable) {
			if (op != "+" && op != "-") {
				println("illegal operator specified...")
				println(usage)
				sys.exit(1)
			}
			if (expr2Type != "second" && expr2Type != "minute" && expr2Type != "hour" && expr2Type != "day") {
				println("illegal expression type specified...")
				println(usage)
				sys.exit(1)
			}
			if (expr2Format != null) {
				println("Not supporting expression 2 format as yet... coming some future sprint...")
				println(usage)
				sys.exit(1)
			}
			true			  
		} else {
			println("insufficient arguments...")
			println(usage)
			sys.exit(1)
			false
		}
		
       	val formatter : DateTimeFormatter = DateTimeFormat.forPattern(expr1Format)
		val dateTime : DateTime  = formatter.parseDateTime(expr1)

		val incr : Int = if (op == "+") expr2.toInt else (expr2.toInt * -1)
		val resultDate : DateTime = expr2Type match {
			case "second" => dateTime.plusSeconds(incr)
			case "minute" => dateTime.plusMinutes(incr)
			case "hour" => dateTime.plusHours(incr)
			case "day" => dateTime.plusDays(incr)
			case _ => {
				println ("illegal dateType...")
				println (usage)
				sys.exit(1)
				new DateTime
			}
		}

		/** format it */

		val fmt : DateTimeFormatter  = DateTimeFormat.forPattern(format)
	  	val fmtdDate : String = fmt.print(resultDate)

	  	println(fmtdDate)

	}

}
