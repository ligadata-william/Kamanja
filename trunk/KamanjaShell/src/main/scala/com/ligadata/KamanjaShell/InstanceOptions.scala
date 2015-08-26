package com.ligadata.KamanjaShell

import com.ligadata.Exceptions.KamanjaInvalidOptionsException

/**
 * @author danielkozin
 */
object InstanceOptions {
  
  val INVALID_OPTIONS: String = "Unable to parse options"
  /**
   * Factory for IntanceOptions
   */
  def getOptions(args: Array[String]): InstanceOptions = {
    var indx = 0
    var io = new InstanceOptions
    
    if (args.size % 2 != 0) throw new KamanjaInvalidOptionsException(this.INVALID_OPTIONS)
    
    // If no Options. return a basic Instance Options
    if (args.size == 0) {
      return io
    }
    
    // Go through arguments
    while (indx < args.size) {
      args(indx) match {
        case ("-zk") => {
          val zkParm = args(indx + 1).split(":")
          io.zkLocation = zkParm(0)
          io.zkPort = zkParm(1)
          io.needZk = false
          indx = indx + 2
        }
        case ("-kafka") => {
          val kafkaParm = args(indx + 1).split(":")
          io.kafkaLocation = kafkaParm(0)
          io.kafkaPort = kafkaParm(1)
          io.needKafka = false
          indx = indx + 2
        }
        case ("-fin") => {
          io.inputFile = args(indx + 1)
          indx = indx + 2
        }
        case ("-conf") => {
          io.configFile  = args(indx + 1)
          indx = indx + 2
        }
      }  
    }
    
    io
  }  
}

class InstanceOptions {
  // Set via the -zk option
  var zkLocation: String = "localhost"
  var zkPort: String = "2181"
  var needZk: Boolean = true
  
  // set via the -kafka option
  var kafkaLocation: String = "localhost"
  var kafkaPort: String = "9092"
  var needKafka: Boolean = true
  
  // scala home overwrite
  
  // java home overwrite
  
  // set via the -fin option
  var inputFile = ""
  
  // set config via -conf
  var configFile = ""
   
}