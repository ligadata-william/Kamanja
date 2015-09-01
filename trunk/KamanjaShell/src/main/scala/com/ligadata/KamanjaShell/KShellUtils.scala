package com.ligadata.KamanjaShell

import java.io.File

/**
 * @author danielkozin
 */
object KShellUtils {
  
    /**
   * save a file to a given directory.  Used to create Zookeepr and Kafka  Configs to
   * start internal processes.
   */
  def savePropFile(props: java.util.Properties, fname:String, path: String) : Unit = {
    var cfile: File = new File(path +"/"+fname)
    var fileOut = new java.io.FileOutputStream(cfile)
    props.store(fileOut, "Kamanja Generated Configuration file")
    fileOut.close
  }
  
  
  /**
   *  Scan the next 100 ports  - These are internal ports and always use LOCALHOST
   */
  def getAvailablePort(port: String): Int = {
    var finalPort = port.toInt
    var stopSearch = false
    var socket: java.net.Socket = null
    
    // Scan until you find the open port or you run out 100 ports.
    while (!stopSearch && finalPort < port.toInt + 100) {
      try {   
        // A little weird, but if this does not cause an exception,
        // that means that the port is open, and being used by someone.
        socket = new java.net.Socket("localhost", finalPort)
        socket.close
        finalPort = finalPort + 1 
      } catch {
        case e: Exception => {
          // Port is not opened.. Use it!
          if (socket != null) socket.close
          stopSearch = true
        }
      }           
    }
    if (stopSearch) return finalPort else return -1
  }
  
}