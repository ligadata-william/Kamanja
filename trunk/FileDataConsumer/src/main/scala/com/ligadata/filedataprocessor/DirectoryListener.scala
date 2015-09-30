package com.ligadata.filedataprocessor

import java.io.{IOException, File, PrintWriter}
import java.nio.file.{Path, FileSystems}

import com.ligadata.Exceptions.{InternalErrorException, MissingArgumentException}

/**
 * Created by danielkozin on 9/24/15.
 */
class DirectoryListener {

}

object LocationWatcher {
  def main (args: Array[String]) : Unit = {

      if (args.size == 0 || args.size > 1) {
        println("Smart File Consumer requires a configuration file as its argument")
        return
      }

      // Read the config and figure out how many consumers to start
      var config = args(0)
      var properties = scala.collection.mutable.Map[String,String]()

      val lines = scala.io.Source.fromFile(config).getLines.toList
      lines.foreach(line => {
        var lProp = line.split("=")
        properties(lProp(0)) = lProp(1)
      })

      var numberOfProcessors = properties(SmartFileAdapterConstants.NUMBER_OF_FILE_CONSUMERS).toInt
      var processors: Array[FileProcessor] = new Array[FileProcessor](numberOfProcessors)
      var threads: Array[Thread] = new Array[Thread](numberOfProcessors)
      var path: Path= null
      try {
         path = FileSystems.getDefault().getPath(properties(SmartFileAdapterConstants.DIRECTORY_TO_WATCH))
      } catch {
        case e: IOException => {
          println ("Unable to find the directory to watch")
          return
        }
      }

      println("Starting "+ numberOfProcessors+" file consumers, reading from "+ path)
      for (i <- 1 to numberOfProcessors) {
        var processor = new FileProcessor(path,i)
        processor.init(properties)
        val watch_thread = new Thread(processor)
        watch_thread .start
      }
  }
}
