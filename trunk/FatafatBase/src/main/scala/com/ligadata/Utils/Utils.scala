
package com.ligadata.Utils

import com.google.common.base.Optional
import java.io.{ InputStream, FileInputStream, File }
import java.util.Properties

object Utils {
  val MaxTransactionsPerPartition: Long = 100000000000000L // 100T per partition (3 years of numbers if we process 1M/sec per partition), we can have 92,233 partitions per node (per EnvContext). At this moment we are taking it as global counter

  def SimpDateFmtTimeFromMs(tmMs: Long): String = {
    new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date(tmMs))
  }

  def GetCurDtTmStr: String = {
    SimpDateFmtTimeFromMs(GetCurDtTmInMs)
  }

  def GetCurDtTmInMs: Long = {
    System.currentTimeMillis
  }

  def elapsed[A](f: => A): (Long, A) = {
    val s = System.nanoTime
    val ret = f
    ((System.nanoTime - s), ret)
  }

  def toArrayStrings(args: String*): String = {
    val v = new StringBuilder(1024)
    // args.toArray.toString
    var cnt = 0
    for (arg <- args) {
      if (cnt > 0)
        v.append(",")
      cnt += 1
      if (arg != null && arg.length > 0)
        v.append(arg)
      else
        v.append("")
    }
    v.toString
  }

  def toArrayValidStrings(args: String*): String = {
    val v = new StringBuilder(1024)
    // args.toArray.toString
    var nextaddcomma = false
    for (arg <- args) {
      if (arg != null && arg.length > 0) {
        if (nextaddcomma)
          v.append(",")
        v.append(arg)
        nextaddcomma = true
      }
    }
    v.toString
  }

  def loadConfiguration(configFile: String, keysLowerCase: Boolean): (Properties, String) = {
    var configs: Properties = null
    var failStr: String = null
    try {
      val file: File = new File(configFile);
      if (file.exists()) {
        val input: InputStream = new FileInputStream(file)
        try {
          // Load configuration
          configs = new Properties()
          configs.load(input);
        } catch {
          case e: Exception =>
            failStr = "Failed to load configuration. Message:" + e.getMessage
            configs = null
        } finally {
          input.close();
        }
        if (keysLowerCase && configs != null) {
          val it = configs.entrySet().iterator()
          val lowercaseconfigs = new Properties()
          while (it.hasNext()) {
            val entry = it.next();
            lowercaseconfigs.setProperty(entry.getKey().asInstanceOf[String].toLowerCase, entry.getValue().asInstanceOf[String])
          }
          configs = lowercaseconfigs
        }
      } else {
        failStr = "Configuration file not found : " + configFile
        configs = null
      }
    } catch {
      case e: Exception =>
        failStr = "Invalid Configuration. Message: " + e.getMessage()
        configs = null
    }
    return (configs, failStr)
  }
  
  def optionToOptional[T](option: Option[T]): Optional[T] =
    option match {
      case Some(value) => Optional.of(value)
      case None => Optional.absent()
    }  
}
