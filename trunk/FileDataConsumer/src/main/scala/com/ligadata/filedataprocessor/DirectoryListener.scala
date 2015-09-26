package com.ligadata.filedataprocessor

import java.io.{File, PrintWriter}
import java.nio.file.FileSystems

/**
 * Created by danielkozin on 9/24/15.
 */
class DirectoryListener {

}

object LocationWatcher {
  def main (args: Array[String]) : Unit = {

    if (args.length > 0 && args(0).equalsIgnoreCase("genFile")) {
      val pw = new PrintWriter(new File("/tmp/shortTestFile.txt" ))
      for (i <- 0 until 2) {
        pw.write("1___eventSource\u0001Application\u0001type\u0001userActivityEvent\u0001id\u0001076f5140-0e8e-41c2-b158-06cbff4a89e6\u0001dateTime\u00012015-09-05T02:04:09.2255288Z\u0001timezone\u0001-07:00\u0001systemUser\u0001NBKNABA\u0001device\u0001F8851FBC63F98\u0001processId\u000111084\u0001processName\u0001SimpleConsole\u0001resource\u0001statement.pdf\u0001resourceType\u0001file\u0001resourceHost\u0001acme.com\u0001resourcePort\u000180\u0001resourceProtocol\u0001http\u0001clientIp\u0001127.0.0.0\u0001appId\u0001SimpleConsoleAppId\u0001user\u0001nbkxxxx\u0001action\u0001download\u0001result\u00010\u0001confidentialDataLabels\u0001firstname;lastname;ssntin;myCustomElement\u0001confidentialRecordCount\u000130\u0001proprietaryDataLabels\u0001PersonNumber;PhoneNumber\u0001proprietaryDataValues\u0001123456789;\u0001proprietaryRecordCount\u00011\u0001myCustomProperty\u0001Custom Value\n")
        pw.write("2___eventSource\u0001Application\u0001type\u0001userActivityEvent\u0001id\u0001f2dd5d45-9c1f-48e4-b3cb-2e283716ee66\u0001dateTime\u00012015-09-05T02:04:09.2595288Z\u0001timezone\u0001-07:00\u0001systemUser\u0001NBKNABA\u0001device\u0001F8851FBC63F98\u0001processId\u000111084\u0001processName\u0001SimpleConsole\u0001resource\u0001statement.pdf\u0001resourceType\u0001file\u0001resourceHost\u0001acme.com\u0001resourcePort\u000180\u0001resourceProtocol\u0001http\u0001clientIp\u0001127.0.0.0\u0001appId\u0001SimpleConsoleAppId\u0001user\u0001nbkxxxx\u0001action\u0001download\u0001result\u00011\u0001errorMessage\u0001Access denied\u0001confidentialDataLabels\u0001firstname;lastname;ssntin\u0001confidentialRecordCount\u000130\u0001proprietaryDataLabels\u0001PersonNumber;PhoneNumber\u0001proprietaryDataValues\u0001123456789;\u0001proprietaryRecordCount\u00011\u0001corrId\u0001e0c01ef3-c24a-4f70-9516-4543c80c5fa4\n")
        pw.write("3___eventSource\u0001Application\u0001type\u0001userActivityEvent\u0001id\u0001e5c405fb-27c7-4b69-8701-ea6bb32389cf\u0001dateTime\u00012015-09-05T02:04:09.2595288Z\u0001timezone\u0001-07:00\u0001systemUser\u0001NBKNABA\u0001device\u0001F8851FBC63F98\u0001processId\u000111084\u0001processName\u0001SimpleConsole\u0001resource\u0001statement.pdf\u0001resourceType\u0001file\u0001resourceHost\u0001acme.com\u0001resourcePort\u000180\u0001resourceProtocol\u0001http\u0001clientIp\u0001127.0.0.0\u0001appId\u0001SimpleConsoleAppId\u0001user\u0001nbkxxxx\u0001action\u0001download\u0001result\u00010\u0001confidentialRecordCount\u000130\u0001proprietaryDataLabels\u0001PersonNumber;PhoneNumber\u0001proprietaryDataValues\u0001123456789;\u0001proprietaryRecordCount\u00011\n")
      }
      pw.close
    } else {
      var config = args(0)
      var properties = scala.collection.mutable.Map[String,String]()
      val lines = scala.io.Source.fromFile(config).getLines.toList
      lines.foreach(line => {
        var lProp = line.split("=")
        properties(lProp(0)) = lProp(1)
      })


      val path = FileSystems.getDefault().getPath("/tmp/watch")

      // Create Consumers for partition ID 1 and 2
      val dir_watcher_1 = new FileProcessor(path, 1)
      val dir_watcher_2 = new FileProcessor(path, 2)

      dir_watcher_1.init(properties)
     // dir_watcher_2.init(properties)

      val watch_thread1 = new Thread(dir_watcher_1)
      val watch_thread2 = new Thread(dir_watcher_2)
      watch_thread1.start()
     // watch_thread2.start()
    }
  }
}
