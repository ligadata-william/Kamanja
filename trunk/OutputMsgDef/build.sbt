name := "OutputMsgDef"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies += "metadata" % "metadata_2.10" % "1.0"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.9"

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.9" 

libraryDependencies ++= Seq("junit" % "junit" % "4.8.1" % "test")

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.1.2" % "test"

