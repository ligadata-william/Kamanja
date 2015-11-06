import sbt._
import Keys._

name := "CleanUtil"

version := "1.0"

scalaVersion := "2.10.4"

shellPrompt := { state =>  "sbt (%s)> ".format(Project.extract(state).currentProject.id) }

libraryDependencies ++= Seq (
  "com.101tec" % "zkclient" % "0.6",
  "org.apache.kafka" % "kafka_2.10" % "0.8.2.2",
  "log4j" % "log4j" % "1.2.17",
  "org.json4s" %% "json4s-native" % "{latestVersion}",
  "org.json4s" %% "json4s-jackson" % "{latestVersion}",
  "org.apache.curator" % "curator-client" % "2.6.0",
  "org.apache.curator" % "curator-framework" % "2.6.0",
  "org.apache.curator" % "curator-recipes" % "2.6.0",
  "org.apache.curator" % "curator-test" % "2.8.0"
    exclude("javax.jms", "jms")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("com.sun.jmx", "jmxri")
)

fork := true