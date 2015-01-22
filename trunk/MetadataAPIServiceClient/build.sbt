name := "MetadataAPIServiceClient"

version := "0.1"

scalaVersion := "2.10.3"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= Seq(
    "uk.co.bigbeeconsultants" %% "bee-client" % "0.21.6",
    "org.slf4j" % "slf4j-api" % "1.7.10",
    "ch.qos.logback" % "logback-core"    % "1.0.13",
    "ch.qos.logback" % "logback-classic" % "1.0.13"
)

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.9" 

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.9" 

resolvers += "Big Bee Consultants" at "http://repo.bigbeeconsultants.co.uk/repo"
