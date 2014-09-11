name := "FileSimpleInputOutputAdapters"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-actors" % _)

