name := "KafkaSimpleInputOutputAdapters"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "log4j" % "log4j" % "1.2.17"

resolvers += "Apache repo" at "https://repository.apache.org/content/repositories/releases"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.1.1"

libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-actors" % _)

