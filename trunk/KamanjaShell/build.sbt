name := "KamanjaShell"

version := "0.1.0"

scalaVersion := "2.10.4"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.1.1"

libraryDependencies ++= Seq (
  "com.101tec" % "zkclient" % "0.4",
  "org.apache.kafka" % "kafka_2.10" % "0.8.1.1"
    exclude("javax.jms", "jms")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("com.sun.jmx", "jmxri")
)


