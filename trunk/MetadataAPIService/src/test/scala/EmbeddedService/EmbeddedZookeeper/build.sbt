name := "EmbeddedZookeeper"

version := "1.0"

shellPrompt := { state => "sbt (%s)> ".format(Project.extract(state).currentProject.id) }

libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.4.0" exclude("com.sun.jmx", "jmxri") exclude("com.sun.jdmk", "jmxtools") exclude("javax.jms", "jms")

libraryDependencies ++= Seq(
"org.apache.curator" % "curator-client" % "2.6.0",
"org.apache.curator" % "curator-framework" % "2.6.0",
"org.apache.curator" % "curator-recipes" % "2.6.0"
)