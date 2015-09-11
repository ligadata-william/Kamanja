name := "JpmmlExample"

version := "1.0"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-feature", "-deprecation")

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.9"

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.9" 

libraryDependencies += "com.google.guava" % "guava" % "18.0" 

libraryDependencies += "org.jpmml" % "pmml-evaluator" % "1.2.4"

libraryDependencies += "org.jpmml" % "pmml-model" % "1.2.5"

libraryDependencies += "org.jpmml" % "pmml-schema" % "1.2.5"

libraryDependencies += "commons-codec" % "commons-codec" % "1.10"

libraryDependencies += "commons-io" % "commons-io" % "2.4"