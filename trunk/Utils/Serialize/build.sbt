name := "Serialize"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.0"

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.9" 

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.9" 

libraryDependencies ++= Seq(
"com.twitter" %% "chill" % "0.3.6",
"org.scalamacros" % "quasiquotes_2.10.4" % "2.0.0-M6"
)

scalacOptions += "-deprecation"
