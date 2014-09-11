import AssemblyKeys._ // put this at the top of the file
import sbt._
import Keys._

shellPrompt := { state =>  "sbt (%s)> ".format(Project.extract(state).currentProject.id) }

assemblySettings

assemblyOption in assembly ~= { _.copy(prependShellScript = Some(defaultShellScript)) }

jarName in assembly := { s"${name.value}-${version.value}" }

// for some reason the merge strategy for non ligadata classes are not working and thus added those conflicting jars in exclusions
// this may result some run time errors

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    // case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
    // case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
    case x if x endsWith "google/common/annotations/GwtCompatible.class" => MergeStrategy.first
    case x if x endsWith "google/common/annotations/GwtIncompatible.class" => MergeStrategy.first
    case x if x endsWith "/apache/commons/beanutils/BasicDynaBean.class" => MergeStrategy.first
	case x if x endsWith "org/apache/commons/beanutils/BasicDynaBean.class" => MergeStrategy.last
	case x if x endsWith "ArrayStack.class" =>  MergeStrategy.last
    case "log4j.properties" => MergeStrategy.first
    case "unwanted.txt"     => MergeStrategy.discard
    case x => old(x)
  }
}

excludedJars in assembly <<= (fullClasspath in assembly) map { cp => 
  val excludes = Set("commons-beanutils-1.7.0.jar", "google-collections-1.0.jar", "commons-collections-3.2.1.jar" )
  cp filter { jar => excludes(jar.data.getName) }
}

name := "AlchemyKVInit"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.joda" % "joda-convert" % "1.6"

libraryDependencies += "joda-time" % "joda-time" % "2.3"

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "0.8.0"

scalacOptions += "-deprecation"
