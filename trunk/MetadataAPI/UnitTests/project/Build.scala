import java.io.File

import sbt._
import Keys._

object myBuild extends Build {

  lazy val UnitTests = project.in(file(".")).dependsOn(MetadataAPI).aggregate(MetadataAPI)

  lazy val MetadataAPI = ProjectRef(file("../../"), "MetadataAPI")

  retrieveManaged := true

  testOptions in Test <+= (target in Test) map {
    t => Tests.Argument(TestFrameworks.ScalaTest, "junitxml(directory=\"%s\")" format (t / "test-reports"))
  }

  parallelExecution := false

  val wdir = new File("./target/scala-2.10/test-classes/jars/lib/workingdir")
  if( !wdir.exists() ){
    wdir.mkdir()
  }

  val appdir = new File("./target/scala-2.10/test-classes/jars/lib/application")
  if( !appdir.exists() ){
    appdir.mkdir()
  }

  copy(new File("lib_managed"))

  copy(new File("../.."))

  private def copy(path: File): Unit = {
    val targetLibDir = "./target/scala-2.10/test-classes/jars/lib/system/"
    if(path.isDirectory ){
      if( path.getPath.contains("MetadataAPI/UnitTests/target/scala-2.10/test-classes/jars/lib/system") ){
	return
      }
      Option(path.listFiles).map(_.toList).getOrElse(Nil).foreach(f => {
        if (f.isDirectory){
          copy(f)
	}
        else if (f.getPath.endsWith(".jar")) {
          try {
	    //System.out.println("Copying " + f + "," + "(file size => " + f.length() + ") to " + targetLibDir + f.getName)
            sbt.IO.copyFile(f, new File(targetLibDir + f.getName))
          }
          catch {
            case e: Exception => throw new Exception("Failed to copy file: " + f + " with exception:\n" + e)
          }
        }
      })
    }
  }
}
