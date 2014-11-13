This project contains a scala script that will accept a project name, full package qualified object, a namespace,
and a version number to produce MetadataAPI slurpable Json metadata definitions for both the methods in the object
as well as the types that the object methods require.  

This script should be placed in the executable path.  When executing the script, it is expected that the
current working directory is the top level sbt project directory (e.g., ~/github/RTD/trunk).

Script syntax:

	extractUdfLibMetadata.scala --sbtProject PmmlUdfs --fullObjectPath com.ligadata.pmml.udfs.Udfs --namespace pmml --versionNumber 100 > /tmp/json.txt

In the example, this is the script values needed to create the json for the core pmml udf library.
