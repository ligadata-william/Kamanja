package com.ligadata.MetadataAPI

object SampleData {

  val sampleScalarTypeStr = """
  {
  "MetadataType" : "ScalarTypeDef",
  "NameSpace" : "system",
  "Name" : "my_char",
  "TypeTypeName" : "tScalar",
  "TypeNameSpace" : "System",
  "TypeName" : "Char",
  "PhysicalName" : "Char",
  "Version" : 100,
  "JarName" : "basetypes_2.10-0.1.0.jar",
  "DependencyJars" : [ "metadata_2.10-1.0.jar" ],
  "Implementation" : "com.ligadata.BaseTypes.CharImpl"
  }
"""

  val sampleNewScalarTypeStr = """
  {
  "MetadataType" : "ScalarTypeDef",
  "NameSpace" : "system",
  "Name" : "my_char",
  "TypeTypeName" : "tScalar",
  "TypeNameSpace" : "System",
  "TypeName" : "Char",
  "PhysicalName" : "Char",
  "Version" : 101,
  "JarName" : "basetypes_2.10-0.1.0.jar",
  "DependencyJars" : [ "metadata_2.10-1.0.jar" ],
  "Implementation" : "com.ligadata.BaseTypes.CharImpl"
  }
"""

  val sampleTupleTypeStr = """
  {
    "MetadataType" : "TupleTypeDef",
    "NameSpace" : "system",
    "Name" : "my_tupleofstringstring",
    "TypeTypeName" : "tTupleN",
    "TypeNameSpace" : "System",
    "TypeName" : "Any",
    "PhysicalName" : null,
    "Version" : 1,
    "JarName" : null,
    "DependencyJars" : [ "basetypes_2.10-0.1.0.jar", "metadata_2.10-1.0.jar" ],
    "Implementation" : null,
    "TupleDefinitions": [ 
      {
	"MetadataType" : "ScalarTypeDef",
	"NameSpace" : "system",
	"Name" : "string",
	"TypeTypeName" : "tScalar",
	"TypeNameSpace" : "System",
	"TypeName" : "String",
	"PhysicalName" : "String",
	"Version" : 100,
	"JarName" : "basetypes_2.10-0.1.0.jar",
	"DependencyJars" : [ "metadata_2.10-1.0.jar" ],
	"Implementation" : "com.ligadata.BaseTypes.StringImpl"
      },
      {
	"MetadataType" : "ScalarTypeDef",
	"NameSpace" : "system",
	"Name" : "string",
	"TypeTypeName" : "tScalar",
	"TypeNameSpace" : "System",
	"TypeName" : "String",
	"PhysicalName" : "String",
	"Version" : 100,
	"JarName" : "basetypes_2.10-0.1.0.jar",
	"DependencyJars" : [ "metadata_2.10-1.0.jar" ],
	"Implementation" : "com.ligadata.BaseTypes.StringImpl"
      } ]
  }
"""

  val sampleConceptStr = """
  {"Concepts" : [
  "NameSpace":"Ligadata",
  "Name":"ProviderId",
  "TypeNameSpace":"System",
  "TypeName" : "String",
  "Version"  : 100 ]
  }
"""

  val sampleFunctionStr = """
  {
  "NameSpace" : "pmml",
  "Name" : "my_min",
  "PhysicalName" : "com.ligadata.pmml.udfs.Udfs.Min",
  "ReturnTypeNameSpace" : "system",
  "ReturnTypeName" : "double",
  "Arguments" : [ {
  "ArgName" : "expr1",
  "ArgTypeNameSpace" : "system",
  "ArgTypeName" : "int"
  }, {
  "ArgName" : "expr2",
  "ArgTypeNameSpace" : "system",
  "ArgTypeName" : "double"
  } ],
  "Version" : 1,
  "JarName" : null,
  "DependantJars" : [ "basetypes_2.10-0.1.0.jar", "metadata_2.10-1.0.jar" ]
  }
"""
}


