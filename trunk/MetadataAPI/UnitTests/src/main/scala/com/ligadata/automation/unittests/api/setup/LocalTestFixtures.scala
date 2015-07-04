package com.ligadata.automation.unittests.api.setup

trait CommonResources {
  def dataDirectory:String = Some(getClass.getResource("/DataDirectories").getPath).getOrElse(throw new Exception("Failed to get resource 'DataDirectories'"))
  def configurationDirectory:String = Some(getClass.getResource("/ConfigurationFiles").getPath).getOrElse(throw new Exception("Failed to get resource 'ConfigurationFiles'"))
  def metadataDirectory:String = Some(getClass.getResource("/Metadata").getPath).getOrElse(throw new Exception("Failed to get resource 'Metadata'"))
  def systemJarsDirectory:String = Some(getClass.getResource("/jars/lib/system").getPath).getOrElse(throw new Exception("Failed to get resource '/jars/lib/system'"))
  def applicationJarsDirectory:String = Some(getClass.getResource("/jars/lib/application").getPath).getOrElse(throw new Exception("Failed to get resource '/jars/lib/application'"))

  def cassandraDataDirectory:String = dataDirectory + "/cassandra"

}

trait LocalTestFixtures extends CommonResources {
  val mdMan: MetadataManager = new MetadataManager(new MetadataAPIProperties)
  val zkServer = EmbeddedZookeeper
}
