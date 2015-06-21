/**
 * Created by wtarver on 1/21/15.
 * MetadataManager is a simple wrapper around some basic methods to perform operations on Metadata. This is meant to help setup along for testing.
 */
package com.ligadata.automation.unittests.api.setup

import java.io.File

import com.ligadata.MetadataAPI.{ApiResult, MetadataAPIImpl => md}
import com.ligadata.Serialize.SerializerManager
import com.ligadata.fatafat.metadata.{AlreadyExistsException, MdMgr}

import scala.collection.mutable
import scala.io.Source
import org.apache.log4j._

case class MetadataAPIProperties(var database: String = "hashmap", 
				 var databaseHost: String = "localhost", 
				 var databaseSchema: String = "metadata",
				 var dataDirectory: String = ConfigDefaults.dataDirectory,
                                 var classPath: String = ConfigDefaults.metadataClasspath, 
				 var znodeBasePath: String = "/ligadata",
                                 var zkConnStr: String = "localhost:2181", 
				 var modelExecLog: String = "true", 
				 var node_id: String = "1",
			         var serviceHost: String = "localhost",
			         var servicePort: String = "8081")

case class MetadataManagerException(message: String) extends Exception(message)

class MetadataManager(var config: MetadataAPIProperties) {

  private val logger = org.apache.log4j.Logger.getLogger(this.getClass)
  private val metadataDir = new File(getClass.getResource("/Metadata").getPath)

  logger.info("metadataDir => " + metadataDir)

  private val scalaHome = System.getenv("SCALA_HOME")
  private val javaHome = System.getenv("JAVA_HOME")

  if(scalaHome == null || javaHome == null) {
    throw new MetadataManagerException("Environment Variable SCALA_HOME or JAVA_HOME not set")
  }

  /// Init is kept separate rather than as a part of the construction of the class in order to prevent execution of certain operations too soon.
  def initMetadataCfg: Unit = {
    if(scalaHome != null)
      md.GetMetadataAPIConfig.setProperty("SCALA_HOME", scalaHome)
    else {
      throw new MetadataManagerException("Failed to retrieve environmental variable 'SCALA_HOME'. You must set this variable in order to run tests.")
    }

    if(javaHome != null)
      md.GetMetadataAPIConfig.setProperty("JAVA_HOME", javaHome)
    else {
      throw new MetadataManagerException("Failed to retrieve environmental variable 'JAVA_HOME'. You must set this variable in order to run tests.")
    }

    val jarPathSystem: String = getClass.getResource("/jars/lib/system").getPath
    logger.info("jarPathSystem => " + jarPathSystem)

    val jarPathApp: String = getClass.getResource("/jars/lib/application").getPath

    logger.info("jarPathApp => " + jarPathApp)

    md.metadataAPIConfig.setProperty("ROOT_DIR", "")
    md.metadataAPIConfig.setProperty("GIT_ROOT", "")

    logger.info("DATABASE => " + config.database)

    md.metadataAPIConfig.setProperty("NODE_ID", config.node_id)
    md.metadataAPIConfig.setProperty("SERVICE_HOST", config.serviceHost)
    md.metadataAPIConfig.setProperty("SERVICE_PORT", config.servicePort)
    md.metadataAPIConfig.setProperty("DATABASE", config.database)
    md.metadataAPIConfig.setProperty("DATABASE_HOST", config.databaseHost)
    md.metadataAPIConfig.setProperty("DATABASE_SCHEMA", config.databaseSchema)
    md.metadataAPIConfig.setProperty("DATABASE_LOCATION", config.dataDirectory)
    md.metadataAPIConfig.setProperty("JAR_TARGET_DIR", jarPathApp)
    md.metadataAPIConfig.setProperty("JAR_PATHS",  jarPathSystem + "," + jarPathApp)
    md.metadataAPIConfig.setProperty("MANIFEST_PATH", metadataDir.getAbsoluteFile + "/manifest.mf")
    md.metadataAPIConfig.setProperty("CLASSPATH", config.classPath)
    md.metadataAPIConfig.setProperty("NOTIFY_ENGINE", "YES")
    md.metadataAPIConfig.setProperty("ZNODE_PATH", config.znodeBasePath)
    md.metadataAPIConfig.setProperty("ZOOKEEPER_CONNECT_STRING", config.zkConnStr)
    md.metadataAPIConfig.setProperty("COMPILER_WORK_DIR", getClass.getResource("/jars/lib/workingdir").getPath)
    md.metadataAPIConfig.setProperty("API_LEADER_SELECTION_ZK_NODE", "/ligadata/metadata")
    md.metadataAPIConfig.setProperty("MODEL_EXEC_LOG", config.modelExecLog)
    md.metadataAPIConfig.setProperty("SECURITY_IMPL_JAR", jarPathSystem + "/simpleapacheshiroadapter_2.10-1.0.jar")
    md.metadataAPIConfig.setProperty("SECURITY_IMPL_CLASS", "com.ligadata.Security.SimpleApacheShiroAdapter")
    md.metadataAPIConfig.setProperty("DO_AUTH", "YES")
    md.metadataAPIConfig.setProperty("AUDIT_IMPL_JAR", jarPathSystem + "/auditadapters_2.10-1.0.jar")
    md.metadataAPIConfig.setProperty("AUDIT_IMPL_CLASS", "com.ligadata.audit.adapters.AuditCassandraAdapter")
    md.metadataAPIConfig.setProperty("DO_AUDIT", "YES")
    md.metadataAPIConfig.setProperty("SSL_CERTIFICATE","/media/home2/installFatafat/certs/keystore.jks")

    md.metadataAPIConfig.setProperty("CONTAINER_FILES_DIR",metadataDir.getAbsoluteFile + "/container")
    md.metadataAPIConfig.setProperty("MESSAGE_FILES_DIR",metadataDir.getAbsoluteFile + "/message")
    md.metadataAPIConfig.setProperty("MODEL_FILES_DIR",metadataDir.getAbsoluteFile + "/model")
    md.metadataAPIConfig.setProperty("FUNCTION_FILES_DIR",metadataDir.getAbsoluteFile + "/function")
    md.metadataAPIConfig.setProperty("CONCEPT_FILES_DIR",metadataDir.getAbsoluteFile + "/concept")
    md.metadataAPIConfig.setProperty("TYPE_FILES_DIR",metadataDir.getAbsoluteFile + "/type")
    md.metadataAPIConfig.setProperty("CONFIG_FILES_DIR",metadataDir.getAbsoluteFile + "/config")

  }
}
