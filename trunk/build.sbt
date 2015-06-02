import sbt._
import Keys._

sbtPlugin := true

version := "0.0.0.1"

scalaVersion := "2.10.4"

shellPrompt := { state =>  "sbt (%s)> ".format(Project.extract(state).currentProject.id) }

net.virtualvoid.sbt.graph.Plugin.graphSettings

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

resolvers += Resolver.file("Local repo", file(System.getProperty("user.home") + "/.ivy2/local"))(Resolver.ivyStylePatterns)

val Organization = "com.ligadata"

lazy val BaseTypes = project.in(file("BaseTypes")) dependsOn(Metadata)

lazy val BaseFunctions = project.in(file("BaseFunctions")) dependsOn(Metadata)

lazy val Serialize = project.in(file("Utils/Serialize")) dependsOn(Metadata)

lazy val ZooKeeperClient   = project.in(file("Utils/ZooKeeper/CuratorClient")) dependsOn(Serialize)

lazy val ZooKeeperListener = project.in(file("Utils/ZooKeeper/CuratorListener")) dependsOn(ZooKeeperClient, Serialize)

lazy val FatafatBase = project.in(file("FatafatBase")) dependsOn(Metadata)

lazy val FatafatManager = project.in(file("FatafatManager")) dependsOn(Metadata, FatafatBase, FatafatData, MetadataBootstrap, MetadataAPI, Serialize, ZooKeeperListener, ZooKeeperLeaderLatch)

lazy val KafkaSimpleInputOutputAdapters = project.in(file("InputOutputAdapters/KafkaSimpleInputOutputAdapters")) dependsOn(FatafatBase)

lazy val IbmMqSimpleInputOutputAdapters = project.in(file("InputOutputAdapters/IbmMqSimpleInputOutputAdapters")) dependsOn(FatafatBase)

lazy val FileSimpleInputOutputAdapters = project.in(file("InputOutputAdapters/FileSimpleInputOutputAdapters")) dependsOn(FatafatBase)

lazy val SimpleEnvContextImpl = project.in(file("EnvContexts/SimpleEnvContextImpl")) dependsOn(FatafatBase, FatafatData, Storage, Serialize)

lazy val Storage = project.in(file("Storage")) dependsOn("Metadata")

lazy val Metadata = project.in(file("Metadata")) 

lazy val MessageDef = project.in(file("MessageDef")) dependsOn(Metadata,MetadataBootstrap)

lazy val LoadtestCommon = project.in(file("Tools/LoadtestCommon")) dependsOn(Storage)

lazy val LoadtestRunner = project.in(file("Tools/LoadtestRunner")) dependsOn(LoadtestCommon)

lazy val LoadtestMaster = project.in(file("Tools/LoadtestMaster")) dependsOn(LoadtestCommon)

lazy val Loadtest = project.in(file("Tools/Loadtest")) dependsOn(Storage)

lazy val PmmlRuntime = project.in(file("Pmml/PmmlRuntime")) dependsOn(Metadata, FatafatBase) 

lazy val PmmlCompiler = project.in(file("Pmml/PmmlCompiler")) dependsOn(PmmlRuntime, PmmlUdfs, Metadata, FatafatBase, MetadataBootstrap)

lazy val PmmlUdfs = project.in(file("Pmml/PmmlUdfs")) dependsOn(Metadata, PmmlRuntime, FatafatBase)

lazy val MethodExtractor = project.in(file("Pmml/MethodExtractor")) dependsOn(PmmlUdfs, Metadata, FatafatBase, Serialize)

lazy val MetadataBootstrap = project.in(file("MetadataBootstrap/Bootstrap")) dependsOn(Metadata, FatafatBase, BaseTypes)

lazy val MetadataAPI = project.in(file("MetadataAPI")) dependsOn(Storage,Metadata,MessageDef,PmmlCompiler,Serialize,ZooKeeperClient,ZooKeeperListener)

lazy val MetadataAPIService = project.in(file("MetadataAPIService")) dependsOn(FatafatBase,MetadataAPI,ZooKeeperLeaderLatch)

lazy val MetadataAPIServiceClient = project.in(file("MetadataAPIServiceClient")) dependsOn(Serialize)

lazy val SimpleKafkaProducer = project.in(file("Utils/SimpleKafkaProducer")) dependsOn(Metadata, FatafatBase)

lazy val KVInit = project.in(file("Utils/KVInit")) dependsOn (Metadata, FatafatBase, FatafatData, MetadataBootstrap, MetadataAPI, Storage)

lazy val ZooKeeperLeaderLatch = project.in(file("Utils/ZooKeeper/CuratorLeaderLatch")) dependsOn(ZooKeeperClient)

lazy val JsonDataGen = project.in(file("Utils/JsonDataGen"))

lazy val NodeInfoExtract  = project.in(file("Utils/NodeInfoExtract")) dependsOn(MetadataAPI)

lazy val Controller = project.in(file("Utils/Controller")) dependsOn(ZooKeeperClient,ZooKeeperListener,KafkaSimpleInputOutputAdapters)

lazy val SimpleApacheShiroAdapter = project.in(file("Utils/Security/SimpleApacheShiroAdapter")) dependsOn(Metadata)

lazy val AuditAdapters = project.in(file("Utils/Audit")) dependsOn(Storage)

lazy val FatafatData = project.in(file("FatafatData")) dependsOn(FatafatBase)

lazy val CustomUdfLib = project.in(file("SampleApplication/CustomUdfLib")) dependsOn(PmmlUdfs)

lazy val ExtractData = project.in(file("Utils/ExtractData")) dependsOn(Metadata, FatafatBase, FatafatData, MetadataBootstrap, MetadataAPI, Storage)

lazy val JdbcDataCollector = project.in(file("Utils/JdbcDataCollector"))

