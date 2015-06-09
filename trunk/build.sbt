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

lazy val BaseTypes = project.in(file("BaseTypes")) dependsOn(Metadata, Exceptions)

lazy val BaseFunctions = project.in(file("BaseFunctions")) dependsOn(Metadata, Exceptions)

lazy val Serialize = project.in(file("Utils/Serialize")) dependsOn(Metadata, Exceptions)

lazy val ZooKeeperClient   = project.in(file("Utils/ZooKeeper/CuratorClient")) dependsOn(Serialize, Exceptions)

lazy val ZooKeeperListener = project.in(file("Utils/ZooKeeper/CuratorListener")) dependsOn(ZooKeeperClient, Serialize, Exceptions)

lazy val Exceptions = project.in(file("Exceptions"))

lazy val FatafatBase = project.in(file("FatafatBase")) dependsOn(Metadata, Exceptions)

lazy val ApiImpl = project.in(file("ApiImpl")) dependsOn(Metadata, FatafatBase, Exceptions)

lazy val FatafatManager = project.in(file("FatafatManager")) dependsOn(Metadata, FatafatBase, ApiImpl, FatafatData, MetadataBootstrap, MetadataAPI, Serialize, ZooKeeperListener, ZooKeeperLeaderLatch, Exceptions)

lazy val IbmMqSimpleInputOutputAdapters = project.in(file("InputOutputAdapters/IbmMqSimpleInputOutputAdapters")) dependsOn(FatafatBase)

lazy val KafkaSimpleInputOutputAdapters = project.in(file("InputOutputAdapters/KafkaSimpleInputOutputAdapters")) dependsOn(FatafatBase, Exceptions)

lazy val FileSimpleInputOutputAdapters = project.in(file("InputOutputAdapters/FileSimpleInputOutputAdapters")) dependsOn(FatafatBase, Exceptions)

lazy val SimpleEnvContextImpl = project.in(file("EnvContexts/SimpleEnvContextImpl")) dependsOn(FatafatBase, FatafatData, Storage, Serialize, Exceptions)

lazy val Storage = project.in(file("Storage")) dependsOn(Metadata, Exceptions)

lazy val Metadata = project.in(file("Metadata")) dependsOn(Exceptions, Exceptions)

lazy val OutputMsgDef  = project.in(file("OutputMsgDef")) dependsOn(Metadata,FatafatBase,BaseTypes)

lazy val MessageDef = project.in(file("MessageDef")) dependsOn(Metadata, MetadataBootstrap, Exceptions)

lazy val LoadtestCommon = project.in(file("Tools/LoadtestCommon")) dependsOn(Storage, Exceptions)

lazy val LoadtestRunner = project.in(file("Tools/LoadtestRunner")) dependsOn(LoadtestCommon, Exceptions)

lazy val LoadtestMaster = project.in(file("Tools/LoadtestMaster")) dependsOn(LoadtestCommon, Exceptions)

lazy val Loadtest = project.in(file("Tools/Loadtest")) dependsOn(Storage, Exceptions)

lazy val PmmlRuntime = project.in(file("Pmml/PmmlRuntime")) dependsOn(Metadata, FatafatBase, Exceptions) 

lazy val PmmlCompiler = project.in(file("Pmml/PmmlCompiler")) dependsOn(PmmlRuntime, PmmlUdfs, Metadata, FatafatBase, MetadataBootstrap, Exceptions)

lazy val PmmlUdfs = project.in(file("Pmml/PmmlUdfs")) dependsOn(Metadata, PmmlRuntime, FatafatBase, Exceptions)

lazy val MethodExtractor = project.in(file("Pmml/MethodExtractor")) dependsOn(PmmlUdfs, Metadata, FatafatBase, Serialize, Exceptions)

lazy val MetadataAPI = project.in(file("MetadataAPI")) dependsOn(Storage,Metadata,MessageDef,PmmlCompiler,Serialize,ZooKeeperClient,ZooKeeperListener,OutputMsgDef,Exceptions)

lazy val MetadataBootstrap = project.in(file("MetadataBootstrap/Bootstrap")) dependsOn(Metadata, FatafatBase, BaseTypes, Exceptions)

lazy val MetadataAPIService = project.in(file("MetadataAPIService")) dependsOn(FatafatBase,MetadataAPI,ZooKeeperLeaderLatch, Exceptions)

lazy val MetadataAPIServiceClient = project.in(file("MetadataAPIServiceClient")) dependsOn(Serialize, Exceptions)

lazy val SimpleKafkaProducer = project.in(file("Utils/SimpleKafkaProducer")) dependsOn(Metadata, FatafatBase, Exceptions)

lazy val KVInit = project.in(file("Utils/KVInit")) dependsOn (Metadata, FatafatBase, FatafatData, MetadataBootstrap, MetadataAPI, Storage, Exceptions)

lazy val ZooKeeperLeaderLatch = project.in(file("Utils/ZooKeeper/CuratorLeaderLatch")) dependsOn(ZooKeeperClient, Exceptions)

lazy val JsonDataGen = project.in(file("Utils/JsonDataGen")) dependsOn(Exceptions)

lazy val NodeInfoExtract  = project.in(file("Utils/NodeInfoExtract")) dependsOn(MetadataAPI, Exceptions)

lazy val Controller = project.in(file("Utils/Controller")) dependsOn(ZooKeeperClient,ZooKeeperListener,KafkaSimpleInputOutputAdapters, Exceptions)

lazy val SimpleApacheShiroAdapter = project.in(file("Utils/Security/SimpleApacheShiroAdapter")) dependsOn(Metadata, Exceptions)

lazy val AuditAdapters = project.in(file("Utils/Audit")) dependsOn(Storage, Exceptions)

lazy val FatafatData = project.in(file("FatafatData")) dependsOn(FatafatBase, Exceptions)

lazy val CustomUdfLib = project.in(file("SampleApplication/CustomUdfLib")) dependsOn(PmmlUdfs, Exceptions)

lazy val JdbcDataCollector = project.in(file("Utils/JdbcDataCollector"))

lazy val ExtractData = project.in(file("Utils/ExtractData")) dependsOn(Metadata, FatafatBase, FatafatData, MetadataBootstrap, MetadataAPI, Storage, Exceptions)

lazy val InterfacesSamples = project.in(file("SampleApplication/InterfacesSamples")) dependsOn(Metadata, FatafatBase, FatafatData, MetadataBootstrap, MetadataAPI, Storage, Exceptions)