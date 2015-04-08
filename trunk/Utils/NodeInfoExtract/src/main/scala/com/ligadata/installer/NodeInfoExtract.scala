package com.ligadata.installer

import scala.collection.immutable.{Set, HashMap}
import scala.io.Source
import java.io._
import com.ligadata.MetadataAPI.{ MetadataAPIImpl }
import com.ligadata.olep.metadata._

class NodeInfoExtract(val metadataAPIConfig : String, val nodeConfigPath : String, val clusterId : String, val installDir : String) {

	//MetadataAPIImpl.InitMdMgrFromBootStrap(metadataAPIConfig)
	MetadataAPIImpl.InitMdMgr(metadataAPIConfig)
	
	/** FIXME: At some point, the engine and MetadataAPI prop name will converge and these keys will likely be wrong!!!!!!!!!!!!!!!!!!! */
	var metadataStoreType : String = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DATABASE")
	var metadataSchemaName : String = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DATABASE_SCHEMA")
	var metadataLocation : String = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DATABASE_LOCATION")
	
	
	//println(result)
	
	def MetadataStoreType : String = metadataStoreType
	def MetadataSchemaName : String = metadataSchemaName
	def MetadataLocation : String = metadataLocation
	
	/** 
	 *  Optionally called when an EngineConfig file with cluster decl in it is supplied as an argument, this
	 *  command updates the metadata referred to by the metadataapiconfig properties file. 
	 */
	def initializeEngineConfig : Unit = {
		if (nodeConfigPath != null) {
			val result : String = MetadataAPIImpl.UploadConfig(Source.fromFile(nodeConfigPath).mkString)
			//println(result)
		} else {
			throw new RuntimeException("initializeEngineConfig erroneously called... logic error ")
		}
	}
	
	def extract : (Array[String], Array[(String,String,String)],Array[(String,String)]) = {
		val nodeInfos : Array[NodeInfo] = MdMgr.GetMdMgr.NodesForCluster(clusterId)
		val ok : Boolean = (nodeInfos != null && nodeInfos.size > 0)
		if (! ok) {
			throw new RuntimeException(s"There are no known nodes in the Metadata for $clusterId... consider using --NodeConfigPath option, check your clusterId value, or choose different MetadataAPI config. ")
		}
		/** 
		 	Since nodes can exist on the same machine, and use the same directory for all node implementations,
		 	some work must be done to create a concise set of node/install directory pairs... 
		 */	
		val configDir : String = installDir + "/config"
	    val ips : Array[String] = nodeInfos.map(info => info.nodeIpAddr).toSet.toSeq.sorted.toArray 
	    val ipIdTargPaths : Array[(String,String,String)] = nodeInfos.map(info => {
		      	(info.nodeIpAddr, info.nodeId, configDir)}
		    ).toSet.toSeq.sorted.toArray 
	   
	    val uniqueNodePaths : Set[String] = nodeInfos.map(info => info.nodeIpAddr + "~" + installDir).toSet 
		val ipPathPairs : Array[(String,String)] = uniqueNodePaths.map(itm => (itm.split('~').head, itm.split('~').last)).toSeq.sorted.toArray
		(ips, ipIdTargPaths, ipPathPairs)
	}
}

object NodeInfoExtract extends App {

    def usage : String = {
"""
NodeInfoExtract --MetadataAPIConfig  <MetadataAPI config file path>
                --NodeConfigPath <OnLEP engine config file path>
                --ipFileName <file name path for the cluster node ips>
                --ipPathPairFileName <file name path for the cluster node ip/path name pairs>
				--installDir <the directory where the cluster is installed>
		        --clusterId <cluster id name> 
      
        Note: The metadata api config file is used to obtain the basic information regarding the metadata.
		The node config path is principally for definining new clusters and is optional if the metadata mentioned
		in the metadata api config file is proper (has a cluster defined in it).  The ipFileName and ipPathPairFileName
		are file names to use for the collecting certain information for processing by the cluster installer or one
		of the cluster start/stop scripts. 
      
		The clusterId is the one to be started/stopped or for install, the key to select nodes for
		the cluster installation.  The system will permit multiple clusters to be defined in the same metadata cache.
      
"""
    }

    override def main( args : Array[String]) : Unit = {
		
        if (args.length == 0) println(usage)
        val arglist = args.toList
        type OptionMap = Map[Symbol, String]
        //println(arglist)
        def nextOption(map: OptionMap, list: List[String]): OptionMap = {
          list match {
            case Nil => map
            case "--MetadataAPIConfig" :: value :: tail =>
              nextOption(map ++ Map('MetadataAPIConfig -> value), tail)
            case "--NodeConfigPath" :: value :: tail =>
              nextOption(map ++ Map('NodeConfigPath -> value), tail)
            case "--clusterId" :: value :: tail =>
              nextOption(map ++ Map('clusterId -> value), tail)
            case "--ipFileName" :: value :: tail =>
              nextOption(map ++ Map('ipFileName -> value), tail)
            case "--ipPathPairFileName" :: value :: tail =>
              nextOption(map ++ Map('ipPathPairFileName -> value), tail)
            case "--workDir" :: value :: tail =>
              nextOption(map ++ Map('workDir -> value), tail)  
            case "--ipIdCfgTargPathQuartetFileName" :: value :: tail =>
              nextOption(map ++ Map('ipIdCfgTargPathQuartetFileName -> value), tail)  
            case "--installDir" :: value :: tail =>
              nextOption(map ++ Map('installDir -> value), tail)  
            case option :: tail =>
              println("Unknown option " + option)
              println(usage)
              sys.exit(1)
          }
        }
    
        val options = nextOption(Map(), arglist)
        
        val metadataAPIConfig = if (options.contains('MetadataAPIConfig)) options.apply('MetadataAPIConfig) else null
        val nodeConfigPath = if (options.contains('NodeConfigPath)) options.apply('NodeConfigPath) else null
        val clusterId = if (options.contains('clusterId)) options.apply('clusterId) else null
        val ipFileName = if (options.contains('ipFileName)) options.apply('ipFileName) else null
        val ipPathPairFileName = if (options.contains('ipPathPairFileName)) options.apply('ipPathPairFileName) else null
        val workDir = if (options.contains('workDir)) options.apply('workDir) else null
        val ipIdCfgTargPathQuartetFileName = if (options.contains('ipIdCfgTargPathQuartetFileName)) options.apply('ipIdCfgTargPathQuartetFileName) else null
        val installDir = if (options.contains('installDir)) options.apply('installDir) else null
        
        val reasonableArguments : Boolean = (metadataAPIConfig != null && metadataAPIConfig.size > 0 
            							&& installDir != null && installDir.size > 0 
            							&& ipFileName != null && ipFileName.size > 0 
            							&& ipPathPairFileName != null && ipPathPairFileName.size > 0 
            							&& workDir != null && workDir.size > 0 
            							&& ipIdCfgTargPathQuartetFileName != null && ipIdCfgTargPathQuartetFileName.size > 0 
            							&& clusterId != null)
        if (! reasonableArguments) {
            println("Your arguments are not satisfactory...Usage:")
            println(usage)
            throw new RuntimeException("Your arguments are not satisfactory")
        }
                    
   		val extractor : NodeInfoExtract = new NodeInfoExtract(metadataAPIConfig, nodeConfigPath, clusterId, installDir)
   		if (nodeConfigPath != null) {
   			extractor.initializeEngineConfig
   		}
   		val (ips, ipIdTargPaths, ipPathPairs) : (Array[String], Array[(String,String,String)], Array[(String,String)]) = extractor.extract 
		
	    writeFileIps(s"$workDir/$ipFileName", ips)
	    writeFilePairs(s"$workDir/$ipPathPairFileName" , ipPathPairs)
	    writeNodeIdConfigs(workDir, ipIdCfgTargPathQuartetFileName, extractor, ipIdTargPaths)
	}

  	private def writeFileIps(outputPath : String, ips : Array[String]) {
		val file = new File(outputPath);
		val bufferedWriter = new BufferedWriter(new FileWriter(file))
		ips.foreach(ip => {
		    bufferedWriter.write(s"$ip\n")
		})
		bufferedWriter.close
  	}
  
	private def writeFilePairs(outputPath : String, ipPathPairs : Array[(String,String)]) {
	    val file = new File(outputPath);
	    val bufferedWriter = new BufferedWriter(new FileWriter(file))
	    ipPathPairs.foreach(pair => {
	        val ip : String = pair._1
	        val path : String = pair._2
	        bufferedWriter.write(s"$ip\n")
	        bufferedWriter.write(s"$path\n")
	    })
	    bufferedWriter.close
	}

	/**
	 * Write an engine config file for each id presented in the ids array. Write another text file
	 * consisting of ids and config file paths.  Each ip, id, and config file path, and targetPath are written on
	 * four lines, one value per line.  These are processed by the install script to distribute
	 * the appropriate config file to the appropriate ip to the appropriate target path for each node found there.
	 * 
	 * These files will be distributed to the appropriate nodes during the installation.
	 * 
	 *  @param workDir the directory where the config files and the nodeId.txt file will be written
	 *  @param idCfgFileName the name of the file that will track the nodeId/config file name line pairs
	 *  @param extractor the NodeInfoExtract instance containing most of the config info.
	 *  @param ipIdTargs the (ip,id, targetPath) triple values for the cluster being processed 
	 */
	private def writeNodeIdConfigs(workDir : String, ipIdCfgTargPathQuartetFileName : String, extractor : NodeInfoExtract, ipIdTargs : Array[(String,String,String)]) {
	    
	    val storeType : String = extractor.MetadataStoreType
		val schemaName : String = extractor.MetadataSchemaName
		val mdLoc : String = extractor.MetadataLocation
		
	    ipIdTargs.foreach(ipIdTargTriple => {
			val (_,id,_) : (String,String,String) = ipIdTargTriple
			
	    	val nodeCfgPath : String = s"$workDir/node$id.cfg"
	    	val file = new File(nodeCfgPath);
	    	val bufferedWriter = new BufferedWriter(new FileWriter(file))
	    	
			bufferedWriter.write(s"# Node Information\n")
			bufferedWriter.write(s"nodeId=$id\n")
			bufferedWriter.write(s"\n")
			bufferedWriter.write(s"#Storing metadata using MetadataStoreType, MetadataSchemaName & MetadataLocation\n")
			bufferedWriter.write(s"MetadataStoreType=$storeType\n")
			bufferedWriter.write(s"MetadataSchemaName=$schemaName\n")
			bufferedWriter.write(s"MetadataLocation=$mdLoc\n")
			
			bufferedWriter.close
	    })
	    
    	val nodeCfgPath : String = s"$workDir/$ipIdCfgTargPathQuartetFileName"
    	val file = new File(nodeCfgPath);
		val bufferedWriter = new BufferedWriter(new FileWriter(file))
		ipIdTargs.foreach(ipIdTargTriple => {
			val (ip,id,targPath) : (String,String,String) = ipIdTargTriple
		    bufferedWriter.write(s"$ip\n")
		    bufferedWriter.write(s"$id\n")
		    bufferedWriter.write(s"$workDir/node$id.cfg\n")
		    bufferedWriter.write(s"$targPath\n")
		})
		bufferedWriter.close
	    
	}
  
}


