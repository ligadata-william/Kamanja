package com.ligadata.installer

import scala.collection.immutable.{Set, HashMap}
import scala.io.Source
import java.io._
import com.ligadata.MetadataAPI.{ MetadataAPIImpl }
import com.ligadata.olep.metadata._

class NodeInfoExtract(val metadataAPIConfig : String, val nodeConfigPath : String) {

	MetadataAPIImpl.InitMdMgrFromBootStrap(metadataAPIConfig)
	
	var metadataStoreType : String = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("MetadataStoreType")
	var metadataSchemaName : String = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("MetadataSchemaName")
	var metadataLocation : String = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("MetadataLocation")
	
	val result : String = MetadataAPIImpl.UploadConfig(Source.fromFile(nodeConfigPath).mkString)
	//println(result)
	
	def MetadataStoreType : String = metadataStoreType
	def MetadataSchemaName : String = metadataSchemaName
	def MetadataLocation : String = metadataLocation
	
	def extract : (Array[String], Array[(String,String,String)],Array[(String,String)]) = {
		val nodeInfos : Map[String,NodeInfo] = MdMgr.GetMdMgr.Nodes
		
		/** 
		 	Since nodes can exist on the same machine, and use the same directory for all node implementations,
		 	some work must be done to create a concise set of node/install directory pairs... 
		 	
		 	Build a Set[String] of (nodeIp,installPath) pairs...
		 	
		 */	
	    val ips : Array[String] = nodeInfos.values.map(info => info.nodeIpAddr).toSet.toSeq.sorted.toArray 
	    val ipIdTargPaths : Array[(String,String,String)] = nodeInfos.values.map(info => {
		      	(info.nodeIpAddr, info.nodeId, info.jarPaths.head.toString)}
		    ).toSet.toSeq.sorted.toArray 
	   
	    val uniqueNodePaths : Set[String] = nodeInfos.values.map(info => info.nodeIpAddr + "~" + info.jarPaths.head).toSet 
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
		        [--ClusterConfigurationName <name>  ... not yet... a hint at the future]
      
        Note: Current implementation REQUIRES the node config path.  In a future version, the config path
		will be made an alternative used for building new clusters configurations.  To replace/upgrade an
		existing configuration, the name of the configuration may be supplied in lieu of the NodeConfigPath.
        This name must reflect one of the defined cluster configuration in the metadata store defined in
        the MetadataAPI config file.
      
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
            case "--ClusterConfigurationName" :: value :: tail =>
              nextOption(map ++ Map('ClusterConfigurationName -> value), tail)
            case "--ipFileName" :: value :: tail =>
              nextOption(map ++ Map('ipFileName -> value), tail)
            case "--ipPathPairFileName" :: value :: tail =>
              nextOption(map ++ Map('ipPathPairFileName -> value), tail)
            case "--workDir" :: value :: tail =>
              nextOption(map ++ Map('workDir -> value), tail)  
            case "--ipIdCfgTargPathQuartetFileName" :: value :: tail =>
              nextOption(map ++ Map('ipIdCfgTargPathQuartetFileName -> value), tail)  
            case option :: tail =>
              println("Unknown option " + option)
              println(usage)
              sys.exit(1)
          }
        }
    
        val options = nextOption(Map(), arglist)
        
        val metadataAPIConfig = if (options.contains('MetadataAPIConfig)) options.apply('MetadataAPIConfig) else null
        val nodeConfigPath = if (options.contains('NodeConfigPath)) options.apply('NodeConfigPath) else null
        val clusterConfigurationName = if (options.contains('ClusterConfigurationName)) options.apply('ClusterConfigurationName) else null
        val ipFileName = if (options.contains('ipFileName)) options.apply('ipFileName) else null
        val ipPathPairFileName = if (options.contains('ipPathPairFileName)) options.apply('ipPathPairFileName) else null
        val workDir = if (options.contains('workDir)) options.apply('workDir) else null
        val ipIdCfgTargPathQuartetFileName = if (options.contains('ipIdCfgTargPathQuartetFileName)) options.apply('ipIdCfgTargPathQuartetFileName) else null
        

        val reasonableArguments : Boolean = (metadataAPIConfig != null && metadataAPIConfig.size > 0 
            							&& nodeConfigPath != null && nodeConfigPath.size > 0 
            							&& ipFileName != null && ipFileName.size > 0 
            							&& ipPathPairFileName != null && ipPathPairFileName.size > 0 
            							&& workDir != null && workDir.size > 0 
            							&& ipIdCfgTargPathQuartetFileName != null && ipIdCfgTargPathQuartetFileName.size > 0 
            							&& clusterConfigurationName == null)
        if (! reasonableArguments) {
            println("Your arguments are not satisfactory...Usage:")
            println(usage)
            sys.exit(1)
        }
                    
   		val extractor : NodeInfoExtract = new NodeInfoExtract(metadataAPIConfig, nodeConfigPath)
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


