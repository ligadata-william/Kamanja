package com.ligadata.installer

import scala.collection.immutable.{Set, HashMap}
import scala.io.Source
import java.io._
import com.ligadata.MetadataAPI.{ MetadataAPIImpl }
import com.ligadata.olep.metadata._

class NodeInfoExtract(val metadataAPIConfig : String, val nodeConfigPath : String) {

	MetadataAPIImpl.InitMdMgrFromBootStrap(metadataAPIConfig)
	
	val result : String = MetadataAPIImpl.UploadConfig(Source.fromFile(nodeConfigPath).mkString)
	//println(result)
	
	def extract : (Array[String],Array[(String,String)]) = {
		val nodeInfos : Map[String,NodeInfo] = MdMgr.GetMdMgr.Nodes
		
		/** 
		 	Since nodes can exist on the same machine, and use the same directory for all node implementations,
		 	some work must be done to create a concise set of node/install directory pairs... 
		 	
		 	Build a Set[String] of (nodeIp,installPath) pairs...
		 */	
    val uniqueNodes : Array[String] = nodeInfos.values.map(info => info.nodeIpAddr).toSet.toSeq.sorted.toArray 
    val uniqueNodePaths : Set[String] = nodeInfos.values.map(info => info.nodeIpAddr + "~" + info.jarPaths.head).toSet 
		val nodeIpPathPairs : Array[(String,String)] = uniqueNodePaths.map(itm => (itm.split('~').head, itm.split('~').last)).toSeq.sorted.toArray
		(uniqueNodes,nodeIpPathPairs)
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
        

        val reasonableArguments : Boolean = (metadataAPIConfig != null && metadataAPIConfig.size > 0 && 
            nodeConfigPath != null && nodeConfigPath.size > 0 && ipFileName != null && ipFileName.size > 0 && ipPathPairFileName != null && ipPathPairFileName.size > 0 && clusterConfigurationName == null)
        if (! reasonableArguments) {
            println("Your arguments are not satisfactory...Usage:")
            println(usage)
            sys.exit(1)
        }
                    
   		val extractor : NodeInfoExtract = new NodeInfoExtract(metadataAPIConfig, nodeConfigPath)
   		val (ips,ipPathPairs) : (Array[String], Array[(String,String)]) = extractor.extract 
		
      writeFileIps(ipFileName, ips)
      writeFilePairs(ipPathPairFileName, ipPathPairs)
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
  
}


