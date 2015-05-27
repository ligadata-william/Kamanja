package com.ligadata.MetadataAPI

import org.apache.log4j._
import com.ligadata.Utils._
import com.ligadata.fatafat.metadata.ObjType._
import com.ligadata.fatafat.metadata._
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.Serialize._
import java.util.Date

import com.ligadata.keyvaluestore._

object ConfigImpl {
  
  lazy val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName) 
  lazy val serializer = SerializerManager.GetSerializer("kryo")
  
  
  
  def AddNode(nodeId:String,nodePort:Int,nodeIpAddr:String,
        jarPaths:List[String],scala_home:String,
        java_home:String, classpath: String,
        clusterId:String,power:Int,
        roles:Int,description:String): String = {
    try{
      // save in memory
      val ni = MdMgr.GetMdMgr.MakeNode(nodeId,nodePort,nodeIpAddr,jarPaths,scala_home,
               java_home,classpath,clusterId,power,roles,description)
      MdMgr.GetMdMgr.AddNode(ni)
      // save in database
      val key = "NodeInfo." + nodeId
      val value = serializer.SerializeObjectToByteArray(ni)
      DaoImpl.SaveObject(key.toLowerCase,value,MetadataAPIImpl.getConfigStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddNode", null, ErrorCodeConstants.Add_Node_Successful + ":" + nodeId)
      apiResult.toString()
    } catch{
      case e:Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddNode", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Node_Failed + ":" + nodeId)
        apiResult.toString()
      }
    } 
  }

  def UpdateNode(nodeId:String,nodePort:Int,nodeIpAddr:String,
     jarPaths:List[String],scala_home:String,
     java_home:String, classpath: String,
     clusterId:String,power:Int,
     roles:Int,description:String): String = {
    AddNode(nodeId,nodePort,nodeIpAddr,jarPaths,scala_home,
      java_home,classpath,
      clusterId,power,roles,description)
  }

  def RemoveNode(nodeId:String) : String = {
    try{
      MdMgr.GetMdMgr.RemoveNode(nodeId)
      val key = "NodeInfo." + nodeId
      DaoImpl.DeleteObject(key.toLowerCase, MetadataAPIImpl.getConfigStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveNode", null, ErrorCodeConstants.Remove_Node_Successful + ":" + nodeId)
      apiResult.toString()
    } catch{
      case e:Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveNode", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Node_Failed + ":" + nodeId)
        apiResult.toString()
      }
    } 
  }
  
  def RemoveConfig(cfgStr: String): String = {
    var keyList = new Array[String](0)
    try {
      // extract config objects
      val cfg = JsonSerializer.parseEngineConfig(cfgStr)
      // process clusterInfo object if it exists
      val clusters = cfg.Clusters
      if ( clusters != None ){
        val ciList = clusters.get
        ciList.foreach(c1 => {
          MdMgr.GetMdMgr.RemoveCluster(c1.ClusterId.toLowerCase)
          var key = "ClusterInfo." + c1.ClusterId
          keyList   = keyList :+ key.toLowerCase
          MdMgr.GetMdMgr.RemoveClusterCfg(c1.ClusterId.toLowerCase)
          key = "ClusterCfgInfo." + c1.ClusterId
          keyList   = keyList :+ key.toLowerCase
          val nodes = c1.Nodes
          nodes.foreach(n => {
            MdMgr.GetMdMgr.RemoveNode(n.NodeId.toLowerCase)
            key = "NodeInfo." + n.NodeId
            keyList   = keyList :+ key.toLowerCase
          })
        })
      }
      val aiList = cfg.Adapters
      if ( aiList != None ) {
        val adapters = aiList.get
        adapters.foreach(a => {
          MdMgr.GetMdMgr.RemoveAdapter(a.Name.toLowerCase)
          val key = "AdapterInfo." + a.Name
          keyList   = keyList :+ key.toLowerCase
        })
      }
      DaoImpl.RemoveObjectList(keyList, MetadataAPIImpl.getMetadataStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveConfig", null, ErrorCodeConstants.Remove_Config_Successful + ":" + cfgStr)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveConfig", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Config_Failed + ":" + cfgStr)
        apiResult.toString()
      }
    }
  }
    
   // All available config objects(format JSON) as a String
  def GetAllCfgObjects(formatType: String): String = {
    var cfgObjList = new Array[Object](0)
    var jsonStr:String = ""
    var jsonStr1:String = ""
    try {
      val clusters = MdMgr.GetMdMgr.Clusters.values.toArray
      if ( clusters.length != 0 ){
        cfgObjList = cfgObjList :+ clusters
        jsonStr1 = JsonSerializer.SerializeCfgObjectListToJson("Clusters", clusters)
        jsonStr1 = jsonStr1.substring(1)
         jsonStr1 = JsonSerializer.replaceLast(jsonStr1,"}",",")
        jsonStr = jsonStr + jsonStr1
      }
      val clusterCfgs = MdMgr.GetMdMgr.ClusterCfgs.values.toArray
      if ( clusterCfgs.length != 0 ){
        cfgObjList = cfgObjList :+ clusterCfgs
        jsonStr1 = JsonSerializer.SerializeCfgObjectListToJson("ClusterCfgs", clusterCfgs)
        jsonStr1 = jsonStr1.substring(1)
        jsonStr1 = JsonSerializer.replaceLast(jsonStr1,"}",",")
        jsonStr = jsonStr + jsonStr1
      }
      val nodes = MdMgr.GetMdMgr.Nodes.values.toArray
      if ( nodes.length != 0 ){
        cfgObjList = cfgObjList :+ nodes
        jsonStr1 = JsonSerializer.SerializeCfgObjectListToJson("Nodes", nodes)
        jsonStr1 = jsonStr1.substring(1)
        jsonStr1 = JsonSerializer.replaceLast(jsonStr1,"}",",")
        jsonStr = jsonStr + jsonStr1
      }
      val adapters = MdMgr.GetMdMgr.Adapters.values.toArray
      if ( adapters.length != 0 ){
        cfgObjList = cfgObjList :+ adapters
        jsonStr1 = JsonSerializer.SerializeCfgObjectListToJson("Adapters", adapters)
        jsonStr1 = jsonStr1.substring(1)
        jsonStr1 = JsonSerializer.replaceLast(jsonStr1,"}",",")
        jsonStr = jsonStr + jsonStr1
      }

      jsonStr = "{" + JsonSerializer.replaceLast(jsonStr,",","") + "}"

      if ( cfgObjList.length == 0 ){
          logger.debug("No Config Objects found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllCfgObjects", null, ErrorCodeConstants.Get_All_Configs_Failed_Not_Available)
          apiResult.toString()
      }
      else{
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllCfgObjects", jsonStr, ErrorCodeConstants.Get_All_Configs_Successful)
        apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllCfgObjects", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Configs_Failed)
        apiResult.toString()
      }
    }
  }
  
  
  def AddAdapter(name:String,typeString:String,dataFormat: String,className: String, 
     jarName: String, dependencyJars: List[String], 
     adapterSpecificCfg: String,inputAdapterToVerify: String): String = {
    try{
      // save in memory
      val ai = MdMgr.GetMdMgr.MakeAdapter(name,typeString,dataFormat,className,jarName,
            dependencyJars,adapterSpecificCfg,inputAdapterToVerify)
      MdMgr.GetMdMgr.AddAdapter(ai)
      // save in database
      val key = "AdapterInfo." + name
      val value = serializer.SerializeObjectToByteArray(ai)
      DaoImpl.SaveObject(key.toLowerCase,value,MetadataAPIImpl.getConfigStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddAdapter", null, ErrorCodeConstants.Add_Adapter_Successful + ":" + name)
      apiResult.toString()
    } catch{
      case e:Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddAdapter", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Adapter_Failed + ":" + name)
        apiResult.toString()
      }
    } 
  }

  def UpdateAdapter(name:String,typeString:String,dataFormat: String,className: String, 
        jarName: String, dependencyJars: List[String], 
        adapterSpecificCfg: String,inputAdapterToVerify: String): String = {
    AddAdapter(name,typeString,dataFormat,className,jarName,dependencyJars,adapterSpecificCfg,inputAdapterToVerify)
  }

  def RemoveAdapter(name:String) : String = {
    try{
      MdMgr.GetMdMgr.RemoveAdapter(name)
      val key = "AdapterInfo." + name
      DaoImpl.DeleteObject(key.toLowerCase, MetadataAPIImpl.getConfigStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveAdapter", null, ErrorCodeConstants.Remove_Adapter_Successful + ":" + name)
      apiResult.toString()
    } catch{
      case e:Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveAdapter", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Adapter_Failed + ":" + name)
        apiResult.toString()
      }
    } 
  }
  
  
  
  def AddCluster(clusterId:String,description:String,privileges:String) : String = {
    try{
      // save in memory
      val ci = MdMgr.GetMdMgr.MakeCluster(clusterId,description,privileges)
      MdMgr.GetMdMgr.AddCluster(ci)
      // save in database
      val key = "ClusterInfo." + clusterId
      val value = serializer.SerializeObjectToByteArray(ci)
      DaoImpl.SaveObject(key.toLowerCase,value,MetadataAPIImpl.getConfigStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddCluster", null, ErrorCodeConstants.Add_Cluster_Successful + ":" + clusterId)
      apiResult.toString()
    } catch{
      case e:Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddCluster", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Cluster_Failed + ":" + clusterId)
        apiResult.toString()
      }
    } 
  }

  def UpdateCluster(clusterId:String,description:String,privileges:String): String = {
    AddCluster(clusterId,description,privileges)
  }

  def RemoveCluster(clusterId:String) : String = {
    try{
      MdMgr.GetMdMgr.RemoveCluster(clusterId)
      val key = "ClusterInfo." + clusterId
      DaoImpl.DeleteObject(key.toLowerCase,MetadataAPIImpl.getConfigStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveCluster", null, ErrorCodeConstants.Remove_Cluster_Successful + ":" + clusterId)
      apiResult.toString()
    } catch{
      case e:Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveCluster", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Cluster_Failed + ":" + clusterId)
        apiResult.toString()
      }
    } 
  }
  
  
  def LoadAllConfigObjectsIntoCache: Boolean = {
    try {
      var keys = scala.collection.mutable.Set[com.ligadata.keyvaluestore.Key]()
      MetadataAPIImpl.getConfigStore.getAllKeys({ (key: Key) => keys.add(key) })
      val keyArray = keys.toArray
      if (keyArray.length == 0) {
        logger.debug("No config objects available in the Database")
        return false
      }
      keyArray.foreach(key => {
        val obj = DaoImpl.GetObject(key, MetadataAPIImpl.getConfigStore)
        val strKey = MetadataUtils.KeyAsStr(key)
        val i = strKey.indexOf(".")
        val objType = strKey.substring(0, i)
        val typeName = strKey.substring(i + 1)
        objType match {
          case "nodeinfo" => {
              val ni = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[NodeInfo]
              MdMgr.GetMdMgr.AddNode(ni)
          }
          case "adapterinfo" => {
              val ai = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[AdapterInfo]
              MdMgr.GetMdMgr.AddAdapter(ai)
          }
          case "clusterinfo" => {
              val ci = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[ClusterInfo]
              MdMgr.GetMdMgr.AddCluster(ci)
          }
          case "clustercfginfo" => {
             val ci = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[ClusterCfgInfo]
             MdMgr.GetMdMgr.AddClusterCfg(ci)
          }
          case _ => {
              throw InternalErrorException("LoadAllConfigObjectsIntoCache: Unknown objectType " + objType)
          }
        }
      })
      return true
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return false
      }
    }
  }
  
  
  
  def UploadConfig(cfgStr: String): String = {
    var keyList = new Array[String](0)
    var valueList = new Array[Array[Byte]](0)
    try {
      // extract config objects
      val cfg = JsonSerializer.parseEngineConfig(cfgStr)
      // process clusterInfo object if it exists
      val clusters = cfg.Clusters

      if ( clusters == None ){
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UploadConfig", null, ErrorCodeConstants.Upload_Config_Failed + ":" + cfgStr)
        apiResult.toString()
      }
      else{
        val ciList = clusters.get
        logger.debug("Found " + ciList.length + " cluster objects ")
        ciList.foreach(c1 => {
          logger.debug("Processing the cluster => " + c1.ClusterId)
          // save in memory
          var ci = MdMgr.GetMdMgr.MakeCluster(c1.ClusterId,null,null)
          MdMgr.GetMdMgr.AddCluster(ci)
          var key = "ClusterInfo." + ci.clusterId
          var value = serializer.SerializeObjectToByteArray(ci)
          keyList   = keyList :+ key.toLowerCase
          valueList = valueList :+ value

          // gather config name-value pairs
          val cfgMap = new scala.collection.mutable.HashMap[String,String] 
          cfgMap("DataStore") = c1.Config.DataStore
          cfgMap("StatusInfo") = c1.Config.StatusInfo
          cfgMap("ZooKeeperInfo") = c1.Config.ZooKeeperInfo
          cfgMap("EnvironmentContext") = c1.Config.EnvironmentContext

          // save in memory
          val cic = MdMgr.GetMdMgr.MakeClusterCfg(c1.ClusterId,cfgMap,null,null)
          MdMgr.GetMdMgr.AddClusterCfg(cic)
          key = "ClusterCfgInfo." + cic.clusterId
          value = serializer.SerializeObjectToByteArray(cic)
          keyList   = keyList :+ key.toLowerCase
          valueList = valueList :+ value

          val nodes = c1.Nodes
          nodes.foreach(n => {
            val ni = MdMgr.GetMdMgr.MakeNode(n.NodeId,n.NodePort,n.NodeIpAddr,n.JarPaths,
                                             n.Scala_home,n.Java_home,n.Classpath,c1.ClusterId,0,0,null)
            MdMgr.GetMdMgr.AddNode(ni)
            val key = "NodeInfo." + ni.nodeId
            val value = serializer.SerializeObjectToByteArray(ni)
            keyList   = keyList :+ key.toLowerCase
            valueList = valueList :+ value
          })
        })
        val aiList = cfg.Adapters
        if ( aiList != None ){
          val adapters = aiList.get
          adapters.foreach(a => {
            var depJars:List[String] = null
            if(a.DependencyJars != None ){
              depJars = a.DependencyJars.get
            }
            var ascfg:String = null
            if(a.AdapterSpecificCfg != None ){
              ascfg = a.AdapterSpecificCfg.get
            }
            var inputAdapterToVerify: String = null
            if(a.InputAdapterToVerify != None ){
              inputAdapterToVerify = a.InputAdapterToVerify.get
            }
            var dataFormat: String = null
            if(a.DataFormat != None ){
              dataFormat = a.DataFormat.get
            }
            // save in memory
            val ai = MdMgr.GetMdMgr.MakeAdapter(a.Name,a.TypeString,dataFormat,a.ClassName,a.JarName,depJars,ascfg, inputAdapterToVerify)
            MdMgr.GetMdMgr.AddAdapter(ai)
            val key = "AdapterInfo." + ai.name
            val value = serializer.SerializeObjectToByteArray(ai)
            keyList   = keyList :+ key.toLowerCase
            valueList = valueList :+ value
          })
        } else {
          logger.debug("Found no adapater objects in the config file")
        }
        DaoImpl.SaveObjectList(keyList,valueList, MetadataAPIImpl.getConfigStore)
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "UploadConfig", null, ErrorCodeConstants.Upload_Config_Successful + ":" + cfgStr)
        apiResult.toString()
      }
    }catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UploadConfig", null, "Error :" + e.toString() + ErrorCodeConstants.Upload_Config_Failed + ":" + cfgStr)
        apiResult.toString()
      }
    }
  }
  
  
  def AddClusterCfg(clusterCfgId:String,cfgMap:scala.collection.mutable.HashMap[String,String],
        modifiedTime:Date,createdTime:Date) : String = {
    try{
      // save in memory
      val ci = MdMgr.GetMdMgr.MakeClusterCfg(clusterCfgId,cfgMap, modifiedTime,createdTime)
      MdMgr.GetMdMgr.AddClusterCfg(ci)
      // save in database
      val key = "ClusterCfgInfo." + clusterCfgId
      val value = serializer.SerializeObjectToByteArray(ci)
      DaoImpl.SaveObject(key.toLowerCase,value, MetadataAPIImpl.getConfigStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddClusterCfg", null, ErrorCodeConstants.Add_Cluster_Config_Successful + ":" + clusterCfgId)
      apiResult.toString()
    } catch{
      case e:Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddClusterCfg", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Cluster_Config_Failed + ":" + clusterCfgId)
        apiResult.toString()
      }
    } 
  }


  def UpdateClusterCfg(clusterCfgId:String,cfgMap:scala.collection.mutable.HashMap[String,String],
           modifiedTime:Date,createdTime:Date): String = {
    AddClusterCfg(clusterCfgId,cfgMap,modifiedTime,createdTime)
  }

  def RemoveClusterCfg(clusterCfgId:String) : String = {
    try{
      MdMgr.GetMdMgr.RemoveClusterCfg(clusterCfgId)
      val key = "ClusterCfgInfo." + clusterCfgId
      DaoImpl.DeleteObject(key.toLowerCase,MetadataAPIImpl.getConfigStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveCLusterCfg", null, ErrorCodeConstants.Remove_Cluster_Config_Successful + ":" + clusterCfgId)
      apiResult.toString()
    } catch{
      case e:Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveCLusterCfg", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Cluster_Config_Failed + ":" + clusterCfgId)
        apiResult.toString()
      }
    } 
  }
  
  // All available adapters(format JSON) as a String
  def GetAllAdapters(formatType: String): String = {
    try {
      val adapters = MdMgr.GetMdMgr.Adapters.values.toArray
      if ( adapters.length == 0 ){
          logger.debug("No Adapters found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllAdapters", null, ErrorCodeConstants.Get_All_Adapters_Failed_Not_Available)
          apiResult.toString()
      }
      else{
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllAdapters", JsonSerializer.SerializeCfgObjectListToJson("Adapters", adapters), ErrorCodeConstants.Get_All_Adapters_Successful)
        apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllAdapters", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Adapters_Failed)

        apiResult.toString()
      }
    }
  }

  // All available clusters(format JSON) as a String
  def GetAllClusters(formatType: String): String = {
    try {
      val clusters = MdMgr.GetMdMgr.Clusters.values.toArray
      if ( clusters.length == 0 ){
          logger.debug("No Clusters found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllClusters", null, ErrorCodeConstants.Get_All_Clusters_Failed_Not_Available)
          apiResult.toString()
      }
      else{
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllClusters", JsonSerializer.SerializeCfgObjectListToJson("Clusters", clusters), ErrorCodeConstants.Get_All_Clusters_Successful)
        apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllClusters", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Clusters_Failed)
        apiResult.toString()
      }
    }
  }
  
  
  
  // All available nodes(format JSON) as a String
  def GetAllNodes(formatType: String): String = {
    try {
      val nodes = MdMgr.GetMdMgr.Nodes.values.toArray
      if ( nodes.length == 0 ){
          logger.debug("No Nodes found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllNodes", null, ErrorCodeConstants.Get_All_Nodes_Failed_Not_Available)
          apiResult.toString()
      }
      else{
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllNodes", JsonSerializer.SerializeCfgObjectListToJson("Nodes", nodes), ErrorCodeConstants.Get_All_Nodes_Successful)
        apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllNodes", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Nodes_Failed)
        apiResult.toString()
      }
    }
  }


  // All available clusterCfgs(format JSON) as a String
  def GetAllClusterCfgs(formatType: String): String = {
    try {
      val clusterCfgs = MdMgr.GetMdMgr.ClusterCfgs.values.toArray
      if ( clusterCfgs.length == 0 ){
          logger.debug("No ClusterCfgs found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllClusterCfgs", null, ErrorCodeConstants.Get_All_Cluster_Configs_Failed_Not_Available)
          apiResult.toString()
      }
      else{
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllClusterCfgs", JsonSerializer.SerializeCfgObjectListToJson("ClusterCfgs", clusterCfgs), ErrorCodeConstants.Get_All_Cluster_Configs_Successful)

        apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllClusterCfgs", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Cluster_Configs_Failed)

        apiResult.toString()
      }
    }
  }

}