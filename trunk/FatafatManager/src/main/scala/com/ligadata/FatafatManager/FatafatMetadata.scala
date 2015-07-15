
package com.ligadata.FatafatManager

import com.ligadata.fatafat.metadata.{ BaseElem, MappedMsgTypeDef, BaseAttributeDef, StructTypeDef, EntityType, AttributeDef, ArrayBufTypeDef, MessageDef, ContainerDef, ModelDef }
import com.ligadata.fatafat.metadata._
import com.ligadata.fatafat.metadata.MdMgr._

import com.ligadata.fatafat.metadataload.MetadataLoad
import scala.collection.mutable.TreeSet
import scala.util.control.Breaks._
import com.ligadata.FatafatBase.{ BaseMsg, MdlInfo, MessageContainerBase, MessageContainerObjBase, BaseMsgObj, BaseContainerObj, BaseContainer, ModelBaseObj, TransformMessage, EnvContext, MdBaseResolveInfo }
import scala.collection.mutable.HashMap
import org.apache.log4j._
import scala.collection.mutable.ArrayBuffer
import com.ligadata.Serialize._
import com.ligadata.ZooKeeper._
import com.ligadata.MetadataAPI.MetadataAPIImpl
import java.util.concurrent.locks.ReentrantReadWriteLock;

class TransformMsgFldsMap(var keyflds: Array[Int], var outputFlds: Array[Int]) {
}

// msgobj is null for Containers
class MsgContainerObjAndTransformInfo(var tranformMsgFlds: TransformMsgFldsMap, var contmsgobj: MessageContainerObjBase) {
  var parents = new ArrayBuffer[(String, String)] // Immediate parent comes at the end, grand parent last but one, ... Messages/Containers. the format is Message/Container Type name and the variable in that.
  var childs = new ArrayBuffer[(String, String)] // Child Messages/Containers (Name & type). We fill this when we create message and populate parent later from this
}

// This is shared by multiple threads to read (because we are not locking). We create this only once at this moment while starting the manager
class FatafatMetadata {
  val LOG = Logger.getLogger(getClass);

  // LOG.setLevel(Level.TRACE)

  // Metadata manager
  val messageObjects = new HashMap[String, MsgContainerObjAndTransformInfo]
  val containerObjects = new HashMap[String, MsgContainerObjAndTransformInfo]
  val modelObjects = new HashMap[String, MdlInfo]

  def ValidateAllRequiredJars(tmpMsgDefs: Option[scala.collection.immutable.Set[MessageDef]], tmpContainerDefs: Option[scala.collection.immutable.Set[ContainerDef]],
    tmpModelDefs: Option[scala.collection.immutable.Set[ModelDef]]): Boolean = {
    val allJarsToBeValidated = scala.collection.mutable.Set[String]();

    if (tmpMsgDefs != None) { // Not found any messages
      tmpMsgDefs.get.foreach(elem => {
        allJarsToBeValidated ++= GetAllJarsFromElem(elem)
      })
    }

    if (tmpContainerDefs != None) { // Not found any messages
      tmpContainerDefs.get.foreach(elem => {
        allJarsToBeValidated ++= GetAllJarsFromElem(elem)
      })
    }

    if (tmpModelDefs != None) { // Not found any messages
      tmpModelDefs.get.foreach(elem => {
        allJarsToBeValidated ++= GetAllJarsFromElem(elem)
      })
    }

    val nonExistsJars = FatafatMdCfg.CheckForNonExistanceJars(allJarsToBeValidated.toSet)
    if (nonExistsJars.size > 0) {
      LOG.error("Not found jars in Messages/Containers/Models Jars List : {" + nonExistsJars.mkString(", ") + "}")
      return false
    }

    true
  }

  def LoadMdMgrElems(loadedJars: TreeSet[String], loader: FatafatClassLoader, mirror: reflect.runtime.universe.Mirror,
    tmpMsgDefs: Option[scala.collection.immutable.Set[MessageDef]], tmpContainerDefs: Option[scala.collection.immutable.Set[ContainerDef]],
    tmpModelDefs: Option[scala.collection.immutable.Set[ModelDef]]): Unit = {
    PrepareMessages(loadedJars, loader, mirror, tmpMsgDefs)
    PrepareContainers(loadedJars, loader, mirror, tmpContainerDefs)
    PrepareModels(loadedJars, loader, mirror, tmpModelDefs)

    LOG.debug("Loaded Metadata Messages:" + messageObjects.map(container => container._1).mkString(","))
    LOG.debug("Loaded Metadata Containers:" + containerObjects.map(container => container._1).mkString(","))
    LOG.debug("Loaded Metadata Models:" + modelObjects.map(container => container._1).mkString(","))
  }

  private[this] def CheckAndPrepMessage(clsName: String, loadedJars: TreeSet[String], loader: FatafatClassLoader, mirror: reflect.runtime.universe.Mirror, msg: MessageDef): Boolean = {
    var isMsg = true
    var curClass: Class[_] = null

    try {
      // If required we need to enable this test
      // Convert class name into a class
      var curClz = Class.forName(clsName, true, loader)
      curClass = curClz

      isMsg = false

      while (curClz != null && isMsg == false) {
        isMsg = ManagerUtils.isDerivedFrom(curClz, "com.ligadata.FatafatBase.BaseMsgObj")
        if (isMsg == false)
          curClz = curClz.getSuperclass()
      }
    } catch {
      case e: Exception => {
        LOG.error("Failed to get classname :" + clsName)
        e.printStackTrace
        return false
      }
    }

    if (isMsg) {
      try {
        var objinst: Any = null
        try {
          // Trying Singleton Object
          val module = mirror.staticModule(clsName)
          val obj = mirror.reflectModule(module)
          objinst = obj.instance
        } catch {
          case e: Exception => {
            // Trying Regular Object instantiation
            objinst = curClass.newInstance
          }
        }
        if (objinst.isInstanceOf[BaseMsgObj]) {
          val messageobj = objinst.asInstanceOf[BaseMsgObj]
          val msgName = msg.FullName.toLowerCase
          var tranformMsgFlds: TransformMsgFldsMap = null
          if (messageobj.NeedToTransformData) {
            val txfnMsg: TransformMessage = messageobj.TransformDataAttributes

            val inputFieldsMap = txfnMsg.inputFields.map(f => f.trim.toLowerCase).view.zipWithIndex.toMap
            val outputFldIdxs = txfnMsg.outputFields.map(f => {
              val fld = inputFieldsMap.getOrElse(f.trim.toLowerCase, -1)
              if (fld < 0) {
                throw new Exception("Output Field \"" + f + "\" not found in input list of fields")
              }
              fld
            })

            val keyfldsIdxs = txfnMsg.outputKeys.map(f => {
              val fld = inputFieldsMap.getOrElse(f.trim.toLowerCase, -1)
              if (fld < 0)
                throw new Exception("Key Field \"" + f + "\" not found in input list of fields")
              fld
            })
            tranformMsgFlds = new TransformMsgFldsMap(keyfldsIdxs, outputFldIdxs)
          }
          val mgsObj = new MsgContainerObjAndTransformInfo(tranformMsgFlds, messageobj)
          GetChildsFromEntity(msg.containerType, mgsObj.childs)
          messageObjects(msgName) = mgsObj

          LOG.debug("Created Message:" + msgName)
          return true
        } else {
          LOG.error("Failed to instantiate message object :" + clsName)
          return false
        }
      } catch {
        case e: Exception => {
          LOG.error("Failed to instantiate message object:" + clsName + ". Reason:" + e.getCause + ". Message:" + e.getMessage())
          return false
        }
      }
    }
    return false
  }

  def PrepareMessage(loadedJars: TreeSet[String], loader: FatafatClassLoader, mirror: reflect.runtime.universe.Mirror, msg: MessageDef, loadJars: Boolean): Unit = {
    if (loadJars)
      LoadJarIfNeeded(msg, loadedJars, loader)
    // else Assuming we are already loaded all the required jars

    var clsName = msg.PhysicalName.trim
    var orgClsName = clsName

    var foundFlg = CheckAndPrepMessage(clsName, loadedJars, loader, mirror, msg)

    if (foundFlg == false) {
      if (clsName.size > 0 && clsName.charAt(clsName.size - 1) != '$') { // if no $ at the end we are taking $
        clsName = clsName + "$"
        foundFlg = CheckAndPrepMessage(clsName, loadedJars, loader, mirror, msg)
      }
    }
    if (foundFlg == false) {
      LOG.error("Failed to instantiate message object :" + orgClsName)
    }
  }

  private[this] def CheckAndPrepContainer(clsName: String, loadedJars: TreeSet[String], loader: FatafatClassLoader, mirror: reflect.runtime.universe.Mirror, container: ContainerDef): Boolean = {
    var isContainer = true
    var curClass: Class[_] = null

    try {
      // If required we need to enable this test
      // Convert class name into a class
      var curClz = Class.forName(clsName, true, loader)
      curClass = curClz

      isContainer = false

      while (curClz != null && isContainer == false) {
        isContainer = ManagerUtils.isDerivedFrom(curClz, "com.ligadata.FatafatBase.BaseContainerObj")
        if (isContainer == false)
          curClz = curClz.getSuperclass()
      }
    } catch {
      case e: Exception => {
        LOG.error("Failed to get classname :" + clsName)
        e.printStackTrace
        return false
      }
    }

    if (isContainer) {
      try {
        var objinst: Any = null
        try {
          // Trying Singleton Object
          val module = mirror.staticModule(clsName)
          val obj = mirror.reflectModule(module)
          objinst = obj.instance
        } catch {
          case e: Exception => {
            // Trying Regular Object instantiation
            objinst = curClass.newInstance
          }
        }

        if (objinst.isInstanceOf[BaseContainerObj]) {
          val containerobj = objinst.asInstanceOf[BaseContainerObj]
          val contName = container.FullName.toLowerCase
          val contObj = new MsgContainerObjAndTransformInfo(null, containerobj)
          GetChildsFromEntity(container.containerType, contObj.childs)
          containerObjects(contName) = contObj

          LOG.debug("Created Container:" + contName)
          return true
        } else {
          LOG.error("Failed to instantiate container object :" + clsName)
          return false
        }
      } catch {
        case e: Exception => {
          LOG.error("Failed to instantiate containerObjects object:" + clsName + ". Reason:" + e.getCause + ". Message:" + e.getMessage())
          return false
        }
      }
    }
    return false
  }

  def PrepareContainer(loadedJars: TreeSet[String], loader: FatafatClassLoader, mirror: reflect.runtime.universe.Mirror, container: ContainerDef, loadJars: Boolean, ignoreClassLoad: Boolean): Unit = {
    if (loadJars)
      LoadJarIfNeeded(container, loadedJars, loader)
    // else Assuming we are already loaded all the required jars

    if (ignoreClassLoad) {
      val contName = container.FullName.toLowerCase
      val containerObj = new MsgContainerObjAndTransformInfo(null, null)
      GetChildsFromEntity(container.containerType, containerObj.childs)
      containerObjects(contName) = containerObj
      LOG.debug("Added Base Container:" + contName)
      return
    }

    var clsName = container.PhysicalName.trim
    var orgClsName = clsName

    var foundFlg = CheckAndPrepContainer(clsName, loadedJars, loader, mirror, container)
    if (foundFlg == false) {
      if (clsName.size > 0 && clsName.charAt(clsName.size - 1) != '$') { // if no $ at the end we are taking $
        clsName = clsName + "$"
        foundFlg = CheckAndPrepContainer(clsName, loadedJars, loader, mirror, container)
      }
    }
    if (foundFlg == false) {
      LOG.error("Failed to instantiate container object :" + orgClsName)
    }
  }

  private[this] def CheckAndPrepModel(clsName: String, loadedJars: TreeSet[String], loader: FatafatClassLoader, mirror: reflect.runtime.universe.Mirror, mdl: ModelDef): Boolean = {
    var isModel = true
    var curClass: Class[_] = null

    try {
      // If required we need to enable this test
      // Convert class name into a class
      var curClz = Class.forName(clsName, true, loader)
      curClass = curClz

      isModel = false

      while (curClz != null && isModel == false) {
        isModel = ManagerUtils.isDerivedFrom(curClz, "com.ligadata.FatafatBase.ModelBaseObj")
        if (isModel == false)
          curClz = curClz.getSuperclass()
      }
    } catch {
      case e: Exception => {
        LOG.error("Failed to get classname :" + clsName)
        e.printStackTrace
        return false
      }
    }

    // LOG.debug("Loading Model:" + mdl.FullName + ". ClassName: " + clsName + ". IsModel:" + isModel)

    if (isModel) {
      try {
        var objinst: Any = null
        try {
          // Trying Singleton Object
          val module = mirror.staticModule(clsName)
          val obj = mirror.reflectModule(module)
          // curClz.newInstance
          objinst = obj.instance
        } catch {
          case e: Exception => {
            // Trying Regular Object instantiation
            objinst = curClass.newInstance
          }
        }

        // val objinst = obj.instance
        if (objinst.isInstanceOf[ModelBaseObj]) {
          val modelobj = objinst.asInstanceOf[ModelBaseObj]
          val mdlName = (mdl.NameSpace.trim + "." + mdl.Name.trim).toLowerCase
          modelObjects(mdlName) = new MdlInfo(modelobj, mdl.jarName, mdl.dependencyJarNames, "Ligadata")
          LOG.debug("Created Model:" + mdlName)
          return true
        } else {
          LOG.error("Failed to instantiate model object :" + clsName)
          LOG.debug("Failed to instantiate model object :" + clsName + ". ObjType0:" + objinst.getClass.getSimpleName + ". ObjType1:" + objinst.getClass.getCanonicalName)
          return false
        }
      } catch {
        case e: Exception =>
          LOG.error("Failed to instantiate model object:" + clsName + ". Reason:" + e.getCause + ". Message:" + e.getMessage)
          return false
      }
    }
    return false
  }

  def PrepareModel(loadedJars: TreeSet[String], loader: FatafatClassLoader, mirror: reflect.runtime.universe.Mirror, mdl: ModelDef, loadJars: Boolean): Unit = {
    if (loadJars)
      LoadJarIfNeeded(mdl, loadedJars, loader)
    // else Assuming we are already loaded all the required jars

    var clsName = mdl.PhysicalName.trim
    var orgClsName = clsName

    var foundFlg = CheckAndPrepModel(clsName, loadedJars, loader, mirror, mdl)

    if (foundFlg == false) {
      if (clsName.size > 0 && clsName.charAt(clsName.size - 1) != '$') { // if no $ at the end we are taking $
        clsName = clsName + "$"
        foundFlg = CheckAndPrepModel(clsName, loadedJars, loader, mirror, mdl)
      }
    }
    if (foundFlg == false) {
      LOG.error("Failed to instantiate model object :" + orgClsName)
    }
  }

  def GetAllJarsFromElem(elem: BaseElem): Set[String] = {
    var allJars: Array[String] = null

    val jarname = if (elem.JarName == null) "" else elem.JarName.trim

    if (elem.DependencyJarNames != null && elem.DependencyJarNames.size > 0 && jarname.size > 0) {
      allJars = elem.DependencyJarNames :+ jarname
    } else if (elem.DependencyJarNames != null && elem.DependencyJarNames.size > 0) {
      allJars = elem.DependencyJarNames
    } else if (jarname.size > 0) {
      allJars = Array(jarname)
    } else {
      return Set[String]()
    }

    return allJars.map(j => FatafatConfiguration.GetValidJarFile(FatafatConfiguration.jarPaths, j)).toSet
  }

  private def LoadJarIfNeeded(elem: BaseElem, loadedJars: TreeSet[String], loader: FatafatClassLoader): Boolean = {
    val allJars = GetAllJarsFromElem(elem)
    if (allJars.size > 0) {
      return ManagerUtils.LoadJars(allJars.toArray, loadedJars, loader)
    } else {
      return true
    }
  }

  private def GetChildsFromEntity(entity: EntityType, childs: ArrayBuffer[(String, String)]): Unit = {
    // mgsObj.childs +=
    if (entity.isInstanceOf[MappedMsgTypeDef]) {
      var attrMap = entity.asInstanceOf[MappedMsgTypeDef].attrMap
      //BUGBUG:: Checking for only one level at this moment
      if (attrMap != null) {
        childs ++= attrMap.filter(a => (a._2.isInstanceOf[AttributeDef] && (a._2.asInstanceOf[AttributeDef].aType.isInstanceOf[MappedMsgTypeDef] || a._2.asInstanceOf[AttributeDef].aType.isInstanceOf[StructTypeDef]))).map(a => (a._2.Name, a._2.asInstanceOf[AttributeDef].aType.FullName))
        // If the attribute is an arraybuffer (not yet handling others)
        childs ++= attrMap.filter(a => (a._2.isInstanceOf[AttributeDef] && a._2.asInstanceOf[AttributeDef].aType.isInstanceOf[ArrayBufTypeDef] && (a._2.asInstanceOf[AttributeDef].aType.asInstanceOf[ArrayBufTypeDef].elemDef.isInstanceOf[MappedMsgTypeDef] || a._2.asInstanceOf[AttributeDef].aType.asInstanceOf[ArrayBufTypeDef].elemDef.isInstanceOf[StructTypeDef]))).map(a => (a._2.Name, a._2.asInstanceOf[AttributeDef].aType.asInstanceOf[ArrayBufTypeDef].elemDef.FullName))
      }
    } else if (entity.isInstanceOf[StructTypeDef]) {
      var memberDefs = entity.asInstanceOf[StructTypeDef].memberDefs
      //BUGBUG:: Checking for only one level at this moment
      if (memberDefs != null) {
        childs ++= memberDefs.filter(a => (a.isInstanceOf[AttributeDef] && (a.asInstanceOf[AttributeDef].aType.isInstanceOf[MappedMsgTypeDef] || a.asInstanceOf[AttributeDef].aType.isInstanceOf[StructTypeDef]))).map(a => (a.Name, a.asInstanceOf[AttributeDef].aType.FullName))
        // If the attribute is an arraybuffer (not yet handling others)
        childs ++= memberDefs.filter(a => (a.isInstanceOf[AttributeDef] && a.asInstanceOf[AttributeDef].aType.isInstanceOf[ArrayBufTypeDef] && (a.asInstanceOf[AttributeDef].aType.asInstanceOf[ArrayBufTypeDef].elemDef.isInstanceOf[MappedMsgTypeDef] || a.asInstanceOf[AttributeDef].aType.asInstanceOf[ArrayBufTypeDef].elemDef.isInstanceOf[StructTypeDef]))).map(a => (a.Name, a.asInstanceOf[AttributeDef].aType.asInstanceOf[ArrayBufTypeDef].elemDef.FullName))
      }
    } else {
      // Nothing to do at this moment
    }
  }

  private def PrepareMessages(loadedJars: TreeSet[String], loader: FatafatClassLoader, mirror: reflect.runtime.universe.Mirror, tmpMsgDefs: Option[scala.collection.immutable.Set[MessageDef]]): Unit = {
    if (tmpMsgDefs == None) // Not found any messages
      return

    val msgDefs = tmpMsgDefs.get

    // Load all jars first
    msgDefs.foreach(msg => {
      // LOG.debug("Loading msg:" + msg.FullName)
      LoadJarIfNeeded(msg, loadedJars, loader)
    })

    msgDefs.foreach(msg => {
      PrepareMessage(loadedJars, loader, mirror, msg, false) // Already Loaded required dependency jars before calling this
    })
  }

  private def PrepareContainers(loadedJars: TreeSet[String], loader: FatafatClassLoader, mirror: reflect.runtime.universe.Mirror, tmpContainerDefs: Option[scala.collection.immutable.Set[ContainerDef]]): Unit = {
    if (tmpContainerDefs == None) // Not found any containers
      return

    val containerDefs = tmpContainerDefs.get

    // Load all jars first
    containerDefs.foreach(container => {
      LoadJarIfNeeded(container, loadedJars, loader)
    })

    val baseContainersPhyName = scala.collection.mutable.Set[String]()
    val baseContainerInfo = MetadataLoad.BaseContainersInfo
    baseContainerInfo.foreach(bc => {
      baseContainersPhyName += bc._3
    })

    containerDefs.foreach(container => {
      PrepareContainer(loadedJars, loader, mirror, container, false, baseContainersPhyName.contains(container.PhysicalName.trim)) // Already Loaded required dependency jars before calling this
    })

  }

  private def PrepareModels(loadedJars: TreeSet[String], loader: FatafatClassLoader, mirror: reflect.runtime.universe.Mirror, tmpModelDefs: Option[scala.collection.immutable.Set[ModelDef]]): Unit = {
    if (tmpModelDefs == None) // Not found any models
      return

    val modelDefs = tmpModelDefs.get

    // Load all jars first
    modelDefs.foreach(mdl => {
      LoadJarIfNeeded(mdl, loadedJars, loader)
    })

    modelDefs.foreach(mdl => {
      PrepareModel(loadedJars, loader, mirror, mdl, false) // Already Loaded required dependency jars before calling this
    })
  }
}

object FatafatMetadata extends MdBaseResolveInfo {
  var envCtxt: EnvContext = null // Engine will set it once EnvContext is initialized
  private[this] val LOG = Logger.getLogger(getClass);
  private[this] val mdMgr = GetMdMgr
  private[this] var loadedJars: TreeSet[String] = _
  private[this] var loader: FatafatClassLoader = _
  private[this] var mirror: reflect.runtime.universe.Mirror = _

  private[this] var messageContainerObjects = new HashMap[String, MsgContainerObjAndTransformInfo]
  private[this] var modelObjects = new HashMap[String, MdlInfo]
  private[this] var zkListener: ZooKeeperListener = _

  private[this] val reent_lock = new ReentrantReadWriteLock(true);

  //LOG.setLevel(Level.TRACE)

  private def UpdateFatafatMdObjects(msgObjects: HashMap[String, MsgContainerObjAndTransformInfo], contObjects: HashMap[String, MsgContainerObjAndTransformInfo],
    mdlObjects: HashMap[String, MdlInfo], removedModels: ArrayBuffer[(String, String, Long)], removedMessages: ArrayBuffer[(String, String, Long)],
    removedContainers: ArrayBuffer[(String, String, Long)]): Unit = {

    var exp: Exception = null

    reent_lock.writeLock().lock();
    try {
      localUpdateFatafatMdObjects(msgObjects, contObjects, mdlObjects, removedModels, removedMessages, removedContainers)
    } catch {
      case e: Exception => { exp = e }
    } finally {
      reent_lock.writeLock().unlock();
    }
    if (exp != null)
      throw exp
  }

  private def localUpdateFatafatMdObjects(msgObjects: HashMap[String, MsgContainerObjAndTransformInfo], contObjects: HashMap[String, MsgContainerObjAndTransformInfo],
    mdlObjects: HashMap[String, MdlInfo], removedModels: ArrayBuffer[(String, String, Long)], removedMessages: ArrayBuffer[(String, String, Long)],
    removedContainers: ArrayBuffer[(String, String, Long)]): Unit = {
    //BUGBUG:: Assuming there is no issues if we remove the objects first and then add the new objects. We are not adding the object in the same order as it added in the transaction. 

    // First removing the objects
    // Removing Models
    if (removedModels != null && removedModels.size > 0) {
      removedModels.foreach(mdl => {
        val elemName = (mdl._1.trim + "." + mdl._2.trim).toLowerCase
        modelObjects -= elemName
      })
    }

    // Removing Messages
    if (removedMessages != null && removedMessages.size > 0) {
      removedMessages.foreach(msg => {
        val elemName = (msg._1.trim + "." + msg._2.trim).toLowerCase
        messageContainerObjects -= elemName //BUGBUG:: It has both Messages & Containers. Are we sure it only removes Messages here?
      })
    }

    // Removing Containers
    if (removedContainers != null && removedContainers.size > 0) {
      removedContainers.foreach(cnt => {
        val elemName = (cnt._1.trim + "." + cnt._2.trim).toLowerCase
        messageContainerObjects -= elemName //BUGBUG:: It has both Messages & Containers. Are we sure it only removes Containers here?
      })
    }

    // Adding new objects now
    // Adding container
    if (contObjects != null && contObjects.size > 0) {
      messageContainerObjects ++= contObjects
      if (envCtxt != null) {
        val containerNames = contObjects.map(container => container._1.toLowerCase).toList.sorted.toArray // Sort topics by names
        envCtxt.AddNewMessageOrContainers(FatafatMetadata.getMdMgr, FatafatConfiguration.dataStoreType, FatafatConfiguration.dataLocation, FatafatConfiguration.dataSchemaName, FatafatConfiguration.databasePrincipal, FatafatConfiguration.databaseKeytab, containerNames, true, FatafatConfiguration.statusInfoStoreType, FatafatConfiguration.statusInfoSchemaName, FatafatConfiguration.statusInfoLocation, FatafatConfiguration.statusInfoPrincipal, FatafatConfiguration.statusInfoKeytab) // Containers
      }
    }

    // Adding Messages
    if (msgObjects != null && msgObjects.size > 0) {
      messageContainerObjects ++= msgObjects
      if (envCtxt != null) {
        val topMessageNames = msgObjects.filter(msg => msg._2.parents.size == 0).map(msg => msg._1.toLowerCase).toList.sorted.toArray // Sort topics by names
        envCtxt.AddNewMessageOrContainers(FatafatMetadata.getMdMgr, FatafatConfiguration.dataStoreType, FatafatConfiguration.dataLocation, FatafatConfiguration.dataSchemaName, FatafatConfiguration.databasePrincipal, FatafatConfiguration.databaseKeytab, topMessageNames, false, FatafatConfiguration.statusInfoStoreType, FatafatConfiguration.statusInfoSchemaName, FatafatConfiguration.statusInfoLocation, FatafatConfiguration.statusInfoPrincipal, FatafatConfiguration.statusInfoKeytab) // Messages
      }
    }

    // Adding Models
    if (mdlObjects != null && mdlObjects.size > 0)
      modelObjects ++= mdlObjects

    // If messages/Containers removed or added, jsut change the parents chain
    if ((removedMessages != null && removedMessages.size > 0) ||
      (removedContainers != null && removedContainers.size > 0) ||
      (contObjects != null && contObjects.size > 0) ||
      (msgObjects != null && msgObjects.size > 0)) {

      // Prepare Parents for each message now
      val childToParentMap = scala.collection.mutable.Map[String, (String, String)]() // ChildType, (ParentType, ChildAttrName) 

      // Clear previous parents
      messageContainerObjects.foreach(c => {
        c._2.parents.clear
      })

      // 1. First prepare one level of parents
      messageContainerObjects.foreach(m => {
        m._2.childs.foreach(c => {
          // Checking whether we already have in childToParentMap or not before we replace. So that way we can check same child under multiple parents.
          val childMsgNm = c._2.toLowerCase
          val fnd = childToParentMap.getOrElse(childMsgNm, null)
          if (fnd != null) {
            LOG.error(s"$childMsgNm is used as child under $c and $fnd._1. First detected $fnd._1, so using as child of $fnd._1 as it is.")
          } else {
            childToParentMap(childMsgNm) = (m._1.toLowerCase, c._1)
          }
        })
      })

      // 2. Now prepare Full Parent Hierarchy
      messageContainerObjects.foreach(m => {
        var curParent = childToParentMap.getOrElse(m._1.toLowerCase, null)
        while (curParent != null) {
          m._2.parents += curParent
          curParent = childToParentMap.getOrElse(curParent._1.toLowerCase, null)
        }
      })

      // 3. Order Parent Hierarchy properly
      messageContainerObjects.foreach(m => {
        m._2.parents.reverse
      })
    }
  }

  def InitBootstrap: Unit = {
    MetadataAPIImpl.InitMdMgrFromBootStrap(FatafatConfiguration.configFile)
  }

  def InitMdMgr(tmpLoadedJars: TreeSet[String], tmpLoader: FatafatClassLoader, tmpMirror: reflect.runtime.universe.Mirror, zkConnectString: String, znodePath: String, zkSessionTimeoutMs: Int, zkConnectionTimeoutMs: Int): Unit = {
    /*
    if (FatafatConfiguration.metadataStoreType.compareToIgnoreCase("cassandra") == 0|| FatafatConfiguration.metadataStoreType.compareToIgnoreCase("hbase") == 0)
      MetadataAPIImpl.InitMdMgr(mdMgr, FatafatConfiguration.metadataStoreType, FatafatConfiguration.metadataLocation, FatafatConfiguration.metadataSchemaName, "")
    else if ((FatafatConfiguration.metadataStoreType.compareToIgnoreCase("treemap") == 0) || (FatafatConfiguration.metadataStoreType.compareToIgnoreCase("hashmap") == 0))
      MetadataAPIImpl.InitMdMgr(mdMgr, FatafatConfiguration.metadataStoreType, "", FatafatConfiguration.metadataSchemaName, FatafatConfiguration.metadataLocation)
*/
    // MetadataAPIImpl.InitMdMgrFromBootStrap(FatafatConfiguration.configFile)

    loadedJars = tmpLoadedJars
    loader = tmpLoader
    mirror = tmpMirror

    val tmpMsgDefs = mdMgr.Messages(true, true)
    val tmpContainerDefs = mdMgr.Containers(true, true)
    val tmpModelDefs = mdMgr.Models(true, true)

    val obj = new FatafatMetadata

    try {
      obj.LoadMdMgrElems(loadedJars, loader, mirror, tmpMsgDefs, tmpContainerDefs, tmpModelDefs)
      // Lock the global object here and update the global objects
      UpdateFatafatMdObjects(obj.messageObjects, obj.containerObjects, obj.modelObjects, null, null, null)
    } catch {
      case e: Exception => {
        LOG.error("Failed to load messages, containers & models from metadata manager. Reason:%s Message:%s".format(e.getCause, e.getMessage))
        throw e
      }
    }

    if (zkConnectString != null && zkConnectString.isEmpty() == false && znodePath != null && znodePath.isEmpty() == false) {
      try {
        CreateClient.CreateNodeIfNotExists(zkConnectString, znodePath)
        zkListener = new ZooKeeperListener
        zkListener.CreateListener(zkConnectString, znodePath, UpdateMetadata, zkSessionTimeoutMs, zkConnectionTimeoutMs)
      } catch {
        case e: Exception => {
          LOG.error("Failed to initialize ZooKeeper Connection. Reason:%s Message:%s".format(e.getCause, e.getMessage))
          throw e
        }
      }
    }
  }

  // Assuming mdMgr is locked at this moment for not to update while doing this operation
  def UpdateMetadata(receivedJsonStr: String): Unit = {

    LOG.debug("Process ZooKeeper notification " + receivedJsonStr)

    if (receivedJsonStr == null || receivedJsonStr.size == 0) {
      // nothing to do
      return
    }

    val zkTransaction = JsonSerializer.parseZkTransaction(receivedJsonStr, "JSON")
    MetadataAPIImpl.UpdateMdMgr(zkTransaction)

    if (zkTransaction == null || zkTransaction.Notifications.size == 0) {
      // nothing to do
      return
    }

    if (mdMgr == null) {
      LOG.error("Metadata Manager should not be NULL while updaing metadta in Fatafat manager.")
      return
    }

    val obj = new FatafatMetadata

    // BUGBUG:: Not expecting added element & Removed element will happen in same transaction at this moment
    // First we are adding what ever we need to add, then we are removing. So, we are locking before we append to global array and remove what ever is gone.
    val removedModels = new ArrayBuffer[(String, String, Long)]
    val removedMessages = new ArrayBuffer[(String, String, Long)]
    val removedContainers = new ArrayBuffer[(String, String, Long)]

    //// Check for Jars -- Begin
    val allJarsToBeValidated = scala.collection.mutable.Set[String]();

    zkTransaction.Notifications.foreach(zkMessage => {
      val key = zkMessage.NameSpace + "." + zkMessage.Name + "." + zkMessage.Version
      LOG.debug("Processing ZooKeeperNotification, the object => " + key + ",objectType => " + zkMessage.ObjectType + ",Operation => " + zkMessage.Operation)
      zkMessage.ObjectType match {
        case "ModelDef" => {
          zkMessage.Operation match {
            case "Add" => {
              try {
                val mdl = mdMgr.Model(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, true)
                if (mdl != None) {
                  allJarsToBeValidated ++= obj.GetAllJarsFromElem(mdl.get)
                }
              } catch {
                case e: Exception => {}
              }
            }
            case _ => {}
          }
        }
        case "MessageDef" => {
          zkMessage.Operation match {
            case "Add" => {
              try {
                val msg = mdMgr.Message(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, true)
                if (msg != None) {
                  allJarsToBeValidated ++= obj.GetAllJarsFromElem(msg.get)
                }
              } catch {
                case e: Exception => {}
              }
            }
            case _ => {}
          }
        }
        case "ContainerDef" => {
          zkMessage.Operation match {
            case "Add" => {
              try {
                val container = mdMgr.Container(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, true)
                if (container != None) {
                  allJarsToBeValidated ++= obj.GetAllJarsFromElem(container.get)
                }
              } catch {
                case e: Exception => {}
              }
            }
            case _ => {}
          }
        }
        case _ => {}
      }
    })

    val nonExistsJars = FatafatMdCfg.CheckForNonExistanceJars(allJarsToBeValidated.toSet)
    if (nonExistsJars.size > 0) {
      LOG.error("Not found jars in Messages/Containers/Models Jars List : {" + nonExistsJars.mkString(", ") + "}")
      // return
    }

    //// Check for Jars -- End

    zkTransaction.Notifications.foreach(zkMessage => {
      val key = zkMessage.NameSpace + "." + zkMessage.Name + "." + zkMessage.Version.toLong
      LOG.debug("Processing ZooKeeperNotification, the object => " + key + ",objectType => " + zkMessage.ObjectType + ",Operation => " + zkMessage.Operation)
      zkMessage.ObjectType match {
        case "ModelDef" => {
          zkMessage.Operation match {
            case "Add" | "Activate" => {
              try {
                val mdl = mdMgr.Model(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, true)
                if (mdl != None) {
                  obj.PrepareModel(loadedJars, loader, mirror, mdl.get, true)
                } else {
                  LOG.error("Failed to find Model:" + key)
                }
              } catch {
                case e: Exception => {
                  LOG.error("Failed to Add Model:" + key)
                }
              }
            }
            case "Remove" | "Deactivate" => {
              try {
                removedModels += ((zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong))
              } catch {
                case e: Exception => {
                  LOG.error("Failed to Remove Model:" + key)
                }
              }
            }
            case _ => {
              LOG.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")
            }
          }
        }
        case "MessageDef" => {
          zkMessage.Operation match {
            case "Add" => {
              try {
                val msg = mdMgr.Message(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, true)
                if (msg != None) {
                  obj.PrepareMessage(loadedJars, loader, mirror, msg.get, true)
                } else {
                  LOG.error("Failed to find Message:" + key)
                }
              } catch {
                case e: Exception => {
                  LOG.error("Failed to Add Message:" + key)
                }
              }
            }
            case "Remove" => {
              try {
                removedMessages += ((zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong))
              } catch {
                case e: Exception => {
                  LOG.error("Failed to Remove Message:" + key)
                }
              }
            }
            case _ => {
              LOG.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")
            }
          }
        }
        case "ContainerDef" => {
          zkMessage.Operation match {
            case "Add" => {
              try {
                val container = mdMgr.Container(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, true)
                if (container != None) {
                  obj.PrepareContainer(loadedJars, loader, mirror, container.get, true, false)
                } else {
                  LOG.error("Failed to find Container:" + key)
                }
              } catch {
                case e: Exception => {
                  LOG.error("Failed to Add Container:" + key)
                }
              }
            }
            case "Remove" => {
              try {
                removedContainers += ((zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong))
              } catch {
                case e: Exception => {
                  LOG.error("Failed to Remove Container:" + key)
                }
              }
            }
            case _ => {
              LOG.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")
            }
          }
        }
        case _ => {
          LOG.warn("Unknown objectType " + zkMessage.ObjectType + " in zookeeper notification, notification is not processed ..")
        }
      }
    })

    // Lock the global object here and update the global objects
    UpdateFatafatMdObjects(obj.messageObjects, obj.containerObjects, obj.modelObjects, removedModels, removedMessages, removedContainers)
  }

  override def getMessgeOrContainerInstance(MsgContainerType: String): MessageContainerBase = {
    var v: MsgContainerObjAndTransformInfo = null

    v = getMessageOrContainer(MsgContainerType)
    if (v != null && v.contmsgobj != null && v.contmsgobj.isInstanceOf[BaseMsgObj]) {
      return v.contmsgobj.asInstanceOf[BaseMsgObj].CreateNewMessage
    } else if (v != null && v.contmsgobj != null && v.contmsgobj.isInstanceOf[BaseContainerObj]) { // NOTENOTE: Not considering Base containers here
      return v.contmsgobj.asInstanceOf[BaseContainerObj].CreateNewContainer
    }
    return null
  }

  def getMessgeInfo(msgType: String): MsgContainerObjAndTransformInfo = {
    var exp: Exception = null
    var v: MsgContainerObjAndTransformInfo = null

    reent_lock.readLock().lock();
    try {
      v = localgetMessgeInfo(msgType)
    } catch {
      case e: Exception => { exp = e }
    } finally {
      reent_lock.readLock().unlock();
    }
    if (exp != null)
      throw exp
    v
  }

  def getModel(mdlName: String): MdlInfo = {
    var exp: Exception = null
    var v: MdlInfo = null

    reent_lock.readLock().lock();
    try {
      v = localgetModel(mdlName)
    } catch {
      case e: Exception => { exp = e }
    } finally {
      reent_lock.readLock().unlock();
    }
    if (exp != null)
      throw exp
    v
  }

  def getContainer(containerName: String): MsgContainerObjAndTransformInfo = {
    var exp: Exception = null
    var v: MsgContainerObjAndTransformInfo = null

    reent_lock.readLock().lock();
    try {
      v = localgetContainer(containerName)
    } catch {
      case e: Exception => { exp = e }
    } finally {
      reent_lock.readLock().unlock();
    }
    if (exp != null)
      throw exp
    v
  }

  def getMessageOrContainer(msgOrContainerName: String): MsgContainerObjAndTransformInfo = {
    var exp: Exception = null
    var v: MsgContainerObjAndTransformInfo = null

    reent_lock.readLock().lock();
    try {
      v = localgetMessgeOrContainer(msgOrContainerName)
    } catch {
      case e: Exception => { exp = e }
    } finally {
      reent_lock.readLock().unlock();
    }
    if (exp != null)
      throw exp
    v
  }

  def getAllMessges: Map[String, MsgContainerObjAndTransformInfo] = {
    var exp: Exception = null
    var v: Map[String, MsgContainerObjAndTransformInfo] = null

    reent_lock.readLock().lock();
    try {
      v = localgetAllMessges
    } catch {
      case e: Exception => { exp = e }
    } finally {
      reent_lock.readLock().unlock();
    }
    if (exp != null)
      throw exp
    v
  }

  def getAllModels: Map[String, MdlInfo] = {
    var exp: Exception = null
    var v: Map[String, MdlInfo] = null

    reent_lock.readLock().lock();
    try {
      v = localgetAllModels
    } catch {
      case e: Exception => { exp = e }
    } finally {
      reent_lock.readLock().unlock();
    }
    if (exp != null)
      throw exp
    v
  }

  def getAllContainers: Map[String, MsgContainerObjAndTransformInfo] = {
    var exp: Exception = null
    var v: Map[String, MsgContainerObjAndTransformInfo] = null

    reent_lock.readLock().lock();
    try {
      v = localgetAllContainers
    } catch {
      case e: Exception => { exp = e }
    } finally {
      reent_lock.readLock().unlock();
    }
    if (exp != null)
      throw exp
    v
  }

  private def localgetMessgeInfo(msgType: String): MsgContainerObjAndTransformInfo = {
    if (messageContainerObjects == null) return null
    val v = messageContainerObjects.getOrElse(msgType.toLowerCase, null)
    if (v != null && v.contmsgobj != null && v.contmsgobj.isInstanceOf[BaseMsgObj])
      return v
    return null
  }

  private def localgetModel(mdlName: String): MdlInfo = {
    if (modelObjects == null) return null
    modelObjects.getOrElse(mdlName.toLowerCase, null)
  }

  private def localgetContainer(containerName: String): MsgContainerObjAndTransformInfo = {
    if (messageContainerObjects == null) return null
    val v = messageContainerObjects.getOrElse(containerName.toLowerCase, null)
    if ((v != null && v.contmsgobj == null) // Base containers 
      || (v != null && v.contmsgobj != null && v.contmsgobj.isInstanceOf[BaseContainerObj]))
      return v
    return null
  }

  private def localgetMessgeOrContainer(msgOrContainerName: String): MsgContainerObjAndTransformInfo = {
    if (messageContainerObjects == null) return null
    val v = messageContainerObjects.getOrElse(msgOrContainerName.toLowerCase, null)
    v
  }

  private def localgetAllMessges: Map[String, MsgContainerObjAndTransformInfo] = {
    if (messageContainerObjects == null) return null
    messageContainerObjects.filter(o => {
      val v = o._2
      (v != null && v.contmsgobj != null && v.contmsgobj.isInstanceOf[BaseMsgObj])
    }).toMap
  }

  private def localgetAllModels: Map[String, MdlInfo] = {
    if (modelObjects == null) return null
    modelObjects.toMap
  }

  private def localgetAllContainers: Map[String, MsgContainerObjAndTransformInfo] = {
    if (messageContainerObjects == null) return null
    messageContainerObjects.filter(o => {
      val v = o._2
      ((v != null && v.contmsgobj == null) // Base containers 
        || (v != null && v.contmsgobj != null && v.contmsgobj.isInstanceOf[BaseContainerObj]))
    }).toMap
  }

  def getMdMgr: MdMgr = mdMgr

  def Shutdown: Unit = {
    if (zkListener != null)
      zkListener.Shutdown
    zkListener = null
  }
}

