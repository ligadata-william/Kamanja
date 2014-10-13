
package com.ligadata.OnLEPManager

import com.ligadata.olep.metadata.{ BaseElem, MappedMsgTypeDef, BaseAttributeDef, StructTypeDef, EntityType, AttributeDef, ArrayBufTypeDef }
//import com.ligadata.olep.metadataload.MetadataLoad
import com.ligadata.edifecs.MetadataLoad
import scala.collection.mutable.TreeSet
import scala.util.control.Breaks._
import com.ligadata.OnLEPBase.{ MdlInfo, BaseMsgObj, BaseContainer, ModelBaseObj, TransformMessage }
import scala.collection.mutable.HashMap
import org.apache.log4j.Logger
import scala.collection.mutable.ArrayBuffer

//import com.ligadata.MetadataAPI._

class TransformMsgFldsMap(var keyflds: Array[Int], var outputFlds: Array[Int]) {
}

class MsgObjAndTransformInfo(var tranformMsgFlds: TransformMsgFldsMap, var msgobj: BaseMsgObj) {
  var parents = new ArrayBuffer[(String, String)] // Immediate parent comes at the end, grand parent last but one, ... Messages/Containers. the format is Message/Container Type name and the variable in that.
  var childs = new ArrayBuffer[(String, String)] // Child Messages/Containers (Name & type). We fill this when we create message and populate parent later from this
}

// This is shared by multiple threads to read (because we are not locking). We create this only once at this moment while starting the manager
object OnLEPMetadata {
  val LOG = Logger.getLogger(getClass);

  // Metadata manager
  val mdMgr = com.ligadata.olep.metadata.MdMgr.GetMdMgr

  val messageObjects = new HashMap[String, MsgObjAndTransformInfo]
  val containerObjects = new HashMap[String, BaseContainer]
  val modelObjects = new HashMap[String, MdlInfo]

  def InitMdMgr(loadedJars: TreeSet[String], loader: OnLEPClassLoader, mirror: reflect.runtime.universe.Mirror): Unit = {
    new MetadataLoad(mdMgr, "", "", "", "").initialize // Test code until we get MdMgr from Ramana

    PrepareMessages(loadedJars, loader, mirror)
    PrepareContainers(loadedJars, loader, mirror)
    PrepareModels(loadedJars, loader, mirror)

    LOG.info("Loaded Metadata Messages:" + messageObjects.map(container => container._1).mkString(","))
    LOG.info("Loaded Metadata Containers:" + containerObjects.map(container => container._1).mkString(","))
    LOG.info("Loaded Metadata Models:" + modelObjects.map(container => container._1).mkString(","))
  }

  def LoadJarIfNeeded(elem: BaseElem, loadedJars: TreeSet[String], loader: OnLEPClassLoader): Boolean = {
    var retVal: Boolean = true
    if (elem.DependencyJarNames != null && elem.DependencyJarNames.size > 0) {
      retVal = ManagerUtils.LoadJars(elem.DependencyJarNames.map(j => OnLEPConfiguration.jarPath + "/" + j), loadedJars, loader)
    }
    if (retVal == false) return retVal
    val jarname = if (elem.JarName == null) "" else elem.JarName.trim
    if (jarname.size > 0) {
      val jars = Array(OnLEPConfiguration.jarPath + "/" + jarname)
      retVal = ManagerUtils.LoadJars(jars, loadedJars, loader)
    }
    retVal
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

  def PrepareMessages(loadedJars: TreeSet[String], loader: OnLEPClassLoader, mirror: reflect.runtime.universe.Mirror): Unit = {
    val tmpMsgDefs = mdMgr.Messages(true, true)

    if (tmpMsgDefs == None) // Not found any messages
      return

    val msgDefs = tmpMsgDefs.get

    // Load all jars first
    msgDefs.foreach(msg => {
      // LOG.info("Loading msg:" + msg.FullName)
      LoadJarIfNeeded(msg, loadedJars, loader)
    })

    msgDefs.foreach(msg => {
      var clsName = msg.PhysicalName.trim
      if (clsName.size > 0 && clsName.charAt(clsName.size - 1) != '$') // if no $ at the end we are taking $
        clsName = clsName + "$"

      var isMsg = true

      try {
        // If required we need to enable this test
        // Convert class name into a class
        var curClz = Class.forName(clsName, true, loader)

        isMsg = false

        while (curClz != null && isMsg == false) {
          isMsg = ManagerUtils.isDerivedFrom(curClz, "com.ligadata.OnLEPBase.BaseMsgObj")
          if (isMsg == false)
            curClz = curClz.getSuperclass()
        }
      } catch {
        case e: Exception => {
          LOG.error("Failed to get classname :" + clsName)
          e.printStackTrace
        }
      }

      if (isMsg) {
        try {
          val module = mirror.staticModule(clsName)
          val obj = mirror.reflectModule(module)
          val objinst = obj.instance
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
            val mgsObj = new MsgObjAndTransformInfo(tranformMsgFlds, messageobj)
            GetChildsFromEntity(msg.containerType, mgsObj.childs)
            messageObjects(msgName) = mgsObj

            LOG.info("Created Message:" + msgName)
          } else {
            LOG.error("Failed to instantiate message object :" + clsName)
          }
        } catch {
          case e: Exception => {
            LOG.error("Failed to instantiate message object:" + clsName + ". Message:" + e.getMessage())
          }
        }
      } else {
        LOG.error("Failed to instantiate message object :" + clsName)
      }
    })

    // Prepare Parents for each message now
    val childToParentMap = scala.collection.mutable.Map[String, (String, String)]() // ChildType, (ParentType, ChildAttrName) 

    // 1. First prepare one level of parents
    messageObjects.foreach(m => {
      m._2.childs.foreach(c => {
        childToParentMap(c._2.toLowerCase) = (m._1.toLowerCase, c._1) // BUGBUG:: Check whether we already have in childToParentMap or not before we replace. So that way we can check same child under multiple parents.
      })
    })

    // 2. Now prepare Full Parent Hierarchy
    messageObjects.foreach(m => {
      var curParent = childToParentMap.getOrElse(m._1.toLowerCase, null)
      while (curParent != null) {
        m._2.parents += curParent
        curParent = childToParentMap.getOrElse(curParent._1.toLowerCase, null)
      }
    })

    // 3. Order Parent Hierarchy properly
    messageObjects.foreach(m => {
      m._2.parents.reverse
    })
  }

  def PrepareContainers(loadedJars: TreeSet[String], loader: OnLEPClassLoader, mirror: reflect.runtime.universe.Mirror): Unit = {

    val tmpContainerDefs = mdMgr.Containers(true, true)

    if (tmpContainerDefs == None) // Not found any containers
      return

    val containerDefs = tmpContainerDefs.get

    // Load all jars first
    containerDefs.foreach(container => {
      LoadJarIfNeeded(container, loadedJars, loader)
    })

    containerDefs.foreach(container => {
      val clsName = container.PhysicalName

      var isContainer = true

      /*
		// If required we need to enable this test
		// Convert class name into a class
		var curClz = Class.forName(clsName, true, loader)
		
		isContainer = false
		
		while (curClz != null && isContainer == false) {
		isContainer = isDerivedFrom(curClz, "com.ligadata.OnLEPBase.BaseContainerObj")
		if (isContainer == false)
		curClz = curClz.getSuperclass()
		}
		*/
      /*
      if (isContainer) {
        try {
          val module = mirror.staticModule(clsName)
          val obj = mirror.reflectModule(module)

          val objinst = obj.instance
          if (objinst.isInstanceOf[BaseContainer]) {
            val containerobj = objinst.asInstanceOf[BaseContainer]
            val containerName = (container.NameSpace.trim + "." + container.Name.trim).toLowerCase
            containerObjects(containerName) = containerobj
          } else
            LOG.error("Failed to instantiate container object :" + clsName)
        } catch {
          case e: Exception => LOG.error("Failed to instantiate container object:" + clsName + ". Message:" + e.getMessage())
        }
      }
*/
      val containerName = (container.NameSpace.trim + "." + container.Name.trim).toLowerCase
      containerObjects(containerName) = null
    })

  }

  def PrepareModels(loadedJars: TreeSet[String], loader: OnLEPClassLoader, mirror: reflect.runtime.universe.Mirror): Unit = {
    val tmpModelDefs = mdMgr.Models(true, true)

    if (tmpModelDefs == None) // Not found any models
      return

    val modelDefs = tmpModelDefs.get

    // Load all jars first
    modelDefs.foreach(mdl => {
      LoadJarIfNeeded(mdl, loadedJars, loader)
    })

    val mirror1: reflect.runtime.universe.Mirror = scala.reflect.runtime.universe.runtimeMirror(loader)

    modelDefs.foreach(mdl => {
      var clsName = mdl.PhysicalName.trim
      if (clsName.size > 0 && clsName.charAt(clsName.size - 1) != '$') // if no $ at the end we are taking $
        clsName = clsName + "$"

      var isModel = true

      try {
        // If required we need to enable this test
        // Convert class name into a class
        var curClz = Class.forName(clsName, true, loader)

        isModel = false

        while (curClz != null && isModel == false) {
          isModel = ManagerUtils.isDerivedFrom(curClz, "com.ligadata.OnLEPBase.ModelBaseObj")
          if (isModel == false)
            curClz = curClz.getSuperclass()
        }
      } catch {
        case e: Exception => {
          LOG.error("Failed to get classname :" + clsName)
          e.printStackTrace
        }
      }

      // LOG.info("Loading Model:" + mdl.FullName + ". ClassName: " + clsName + ". IsModel:" + isModel)

      if (isModel) {
        try {
          val module = mirror1.staticModule(clsName)
          val obj = mirror1.reflectModule(module)

          val objinst = obj.instance
          // val objinst = obj.instance
          if (objinst.isInstanceOf[ModelBaseObj]) {
            val modelobj = objinst.asInstanceOf[ModelBaseObj]
            val mdlName = (mdl.NameSpace.trim + "." + mdl.Name.trim).toLowerCase
            modelObjects(mdlName) = new MdlInfo(modelobj, mdl.jarName, mdl.dependencyJarNames, "Ligadata")
            LOG.info("Created Model:" + mdlName)
          } else {
            LOG.error("Failed to instantiate model object :" + clsName)
            LOG.info("Failed to instantiate model object :" + clsName + ". ObjType0:" + objinst.getClass.getSimpleName + ". ObjType1:" + objinst.getClass.getCanonicalName)
          }
        } catch {
          case e: Exception => LOG.error("Failed to instantiate model object:" + clsName + ". Message:" + e.getMessage)
        }
      } else {
        LOG.error("Failed to instantiate model object :" + clsName)
      }
    })
  }

  def getMessgeInfo(msgType: String): MsgObjAndTransformInfo = {
    messageObjects.getOrElse(msgType.toLowerCase, null)
  }

  def getModel(mdlName: String): MdlInfo = {
    modelObjects.getOrElse(mdlName.toLowerCase, null)
  }

  def getContainer(containerName: String): BaseContainer = {
    containerObjects.getOrElse(containerName.toLowerCase, null)
  }
}

