package com.ligadata.pmml.udfs

import scala.reflect.ClassTag
import scala.collection.GenSeq
import scala.collection.mutable._
import scala.math._
import scala.collection.immutable.StringLike
import scala.collection.immutable.List
import scala.collection.immutable.Map
import scala.collection.immutable.Set
import scala.collection.immutable.Iterable
import scala.collection.mutable.{ Map => MutableMap }
import scala.collection.mutable.{ Set => MutableSet }
import scala.collection.mutable.{ Iterable => MutableIterable }
import scala.collection.mutable.ArraySeq
import scala.collection.mutable.TreeSet
import scala.collection.GenSeq
import scala.reflect.ClassTag

import org.joda.time.base
import org.joda.time.chrono
import org.joda.time.convert
import org.joda.time.field
import org.joda.time.format
import org.joda.time.tz
import org.joda.time.LocalDate
import org.joda.time.DateTime
import org.joda.time.Years
import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.chrono.JulianChronology

import org.apache.log4j.Logger

import com.ligadata.Pmml.Runtime._
import com.ligadata.OnLEPBase._

/**
 * These are the udfs supplied with the system.
 */
object Udfs extends com.ligadata.pmml.udfs.UdfBase with LogTrait {

  /** Exists checking implemented in the EnvContext */

  def Contains(xId: Long, gCtx: EnvContext, containerName: String, key: String): Boolean = {
    val itExists: Boolean = if (gCtx != null) gCtx.contains(xId, containerName, key) else false
    itExists
  }
  def ContainsAny(xId: Long, gCtx: EnvContext, containerName: String, keys: Array[String]): Boolean = {
    val itExists: Boolean = if (gCtx != null) gCtx.containsAny(xId, containerName, keys) else false
    itExists
  }
  def ContainsAll(xId: Long, gCtx: EnvContext, containerName: String, keys: Array[String]): Boolean = {
    val allExist: Boolean = if (gCtx != null) gCtx.containsAll(xId, containerName, keys) else false
    allExist
  }

  /**
   * runtime state write functions NOTE: macros use these functions ... the ctx is not directly
   *  supported in the Pmml
   */

  def Put(ctx: Context, variableName: String, value: String): Boolean = {
    var set: Boolean = (ctx != null)
    if (set) {
      set = ctx.valuePut(variableName, new StringDataValue(value))
    }
    set
  }

  def Put(ctx: Context, variableName: String, value: Int): Boolean = {
    var set: Boolean = (ctx != null)
    if (set) {
      set = ctx.valuePut(variableName, new IntDataValue(value))
    }
    set
  }

  def Put(ctx: Context, variableName: String, value: Long): Boolean = {
    var set: Boolean = (ctx != null)
    if (set) {
      set = ctx.valuePut(variableName, new LongDataValue(value))
    }
    set
  }

  def Put(ctx: Context, variableName: String, value: Double): Boolean = {
    var set: Boolean = (ctx != null)
    if (set) {
      set = ctx.valuePut(variableName, new DoubleDataValue(value))
    }
    set
  }

  def Put(ctx: Context, variableName: String, value: Any): Boolean = {
    var set: Boolean = (ctx != null)
    if (set) {
      set = ctx.valuePut(variableName, new AnyDataValue(value))
    }
    set
  }

  def Put(ctx: Context, variableName: String, value: Boolean): Boolean = {
    var set: Boolean = (ctx != null)
    if (set) {
      set = ctx.valuePut(variableName, new BooleanDataValue(value))
    }
    set
  }

  def Put(ctx: Context, variableName: String, value: Float): Boolean = {
    var set: Boolean = (ctx != null)
    if (set) {
      set = ctx.valuePut(variableName, new FloatDataValue(value))
    }
    set
  }

  /** runtime state increment function */

  def incrementBy(ctx: Context, variableName: String, value: Int): Boolean = {
    var set: Boolean = (ctx != null)
    if (set) {
      set = ctx.valueIncr(variableName, value)
    }
    set
  }

  /**
   * EnvContext Get functions
   * FIXME:  Perhaps we should support the various flavor of keys?
   */

  def Get(xId: Long, gCtx: EnvContext, containerId: String, key: String): MessageContainerBase = {
    gCtx.getObject(xId, containerId, key.toString)
  }

  def Get(xId: Long, gCtx: EnvContext, containerId: String, key: Int): MessageContainerBase = {
    gCtx.getObject(xId, containerId, key.toString)
  }

  def Get(xId: Long, gCtx: EnvContext, containerId: String, key: Long): MessageContainerBase = {
    gCtx.getObject(xId, containerId, key.toString)
  }

  def Get(xId: Long, gCtx: EnvContext, containerId: String, key: Double): MessageContainerBase = {
    gCtx.getObject(xId, containerId, key.toString)
  }

  def Get(xId: Long, gCtx: EnvContext, containerId: String, key: Float): MessageContainerBase = {
    gCtx.getObject(xId, containerId, key.toString)
  }

  /**
   * EnvContext GetArray functions
   */

  def GetArray(xId: Long, gCtx: EnvContext, containerId: String): Array[MessageContainerBase] = {
    gCtx.getAllObjects(xId, containerId)
  }

  /**
   * EnvContext Put functions
   * FIXME:  Perhaps we should support the various flavor of keys?
   */

  def Put(xId: Long, gCtx: EnvContext, containerId: String, key: String, value: MessageContainerBase): Boolean = {
    gCtx.setObject(xId, containerId, key.toString, value)
    true
  }

  def Put(xId: Long, gCtx: EnvContext, containerId: String, key: Int, value: MessageContainerBase): Boolean = {
    gCtx.setObject(xId, containerId, key.toString, value)
    true
  }

  def Put(xId: Long, gCtx: EnvContext, containerId: String, key: Long, value: MessageContainerBase): Boolean = {
    gCtx.setObject(xId, containerId, key.toString, value)
    true
  }

  def Put(xId: Long, gCtx: EnvContext, containerId: String, key: Double, value: MessageContainerBase): Boolean = {
    gCtx.setObject(xId, containerId, key.toString, value)
    true
  }

  def Put(xId: Long, gCtx: EnvContext, containerId: String, key: Float, value: MessageContainerBase): Boolean = {
    gCtx.setObject(xId, containerId, key.toString, value)
    true
  }

  /** if expressions */

  def If[T](boolexpr: Boolean, expr1: T, expr2: T): T = {
    if (boolexpr) expr1 else expr2
  }


  /** logical and */
  def And(boolexpr : Boolean*): Boolean = {
	boolexpr.reduceLeft(_ && _) 
  }
  
  def And(boolexpr : Int*): Int = {
	if (boolexpr.filter(_ == 0).size == 0) 1 else 0
  }
  
  /** logical or */
  def Or(boolexpr : Boolean*): Boolean = {
	boolexpr.reduceLeft(_ || _) 
  }
  
  def Or(boolexpr : Int*): Int = {
	if (boolexpr.filter(_ != 0).size > 0) 1 else 0 
  }
  

  def IsIn(fldRefExpr: String, setExprs: ArrayBuffer[String]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length > 0
  }

  def IsIn(fldRefExpr: Int, setExprs: ArrayBuffer[Int]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length > 0
  }

  def IsIn(fldRefExpr: Float, setExprs: ArrayBuffer[Float]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length > 0
  }

  def IsIn(fldRefExpr: Double, setExprs: ArrayBuffer[Double]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length > 0
  }

  def IsIn(fldRefExpr: String, setExprs: Array[String]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length > 0
  }

  def IsIn(fldRefExpr: Int, setExprs: Array[Int]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length > 0
  }

  def IsIn(fldRefExpr: Float, setExprs: Array[Float]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length > 0
  }

  def IsIn(fldRefExpr: Double, setExprs: Array[Double]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length > 0
  }

  def IsIn(fldRefExpr: String, setExprs: List[String]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length > 0
  }

  def IsIn(fldRefExpr: Int, setExprs: List[Int]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length > 0
  }

  def IsIn(fldRefExpr: Float, setExprs: List[Float]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length > 0
  }

  def IsIn(fldRefExpr: Double, setExprs: List[Double]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length > 0
  }

  def IsIn(fldRefExpr: String, setExprs: Set[String]): Boolean = {
    setExprs.contains(fldRefExpr)
  }

  def IsIn(fldRefExpr: Int, setExprs: Set[Int]): Boolean = {
    setExprs.contains(fldRefExpr)
  }

  def IsIn(fldRefExpr: Float, setExprs: Set[Float]): Boolean = {
    setExprs.contains(fldRefExpr)
  }

  def IsIn(fldRefExpr: Double, setExprs: Set[Double]): Boolean = {
    setExprs.contains(fldRefExpr)
  }

  def IsIn(fldRefExpr: String, setExprs: MutableSet[String]): Boolean = {
    setExprs.contains(fldRefExpr)
  }

  def IsIn(fldRefExpr: Int, setExprs: MutableSet[Int]): Boolean = {
    setExprs.contains(fldRefExpr)
  }

  def IsIn(fldRefExpr: Float, setExprs: MutableSet[Float]): Boolean = {
    setExprs.contains(fldRefExpr)
  }

  def IsIn(fldRefExpr: Double, setExprs: MutableSet[Double]): Boolean = {
    setExprs.contains(fldRefExpr)
  }

 /** FoundInAnyRange */
  def FoundInAnyRange(fldRefExpr: String, tuples: Array[(String,String)], inclusive : Boolean): Boolean = {
    tuples.filter(tup =>  { 
    	if (inclusive) 
    	  (fldRefExpr >= tup._1 && fldRefExpr <= tup._2)
    	else 
    	  (fldRefExpr > tup._1 && fldRefExpr < tup._2)   	  
    }).length > 0
  }

  def FoundInAnyRange(fldRefExpr: Int, tuples: Array[(Int,Int)], inclusive : Boolean): Boolean = {
    tuples.filter(tup =>  { 
    	if (inclusive) 
    	  (fldRefExpr >= tup._1 && fldRefExpr <= tup._2)
    	else 
    	  (fldRefExpr > tup._1 && fldRefExpr < tup._2)   	  
    }).length > 0
  }

  def FoundInAnyRange(fldRefExpr: Long, tuples: Array[(Long,Long)], inclusive : Boolean): Boolean = {
    tuples.filter(tup =>  { 
    	if (inclusive) 
    	  (fldRefExpr >= tup._1 && fldRefExpr <= tup._2)
    	else 
    	  (fldRefExpr > tup._1 && fldRefExpr < tup._2)   	  
    }).length > 0
  }

  def FoundInAnyRange(fldRefExpr: Float, tuples: Array[(Float,Float)], inclusive : Boolean): Boolean = {
     tuples.filter(tup =>  { 
    	if (inclusive) 
    	  (fldRefExpr >= tup._1 && fldRefExpr <= tup._2)
    	else 
    	  (fldRefExpr > tup._1 && fldRefExpr < tup._2)   	  
    }).length > 0
  }

  def FoundInAnyRange(fldRefExpr: Double, tuples: Array[(Double,Double)], inclusive : Boolean): Boolean = {
    tuples.filter(tup =>  { 
    	if (inclusive) 
    	  (fldRefExpr >= tup._1 && fldRefExpr <= tup._2)
    	else 
    	  (fldRefExpr > tup._1 && fldRefExpr < tup._2)   	  
    }).length > 0
  }

  /** AnyBetween */
  def AnyBetween(arrayExpr: ArrayBuffer[String], leftMargin: String, rightMargin: String, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length > 0
  }

  def AnyBetween(arrayExpr: ArrayBuffer[Int], leftMargin: Int, rightMargin: Int, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length > 0
  }

  def AnyBetween(arrayExpr: ArrayBuffer[Long], leftMargin: Long, rightMargin: Long, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length > 0
  }

  def AnyBetween(arrayExpr: ArrayBuffer[Float], leftMargin: Float, rightMargin: Float, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length > 0
  }

  def AnyBetween(arrayExpr: ArrayBuffer[Double], leftMargin: Double, rightMargin: Double, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length > 0
  }

  def AnyBetween(arrayExpr: Array[String], leftMargin: String, rightMargin: String, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length > 0
  }

  def AnyBetween(arrayExpr: Array[Int], leftMargin: Int, rightMargin: Int, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length > 0
  }

  def AnyBetween(arrayExpr: Array[Float], leftMargin: Float, rightMargin: Float, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length > 0
  }

  def AnyBetween(arrayExpr: Array[Double], leftMargin: Double, rightMargin: Double, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length > 0
  }

  /** NotAnyBetween */
  def NotAnyBetween(arrayExpr: ArrayBuffer[String], leftMargin: String, rightMargin: String, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length == 0
  }

  def NotAnyBetween(arrayExpr: ArrayBuffer[Int], leftMargin: Int, rightMargin: Int, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length == 0
  }

  def NotAnyBetween(arrayExpr: ArrayBuffer[Long], leftMargin: Long, rightMargin: Long, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length == 0
  }

  def NotAnyBetween(arrayExpr: ArrayBuffer[Float], leftMargin: Float, rightMargin: Float, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length == 0
  }

  def NotAnyBetween(arrayExpr: ArrayBuffer[Double], leftMargin: Double, rightMargin: Double, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length == 0
  }

  def NotAnyBetween(arrayExpr: Array[String], leftMargin: String, rightMargin: String, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length == 0
  }

  def NotAnyBetween(arrayExpr: Array[Long], leftMargin: Long, rightMargin: Long, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length == 0
  }

  def NotAnyBetween(arrayExpr: Array[Int], leftMargin: Int, rightMargin: Int, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length == 0
  }

  def NotAnyBetween(arrayExpr: Array[Float], leftMargin: Float, rightMargin: Float, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length == 0
  }

  def NotAnyBetween(arrayExpr: Array[Double], leftMargin: Double, rightMargin: Double, inclusive: Boolean): Boolean = {
    arrayExpr.filter(Between(_, leftMargin, rightMargin, inclusive)).length == 0
  }

  /** Contains */
  def Contains(arrayExpr: ArrayBuffer[String], key: String): Boolean = {
    arrayExpr.contains(key)
  }

  def Contains(arrayExpr: ArrayBuffer[Long], key: Long): Boolean = {
    arrayExpr.contains(key)
  }

  def Contains(arrayExpr: ArrayBuffer[Int], key: Int): Boolean = {
    arrayExpr.contains(key)
  }

  def Contains(arrayExpr: ArrayBuffer[Float], key: Float): Boolean = {
    arrayExpr.contains(key)
  }

  def Contains(arrayExpr: ArrayBuffer[Double], key: Double): Boolean = {
    arrayExpr.contains(key)
  }

  def Contains(arrayExpr: Array[String], key: String): Boolean = {
    arrayExpr.contains(key)
  }

  def Contains(arrayExpr: Array[Long], key: Long): Boolean = {
    arrayExpr.contains(key)
  }

  def Contains(arrayExpr: Array[Int], key: Int): Boolean = {
    arrayExpr.contains(key)
  }

  def Contains(arrayExpr: Array[Float], key: Float): Boolean = {
    arrayExpr.contains(key)
  }

  def Contains(arrayExpr: Array[Double], key: Double): Boolean = {
    arrayExpr.contains(key)
  }

  def Contains(setExpr: Set[String], key: String): Boolean = {
    setExpr.contains(key)
  }

  def Contains(setExpr: Set[Long], key: Long): Boolean = {
    setExpr.contains(key)
  }

  def Contains(setExpr: Set[Int], key: Int): Boolean = {
    setExpr.contains(key)
  }

  def Contains(setExpr: Set[Float], key: Float): Boolean = {
    setExpr.contains(key)
  }

  def Contains(setExpr: Set[Double], key: Double): Boolean = {
    setExpr.contains(key)
  }

  def Contains(setExpr: MutableSet[String], key: String): Boolean = {
    setExpr.contains(key)
  }

  def Contains(setExpr: MutableSet[Long], key: Long): Boolean = {
    setExpr.contains(key)
  }

  def Contains(setExpr: MutableSet[Int], key: Int): Boolean = {
    setExpr.contains(key)
  }

  def Contains(setExpr: MutableSet[Float], key: Float): Boolean = {
    setExpr.contains(key)
  }

  def Contains(setExpr: MutableSet[Double], key: Double): Boolean = {
    setExpr.contains(key)
  }

  def ContainsAny(setExpr: Set[String], keys: Array[String]): Boolean = {
    (keys.filter(key => setExpr.contains(key)).length > 0)
  }

  def ContainsAny(setExpr: Set[Long], keys: Array[Long]): Boolean = {
    (keys.filter(key => setExpr.contains(key)).length > 0)
  }

  def ContainsAny(setExpr: Set[Int], keys: Array[Int]): Boolean = {
    (keys.filter(key => setExpr.contains(key)).length > 0)
  }

  def ContainsAny(setExpr: Set[Float], keys: Array[Float]): Boolean = {
    (keys.filter(key => setExpr.contains(key)).length > 0)
  }

  def Contains(setExpr: Set[Double], keys: Array[Double]): Boolean = {
    (keys.filter(key => setExpr.contains(key)).length > 0)
  }

  def ContainsAny(setExpr: MutableSet[String], keys: Array[String]): Boolean = {
    (keys.filter(key => setExpr.contains(key)).length > 0)
  }

  def ContainsAny(setExpr: MutableSet[Long], keys: Array[Long]): Boolean = {
    (keys.filter(key => setExpr.contains(key)).length > 0)
  }

  def ContainsAny(setExpr: MutableSet[Int], keys: Array[Int]): Boolean = {
    (keys.filter(key => setExpr.contains(key)).length > 0)
  }

  def ContainsAny(setExpr: MutableSet[Float], keys: Array[Float]): Boolean = {
    (keys.filter(key => setExpr.contains(key)).length > 0)
  }

  def Contains(setExpr: MutableSet[Double], keys: Array[Double]): Boolean = {
    (keys.filter(key => setExpr.contains(key)).length > 0)
  }

  /** Intersect */
  def Intersect[T: ClassTag](left: Array[T], right: Array[T]): Set[T] = {
    if (left == null || right == null || left.size == 0 || right.size == 0)
      return Array[T]().toSet
    (left.toSet & right.toSet)
  }

  def Intersect[T: ClassTag](left: Array[T], right: Set[T]): Set[T] = {
    if (left == null || right == null || left.size == 0 || right.size == 0)
      return Array[T]().toSet
    (left.toSet & right)
  }

  def Intersect[T: ClassTag](left: Set[T], right: Array[T]): Set[T] = {
    if (left == null || right == null || left.size == 0 || right.size == 0)
      return Array[T]().toSet
    (left & right.toSet)
  }

  def Intersect[T: ClassTag](left: Set[T], right: Set[T]): Set[T] = {
    if (left == null || right == null || left.size == 0 || right.size == 0)
      return Array[T]().toSet
    (left & right)
  }

  def Intersect[T: ClassTag](left: Array[T], right: TreeSet[T]): Set[T] = {
    if (left == null || right == null || left.size == 0 || right.size == 0)
      return Array[T]().toSet
    (left.toSet & right)
  }

  def Intersect[T: ClassTag](left: TreeSet[T], right: Array[T]): Set[T] = {
    if (left == null || right == null || left.size == 0 || right.size == 0)
      return Array[T]().toSet
    (left & right.toSet).toSet
  }

  def Intersect[T: ClassTag](left: TreeSet[T], right: TreeSet[T]): Set[T] = {
    if (left == null || right == null || left.size == 0 || right.size == 0)
      return Array[T]().toSet
    (left & right).toSet
  }

  def Intersect[T: ClassTag](left: Set[T], right: TreeSet[T]): Set[T] = {
    if (left == null || right == null || left.size == 0 || right.size == 0)
      return Array[T]().toSet
    (left & right).toSet
  }

  def Intersect[T: ClassTag](left: TreeSet[T], right: Set[T]): Set[T] = {
    if (left == null || right == null || left.size == 0 || right.size == 0)
      return Array[T]().toSet
    (left & right).toSet
  }

  /** Union */
  def Union[T: ClassTag](left: ArrayBuffer[T], right: ArrayBuffer[T]): Set[T] = {
    if (left == null || left.size == 0 && right == null || right.size == 0) {
      Array[T]().toSet
    } else if (left == null || left.size == 0) {
      right.toSet
    } else if (right == null || right.size == 0) {
      left.toSet
    } else {
      (left.union(right)).toSet
    }
  }

  def Union[T: ClassTag](left: Array[T], right: Array[T]): Set[T] = {
    if (left == null || left.size == 0 && right == null || right.size == 0) {
      Array[T]().toSet
    } else if (left == null || left.size == 0) {
      right.toSet
    } else if (right == null || right.size == 0) {
      left.toSet
    } else {
      (left.union(right)).toSet
    }
  }

  def Union[T: ClassTag](left: Array[T], right: Set[T]): Set[T] = {
    if (left == null || left.size == 0 && right == null || right.size == 0) {
      Array[T]().toSet
    } else if (left == null || left.size == 0) {
      right.toSet
    } else if (right == null || right.size == 0) {
      left.toSet
    } else {
      (left.toSet.union(right))
    }
  }

  def Union[T: ClassTag](left: Set[T], right: Array[T]): Set[T] = {
    if (left == null || left.size == 0 && right == null || right.size == 0) {
      Array[T]().toSet
    } else if (left == null || left.size == 0) {
      right.toSet
    } else if (right == null || right.size == 0) {
      left.toSet
    } else {
      (left.union(right.toSet))
    }
  }

  def Union[T: ClassTag](left: Set[T], right: Set[T]): Set[T] = {
    if (left == null || left.size == 0 && right == null || right.size == 0) {
      Array[T]().toSet
    } else if (left == null || left.size == 0) {
      right.toSet
    } else if (right == null || right.size == 0) {
      left.toSet
    } else {
      left.union(right)
    }
  }

  /** Last && First for ordered collections */

  def Last(coll: Array[Any]): Any = {
    coll.last
  }

  def Last(coll: ArrayBuffer[Any]): Any = {
    coll.last
  }

  def Last[T: ClassTag](coll: ArrayBuffer[T]): T = {
    coll.last
  }

  def Last(coll: Queue[Any]): Any = {
    coll.last
  }

  def Last(coll: SortedSet[Any]): Any = {
    coll.last
  }

  def First(coll: Array[Any]): Any = {
    coll.head
  }

  def First(coll: ArrayBuffer[Any]): Any = {
    coll.head
  }

  def First(coll: Queue[Any]): Any = {
    coll.head
  }

  def First(coll: SortedSet[Any]): Any = {
    coll.head
  }

  /** Not & NotIn */
  def Not(boolexpr: Boolean): Boolean = {
    !boolexpr
  }

  def IsNotIn(fldRefExpr: String, setExprs: ArrayBuffer[String]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length == 0
  }

  def IsNotIn(fldRefExpr: Int, setExprs: ArrayBuffer[Int]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length == 0
  }

  def IsNotIn(fldRefExpr: Float, setExprs: ArrayBuffer[Float]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length == 0
  }

  def IsNotIn(fldRefExpr: Double, setExprs: ArrayBuffer[Double]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length == 0
  }

  def IsNotIn(fldRefExpr: String, setExprs: List[String]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length == 0
  }

  def IsNotIn(fldRefExpr: Int, setExprs: List[Int]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length == 0
  }

  def IsNotIn(fldRefExpr: Float, setExprs: List[Float]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length == 0
  }

  def IsNotIn(fldRefExpr: Double, setExprs: List[Double]): Boolean = {
    setExprs.filter(_ == fldRefExpr).length == 0
  }

  /** Between */

  def Between(thisOne: String, leftMargin: String, rightMargin: String, inclusive: Boolean): Boolean = {
    if (inclusive) (thisOne >= leftMargin && thisOne <= rightMargin) else (thisOne > leftMargin && thisOne < rightMargin)
  }

  def Between(thisOne: Int, leftMargin: Int, rightMargin: Int, inclusive: Boolean): Boolean = {
    if (inclusive) (thisOne >= leftMargin && thisOne <= rightMargin) else (thisOne > leftMargin && thisOne < rightMargin)
  }

  def Between(thisOne: Long, leftMargin: Long, rightMargin: Long, inclusive: Boolean): Boolean = {
    if (inclusive) (thisOne >= leftMargin && thisOne <= rightMargin) else (thisOne > leftMargin && thisOne < rightMargin)
  }

  def Between(thisOne: Int, leftMargin: Long, rightMargin: Long, inclusive: Boolean): Boolean = {
    if (inclusive) (thisOne >= leftMargin && thisOne <= rightMargin) else (thisOne > leftMargin && thisOne < rightMargin)
  }

  def Between(thisOne: Double, leftMargin: Double, rightMargin: Int, inclusive: Boolean): Boolean = {
    if (inclusive) (thisOne >= leftMargin && thisOne <= rightMargin) else (thisOne > leftMargin && thisOne < rightMargin)
  }

  def Between(thisOne: Int, leftMargin: Int, rightMargin: Double, inclusive: Boolean): Boolean = {
    if (inclusive) (thisOne >= leftMargin && thisOne <= rightMargin) else (thisOne > leftMargin && thisOne < rightMargin)
  }

  def Between(thisOne: Double, leftMargin: Double, rightMargin: Double, inclusive: Boolean): Boolean = {
    if (inclusive) (thisOne >= leftMargin && thisOne <= rightMargin) else (thisOne > leftMargin && thisOne < rightMargin)
  }

  def Between(thisOne: Double, leftMargin: Double, rightMargin: Float, inclusive: Boolean): Boolean = {
    if (inclusive) (thisOne >= leftMargin && thisOne <= rightMargin) else (thisOne > leftMargin && thisOne < rightMargin)
  }

  def Between(thisOne: Float, leftMargin: Float, rightMargin: Double, inclusive: Boolean): Boolean = {
    if (inclusive) (thisOne >= leftMargin && thisOne <= rightMargin) else (thisOne > leftMargin && thisOne < rightMargin)
  }

  def Between(thisOne: Float, leftMargin: Float, rightMargin: Int, inclusive: Boolean): Boolean = {
    if (inclusive) (thisOne >= leftMargin && thisOne <= rightMargin) else (thisOne > leftMargin && thisOne < rightMargin)
  }

  def Between(thisOne: Int, leftMargin: Int, rightMargin: Float, inclusive: Boolean): Boolean = {
    if (inclusive) (thisOne >= leftMargin && thisOne <= rightMargin) else (thisOne > leftMargin && thisOne < rightMargin)
  }

  def Between(thisOne: Float, leftMargin: Float, rightMargin: Float, inclusive: Boolean): Boolean = {
    if (inclusive) (thisOne >= leftMargin && thisOne <= rightMargin) else (thisOne > leftMargin && thisOne < rightMargin)
  }

  /** GreaterThan */

  def GreaterThan(expr1: String, expr2: String): Boolean = {
    (expr1 > expr2)
  }

  def GreaterThan(expr1: Int, expr2: Int): Boolean = {
    (expr1 > expr2)
  }

  def GreaterThan(expr1: Double, expr2: Int): Boolean = {
    (expr1 > expr2)
  }

  def GreaterThan(expr1: Int, expr2: Double): Boolean = {
    (expr1 > expr2)
  }

  def GreaterThan(expr1: Double, expr2: Double): Boolean = {
    (expr1 > expr2)
  }

  def GreaterThan(expr1: Double, expr2: Float): Boolean = {
    (expr1 > expr2)
  }

  def GreaterThan(expr1: Float, expr2: Double): Boolean = {
    (expr1 > expr2)
  }

  def GreaterThan(expr1: Float, expr2: Int): Boolean = {
    (expr1 > expr2)
  }

  def GreaterThan(expr1: Int, expr2: Float): Boolean = {
    (expr1 > expr2)
  }

  def GreaterThan(expr1: Float, expr2: Float): Boolean = {
    (expr1 > expr2)
  }

  /** GreaterOrEqual */

  def GreaterOrEqual(expr1: String, expr2: String): Boolean = {
    (expr1 >= expr2)
  }

  def GreaterOrEqual(expr1: Int, expr2: Int): Boolean = {
    (expr1 >= expr2)
  }

  def GreaterOrEqual(expr1: Double, expr2: Int): Boolean = {
    (expr1 >= expr2)
  }

  def GreaterOrEqual(expr1: Int, expr2: Double): Boolean = {
    (expr1 >= expr2)
  }

  def GreaterOrEqual(expr1: Double, expr2: Double): Boolean = {
    (expr1 >= expr2)
  }

  def GreaterOrEqual(expr1: Double, expr2: Float): Boolean = {
    (expr1 >= expr2)
  }

  def GreaterOrEqual(expr1: Float, expr2: Double): Boolean = {
    (expr1 >= expr2)
  }

  def GreaterOrEqual(expr1: Float, expr2: Int): Boolean = {
    (expr1 >= expr2)
  }

  def GreaterOrEqual(expr1: Int, expr2: Float): Boolean = {
    (expr1 >= expr2)
  }

  def GreaterOrEqual(expr1: Float, expr2: Float): Boolean = {
    (expr1 >= expr2)
  }

  /** LessOrEqual */

  def LessOrEqual(expr1: String, expr2: String): Boolean = {
    (expr1 <= expr2)
  }

  def LessOrEqual(expr1: Int, expr2: Int): Boolean = {
    (expr1 <= expr2)
  }

  def LessOrEqual(expr1: Double, expr2: Int): Boolean = {
    (expr1 <= expr2)
  }

  def LessOrEqual(expr1: Int, expr2: Double): Boolean = {
    (expr1 <= expr2)
  }

  def LessOrEqual(expr1: Double, expr2: Double): Boolean = {
    (expr1 <= expr2)
  }

  def LessOrEqual(expr1: Double, expr2: Float): Boolean = {
    (expr1 <= expr2)
  }

  def LessOrEqual(expr1: Float, expr2: Double): Boolean = {
    (expr1 <= expr2)
  }

  def LessOrEqual(expr1: Float, expr2: Int): Boolean = {
    (expr1 <= expr2)
  }

  def LessOrEqual(expr1: Int, expr2: Float): Boolean = {
    (expr1 <= expr2)
  }

  def LessOrEqual(expr1: Float, expr2: Float): Boolean = {
    (expr1 <= expr2)
  }

  /** LessThan */

  def LessThan(expr1: String, expr2: String): Boolean = {
    (expr1 < expr2)
  }

  def LessThan(expr1: Int, expr2: Int): Boolean = {
    (expr1 < expr2)
  }

  def LessThan(expr1: Double, expr2: Int): Boolean = {
    (expr1 < expr2)
  }

  def LessThan(expr1: Int, expr2: Double): Boolean = {
    (expr1 < expr2)
  }

  def LessThan(expr1: Double, expr2: Double): Boolean = {
    (expr1 < expr2)
  }

  def LessThan(expr1: Double, expr2: Float): Boolean = {
    (expr1 < expr2)
  }

  def LessThan(expr1: Float, expr2: Double): Boolean = {
    (expr1 < expr2)
  }

  def LessThan(expr1: Float, expr2: Int): Boolean = {
    (expr1 < expr2)
  }

  def LessThan(expr1: Int, expr2: Float): Boolean = {
    (expr1 < expr2)
  }

  def LessThan(expr1: Float, expr2: Float): Boolean = {
    (expr1 < expr2)
  }

  /** Equal */

  def Equal(expr1: String, expr2: String): Boolean = {
    (expr1 == expr2)
  }

  def Equal(expr1: Int, expr2: Int): Boolean = {
    (expr1 == expr2)
  }

  def Equal(expr1: Double, expr2: Int): Boolean = {
    (expr1 == expr2)
  }

  def Equal(expr1: Int, expr2: Double): Boolean = {
    (expr1 == expr2)
  }

  def Equal(expr1: Double, expr2: Double): Boolean = {
    (expr1 == expr2)
  }

  def Equal(expr1: Double, expr2: Float): Boolean = {
    (expr1 == expr2)
  }

  def Equal(expr1: Float, expr2: Double): Boolean = {
    (expr1 == expr2)
  }

  def Equal(expr1: Float, expr2: Int): Boolean = {
    (expr1 == expr2)
  }

  def Equal(expr1: Int, expr2: Float): Boolean = {
    (expr1 == expr2)
  }

  def Equal(expr1: Float, expr2: Float): Boolean = {
    (expr1 == expr2)
  }

  def Equal(expr1: Boolean, expr2: Boolean): Boolean = {
    (expr1 == expr2)
  }

  /** NotEqual */

  def NotEqual(expr1: String, expr2: String): Boolean = {
    !(expr1 == expr2)
  }

  def NotEqual(expr1: Int, expr2: Int): Boolean = {
    !(expr1 == expr2)
  }

  def NotEqual(expr1: Double, expr2: Int): Boolean = {
    !(expr1 == expr2)
  }

  def NotEqual(expr1: Int, expr2: Double): Boolean = {
    !(expr1 == expr2)
  }

  def NotEqual(expr1: Double, expr2: Double): Boolean = {
    !(expr1 == expr2)
  }

  def NotEqual(expr1: Double, expr2: Float): Boolean = {
    !(expr1 == expr2)
  }

  def NotEqual(expr1: Float, expr2: Double): Boolean = {
    !(expr1 == expr2)
  }

  def NotEqual(expr1: Float, expr2: Int): Boolean = {
    !(expr1 == expr2)
  }

  def NotEqual(expr1: Int, expr2: Float): Boolean = {
    !(expr1 == expr2)
  }

  def NotEqual(expr1: Float, expr2: Float): Boolean = {
    !(expr1 == expr2)
  }

  def NotEqual(expr1: Boolean, expr2: Boolean): Boolean = {
    !(expr1 == expr2)
  }

  /**   +, -, * and / */

  def Plus(expr1: String, expr2: String): String = {
    (expr1 + expr2)
  }

  def Plus(expr1: Int, expr2: Int): Int = {
    (expr1 + expr2)
  }

  def Plus(expr1: Int, expr2: Int, expr3: Int): Int = {
    (expr1 + expr2 + expr3)
  }

  def Plus(expr1: Long, expr2: Long, expr3: Long): Long = {
    (expr1 + expr2 + expr3)
  }

  def Plus(expr1: Int, expr2: Int, expr3: Int, expr4: Int): Int = {
    (expr1 + expr2 + expr3 + expr4)
  }

  def Plus(expr1: Long, expr2: Long, expr3: Long, expr4: Long): Long = {
    (expr1 + expr2 + expr3 + expr4)
  }

  def Plus(expr1: Int, expr2: Int, expr3: Int, expr4: Int, expr5: Int): Int = {
    (expr1 + expr2 + expr3 + expr4 + expr5)
  }

  def Plus(expr1: Long, expr2: Long, expr3: Long, expr4: Long, expr5: Long): Long = {
    (expr1 + expr2 + expr3 + expr4 + expr5)
  }

  def Plus(expr1: Int, expr2: Int, expr3: Int, expr4: Int, expr5: Int, expr6: Int): Int = {
    (expr1 + expr2 + expr3 + expr4 + expr5 + expr6)
  }

  def Plus(expr1: Long, expr2: Long, expr3: Long, expr4: Long, expr5: Long, expr6: Long): Long = {
    (expr1 + expr2 + expr3 + expr4 + expr5 + expr6)
  }

  def Plus(expr1: Int, expr2: Int, expr3: Int, expr4: Int, expr5: Int, expr6: Int, expr7: Int): Int = {
    (expr1 + expr2 + expr3 + expr4 + expr5 + expr6 + expr7)
  }

  def Plus(expr1: Long, expr2: Long, expr3: Long, expr4: Long, expr5: Long, expr6: Long, expr7: Long): Long = {
    (expr1 + expr2 + expr3 + expr4 + expr5 + expr6 + expr7)
  }

  def Plus(expr1: Int, expr2: Int, expr3: Int, expr4: Int, expr5: Int, expr6: Int, expr7: Int, expr8: Int): Int = {
    (expr1 + expr2 + expr3 + expr4 + expr5 + expr6 + expr7 + expr8)
  }

  def Plus(expr1: Int, expr2: Int, expr3: Int, expr4: Int, expr5: Int, expr6: Int, expr7: Int, expr8: Long): Long = {
    (expr1 + expr2 + expr3 + expr4 + expr5 + expr6 + expr7 + expr8)
  }

  def Plus(expr1: Double, expr2: Double): Double = {
    (expr1 + expr2)
  }

  def Plus(expr1: Double, expr2: Double, expr3: Double): Double = {
    (expr1 + expr2 + expr3)
  }

  def Plus(expr1: Double, expr2: Double, expr3: Double, expr4: Double): Double = {
    (expr1 + expr2 + expr3 + expr4)
  }

  def Plus(expr1: Double, expr2: Double, expr3: Double, expr4: Double, expr5: Double): Double = {
    (expr1 + expr2 + expr3 + expr4 + expr5)
  }

  def Plus(expr1: Double, expr2: Double, expr3: Double, expr4: Double, expr5: Double, expr6: Double): Double = {
    (expr1 + expr2 + expr3 + expr4 + expr5 + expr6)
  }

  def Plus(expr1: Double, expr2: Int): Double = {
    (expr1 + expr2)
  }

  def Plus(expr1: Int, expr2: Double): Double = {
    (expr1 + expr2)
  }

  def Plus(expr1: Int, expr2: Long): Double = {
    (expr1 + expr2)
  }

  def Plus(expr1: Long, expr2: Int): Long = {
    (expr1 + expr2)
  }

  def Plus(expr1: Int, expr2: Float): Float = {
    (expr1 + expr2)
  }

  def Plus(expr1: Float, expr2: Int): Float = {
    (expr1 + expr2)
  }

  def Plus(expr1: Long, expr2: Long): Long = {
    (expr1 + expr2)
  }

  def Plus(expr1: Double, expr2: Long): Double = {
    (expr1 + expr2)
  }

  def Plus(expr1: Long, expr2: Double): Double = {
    (expr1 + expr2)
  }

  def Plus(expr1: Long, expr2: Float): Float = {
    (expr1 + expr2)
  }

  def Plus(expr1: Float, expr2: Long): Float = {
    (expr1 + expr2)
  }

  def Plus(expr1: Double, expr2: Float): Double = {
    (expr1 + expr2)
  }

  def Plus(expr1: Float, expr2: Double): Double = {
    (expr1 + expr2)
  }

  def Plus(expr1: Float, expr2: Float): Float = {
    (expr1 + expr2)
  }

  def Plus(exprs: ArrayBuffer[String]): String = {
    exprs.reduceLeft(_ + _)
  }

  def Plus(exprs: ArrayBuffer[Int]): Int = {
    exprs.reduceLeft(_ + _)
  }

  def Plus(exprs: ArrayBuffer[Long]): Long = {
    exprs.reduceLeft(_ + _)
  }

  def Plus(exprs: ArrayBuffer[Double]): Double = {
    exprs.reduceLeft(_ + _)
  }

  def Plus(exprs: ArrayBuffer[Float]): Float = {
    exprs.reduceLeft(_ + _)
  }

  def Plus(exprs: Array[String]): String = {
    exprs.reduceLeft(_ + _)
  }

  def Plus(exprs: Array[Int]): Int = {
    exprs.reduceLeft(_ + _)
  }

  def Plus(exprs: Array[Long]): Long = {
    exprs.reduceLeft(_ + _)
  }

  def Plus(exprs: Array[Double]): Double = {
    exprs.reduceLeft(_ + _)
  }

  def Plus(exprs: Array[Float]): Float = {
    exprs.reduceLeft(_ + _)
  }

  /** - */

  def Minus(expr1: Int, expr2: Int): Int = {
    (expr1 - expr2)
  }

  def Minus(expr1: Double, expr2: Int): Double = {
    (expr1 - expr2)
  }

  def Minus(expr1: Int, expr2: Double): Double = {
    (expr1 - expr2)
  }

  def Minus(expr1: Int, expr2: Long): Double = {
    (expr1 - expr2)
  }

  def Minus(expr1: Long, expr2: Int): Long = {
    (expr1 - expr2)
  }

  def Minus(expr1: Int, expr2: Float): Float = {
    (expr1 - expr2)
  }

  def Minus(expr1: Float, expr2: Int): Float = {
    (expr1 - expr2)
  }

  def Minus(expr1: Long, expr2: Long): Long = {
    (expr1 - expr2)
  }

  def Minus(expr1: Double, expr2: Long): Double = {
    (expr1 - expr2)
  }

  def Minus(expr1: Long, expr2: Double): Double = {
    (expr1 - expr2)
  }

  def Minus(expr1: Long, expr2: Float): Float = {
    (expr1 - expr2)
  }

  def Minus(expr1: Float, expr2: Long): Float = {
    (expr1 - expr2)
  }

  def Minus(expr1: Double, expr2: Double): Double = {
    (expr1 - expr2)
  }

  def Minus(expr1: Double, expr2: Float): Double = {
    (expr1 - expr2)
  }

  def Minus(expr1: Float, expr2: Double): Double = {
    (expr1 - expr2)
  }

  def Minus(expr1: Float, expr2: Float): Float = {
    (expr1 - expr2)
  }

  def Minus(exprs: ArrayBuffer[Int]): Int = {
    exprs.reduceLeft(_ - _)
  }

  def Minus(exprs: ArrayBuffer[Long]): Long = {
    exprs.reduceLeft(_ - _)
  }

  def Minus(exprs: ArrayBuffer[Double]): Double = {
    exprs.reduceLeft(_ - _)
  }

  def Minus(exprs: ArrayBuffer[Float]): Float = {
    exprs.reduceLeft(_ - _)
  }

  def Minus(exprs: Array[Int]): Int = {
    exprs.reduceLeft(_ - _)
  }

  def Minus(exprs: Array[Long]): Long = {
    exprs.reduceLeft(_ - _)
  }

  def Minus(exprs: Array[Double]): Double = {
    exprs.reduceLeft(_ - _)
  }

  def Minus(exprs: Array[Float]): Float = {
    exprs.reduceLeft(_ - _)
  }

  /** '*' */

  def Multiply(expr1: Int, expr2: Int): Int = {
    (expr1 * expr2)
  }

  def Multiply(expr1: Double, expr2: Int): Double = {
    (expr1 * expr2)
  }

  def Multiply(expr1: Int, expr2: Double): Double = {
    (expr1 * expr2)
  }

  def Multiply(expr1: Int, expr2: Long): Double = {
    (expr1 * expr2)
  }

  def Multiply(expr1: Long, expr2: Int): Long = {
    (expr1 * expr2)
  }

  def Multiply(expr1: Int, expr2: Float): Float = {
    (expr1 * expr2)
  }

  def Multiply(expr1: Float, expr2: Int): Float = {
    (expr1 * expr2)
  }

  def Multiply(expr1: Long, expr2: Long): Long = {
    (expr1 * expr2)
  }

  def Multiply(expr1: Double, expr2: Long): Double = {
    (expr1 * expr2)
  }

  def Multiply(expr1: Long, expr2: Double): Double = {
    (expr1 * expr2)
  }

  def Multiply(expr1: Long, expr2: Float): Float = {
    (expr1 * expr2)
  }

  def Multiply(expr1: Float, expr2: Long): Float = {
    (expr1 * expr2)
  }

  def Multiply(expr1: Double, expr2: Double): Double = {
    (expr1 * expr2)
  }

  def Multiply(expr1: Double, expr2: Float): Double = {
    (expr1 * expr2)
  }

  def Multiply(expr1: Float, expr2: Double): Double = {
    (expr1 * expr2)
  }

  def Multiply(expr1: Float, expr2: Float): Float = {
    (expr1 * expr2)
  }

  def Multiply(exprs: ArrayBuffer[Int]): Int = {
    exprs.reduceLeft(_ * _)
  }

  def Multiply(exprs: ArrayBuffer[Long]): Long = {
    exprs.reduceLeft(_ * _)
  }

  def Multiply(exprs: ArrayBuffer[Double]): Double = {
    exprs.reduceLeft(_ * _)
  }

  def Multiply(exprs: ArrayBuffer[Float]): Float = {
    exprs.reduceLeft(_ * _)
  }

  def Multiply(exprs: Array[Int]): Int = {
    exprs.reduceLeft(_ * _)
  }

  def Multiply(exprs: Array[Long]): Long = {
    exprs.reduceLeft(_ * _)
  }

  def Multiply(exprs: Array[Double]): Double = {
    exprs.reduceLeft(_ * _)
  }

  def Multiply(exprs: Array[Float]): Float = {
    exprs.reduceLeft(_ * _)
  }

  /** '/' */

  def Divide(expr1: Int, expr2: Int): Int = {
    (expr1 / expr2)
  }

  def Divide(expr1: Double, expr2: Int): Double = {
    (expr1 / expr2)
  }

  def Divide(expr1: Int, expr2: Double): Double = {
    (expr1 / expr2)
  }

  def Divide(expr1: Int, expr2: Long): Double = {
    (expr1 / expr2)
  }

  def Divide(expr1: Long, expr2: Int): Long = {
    (expr1 / expr2)
  }

  def Divide(expr1: Int, expr2: Float): Float = {
    (expr1 / expr2)
  }

  def Divide(expr1: Float, expr2: Int): Float = {
    (expr1 / expr2)
  }

  def Divide(expr1: Long, expr2: Long): Long = {
    (expr1 / expr2)
  }

  def Divide(expr1: Double, expr2: Long): Double = {
    (expr1 / expr2)
  }

  def Divide(expr1: Long, expr2: Double): Double = {
    (expr1 / expr2)
  }

  def Divide(expr1: Long, expr2: Float): Float = {
    (expr1 / expr2)
  }

  def Divide(expr1: Float, expr2: Long): Float = {
    (expr1 / expr2)
  }

  def Divide(expr1: Double, expr2: Double): Double = {
    (expr1 / expr2)
  }

  def Divide(expr1: Double, expr2: Float): Double = {
    (expr1 / expr2)
  }

  def Divide(expr1: Float, expr2: Double): Double = {
    (expr1 / expr2)
  }

  def Divide(expr1: Float, expr2: Float): Float = {
    (expr1 / expr2)
  }

  def Divide(exprs: ArrayBuffer[Int]): Int = {
    exprs.reduceLeft(_ / _)
  }

  def Divide(exprs: ArrayBuffer[Long]): Long = {
    exprs.reduceLeft(_ / _)
  }

  def Divide(exprs: ArrayBuffer[Double]): Double = {
    exprs.reduceLeft(_ / _)
  }

  def Divide(exprs: ArrayBuffer[Float]): Float = {
    exprs.reduceLeft(_ / _)
  }

  /**  min, max, sum, avg, median, product */

  def Min(expr1: Int, expr2: Int): Int = {
    (min(expr1, expr2))
  }

  def Min(expr1: Double, expr2: Int): Double = {
    (min(expr1, expr2))
  }

  def Min(expr1: Int, expr2: Double): Double = {
    (min(expr1, expr2))
  }

  def Min(expr1: Int, expr2: Long): Double = {
    (min(expr1, expr2))
  }

  def Min(expr1: Long, expr2: Int): Long = {
    (min(expr1, expr2))
  }

  def Min(expr1: Int, expr2: Float): Float = {
    (min(expr1, expr2))
  }

  def Min(expr1: Float, expr2: Int): Float = {
    (min(expr1, expr2))
  }

  def Min(expr1: Long, expr2: Long): Long = {
    (min(expr1, expr2))
  }

  def Min(expr1: Double, expr2: Long): Double = {
    (min(expr1, expr2))
  }

  def Min(expr1: Long, expr2: Double): Double = {
    (min(expr1, expr2))
  }

  def Min(expr1: Long, expr2: Float): Float = {
    (min(expr1, expr2))
  }

  def Min(expr1: Float, expr2: Long): Float = {
    (min(expr1, expr2))
  }

  def Min(expr1: Double, expr2: Double): Double = {
    (min(expr1, expr2))
  }

  def Min(expr1: Double, expr2: Float): Double = {
    (min(expr1, expr2))
  }

  def Min(expr1: Float, expr2: Double): Double = {
    (min(expr1, expr2))
  }

  def Min(expr1: Float, expr2: Float): Float = {
    (min(expr1, expr2))
  }

  def Min(exprs: ArrayBuffer[Int]): Int = {
    exprs.reduceLeft(min(_, _))
  }

  def Min(exprs: ArrayBuffer[Long]): Long = {
    exprs.reduceLeft(min(_, _))
  }

  def Min(exprs: ArrayBuffer[Double]): Double = {
    exprs.reduceLeft(min(_, _))
  }

  def Min(exprs: ArrayBuffer[Float]): Float = {
    exprs.reduceLeft(min(_, _))
  }

  def Min(exprs: Array[Int]): Int = {
    exprs.reduceLeft(min(_, _))
  }

  def Min(exprs: Array[Long]): Long = {
    exprs.reduceLeft(min(_, _))
  }

  def Min(exprs: Array[Double]): Double = {
    exprs.reduceLeft(min(_, _))
  }

  def Min(exprs: Array[Float]): Float = {
    exprs.reduceLeft(min(_, _))
  }

  def Min(exprs: List[Int]): Int = {
    exprs.reduceLeft(min(_, _))
  }

  def Min(exprs: List[Long]): Long = {
    exprs.reduceLeft(min(_, _))
  }

  def Min(exprs: List[Double]): Double = {
    exprs.reduceLeft(min(_, _))
  }

  def Min(exprs: List[Float]): Float = {
    exprs.reduceLeft(min(_, _))
  }

  /** max */

  def Max(expr1: Int, expr2: Int): Int = {
    (min(expr1, expr2))
  }

  def Max(expr1: Double, expr2: Int): Double = {
    (min(expr1, expr2))
  }

  def Max(expr1: Int, expr2: Double): Double = {
    (min(expr1, expr2))
  }

  def Max(expr1: Int, expr2: Long): Double = {
    (min(expr1, expr2))
  }

  def Max(expr1: Long, expr2: Int): Long = {
    (min(expr1, expr2))
  }

  def Max(expr1: Int, expr2: Float): Float = {
    (min(expr1, expr2))
  }

  def Max(expr1: Float, expr2: Int): Float = {
    (min(expr1, expr2))
  }

  def Max(expr1: Long, expr2: Long): Long = {
    (min(expr1, expr2))
  }

  def Max(expr1: Double, expr2: Long): Double = {
    (min(expr1, expr2))
  }

  def Max(expr1: Long, expr2: Double): Double = {
    (min(expr1, expr2))
  }

  def Max(expr1: Long, expr2: Float): Float = {
    (min(expr1, expr2))
  }

  def Max(expr1: Float, expr2: Long): Float = {
    (min(expr1, expr2))
  }

  def Max(expr1: Double, expr2: Double): Double = {
    (min(expr1, expr2))
  }

  def Max(expr1: Double, expr2: Float): Double = {
    (min(expr1, expr2))
  }

  def Max(expr1: Float, expr2: Double): Double = {
    (min(expr1, expr2))
  }

  def Max(expr1: Float, expr2: Float): Float = {
    (min(expr1, expr2))
  }

  def Max(exprs: ArrayBuffer[Int]): Int = {
    exprs.reduceLeft(min(_, _))
  }

  def Max(exprs: ArrayBuffer[Long]): Long = {
    exprs.reduceLeft(min(_, _))
  }

  def Max(exprs: ArrayBuffer[Double]): Double = {
    exprs.reduceLeft(min(_, _))
  }

  def Max(exprs: ArrayBuffer[Float]): Float = {
    exprs.reduceLeft(min(_, _))
  }

  def Max(exprs: Array[Int]): Int = {
    exprs.reduceLeft(min(_, _))
  }

  def Max(exprs: Array[Long]): Long = {
    exprs.reduceLeft(min(_, _))
  }

  def Max(exprs: Array[Double]): Double = {
    exprs.reduceLeft(min(_, _))
  }

  def Max(exprs: Array[Float]): Float = {
    exprs.reduceLeft(min(_, _))
  }

  def Max(exprs: List[Int]): Int = {
    exprs.reduceLeft(min(_, _))
  }

  def Max(exprs: List[Long]): Long = {
    exprs.reduceLeft(min(_, _))
  }

  def Max(exprs: List[Double]): Double = {
    exprs.reduceLeft(min(_, _))
  }

  def Max(exprs: List[Float]): Float = {
    exprs.reduceLeft(min(_, _))
  }

  /** sum */

  //def Sum[A](exprs: ArrayBuffer[A]): A = {
  //  exprs.reduceLeft(_ + _)
  //}

  def Sum(exprs: ArrayBuffer[Int]): Int = {
    if (exprs != null && exprs.size > 0)
      exprs.reduceLeft(_ + _)
    else
      0
  }

  def Sum(exprs: ArrayBuffer[Long]): Long = {
    if (exprs != null && exprs.size > 0)
      exprs.reduceLeft(_ + _)
    else
      0
  }

  def Sum(exprs: ArrayBuffer[Double]): Double = {
    if (exprs != null && exprs.size > 0)
      exprs.reduceLeft(_ + _)
    else
      0
  }

  def Sum(exprs: ArrayBuffer[Float]): Float = {
    if (exprs != null && exprs.size > 0)
      exprs.reduceLeft(_ + _)
    else
      0
  }

  def Sum(exprs: Array[Int]): Int = {
    if (exprs != null && exprs.size > 0)
      exprs.reduceLeft(_ + _)
    else
      0
  }

  def Sum(exprs: Array[Long]): Long = {
    if (exprs != null && exprs.size > 0)
      exprs.reduceLeft(_ + _)
    else
      0
  }

  def Sum(exprs: Array[Double]): Double = {
    if (exprs != null && exprs.size > 0)
      exprs.reduceLeft(_ + _)
    else
      0
  }

  def Sum(exprs: Array[Float]): Float = {
    if (exprs != null && exprs.size > 0)
      exprs.reduceLeft(_ + _)
    else
      0
  }

  def Sum(tuples: Tuple2[Int, Int]): Int = {
    val exprs: Array[Int] = ToArray(tuples)
    if (exprs != null && exprs.size > 0)
      exprs.reduceLeft(_ + _)
    else
      0
  }

  def Sum(tuples: Tuple3[Int, Int, Int]): Int = {
    val exprs: Array[Int] = ToArray(tuples)
    if (exprs != null && exprs.size > 0)
      exprs.reduceLeft(_ + _)
    else
      0
  }

  def Sum(tuples: Tuple4[Int, Int, Int, Int]): Int = {
    val exprs: Array[Int] = ToArray(tuples)
    if (exprs != null && exprs.size > 0)
      exprs.reduceLeft(_ + _)
    else
      0
  }

  def Sum(tuples: Tuple5[Int, Int, Int, Int, Int]): Int = {
    val exprs: Array[Int] = ToArray(tuples)
    if (exprs != null && exprs.size > 0)
      exprs.reduceLeft(_ + _)
    else
      0
  }

  def Sum(tuples: Tuple6[Int, Int, Int, Int, Int, Int]): Int = {
    val exprs: Array[Int] = ToArray(tuples)
    if (exprs != null && exprs.size > 0)
      exprs.reduceLeft(_ + _)
    else
      0
  }

  def Sum(tuples: Tuple7[Int, Int, Int, Int, Int, Int, Int]): Int = {
    val exprs: Array[Int] = ToArray(tuples)
    if (exprs != null && exprs.size > 0)
      exprs.reduceLeft(_ + _)
    else
      0
  }

  def Sum(tuples: Tuple8[Int, Int, Int, Int, Int, Int, Int, Int]): Int = {
    val exprs: Array[Int] = ToArray(tuples)
    if (exprs != null && exprs.size > 0)
      exprs.reduceLeft(_ + _)
    else
      0
  }

  def Sum(tuples: Tuple9[Int, Int, Int, Int, Int, Int, Int, Int, Int]): Int = {
    val exprs: Array[Int] = ToArray(tuples)
    if (exprs != null && exprs.size > 0)
      exprs.reduceLeft(_ + _)
    else
      0
  }

  def Sum(tuples: Tuple10[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]): Int = {
    val exprs: Array[Int] = ToArray(tuples)
    if (exprs != null && exprs.size > 0)
      exprs.reduceLeft(_ + _)
    else
      0
  }

  /** Sum functions for Tuple2 */
  def Sum(tuples: Tuple2[Float, Float]): Float = {
    val exprs: Array[Float] = ToArray(tuples)
    if (exprs != null && exprs.size > 0)
      exprs.reduceLeft(_ + _)
    else
      0
  }

  def SumToFloat(tuples: Tuple2[Any, Any]): Float = {
    val exprs: Array[Float] = ToArrayOfFloat(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToArrayOfFloat(tuples: Array[Tuple2[Any, Any]]): Array[Float] = {
    val exprs: Array[Float] = tuples.map(tuple => SumToFloat(tuple))
    //exprs.reduceLeft(_ + _)
    exprs
  }

  def SumToDouble(tuples: Tuple2[Any, Any]): Double = {
    val exprs: Array[Double] = ToArrayOfDouble(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToArrayOfDouble(tuples: Array[Tuple2[Any, Any]]): Array[Double] = {
    val exprs: Array[Double] = tuples.map(tuple => SumToDouble(tuple))
    exprs
  }

  def SumToInt(tuples: Tuple2[Any, Any]): Int = {
    val exprs: Array[Int] = ToArrayOfInt(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToArrayOfInt(tuples: Array[Tuple2[Any, Any]]): Array[Int] = {
    val exprs: Array[Int] = tuples.map(tuple => SumToInt(tuple))
    exprs
  }

  /** Sum functions for Tuple3 */
  def Sum(tuples: Tuple3[Float, Float, Float]): Float = {
    val exprs: Array[Float] = ToArray(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToFloat(tuples: Tuple3[Any, Any, Any]): Float = {
    val exprs: Array[Float] = ToArrayOfFloat(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToArrayOfFloat(tuples: Array[Tuple3[Any, Any, Any]]): Array[Float] = {
    val exprs: Array[Float] = tuples.map(tuple => SumToFloat(tuple))
    //exprs.reduceLeft(_ + _)
    exprs
  }

  def SumToDouble(tuples: Tuple3[Any, Any, Any]): Double = {
    val exprs: Array[Double] = ToArrayOfDouble(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToArrayOfDouble(tuples: Array[Tuple3[Any, Any, Any]]): Array[Double] = {
    val exprs: Array[Double] = tuples.map(tuple => SumToDouble(tuple))
    exprs
  }

  def SumToInt(tuples: Tuple3[Any, Any, Any]): Int = {
    val exprs: Array[Int] = ToArrayOfInt(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToArrayOfInt(tuples: Array[Tuple3[Any, Any, Any]]): Array[Int] = {
    val exprs: Array[Int] = tuples.map(tuple => SumToInt(tuple))
    exprs
  }

  /** Sum functions for Tuple4 */
  def Sum(tuples: Tuple4[Float, Float, Float, Float]): Float = {
    val exprs: Array[Float] = ToArray(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToFloat(tuples: Tuple4[Any, Any, Any, Any]): Float = {
    val exprs: Array[Float] = ToArrayOfFloat(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToArrayOfFloat(tuples: Array[Tuple4[Any, Any, Any, Any]]): Array[Float] = {
    val exprs: Array[Float] = tuples.map(tuple => SumToFloat(tuple))
    //exprs.reduceLeft(_ + _)
    exprs
  }

  def SumToDouble(tuples: Tuple4[Any, Any, Any, Any]): Double = {
    val exprs: Array[Double] = ToArrayOfDouble(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToArrayOfDouble(tuples: Array[Tuple4[Any, Any, Any, Any]]): Array[Double] = {
    val exprs: Array[Double] = tuples.map(tuple => SumToDouble(tuple))
    exprs
  }

  def SumToInt(tuples: Tuple4[Any, Any, Any, Any]): Int = {
    val exprs: Array[Int] = ToArrayOfInt(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToArrayOfInt(tuples: Array[Tuple4[Any, Any, Any, Any]]): Array[Int] = {
    val exprs: Array[Int] = tuples.map(tuple => SumToInt(tuple))
    exprs
  }

  /** Sum functions for Tuple5 */
  def Sum(tuples: Tuple5[Float, Float, Float, Float, Float]): Float = {
    val exprs: Array[Float] = ToArray(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToFloat(tuples: Tuple5[Any, Any, Any, Any, Any]): Float = {
    val exprs: Array[Float] = ToArrayOfFloat(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToArrayOfFloat(tuples: Array[Tuple5[Any, Any, Any, Any, Any]]): Array[Float] = {
    val exprs: Array[Float] = tuples.map(tuple => SumToFloat(tuple))
    exprs
  }

  def SumToDouble(tuples: Tuple5[Any, Any, Any, Any, Any]): Double = {
    val exprs: Array[Double] = ToArrayOfDouble(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToArrayOfDouble(tuples: Array[Tuple5[Any, Any, Any, Any, Any]]): Array[Double] = {
    val exprs: Array[Double] = tuples.map(tuple => SumToDouble(tuple))
    exprs
  }

  def SumToInt(tuples: Tuple5[Any, Any, Any, Any, Any]): Int = {
    val exprs: Array[Int] = ToArrayOfInt(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToArrayOfInt(tuples: Array[Tuple5[Any, Any, Any, Any, Any]]): Array[Int] = {
    val exprs: Array[Int] = tuples.map(tuple => SumToInt(tuple))
    exprs
  }

  /** Sum functions for Tuple6 */
  def Sum(tuples: Tuple6[Float, Float, Float, Float, Float, Float]): Float = {
    val exprs: Array[Float] = ToArray(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToFloat(tuples: Tuple6[Any, Any, Any, Any, Any, Any]): Float = {
    val exprs: Array[Float] = ToArrayOfFloat(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToArrayOfFloat(tuples: Array[Tuple6[Any, Any, Any, Any, Any, Any]]): Array[Float] = {
    val exprs: Array[Float] = tuples.map(tuple => SumToFloat(tuple))
    exprs
  }

  def SumToDouble(tuples: Tuple6[Any, Any, Any, Any, Any, Any]): Double = {
    val exprs: Array[Double] = ToArrayOfDouble(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToArrayOfDouble(tuples: Array[Tuple6[Any, Any, Any, Any, Any, Any]]): Array[Double] = {
    val exprs: Array[Double] = tuples.map(tuple => SumToDouble(tuple))
    exprs
  }

  def SumToInt(tuples: Tuple6[Any, Any, Any, Any, Any, Any]): Int = {
    val exprs: Array[Int] = ToArrayOfInt(tuples)
    exprs.reduceLeft(_ + _)
  }

  def SumToArrayOfInt(tuples: Array[Tuple6[Any, Any, Any, Any, Any, Any]]): Array[Int] = {
    val exprs: Array[Int] = tuples.map(tuple => SumToInt(tuple))
    exprs
  }

  /** FIXME: Do SumTo<Scalar> and SumToArrayOf<Scalar> for the remaining Tuple<N> */
  def Sum(tuples: Tuple7[Float, Float, Float, Float, Float, Float, Float]): Float = {
    val exprs: Array[Float] = ToArray(tuples)
    exprs.reduceLeft(_ + _)
  }

  def Sum(tuples: Tuple8[Float, Float, Float, Float, Float, Float, Float, Float]): Float = {
    val exprs: Array[Float] = ToArray(tuples)
    exprs.reduceLeft(_ + _)
  }

  def Sum(tuples: Tuple9[Float, Float, Float, Float, Float, Float, Float, Float, Float]): Float = {
    val exprs: Array[Float] = ToArray(tuples)
    exprs.reduceLeft(_ + _)
  }

  def Sum(tuples: Tuple10[Float, Float, Float, Float, Float, Float, Float, Float, Float, Float]): Float = {
    val exprs: Array[Float] = ToArray(tuples)
    exprs.reduceLeft(_ + _)
  }

  def Sum(tuples: Tuple2[Double, Double]): Double = {
    val exprs: Array[Double] = ToArray(tuples)
    exprs.reduceLeft(_ + _)
  }

  def Sum(tuples: Tuple3[Double, Double, Double]): Double = {
    val exprs: Array[Double] = ToArray(tuples)
    exprs.reduceLeft(_ + _)
  }

  def Sum(tuples: Tuple4[Double, Double, Double, Double]): Double = {
    val exprs: Array[Double] = ToArray(tuples)
    exprs.reduceLeft(_ + _)
  }

  def Sum(tuples: Tuple5[Double, Double, Double, Double, Double]): Double = {
    val exprs: Array[Double] = ToArray(tuples)
    exprs.reduceLeft(_ + _)
  }

  def Sum(tuples: Tuple6[Double, Double, Double, Double, Double, Double]): Double = {
    val exprs: Array[Double] = ToArray(tuples)
    exprs.reduceLeft(_ + _)
  }

  def Sum(tuples: Tuple7[Double, Double, Double, Double, Double, Double, Double]): Double = {
    val exprs: Array[Double] = ToArray(tuples)
    exprs.reduceLeft(_ + _)
  }

  def Sum(tuples: Tuple8[Double, Double, Double, Double, Double, Double, Double, Double]): Double = {
    val exprs: Array[Double] = ToArray(tuples)
    exprs.reduceLeft(_ + _)
  }

  def Sum(tuples: Tuple9[Double, Double, Double, Double, Double, Double, Double, Double, Double]): Double = {
    val exprs: Array[Double] = ToArray(tuples)
    exprs.reduceLeft(_ + _)
  }

  def Sum(tuples: Tuple10[Double, Double, Double, Double, Double, Double, Double, Double, Double, Double]): Double = {
    val exprs: Array[Double] = ToArray(tuples)
    exprs.reduceLeft(_ + _)
  }

  def Sum(exprs: List[Int]): Int = {
    exprs.reduceLeft(_ + _)
  }

  def Sum(exprs: List[Long]): Long = {
    exprs.reduceLeft(_ + _)
  }

  def Sum(exprs: List[Double]): Double = {
    exprs.reduceLeft(_ + _)
  }

  def Sum(exprs: List[Float]): Float = {
    exprs.reduceLeft(_ + _)
  }

  /** avg (average)*/

  def Avg(exprs: ArrayBuffer[Int]): Int = {
    Sum(exprs) / exprs.length
  }

  def Avg(exprs: ArrayBuffer[Long]): Long = {
    Sum(exprs) / exprs.length
  }

  def Avg(exprs: ArrayBuffer[Double]): Double = {
    Sum(exprs) / exprs.length
  }

  def Avg(exprs: ArrayBuffer[Float]): Float = {
    Sum(exprs) / exprs.length
  }

  def Avg(exprs: Array[Int]): Int = {
    Sum(exprs) / exprs.length
  }

  def Avg(exprs: Array[Long]): Long = {
    Sum(exprs) / exprs.length
  }

  def Avg(exprs: Array[Double]): Double = {
    Sum(exprs) / exprs.length
  }

  def Avg(exprs: Array[Float]): Float = {
    Sum(exprs) / exprs.length
  }

  def Avg(exprs: List[Int]): Int = {
    Sum(exprs) / exprs.length
  }

  def Avg(exprs: List[Long]): Long = {
    Sum(exprs) / exprs.length
  }

  def Avg(exprs: List[Double]): Double = {
    Sum(exprs) / exprs.length
  }

  def Avg(exprs: List[Float]): Float = {
    Sum(exprs) / exprs.length
  }

  /** Count (count)*/

  def Count[A](exprs: ArrayBuffer[A]): Int = {
    exprs.length
  }

  def Count[A](exprs: Array[A]): Int = {
    exprs.length
  }

  def Count[A](exprs: List[A]): Int = {
    exprs.length
  }

  def Count[A](exprs: Set[A]): Int = {
    exprs.size
  }

  def Count[A](exprs: Queue[A]): Int = {
    exprs.size
  }

  def Count[A, B](exprs: Map[A, B]): Int = {
    exprs.size
  }

  def Count[A, B](exprs: HashMap[A, B]): Int = {
    exprs.size
  }

  /**
   * Aggregation functions supported by Pmml
   */

  /** "Multiset" builds sets of item values for each group by key 
  def MultiSet[A: ClassTag, B: ClassTag](exprs: ArrayBuffer[A], groupByKey: ArrayBuffer[B])(implicit cmp: Ordering[B]): HashMap[B, ArrayBuffer[A]] = {
    var map = HashMap[B, ArrayBuffer[A]]()
    var grpleft: List[A] = exprs.toList
    val distinct: Array[B] = groupByKey.toSet.toArray.sorted
    distinct.foreach((key: B) => {
      val grp: Map[Boolean, List[A]] = grpleft.groupBy(_ == key)
      val setItems: List[A] = grp(true)
      val values: ArrayBuffer[A] = new ArrayBuffer[A]
      values ++ setItems
      map(key) = values
      grpleft = grp(false)
    })
    map
  }

   "multiset" (similar to Ligadata 'each')
  def MultiSet(exprs: ArrayBuffer[Int], groupByKey: ArrayBuffer[Int]): HashMap[Int, ArrayBuffer[Int]] = {
    var map = HashMap[Int, ArrayBuffer[Int]]()
    var grpleft: List[Int] = exprs.toList
    val distinct = groupByKey.toSet.toArray.sorted
    distinct.foreach((key: Int) => {
      val grp: Map[Boolean, List[Int]] = grpleft.groupBy(_ == key)
      val setItems: List[Int] = grp(true)
      var arrItems: ArrayBuffer[Int] = ArrayBuffer[Int]()
      for (itm <- setItems) { arrItems += itm }
      map(key) = arrItems
      grpleft = grp(false)
    })
    map
  } */

  def Sum[B: ClassTag](exprs: ArrayBuffer[Int], groupByKey: ArrayBuffer[B])(implicit cmp: Ordering[B]): HashMap[B, Int] = {
    var map = HashMap[B, Int]()
    var grpleft: List[Int] = exprs.toList
    val distinct = groupByKey.toSet.toArray.sorted
    distinct.foreach((key: B) => {
      val grp: Map[Boolean, List[Int]] = grpleft.groupBy(_ == key)
      val setItems: List[Int] = grp(true)
      map(key) = Sum(setItems)
      grpleft = grp(false)
    })
    map
  }

  def Sum[A: ClassTag, B: ClassTag](exprs: ArrayBuffer[Double], groupByKey: ArrayBuffer[B])(implicit cmp: Ordering[B]): HashMap[B, Double] = {
    var map = HashMap[B, Double]()
    var grpleft: List[Double] = exprs.toList
    val distinct = groupByKey.toSet.toArray.sorted
    distinct.foreach((key: B) => {
      val grp: Map[Boolean, List[Double]] = grpleft.groupBy(_ == key)
      val setItems: List[Double] = grp(true)
      map(key) = Sum(setItems)
      grpleft = grp(false)
    })
    map
  }

  def Avg(exprs: ArrayBuffer[Int], groupByKey: ArrayBuffer[Int]): HashMap[Int, Int] = {
    var map = HashMap[Int, Int]()
    var grpleft: List[Int] = exprs.toList
    val distinct = groupByKey.toSet.toArray.sorted
    distinct.foreach((key: Int) => {
      val grp: Map[Boolean, List[Int]] = grpleft.groupBy(_ == key)
      val setItems: List[Int] = grp(true)
      map(key) = Avg(setItems)
      grpleft = grp(false)
    })
    map
  }

  def Count(exprs: ArrayBuffer[Int], groupByKey: ArrayBuffer[Int]): HashMap[Int, Int] = {
    var map = HashMap[Int, Int]()
    var grpleft: List[Int] = exprs.toList
    val distinct = groupByKey.toSet.toArray.sorted
    distinct.foreach((key: Int) => {
      val grp: Map[Boolean, List[Int]] = grpleft.groupBy(_ == key)
      val setItems: List[Int] = grp(true)
      map(key) = Count(setItems)
      grpleft = grp(false)
    })
    map
  }

  def Min(exprs: ArrayBuffer[Int], groupByKey: ArrayBuffer[Int]): HashMap[Int, Int] = {
    var map = HashMap[Int, Int]()
    var grpleft: List[Int] = exprs.toList
    val distinct = groupByKey.toSet.toArray.sorted
    distinct.foreach((key: Int) => {
      val grp: Map[Boolean, List[Int]] = grpleft.groupBy(_ == key)
      val setItems: List[Int] = grp(true)
      map(key) = Min(setItems)
      grpleft = grp(false)
    })
    map
  }

  def Max(exprs: ArrayBuffer[Int], groupByKey: ArrayBuffer[Int]): HashMap[Int, Int] = {
    var map = HashMap[Int, Int]()
    var grpleft: List[Int] = exprs.toList
    val distinct = groupByKey.toSet.toArray.sorted
    distinct.foreach((key: Int) => {
      val grp: Map[Boolean, List[Int]] = grpleft.groupBy(_ == key)
      val setItems: List[Int] = grp(true)
      map(key) = Max(setItems)
      grpleft = grp(false)
    })
    map
  }

  /**
   * median (where mean of two middle values is taken for even number of elements
   *  Question: Should we coerce return type to Double?
   */

  def Median(exprs: ArrayBuffer[Int]): Int = {
    val mid = exprs.length
    if (mid % 2 == 0) {
      exprs(mid)
    } else {
      val midplus = mid + 1
      (exprs(mid) + exprs(midplus) / 2)
    }
  }

  def Median(exprs: ArrayBuffer[Long]): Long = {
    val mid = exprs.length
    if (mid % 2 == 0) {
      exprs(mid)
    } else {
      val midplus = mid + 1
      (exprs(mid) + exprs(midplus) / 2)
    }
  }

  def Median(exprs: ArrayBuffer[Double]): Double = {
    val mid = exprs.length
    if (mid % 2 == 0) {
      exprs(mid)
    } else {
      val midplus = mid + 1
      (exprs(mid) + exprs(midplus) / 2)
    }
  }

  def Median(exprs: ArrayBuffer[Float]): Float = {
    val mid = exprs.length
    if (mid % 2 == 0) {
      exprs(mid)
    } else {
      val midplus = mid + 1
      (exprs(mid) + exprs(midplus) / 2)
    }
  }

  def Median(exprs: Array[Int]): Int = {
    val mid = exprs.length
    if (mid % 2 == 0) {
      exprs(mid)
    } else {
      val midplus = mid + 1
      (exprs(mid) + exprs(midplus) / 2)
    }
  }

  def Median(exprs: Array[Long]): Long = {
    val mid = exprs.length
    if (mid % 2 == 0) {
      exprs(mid)
    } else {
      val midplus = mid + 1
      (exprs(mid) + exprs(midplus) / 2)
    }
  }

  def Median(exprs: Array[Double]): Double = {
    val mid = exprs.length
    if (mid % 2 == 0) {
      exprs(mid)
    } else {
      val midplus = mid + 1
      (exprs(mid) + exprs(midplus) / 2)
    }
  }

  def Median(exprs: Array[Float]): Float = {
    val mid = exprs.length
    if (mid % 2 == 0) {
      exprs(mid)
    } else {
      val midplus = mid + 1
      (exprs(mid) + exprs(midplus) / 2)
    }
  }

  /** product */

  def Product(exprs: ArrayBuffer[Int]): Int = {
    Multiply(exprs)
  }

  def Product(exprs: ArrayBuffer[Long]): Long = {
    Multiply(exprs)
  }

  def Product(exprs: ArrayBuffer[Double]): Double = {
    Multiply(exprs)
  }

  def Product(exprs: ArrayBuffer[Float]): Float = {
    Multiply(exprs)
  }

  def Product(exprs: Array[Int]): Int = {
    Multiply(exprs)
  }

  def Product(exprs: Array[Long]): Long = {
    Multiply(exprs)
  }

  def Product(exprs: Array[Double]): Double = {
    Multiply(exprs)
  }

  def Product(exprs: Array[Float]): Float = {
    Multiply(exprs)
  }

  /**  log10, ln, sqrt, abs, exp, pow, threshold, floor, ceil, round */

  /**
   * def log10(expr : Int) : Double = {
   * log10(expr)
   * }
   * def log10(expr : Long) : Double = {
   * log10(expr)
   * }
   * def log10(expr : Float) : Double = {
   * log10(expr)
   * }
   */
  def log10(expr: Double): Double = {
    log10(expr)
  }

  /**
   * def ln(expr : Int) : Double = {
   * log(expr)
   * }
   * def ln(expr : Long) : Double = {
   * log(expr)
   * }
   * def ln(expr : Float) : Double = {
   * log(expr)
   * }
   */
  def ln(expr: Double): Double = {
    log(expr)
  }

  /**
   * def sqrt(expr : Int) : Double = {
   * log(expr)
   * }
   * def sqrt(expr : Long) : Double = {
   * log(expr)
   * }
   * def sqrt(expr : Float) : Double = {
   * log(expr)
   * }
   */
  def sqrt(expr: Double): Double = {
    log(expr)
  }

  def abs(expr: Int): Int = {
    abs(expr)
  }
  def abs(expr: Long): Long = {
    abs(expr)
  }
  def abs(expr: Float): Float = {
    abs(expr)
  }
  def abs(expr: Double): Double = {
    abs(expr)
  }

  /**
   * def exp(expr : Int) : Double = {
   * exp(expr)
   * }
   * def exp(expr : Long) : Double = {
   * exp(expr)
   * }
   * def exp(expr : Float) : Double = {
   * exp(expr)
   * }
   */
  def exp(expr: Double): Double = {
    exp(expr)
  }

  /**
   * def pow(x : Int, y : Int) : Double = {
   * pow(x,y)
   * }
   * def pow(x : Long, y : Int) : Double = {
   * pow(x,y)
   * }
   * def pow(x : Float, y : Int) : Double = {
   * pow(x,y)
   * }
   */
  def pow(x: Double, y: Int): Double = {
    pow(x, y)
  }

  /** The function threshold(x,y) returns 1 if x>y and 0 otherwise */
  def threshold(x: Int, y: Int): Int = {
    if (GreaterThan(x, y)) 1 else 0
  }
  def threshold(x: Long, y: Long): Int = {
    if (GreaterThan(x, y)) 1 else 0
  }
  def threshold(x: Float, y: Float): Int = {
    if (GreaterThan(x, y)) 1 else 0
  }
  def threshold(x: Double, y: Double): Int = {
    if (GreaterThan(x, y)) 1 else 0
  }

  def floor(expr: Double): Double = {
    floor(expr)
  }

  def ceil(expr: Double): Double = {
    floor(expr)
  }

  def round(expr: Double): Double = {
    round(expr)
  }

  /** isMissing, isNotMissing */

  def uppercase(str: String): String = {
    str.toUpperCase()
  }

  def lowercase(str: String): String = {
    str.toLowerCase()
  }

  def substring(str: String, startidx: Int, len: Int): String = {
    str.substring(startidx, (startidx + len - 1))
  }

  def substring(str: String, startidx: Int): String = {
    str.substring(startidx)
  }

  def startsWith(inThis: String, findThis: String): Boolean = {
    inThis.startsWith(findThis)
  }

  def endsWith(inThis: String, findThis: String): Boolean = {
    inThis.endsWith(findThis)
  }

  def trimBlanks(str: String): String = {
    str.trim()
  }

  def dateDaysSinceYear(yr: Int): Int = {
    val dt: org.joda.time.LocalTime = new org.joda.time.LocalTime(yr, 1, 1)
    var now: org.joda.time.LocalTime = new org.joda.time.LocalTime()
    val days: org.joda.time.Days = org.joda.time.Days.daysBetween(dt, now)
    days.getDays
  }

  def dateSecondsSinceYear(yr: Int): Int = {
    val dt: org.joda.time.LocalTime = new org.joda.time.LocalTime(yr, 1, 1)
    var now: org.joda.time.LocalTime = new org.joda.time.LocalTime()
    val secs: org.joda.time.Seconds = org.joda.time.Seconds.secondsBetween(dt, now)
    secs.getSeconds
  }

  def dateSecondsSinceMidnight(): Int = {
    var now: org.joda.time.LocalTime = new org.joda.time.LocalTime()
    val secs: Int = now.getHourOfDay() * 60 * 60 + now.getMinuteOfHour() * 60
    secs
  }

  def dateMilliSecondsSinceMidnight(): Int = {
    dateSecondsSinceMidnight() * 1000
  }

  def Timenow(): Long = {
    dateSecondsSinceMidnight() * 1000
  }

  def AsSeconds(milliSecs: Long): Long = {
    milliSecs / 1000
  }

  def Now(): Long = {
    var now: org.joda.time.DateTime = new org.joda.time.DateTime()
    now.getMillis()
  }

  /**
   *  Answer the number of millisecs numYrs ago.
   */
  def YearsAgo(numYrs: Int): Long = {
    val rightNow: org.joda.time.DateTime = new org.joda.time.DateTime()
    val someTimeAgo = rightNow.minusYears(numYrs)
    someTimeAgo.getMillis()
  }

  /**
   *  Answer the number of millisecs a numYrs ago from the supplied ISO 8601 compressed int date
   *
   *  @param someDate an 8601 date compressed into integer (format OHHMMSSCC, WHERE CC REPRESENTS HUNDREDTHS OF A SECOND)
   *  @param numDays number of weeks to subtract
   *  
   *  @return millisecs for someDate - numYrs
   */
  def YearsAgo(someDate: Int, numYrs: Int): Long = {
    val dateAsMillisecs : Long = toDateTime(someDate).getMillis()
    val someDt : org.joda.time.DateTime = new org.joda.time.DateTime(dateAsMillisecs)
    val someTimeAgo = someDt.minusYears(numYrs)
    someTimeAgo.getMillis()
  }

  /**
   *  Answer an ISO8601 compressed integer for a numDays ago from the supplied ISO 8601 compressed int date 
   *
   *  @param someDate an 8601 date compressed into integer (format OHHMMSSCC, WHERE CC REPRESENTS HUNDREDTHS OF A SECOND)
   *  @param numDays number of years to subtract
   *  
   *  @return ISO8601 date for someDate - numYrs
   */
  def YearsAgoAsISO8601(someDate: Int, numYrs: Int): Int = {
    AsCompressedDate(YearsAgo(someDate, numYrs))
  }

  /**
   *  Answer the number of millisecs numMos ago.
   */
  def MonthsAgo(numMos: Int): Long = {
    val rightNow: org.joda.time.DateTime = new org.joda.time.DateTime()
    val someTimeAgo = rightNow.minusMonths(numMos)
    someTimeAgo.getMillis()
  }

  /**
   *  Answer the number of millisecs a numMos ago from the supplied ISO 8601 compressed int date
   *
   *  @param someDate an 8601 date compressed into integer (format OHHMMSSCC, WHERE CC REPRESENTS HUNDREDTHS OF A SECOND)
   *  @param numDays number of weeks to subtract
   *  
   *  @return millisecs for someDate - numMos
   */
  def MonthsAgo(someDate: Int, numMos: Int): Long = {
    val dateAsMillisecs : Long = toDateTime(someDate).getMillis()
    val someDt : org.joda.time.DateTime = new org.joda.time.DateTime(dateAsMillisecs)
    val someTimeAgo = someDt.minusMonths(numMos)
    someTimeAgo.getMillis()
  }

  /**
   *  Answer an ISO8601 compressed integer for a numDays ago from the supplied ISO 8601 compressed int date 
   *
   *  @param someDate an 8601 date compressed into integer (format OHHMMSSCC, WHERE CC REPRESENTS HUNDREDTHS OF A SECOND)
   *  @param numDays number of months to subtract
   *  
   *  @return ISO8601 date for someDate - numMos
   */
  def MonthsAgoAsISO8601(someDate: Int, numMos: Int): Int = {
    AsCompressedDate(MonthsAgo(someDate, numMos))
  }

  /**
   *  Answer the number of millisecs numWks ago.
   */
  def WeeksAgo(numWks: Int): Long = {
    val rightNow: org.joda.time.DateTime = new org.joda.time.DateTime()
    val someTimeAgo = rightNow.minusWeeks(numWks)
    someTimeAgo.getMillis()
  }

  /**
   *  Answer the number of millisecs a numWks ago from the supplied ISO 8601 compressed int date
   *
   *  @param someDate an 8601 date compressed into integer (format OHHMMSSCC, WHERE CC REPRESENTS HUNDREDTHS OF A SECOND)
   *  @param numDays number of weeks to subtract
   *  
   *  @return millisecs for someDate - numWks
   */
  def WeeksAgo(someDate: Int, numWks: Int): Long = {
    val dateAsMillisecs : Long = toDateTime(someDate).getMillis()
    val someDt : org.joda.time.DateTime = new org.joda.time.DateTime(dateAsMillisecs)
    val someTimeAgo = someDt.minusWeeks(numWks)
    someTimeAgo.getMillis()
  }

  /**
   *  Answer an ISO8601 compressed integer for a numDays ago from the supplied ISO 8601 compressed int date 
   *
   *  @param someDate an 8601 date compressed into integer (format OHHMMSSCC, WHERE CC REPRESENTS HUNDREDTHS OF A SECOND)
   *  @param numDays number of weeks to subtract
   *  
   *  @return ISO8601 date for someDate - numWks
   */
  def WeeksAgoAsISO8601(someDate: Int, numWks: Int): Int = {
    AsCompressedDate(WeeksAgo(someDate, numWks))
  }

   /**
   *  Answer the number of millisecs a numDays ago.
   */
  def DaysAgo(numDays: Int): Long = {
    val rightNow: org.joda.time.DateTime = new org.joda.time.DateTime()
    val someTimeAgo = rightNow.minusDays(numDays)
    someTimeAgo.getMillis()
  }

  /**
   *  Answer the number of millisecs a numDays ago from the supplied ISO 8601 compressed int date 
   *
   *  @param someDate an 8601 date compressed into integer (format OHHMMSSCC, WHERE CC REPRESENTS HUNDREDTHS OF A SECOND)
   *  @param numDays number of days to subtract
   *  
   *  @return millisecs for someDate - numDays
   */
  def DaysAgo(someDate: Int, numDays: Int): Long = {
    val dateAsMillisecs : Long = toDateTime(someDate).getMillis()
    val someDt : org.joda.time.DateTime = new org.joda.time.DateTime(dateAsMillisecs)
    val someTimeAgo = someDt.minusDays(numDays)
    someTimeAgo.getMillis()
  }

  /**
   *  Answer an ISO8601 compressed integer for a numDays ago from the supplied ISO 8601 compressed int date 
   *
   *  @param someDate an 8601 date compressed into integer (format OHHMMSSCC, WHERE CC REPRESENTS HUNDREDTHS OF A SECOND)
   *  @param numDays number of days to subtract
   *  
   *  @return ISO8601 int for someDate - numDays
   */
  def DaysAgoAsISO8601(someDate: Int, numDays: Int): Int = {
    AsCompressedDate(DaysAgo(someDate, numDays))
  }

  /** Coerce the yyyymmdd ISO8601 type compressed in integer to a DateTime
   *  
   *   @param yyyymmdd  ISO8601 type int compressed into integer
   *   @return joda DateTime 
   */
  def toDateTime(yyyymmdd: Int): DateTime = {
    val yyyy: Int = yyyymmdd / 10000
    val mm: Int = (yyyymmdd % 1000) / 100
    val day: Int = yyyymmdd % 100
    val someDate: DateTime = new DateTime(yyyy, mm, day, 0, 0)
    someDate
  }

  /** 
   *  Coerce the YYDDD Julian date to millisecs.  21st century
   *  dates are assumed.  If a bad Julian value is supplied, an error
   *  message is logged and the epoch (i.e., 0) is returned.
   *  
   *   @param yyddd  21st century julian date integer
   *   @return Long value of millisecs from epoch.
   */
  def toMillisFromJulian(yyddd: Int): Long = {
    val ydStr : String = yyddd.toString
    val yydddStr : String = if (ydStr.size == 4) ("0" + ydStr) else ydStr
    val reasonable : Boolean = if (yydddStr.length == 5) {
    	val yy : Int = yydddStr.slice(0, 2).toInt
    	val ddd : Int = yydddStr.slice(2,5).toInt
    	(yy >= 1 && yy <= 99 && ddd >= 1 && ddd <= 366)	
    } else {
    	false
    }
    val millis : Long = if (! reasonable) {
    	logger.error(s"toMillisFromJulian(yyddd = $yydddStr) ... malformed Julian date... expect YYDDD where YY>0 && YY <= 99 && DDD>0 && DDD<366")
    	0
    } else { 
	    val formatter : DateTimeFormatter  = DateTimeFormat.forPattern("yyyyDDD").withChronology(JulianChronology.getInstance)
	    val lcd : DateTime = formatter.parseDateTime("20" + yydddStr);
	    lcd.getMillis()
    }
    millis
  }

  /**
   *  Convert time formatted in integer (compressed decimal)
   *  to millisecs.
   *
   *      Format: OHHMMSSCC, WHERE CC REPRESENTS HUNDREDTHS OF A SECOND
   *
   *  @param time, an Int
   *  @return time, an Int
   */
  def CompressedTimeHHMMSSCC2MilliSecs(compressedTime: Int): Long = {
    val hours = (compressedTime / 1000000) % 100
    val minutes = (compressedTime / 10000) % 100
    val seconds = (compressedTime / 100) % 100
    val millisecs = (compressedTime % 100) * 10

    val millis = (hours * 60 * 60 + minutes * 60 + seconds) * 1000 + millisecs

    millis
  }

  /**
   *  Convert time formatted in integer (compressed decimal)
   *  to seconds.
   *
   *      Format: OHHMMSSCC, WHERE CC REPRESENTS HUNDREDTHS OF A SECOND
   *
   *  @param time, an Int
   *  @return time, an Int
   */
  def CompressedTimeHHMMSSCC2Secs(compressedTime: Int): Int = {
    val hours = (compressedTime / 1000000) % 100
    val minutes = (compressedTime / 10000) % 100
    val seconds = (compressedTime / 100) % 100
    val millisecs = (compressedTime % 100) * 10

    val evtseconds = hours * 60 * 60 + minutes * 60 + seconds + (if (millisecs >= 500) 1 else 0)

    evtseconds
  }

  /** Convert millisecs to ISO8601 style date in integer */

  def AsCompressedDate(milliSecs: Long): Int = {
    val dt: LocalDate = new LocalDate(milliSecs)
    val dtAtMidnight: DateTime = dt.toDateTimeAtStartOfDay
    val yr: Int = dtAtMidnight.year().get()
    val mo: Int = dtAtMidnight.monthOfYear().get()
    val day: Int = dtAtMidnight.dayOfMonth().get()
    val compressedDate: Int = yr * 10000 + mo * 100 + day

    compressedDate
  }

  /** Extract the Month from the IS08601 compressed date */
  def MonthFromISO8601Int(dt: Int): Int = {
    val mm: Int = (dt % 1000) / 100
    mm
  }

  /** Extract the Year from the IS08601 compressed date */
  def YearFromISO8601Int(dt: Int): Int = {
    val yyyy: Int = dt / 10000
    yyyy
  }

  /** Extract the Day of the Month from the IS08601 compressed date */
  def DayOfMonthFromISO8601Int(dt: Int): Int = {
    val day: Int = dt % 100
    day
  }

  /** Calculate age from yyyymmdd ISO8601 type compressed in integer */
  def AgeCalc(yyyymmdd: Int): Int = {
    val yyyy: Int = yyyymmdd / 10000
    val mm: Int = (yyyymmdd % 1000) / 100
    val day: Int = yyyymmdd % 100
    val birthDate: LocalDate = new LocalDate(yyyy, mm, day)
    val age: Int = Years.yearsBetween(birthDate, new LocalDate).getYears
    age
  }

  def MakePairs(left: String, right: Array[String]): Array[(String, String)] = {
    if (left == null || right == null || right.size == 0 || left.length == 0)
      return new Array[(String, String)](0)
    right.filter(v => v != null && v.length > 0).map(v => (left, v))
  }

  def MakeOrderedPairs(left: String, right: Array[String]): Array[(String, String)] = {
    if (left == null || right == null || right.size == 0 || left.length == 0)
      return new Array[(String, String)](0)
    right.filter(v => v != null && v.length > 0).map(v => {
      if (left.compareTo(v) > 0)
        (v, left)
      else
        (left, v)
    })
  }

  def MakeOrderedPairs(left: String, right: ArrayBuffer[String]): Array[(String, String)] = {
    if (left == null || right == null || right.size == 0 || left.length == 0)
      return new Array[(String, String)](0)
    right.filter(v => v != null && v.length > 0).map(v => {
      if (left.compareTo(v) > 0)
        (v, left)
      else
        (left, v)
    }).toArray
  }

  def MakeStrings(arr: Array[(String, String)], separator: String): Array[String] = {
    if (arr == null || arr.size == 0)
      return new Array[String](0)
    arr.filter(v => v != null).map(v => ("(" + v._1 + separator + v._2 + ")"))
  }

  def ToSet[T: ClassTag](arr: Array[T]): Set[T] = {
    if (arr == null || arr.size == 0)
      return Set[T]().toSet
    arr.toSet
  }

  def ToSet[T: ClassTag](arr: ArrayBuffer[T]): Set[T] = {
    if (arr == null || arr.size == 0)
      return Set[T]().toSet
    arr.toSet
  }

  def ToSet(arr: ArrayBuffer[Any]): Set[Any] = {
    if (arr == null || arr.size == 0)
      return Array[Any]().toSet
    arr.toSet
  }

  def ToSet(arr: Array[Any]): Set[Any] = {
    if (arr == null || arr.size == 0)
      return Array[Any]().toSet
    arr.toSet
  }

  def ToSet(q: Queue[Any]): Set[Any] = {
    if (q == null || q.size == 0)
      return Queue[Any]().toSet
    q.toSet
  }

  def ToSet(l: List[Any]): Set[Any] = {
    if (l == null || l.size == 0)
      return Queue[Any]().toSet
    l.toSet
  }

  def ToArray[T: ClassTag](set: MutableSet[T]): Array[T] = {
    if (set == null || set.size == 0)
      return Array[T]()
    set.toArray
  }

  def ToArray[T: ClassTag](set: Set[T]): Array[T] = {
    if (set == null || set.size == 0)
      return Array[T]()
    set.toArray
  }

  def ToArray(set: MutableSet[Any]): Array[Any] = {
    if (set == null || set.size == 0)
      return Array[Any]()
    set.toArray
  }

  def ToArray(set: Set[Any]): Array[Any] = {
    if (set == null || set.size == 0)
      return Array[Any]()
    set.toArray
  }

  def ToArray[T: ClassTag](arr: ArrayBuffer[T]): Array[T] = {
    if (arr == null || arr.size == 0)
      return Array[T]()
    arr.toArray
  }

  def ToArray(arr: ArrayBuffer[Any]): Array[Any] = {
    if (arr == null || arr.size == 0)
      return Array[Any]()
    arr.toArray
  }

  def ToArray[T: ClassTag](arr: Array[T]): Array[T] = {
    if (arr == null || arr.size == 0)
      return Array[T]()
    arr.toArray
  }

  def ToArray(arr: Array[Any]): Array[Any] = {
    if (arr == null || arr.size == 0)
      return Array[Any]()
    arr.toArray
  }

  def ToArray[T: ClassTag](set: SortedSet[T]): Array[T] = {
    if (set == null || set.size == 0)
      return Array[T]()
    set.toArray
  }

  def ToArray(set: SortedSet[Any]): Array[Any] = {
    if (set == null || set.size == 0)
      return Array[Any]()
    set.toArray
  }

  def ToArray[T: ClassTag](ts: TreeSet[T]): Array[T] = {
    if (ts == null || ts.size == 0)
      return Array[T]()
    ts.toArray
  }

  def ToArray(ts: TreeSet[Any]): Array[Any] = {
    if (ts == null || ts.size == 0)
      return Array[Any]()
    ts.toArray
  }

  def ToArray[T: ClassTag](l: List[T]): Array[T] = {
    if (l == null || l.size == 0)
      return Array[T]()
    l.toArray
  }

  def ToArray(l: List[Any]): Array[Any] = {
    if (l == null || l.size == 0)
      return Array[Any]()
    l.toArray
  }

  def ToArray[T: ClassTag](q: Queue[T]): Array[T] = {
    if (q == null || q.size == 0)
      return Array[T]()
    q.toArray
  }

  def ToArray(q: Queue[Any]): Array[Any] = {
    if (q == null || q.size == 0)
      return Array[Any]()
    q.toArray
  }

  /**
   *
   * Suppress stack to array coercion until Stack based types are supported in the MdMgr ....
   *
   * def ToArray[T : ClassTag](stack: Stack[T]): Array[T] = {
   * if (stack == null || stack.size == 0)
   * return Array[T]()
   * stack.toArray
   * }
   *
   * def ToArray(stack: Stack[Any]): Array[Any] = {
   * if (stack == null || stack.size == 0)
   * return Array[Any]()
   * stack.toArray
   * }
   */

  /** ToArrayOf<Scalar> for Tuple1 */
  def ToArray(tuple: Tuple1[Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  /** ToArrayOf<Scalar> for Tuple2 */
  def ToArray(tuple: Tuple2[Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  /** if the tuple doesn't contain appropriate numeric values, a 0 is returned at that position */
  def ToArrayOfFloat(tuple: Tuple2[Any, Any]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => if (itm.isInstanceOf[Float] || itm.isInstanceOf[Int] || itm.isInstanceOf[Long]) itm.asInstanceOf[Float] else 0)
    fArray
  }

  /** if the tuple doesn't contain appropriate numeric values, a 0 is returned at that position */
  def ToArrayOfDouble(tuple: Tuple2[Any, Any]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => if (itm.isInstanceOf[Double] || itm.isInstanceOf[Float] || itm.isInstanceOf[Int] || itm.isInstanceOf[Long]) itm.asInstanceOf[Double] else 0)
    dArray
  }

  /** if the tuple doesn't contain appropriate numeric values, a 0 is returned at that position */
  def ToArrayOfInt(tuple: Tuple2[Any, Any]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
     val iArray: Array[Int] = arr.map(itm => if (itm.isInstanceOf[Int]) itm.asInstanceOf[Int] else 0)
    iArray
  }

  /** ToArrayOf<Scalar> for Tuple3 */
  def ToArray(tuple: Tuple3[Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  /** if the tuple doesn't contain appropriate numeric values, a 0 is returned at that position */
  def ToArrayOfFloat(tuple: Tuple3[Any, Any, Any]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => if (itm.isInstanceOf[Float] || itm.isInstanceOf[Int] || itm.isInstanceOf[Long]) itm.asInstanceOf[Float] else 0)
    fArray
  }

  /** if the tuple doesn't contain appropriate numeric values, a 0 is returned at that position */
  def ToArrayOfDouble(tuple: Tuple3[Any, Any, Any]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => if (itm.isInstanceOf[Double] || itm.isInstanceOf[Float] || itm.isInstanceOf[Int] || itm.isInstanceOf[Long]) itm.asInstanceOf[Double] else 0)
    dArray
  }

  /** if the tuple doesn't contain appropriate numeric values, a 0 is returned at that position */
  def ToArrayOfInt(tuple: Tuple3[Any, Any, Any]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => if (itm.isInstanceOf[Int]) itm.asInstanceOf[Int] else 0)
    iArray
  }

  /** ToArrayOf<Scalar> for Tuple4 */
  def ToArray(tuple: Tuple4[Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  /** if the tuple doesn't contain appropriate numeric values, a 0 is returned at that position */
  def ToArrayOfFloat(tuple: Tuple4[Any, Any, Any, Any]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => if (itm.isInstanceOf[Float] || itm.isInstanceOf[Int] || itm.isInstanceOf[Long]) itm.asInstanceOf[Float] else 0)
    fArray
  }

  /** if the tuple doesn't contain appropriate numeric values, a 0 is returned at that position */
  def ToArrayOfDouble(tuple: Tuple4[Any, Any, Any, Any]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => if (itm.isInstanceOf[Double] || itm.isInstanceOf[Float] || itm.isInstanceOf[Int] || itm.isInstanceOf[Long]) itm.asInstanceOf[Double] else 0)
    dArray
  }

  /** if the tuple doesn't contain appropriate numeric values, a 0 is returned at that position */
  def ToArrayOfInt(tuple: Tuple4[Any, Any, Any, Any]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => if (itm.isInstanceOf[Int]) itm.asInstanceOf[Int] else 0)
    iArray
  }

  /** ToArrayOf<Scalar> for Tuple5 */
  def ToArray(tuple: Tuple5[Any, Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  /** if the tuple doesn't contain appropriate numeric values, a 0 is returned at that position */
  def ToArrayOfFloat(tuple: Tuple5[Any, Any, Any, Any, Any]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => if (itm.isInstanceOf[Float] || itm.isInstanceOf[Int] || itm.isInstanceOf[Long]) itm.asInstanceOf[Float] else 0)
    fArray
  }

  /** if the tuple doesn't contain appropriate numeric values, a 0 is returned at that position */
  def ToArrayOfDouble(tuple: Tuple5[Any, Any, Any, Any, Any]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => if (itm.isInstanceOf[Double] || itm.isInstanceOf[Float] || itm.isInstanceOf[Int] || itm.isInstanceOf[Long]) itm.asInstanceOf[Double] else 0)
    dArray
  }

  /** if the tuple doesn't contain appropriate numeric values, a 0 is returned at that position */
  def ToArrayOfInt(tuple: Tuple5[Any, Any, Any, Any, Any]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => if (itm.isInstanceOf[Int]) itm.asInstanceOf[Int] else 0)
    iArray
  }

  /** ToArrayOf<Scalar> for Tuple6 */
  def ToArray(tuple: Tuple6[Any, Any, Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  /** if the tuple doesn't contain appropriate numeric values, a 0 is returned at that position */
  def ToArrayOfFloat(tuple: Tuple6[Any, Any, Any, Any, Any, Any]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => if (itm.isInstanceOf[Float] || itm.isInstanceOf[Int] || itm.isInstanceOf[Long]) itm.asInstanceOf[Float] else 0)
    fArray
  }

  /** if the tuple doesn't contain appropriate numeric values, a 0 is returned at that position */
  def ToArrayOfDouble(tuple: Tuple6[Any, Any, Any, Any, Any, Any]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => if (itm.isInstanceOf[Double] || itm.isInstanceOf[Float] || itm.isInstanceOf[Int] || itm.isInstanceOf[Long]) itm.asInstanceOf[Double] else 0)
    dArray
  }

  /** if the tuple doesn't contain appropriate numeric values, a 0 is returned at that position */
  def ToArrayOfInt(tuple: Tuple6[Any, Any, Any, Any, Any, Any]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => if (itm.isInstanceOf[Int]) itm.asInstanceOf[Int] else 0)
    iArray
  }

  /** FIXME: Do ToArrayOf<Scalar> for the remaining tuples */
  def ToArray(tuple: Tuple7[Any, Any, Any, Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  def ToArray(tuple: Tuple8[Any, Any, Any, Any, Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  def ToArray(tuple: Tuple9[Any, Any, Any, Any, Any, Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  def ToArray(tuple: Tuple10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  def ToArray(tuple: Tuple11[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  def ToArray(tuple: Tuple12[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  def ToArray(tuple: Tuple13[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  def ToArray(tuple: Tuple14[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  def ToArray(tuple: Tuple15[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  def ToArray(tuple: Tuple16[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  def ToArray(tuple: Tuple17[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  def ToArray(tuple: Tuple18[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  def ToArray(tuple: Tuple19[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  def ToArray(tuple: Tuple20[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  def ToArray(tuple: Tuple21[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  def ToArray(tuple: Tuple22[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]): Array[Any] = {
    tuple.productIterator.toArray
  }

  def ToArray(tuple: Tuple1[Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple2[Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple3[Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple4[Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple5[Int, Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple6[Int, Int, Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple7[Int, Int, Int, Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple8[Int, Int, Int, Int, Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple9[Int, Int, Int, Int, Int, Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple10[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple11[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple12[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple13[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple14[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple15[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple16[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple17[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple18[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple19[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple20[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple21[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple22[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]): Array[Int] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val iArray: Array[Int] = arr.map(itm => itm.asInstanceOf[Int])
    iArray
  }

  def ToArray(tuple: Tuple1[Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple2[Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple3[Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple4[Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple5[Float, Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple6[Float, Float, Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple7[Float, Float, Float, Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple8[Float, Float, Float, Float, Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple9[Float, Float, Float, Float, Float, Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple10[Float, Float, Float, Float, Float, Float, Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple11[Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple12[Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple13[Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple14[Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple15[Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple16[Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple17[Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple18[Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple19[Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple20[Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple21[Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple22[Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float]): Array[Float] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val fArray: Array[Float] = arr.map(itm => itm.asInstanceOf[Float])
    fArray
  }

  def ToArray(tuple: Tuple1[Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple2[Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple3[Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple4[Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple5[Double, Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple6[Double, Double, Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple7[Double, Double, Double, Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple8[Double, Double, Double, Double, Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple9[Double, Double, Double, Double, Double, Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple10[Double, Double, Double, Double, Double, Double, Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple11[Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple12[Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple13[Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple14[Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple15[Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple16[Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple17[Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple18[Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple19[Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple20[Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple21[Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToArray(tuple: Tuple22[Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double]): Array[Double] = {
    val arr: Array[Any] = tuple.productIterator.toArray
    val dArray: Array[Double] = arr.map(itm => itm.asInstanceOf[Double])
    dArray
  }

  def ToMap[T: ClassTag, U: ClassTag](set: MutableSet[(T, U)]): Map[T, U] = {
    if (set == null || set.size == 0)
      return Map[T, U]()
    set.toMap
  }

  def ToMap[T: ClassTag, U: ClassTag](set: Set[(T, U)]): Map[T, U] = {
    if (set == null || set.size == 0)
      return Map[T, U]()
    set.toMap
  }

  def ToMap(set: MutableSet[(Any, Any)]): Map[Any, Any] = {
    if (set == null || set.size == 0)
      return Map[Any, Any]()
    set.toMap
  }

  def ToMap(set: Set[(Any, Any)]): Map[Any, Any] = {
    if (set == null || set.size == 0)
      return Map[Any, Any]()
    set.toMap
  }

  def ToMap[T: ClassTag, U: ClassTag](arr: ArrayBuffer[(T, U)]): Map[T, U] = {
    if (arr == null || arr.size == 0)
      return Map[T, U]()
    arr.toMap
  }

  def ToMap(arr: ArrayBuffer[(Any, Any)]): Map[Any, Any] = {
    if (arr == null || arr.size == 0)
      return Map[Any, Any]()
    arr.toMap
  }

  def ToMap[T: ClassTag, U: ClassTag](arr: Array[(T, U)]): Map[T, U] = {
    if (arr == null || arr.size == 0)
      return Map[T, U]()
    arr.toMap
  }

  def ToMap(arr: Array[(Any, Any)]): Map[Any, Any] = {
    if (arr == null || arr.size == 0)
      return Map[Any, Any]()
    arr.toMap
  }

  def ToMap[T: ClassTag, U: ClassTag](set: SortedSet[(T, U)]): Map[T, U] = {
    if (set == null || set.size == 0)
      return Map[T, U]()
    set.toMap
  }

  def ToMap(set: SortedSet[(Any, Any)]): Map[Any, Any] = {
    if (set == null || set.size == 0)
      return Map[Any, Any]()
    set.toMap
  }

  def ToMap[T: ClassTag, U: ClassTag](ts: TreeSet[(T, U)]): Map[T, U] = {
    if (ts == null || ts.size == 0)
      return Map[T, U]()
    ts.toMap
  }

  def ToMap(ts: TreeSet[(Any, Any)]): Map[Any, Any] = {
    if (ts == null || ts.size == 0)
      return Map[Any, Any]()
    ts.toMap
  }

  def ToMap[T: ClassTag, U: ClassTag](l: List[(T, U)]): Map[T, U] = {
    if (l == null || l.size == 0)
      return Map[T, U]()
    l.toMap
  }

  def ToMap(l: List[(Any, Any)]): Map[Any, Any] = {
    if (l == null || l.size == 0)
      return Map[Any, Any]()
    l.toMap
  }

  def ToMap[T: ClassTag, U: ClassTag](q: Queue[(T, U)]): Map[T, U] = {
    if (q == null || q.size == 0)
      return Map[T, U]()
    q.toMap
  }

  def ToMap(q: Queue[(Any, Any)]): Map[Any, Any] = {
    if (q == null || q.size == 0)
      return Map[Any, Any]()
    q.toMap
  }

  /**
   * Suppress Stack type functions until MdMgr supports them properly
   * def ToMap[T : ClassTag, U : ClassTag](stack: Stack[(T,U)]): Map[T,U] = {
   * if (stack == null || stack.size == 0)
   * return Map[T,U]()
   * stack.toMap
   * }
   *
   * def ToMap(stack: Stack[(Any,Any)]): Map[Any,Any] = {
   * if (stack == null || stack.size == 0)
   * return Map[Any,Any]()
   * stack.toMap
   * }
   */

  def Zip[T: ClassTag, U: ClassTag](receiver: Array[T], other: Array[U]): Array[(T, U)] = {
    if (receiver == null || receiver.size == 0) {
      return Array[(T, U)]()
    }
    receiver.zip(other)
  }

  def Zip[T: ClassTag, U: ClassTag](receiver: ArrayBuffer[T], other: ArrayBuffer[U]): ArrayBuffer[(T, U)] = {
    if (receiver == null || receiver.size == 0) {
      return ArrayBuffer[(T, U)]()
    }
    receiver.zip(other)
  }

  def Zip[T: ClassTag, U: ClassTag](receiver: List[T], other: List[U]): List[(T, U)] = {
    if (receiver == null || receiver.size == 0) {
      return List[(T, U)]()
    }
    receiver.zip(other)
  }

  def Zip[T: ClassTag, U: ClassTag](receiver: Queue[T], other: Queue[U]): Queue[(T, U)] = {
    if (receiver == null || receiver.size == 0) {
      return Queue[(T, U)]()
    }
    receiver.zip(other)
  }

  /**
   * 	NOTE: Zipping mutable.Set and/or immutable.Set is typically not a good idea unless the pairing
   *  	done absolutely does not matter.  Use SortedSet or TreeSet for predictable pairings.
   */
  def Zip[T: ClassTag, U: ClassTag](receiver: Set[T], other: Set[U]): Set[(T, U)] = {
    if (receiver == null || receiver.size == 0) {
      return Set[(T, U)]()
    }
    receiver.zip(other)
  }

  def Zip[T: ClassTag, U: ClassTag](receiver: MutableSet[T], other: MutableSet[U]): MutableSet[(T, U)] = {
    if (receiver == null || receiver.size == 0) {
      return MutableSet[(T, U)]()
    }
    receiver.zip(other)
  }

  def Zip[T: ClassTag, U: ClassTag](receiver: SortedSet[T], other: SortedSet[U])(implicit cmp: Ordering[(T, U)]): SortedSet[(T, U)] = {
    if (receiver == null || receiver.size == 0) {
      return SortedSet[(T, U)]()
    }
    receiver.zip(other)
  }

  def Zip[T: ClassTag, U: ClassTag](receiver: TreeSet[T], other: TreeSet[U])(implicit cmp: Ordering[(T, U)]): TreeSet[(T, U)] = {
    if (receiver == null || receiver.size == 0) {
      return TreeSet[(T, U)]()
    }
    receiver.zip(other)
  }

  /** MapKeys = collect.keys*/

  def MapKeys[T: ClassTag, U: ClassTag](receiver: MutableMap[T, U]): Array[T] = {
    receiver.keys.toArray
  }

  def MapKeys[T: ClassTag, U: ClassTag](receiver: Map[T, U]): Array[T] = {
    receiver.keys.toArray
  } 

  def MapKeys(receiver: MutableMap[Any, Any]): Array[Any] = {
    receiver.keys.toArray
  }

  def MapKeys(receiver: Map[Any, Any]): Array[Any] = {
    receiver.keys.toArray
  }

  /** MapValues = collect.values*/

  def MapValues[T: ClassTag, U: ClassTag](receiver: MutableMap[T, U]): Array[U] = {
    receiver.values.toArray
  }

  def MapValues[T: ClassTag, U: ClassTag](receiver: Map[T, U]): Array[U] = {
    receiver.values.toArray
  }  

  def MapValues(receiver: MutableMap[Any, Any]): Array[Any] = {
    receiver.values.toArray
  }

  def MapValues(receiver: Map[Any, Any]): Array[Any] = {
    receiver.values.toArray
  }

  /** CollectionLength */

  def CollectionLength[T: ClassTag](coll: Array[T]): Int = {
    coll.length
  }

  def CollectionLength[T: ClassTag](coll: ArrayBuffer[T]): Int = {
    coll.size
  }

  def CollectionLength[T: ClassTag](coll: MutableSet[T]): Int = {
    coll.size
  }

  def CollectionLength[T: ClassTag](coll: Set[T]): Int = {
    coll.size
  }

  def CollectionLength[T: ClassTag](coll: TreeSet[T]): Int = {
    coll.size
  }

  def CollectionLength[T: ClassTag](coll: SortedSet[T]): Int = {
    coll.size
  }

  def CollectionLength[T: ClassTag](coll: List[T]): Int = {
    coll.size
  }

  def CollectionLength[T: ClassTag](coll: Queue[T]): Int = {
    coll.size
  }

  /**
   * Suppress functions that use Stack  and Vector until Mdmgr supports it
   * def CollectionLength[T : ClassTag](coll : Stack[T]) : Int = {
   * coll.size
   * }
   *
   * def CollectionLength[T : ClassTag](coll : Vector[T]) : Int = {
   * coll.size
   * }
   *
   */

  def CollectionLength[K: ClassTag, V: ClassTag](coll: MutableMap[K, V]): Int = {
    coll.size
  }

  def CollectionLength[K: ClassTag, V: ClassTag](coll: Map[K, V]): Int = {
    coll.size
  }

  def CollectionLength[K: ClassTag, V: ClassTag](coll: HashMap[K, V]): Int = {
    coll.size
  }
}
