package com.ligadata.pmml.udfs

import scala.reflect.ClassTag
import scala.collection.GenSeq
import scala.collection.mutable._
import scala.math._
import scala.collection.immutable.StringLike
import scala.collection.immutable.List
import scala.collection.immutable.Map
import scala.collection.immutable.Set
import scala.collection.mutable.{Set => MutableSet}
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
import org.joda.time.Years

import com.ligadata.Pmml.Runtime._
import com.ligadata.OnLEPBase._

/**
	These are the udfs supplied with the system.  
 */

object Udfs extends com.ligadata.pmml.udfs.UdfBase {

  /** runtime state write functions NOTE: macros use these functions ... the ctx is not directly
   *  supported in the Pmml */
  
  def Put(ctx : Context, variableName : String, value : String) : Boolean = {
	  var set : Boolean = (ctx != null)
	  if (set) {
		  set = ctx.valuePut(variableName, new StringDataValue(value))
	  }
	  set
  }
    	
  def Put(ctx : Context, variableName : String, value : Int) : Boolean = {
	  var set : Boolean = (ctx != null)
	  if (set) {
		  set = ctx.valuePut(variableName, new IntDataValue(value))
	  }
	  set 
  }

  def Put(ctx : Context, variableName : String, value : Long) : Boolean = {
	  var set : Boolean = (ctx != null)
	  if (set) {
		  set = ctx.valuePut(variableName, new LongDataValue(value))
	  }
	  set 
  }

  def Put(ctx : Context, variableName : String, value : Double) : Boolean = {
	  var set : Boolean = (ctx != null)
	  if (set) {
		  set = ctx.valuePut(variableName, new DoubleDataValue(value))
	  }
	  set 
  }

  def Put(ctx : Context, variableName : String, value : Any) : Boolean = {
	  var set : Boolean = (ctx != null)
	  if (set) {
		  set = ctx.valuePut(variableName, new AnyDataValue(value))
	  }
	  set 
  }

  def Put(ctx : Context, variableName : String, value : Boolean) : Boolean = {
	  var set : Boolean = (ctx != null)
	  if (set) {
		  set = ctx.valuePut(variableName, new BooleanDataValue(value))
	  }
	  set 
  }

  def Put(ctx : Context, variableName : String, value : Float) : Boolean = {
	  var set : Boolean = (ctx != null)
	  if (set) {
		  set = ctx.valuePut(variableName, new FloatDataValue(value))
	  }
	  set 
  }

  /** runtime state increment function */
  
  def incrementBy(ctx : Context, variableName : String, value : Int) : Boolean = {
	  var set : Boolean = (ctx != null)
	  if (set) {
		  set = ctx.valueIncr(variableName, value)
	  }
	  set 
  }
  
  /** 
	  EnvContext Get functions  
	  FIXME:  Perhaps we should support the various flavor of keys?
   */
    
  def Get(gCtx : EnvContext, containerId : String, key : String) : BaseContainer = {
	  gCtx.getObject(containerId, key.toString) 
  }
  
  def Get(gCtx : EnvContext, containerId : String, key : Int) : BaseContainer = {
	  gCtx.getObject(containerId, key.toString) 
  }
  
  def Get(gCtx : EnvContext, containerId : String, key : Long) : BaseContainer = {
	  gCtx.getObject(containerId, key.toString) 
  }
  
  def Get(gCtx : EnvContext, containerId : String, key : Double) : BaseContainer = {
	  gCtx.getObject(containerId, key.toString) 
  }
  
  def Get(gCtx : EnvContext, containerId : String, key : Float) : BaseContainer = {
	  gCtx.getObject(containerId, key.toString) 
  }
  
  /** 
	  EnvContext GetArray functions  
	  FIXME:  Perhaps we should support the various flavor of keys?
   */
    
  def GetArray(gCtx : EnvContext, containerId : String, key : String) : Array[BaseContainer] = {
	  gCtx.getObjects(containerId, key.toString) 
  }
  
  def GetArray(gCtx : EnvContext, containerId : String, key : Int) : Array[BaseContainer] = {
	  gCtx.getObjects(containerId, key.toString) 
  }
  
  def GetArray(gCtx : EnvContext, containerId : String, key : Long) : Array[BaseContainer] = {
	  gCtx.getObjects(containerId, key.toString) 
  }
  
  def GetArray(gCtx : EnvContext, containerId : String, key : Double) : Array[BaseContainer] = {
	  gCtx.getObjects(containerId, key.toString) 
  }
   
  def GetArray(gCtx : EnvContext, containerId : String, key : Float) : Array[BaseContainer] = {
	  gCtx.getObjects(containerId, key.toString) 
  }
 
  /** 
	  EnvContext Put functions  
	  FIXME:  Perhaps we should support the various flavor of keys?
   */
    
  def Put(gCtx : EnvContext, containerId : String, key : String, value : BaseContainer) : Boolean = {
	  gCtx.setObject(containerId, key.toString, value) 
	  true
  }
  
  def Put(gCtx : EnvContext, containerId : String, key : Int, value : BaseContainer) : Boolean = {
	  gCtx.setObject(containerId, key.toString, value) 
	  true
  }
  
  def Put(gCtx : EnvContext, containerId : String, key : Long, value : BaseContainer) : Boolean = {
	  gCtx.setObject(containerId, key.toString, value) 
 	  true
 }
  
  def Put(gCtx : EnvContext, containerId : String, key : Double, value : BaseContainer) : Boolean = {
	  gCtx.setObject(containerId, key.toString, value) 
 	  true
 }
  
  def Put(gCtx : EnvContext, containerId : String, key : Float, value : BaseContainer) : Boolean = {
	  gCtx.setObject(containerId, key.toString, value) 
	  true
  }
  
  /** comparisons */

  def If(boolexpr: Boolean): Boolean = {
    boolexpr
  }

  def If(boolexpr: Boolean, boolexpr1: Boolean): Boolean = {
    And(boolexpr, boolexpr1)
  }

  def If(boolexpr: Boolean, boolexpr1: Boolean, boolexpr2: Boolean): Boolean = {
    And(boolexpr, boolexpr1, boolexpr2)
  }

  def If(boolexpr: Boolean, boolexpr1: Boolean, boolexpr2: Boolean, boolexpr3: Boolean): Boolean = {
    And(boolexpr, boolexpr1, boolexpr2, boolexpr3)
  }

  def If(boolexprs: ArrayBuffer[Boolean]): Boolean = {
    boolexprs.reduceLeft(_ && _)
  }

  def And(boolexpr: Boolean, boolexpr1: Boolean): Boolean = {
    (boolexpr && boolexpr1)
  }

  def And(boolexpr: Boolean, boolexpr1: Boolean, boolexpr2: Boolean): Boolean = {
    (boolexpr && boolexpr1 && boolexpr2)
  }

  def And(boolexpr: Boolean, boolexpr1: Boolean, boolexpr2: Boolean, boolexpr3: Boolean): Boolean = {
    (boolexpr && boolexpr1 && boolexpr2 && boolexpr3)
  }

  def And(boolexpr: Boolean, boolexpr1: Boolean, boolexpr2: Boolean, boolexpr3: Boolean, boolexpr4: Boolean): Boolean = {
    (boolexpr && boolexpr1 && boolexpr2 && boolexpr3 && boolexpr4)
  }

  def And(boolexpr: Boolean, boolexpr1: Boolean, boolexpr2: Boolean, boolexpr3: Boolean, boolexpr4: Boolean, boolexpr5: Boolean): Boolean = {
    (boolexpr && boolexpr1 && boolexpr2 && boolexpr3 && boolexpr4 && boolexpr5)
  }

  def And(boolexpr: Boolean, boolexpr1: Boolean, boolexpr2: Boolean, boolexpr3: Boolean, boolexpr4: Boolean, boolexpr5: Boolean, boolexpr6: Boolean): Boolean = {
    (boolexpr && boolexpr1 && boolexpr2 && boolexpr3 && boolexpr4 && boolexpr5 && boolexpr6)
  }

  def Or(boolexpr: Boolean, boolexpr1: Boolean): Boolean = {
    (boolexpr || boolexpr1)
  }

  def Or(boolexpr: Boolean, boolexpr1: Boolean, boolexpr2: Boolean): Boolean = {
    (boolexpr || boolexpr1 || boolexpr2)
  }

  def Or(boolexpr: Boolean, boolexpr1: Boolean, boolexpr2: Boolean, boolexpr3: Boolean): Boolean = {
    (boolexpr || boolexpr1 || boolexpr2 || boolexpr3)
  }

  def Or(boolexpr: Boolean, boolexpr1: Boolean, boolexpr2: Boolean, boolexpr3: Boolean, boolexpr4: Boolean): Boolean = {
    (boolexpr || boolexpr1 || boolexpr2 || boolexpr3 || boolexpr4)
  }

  def Or(boolexpr: Boolean, boolexpr1: Boolean, boolexpr2: Boolean, boolexpr3: Boolean, boolexpr4: Boolean, boolexpr5: Boolean): Boolean = {
    (boolexpr || boolexpr1 || boolexpr2 || boolexpr3 || boolexpr4 || boolexpr5)
  }

  def Or(boolexpr: Boolean, boolexpr1: Boolean, boolexpr2: Boolean, boolexpr3: Boolean, boolexpr4: Boolean, boolexpr5: Boolean, boolexpr6: Boolean): Boolean = {
    (boolexpr || boolexpr1 || boolexpr2 || boolexpr3 || boolexpr4 || boolexpr5 || boolexpr6)
  }

  def Or(boolexprs: ArrayBuffer[Boolean]): Boolean = {
    boolexprs.reduceLeft(_ || _)
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

  /** Intersect */
  def Intersect[T  : ClassTag](left: Array[T], right: Array[T]): Set[T] = {
    if (left == null || right == null || left.size == 0 || right.size == 0)
      return Array[T]().toSet
    (left.toSet & right.toSet)
  }

  def Intersect[T  : ClassTag](left: Array[T], right: Set[T]): Set[T] = {
    if (left == null || right == null || left.size == 0 || right.size == 0)
      return Array[T]().toSet
    (left.toSet & right)
  }

  def Intersect[T  : ClassTag](left: Set[T], right: Array[T]): Set[T] = {
    if (left == null || right == null || left.size == 0 || right.size == 0)
      return Array[T]().toSet
    (left & right.toSet)
  }

  def Intersect[T  : ClassTag](left: Set[T], right: Set[T]): Set[T] = {
    if (left == null || right == null || left.size == 0 || right.size == 0)
      return Array[T]().toSet
    (left & right)
  }

  def Intersect[T  : ClassTag](left: Array[T], right: TreeSet[T]): Set[T] = {
    if (left == null || right == null || left.size == 0 || right.size == 0)
      return Array[T]().toSet
    (left.toSet & right)
  }

  def Intersect[T  : ClassTag](left: TreeSet[T], right: Array[T]): Set[T] = {
    if (left == null || right == null || left.size == 0 || right.size == 0)
      return Array[T]().toSet
    (left & right.toSet).toSet
  }

  def Intersect[T  : ClassTag](left: TreeSet[T], right: TreeSet[T]): Set[T] = {
    if (left == null || right == null || left.size == 0 || right.size == 0)
      return Array[T]().toSet
    (left & right).toSet
  }

  def Intersect[T  : ClassTag](left: Set[T], right: TreeSet[T]): Set[T] = {
    if (left == null || right == null || left.size == 0 || right.size == 0)
      return Array[T]().toSet
    (left & right).toSet
  }

  def Intersect[T  : ClassTag](left: TreeSet[T], right: Set[T]): Set[T] = {
    if (left == null || right == null || left.size == 0 || right.size == 0)
      return Array[T]().toSet
    (left & right).toSet
  }
  
  /** Union */
  def Union[T  : ClassTag](left: ArrayBuffer[T], right: ArrayBuffer[T]): Set[T] = {
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

  def Union[T  : ClassTag](left: Array[T], right: Array[T]): Set[T] = {
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

  def Union[T  : ClassTag](left: Array[T], right: Set[T]): Set[T] = {
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

  def Union[T  : ClassTag](left: Set[T], right: Array[T]): Set[T] = {
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

  def Union[T  : ClassTag](left: Set[T], right: Set[T]): Set[T] = {
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
  
  def Last(coll : Array[Any]) : Any = {
	coll.last
  }

  def Last(coll : ArrayBuffer[Any]) : Any = {
	coll.last
  }

  def Last[T  : ClassTag](coll : ArrayBuffer[T]) : T = {
	coll.last
  }

  def Last(coll : Queue[Any]) : Any = {
	coll.last
  }

  def Last(coll : SortedSet[Any]) : Any = {
	coll.last
  }

  def First(coll : Array[Any]) : Any = {
	coll.head
  }

  def First(coll : ArrayBuffer[Any]) : Any = {
	coll.head
  }

  def First(coll : Queue[Any]) : Any = {
	coll.head
  }

  def First(coll : SortedSet[Any]) : Any = {
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

  def Sum(exprs: ArrayBuffer[Int]): Int = {
    exprs.reduceLeft(_ + _)
  }

  def Sum(exprs: ArrayBuffer[Long]): Long = {
    exprs.reduceLeft(_ + _)
  }

  def Sum(exprs: ArrayBuffer[Double]): Double = {
    exprs.reduceLeft(_ + _)
  }

  def Sum(exprs: ArrayBuffer[Float]): Float = {
    exprs.reduceLeft(_ + _)
  }

  def Sum(exprs: Array[Int]): Int = {
    exprs.reduceLeft(_ + _)
  }

  def Sum(exprs: Array[Long]): Long = {
    exprs.reduceLeft(_ + _)
  }

  def Sum(exprs: Array[Double]): Double = {
    exprs.reduceLeft(_ + _)
  }

  def Sum(exprs: Array[Float]): Float = {
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

  def Count(exprs: ArrayBuffer[Int]): Int = {
    exprs.length
  }

  def Count(exprs: ArrayBuffer[Long]): Long = {
    exprs.length
  }

  def Count(exprs: ArrayBuffer[Double]): Double = {
    exprs.length
  }

  def Count(exprs: ArrayBuffer[Float]): Float = {
    exprs.length
  }

  def Count(exprs: Array[Int]): Int = {
    exprs.length
  }

  def Count(exprs: Array[Long]): Long = {
    exprs.length
  }

  def Count(exprs: Array[Double]): Double = {
    exprs.length
  }

  def Count(exprs: Array[Float]): Float = {
    exprs.length
  }

  def Count(exprs: List[Int]): Int = {
    exprs.length
  }

  def Count(exprs: List[Long]): Long = {
    exprs.length
  }

  def Count(exprs: List[Double]): Double = {
    exprs.length
  }

  def Count(exprs: List[Float]): Float = {
    exprs.length
  }

  /**
		Aggregation functions supported by Pmml
		FIXME: This needs to be made into template 

	 	The implementation below has one error.  An Ordering is needed that can compare 
	 	
		
		An Ordering[T] is implemented by specifying compare(a:T, b:T), which decides how to order two instances a and b. Instances of Ordering[T] can be used by things like scala.util.Sorting to sort collections like Array[T].
		
		For example:
		
		import scala.util.Sorting
		
		case class Person(name:String, age:Int)
		val people = Array(Person("bob", 30), Person("ann", 32), Person("carl", 19))
		
		// sort by age
		object AgeOrdering extends Ordering[Person] {
		  def compare(a:Person, b:Person) = a.age compare b.age
		}
		Sorting.quickSort(people)(AgeOrdering)
   */

	/** "Multiset" builds sets of item values for each group by key
	    def MultiSet[A : ClassTag,B : ClassTag](exprs : ArrayBuffer[A] , groupByKey : ArrayBuffer[B] ) : HashMap[B,ArrayBuffer[A]] = {
		    var map = HashMap[B, ArrayBuffer[A]]()
		    var grpleft : List[A] = exprs.toList
		    val distinct = groupByKey.toSet.toArray.sorted
		    distinct.foreach((key : Int) => {
		    val grp : Map [Boolean, List[A]] = grpleft.groupBy(_ == key)
		    val setItems : List[A] = grp(true)
		    map(key) = new ArrayBuffer(setItems.toArray:_*)
		    grpleft = grp(false)
		    })
		    map
	    }
   */

  /** "multiset" (similar to Ligadata 'each') */
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
  }

  def Sum(exprs: ArrayBuffer[Int], groupByKey: ArrayBuffer[Int]): HashMap[Int, Int] = {
    var map = HashMap[Int, Int]()
    var grpleft: List[Int] = exprs.toList
    val distinct = groupByKey.toSet.toArray.sorted
    distinct.foreach((key: Int) => {
      val grp: Map[Boolean, List[Int]] = grpleft.groupBy(_ == key)
      val setItems: List[Int] = grp(true)
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

  def AsSeconds(milliSecs : Long): Long = {
	  milliSecs / 1000
  }

 


  /** 
   *  Convert time formatted in integer (compressed decimal) 
   *  to seconds.
   *  
   *  		Format: OHHMMSSCC, WHERE CC REPRESENTS HUNDREDTHS OF A SECOND 
   *  
   *  @param time, an Int
   *  @return time, an Int
   */
  def CompressedTimeHHMMSSCC2Secs(compressedTime : Int) : Int = {
	val hours = (compressedTime / 1000000) % 100
	val minutes = (compressedTime / 10000) % 100
	val seconds = (compressedTime / 100) % 100
	val millisecs = (compressedTime % 100) * 10
	
	val evtseconds = hours * 60 *  60 + minutes * 60 + seconds + (if (millisecs >= 500) 1 else 0)
	
	evtseconds
  }

  /** Calculate age from yyyymmdd ISO8601 type compressed in integer */
  def AgeCalc(yyyymmdd: Int): Int = {
    val birthDate: LocalDate = new LocalDate(yyyymmdd / 10000, (yyyymmdd % 1000) / 100, yyyymmdd % 100)
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

  def ToSet[T  : ClassTag](arr: Array[T]): Set[T] = {
    if (arr == null || arr.size == 0)
      return Set[T]().toSet
    arr.toSet
  }

  def ToSet[T  : ClassTag](arr: ArrayBuffer[T]): Set[T] = {
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

  def ToArray[T : ClassTag](set: MutableSet[T]): Array[T] = {
    if (set == null || set.size == 0)
      return Array[T]()
    set.toArray
  }
  
  def ToArray[T : ClassTag](set: Set[T]): Array[T] = {
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
  
  def ToArray[T : ClassTag](arr: ArrayBuffer[T]): Array[T] = {
    if (arr == null || arr.size == 0)
      return Array[T]()
    arr.toArray
  }
  
  def ToArray(arr: ArrayBuffer[Any]): Array[Any] = {
    if (arr == null || arr.size == 0)
      return Array[Any]()
    arr.toArray
  }
  
  def ToArray[T : ClassTag](arr: Array[T]): Array[T] = {
    if (arr == null || arr.size == 0)
      return Array[T]()
    arr.toArray
  }
  
  def ToArray(arr: Array[Any]): Array[Any] = {
    if (arr == null || arr.size == 0)
      return Array[Any]()
    arr.toArray
  }
  
  def ToArray[T : ClassTag](set: SortedSet[T]): Array[T] = {
    if (set == null || set.size == 0)
      return Array[T]()
    set.toArray
  }
  
  def ToArray(set: SortedSet[Any]): Array[Any] = {
    if (set == null || set.size == 0)
      return Array[Any]()
    set.toArray
  }
  
  def ToArray[T : ClassTag](ts: TreeSet[T]): Array[T] = {
    if (ts == null || ts.size == 0)
      return Array[T]()
    ts.toArray
  }
  
  def ToArray(ts: TreeSet[Any]): Array[Any] = {
    if (ts == null || ts.size == 0)
      return Array[Any]()
    ts.toArray
  }
  
  def ToArray[T : ClassTag](l: List[T]): Array[T] = {
    if (l == null || l.size == 0)
      return Array[T]()
    l.toArray
  }
  
  def ToArray(l: List[Any]): Array[Any] = {
    if (l == null || l.size == 0)
      return Array[Any]()
    l.toArray
  }
  
  def ToArray[T : ClassTag](q: Queue[T]): Array[T] = {
    if (q == null || q.size == 0)
      return Array[T]()
    q.toArray
  }
  
   def ToArray(q: Queue[Any]): Array[Any] = {
    if (q == null || q.size == 0)
      return Array[Any]()
    q.toArray
  }
  
 def ToArray[T : ClassTag](stack: Stack[T]): Array[T] = {
    if (stack == null || stack.size == 0)
      return Array[T]()
    stack.toArray
  }
  
  def ToArray(stack: Stack[Any]): Array[Any] = {
    if (stack == null || stack.size == 0)
      return Array[Any]()
    stack.toArray
  }
  
  def CollectionLength[T : ClassTag](coll : Array[T]) : Int = {
    coll.length
  }
  
  def CollectionLength[T : ClassTag](coll : ArrayBuffer[T]) : Int = {
    coll.size
  }
  
  def CollectionLength[T : ClassTag](coll : Set[T]) : Int = {
    coll.size
  }
  
  def CollectionLength[T : ClassTag](coll : TreeSet[T]) : Int = {
    coll.size
  }
  
  def CollectionLength[T : ClassTag](coll : SortedSet[T]) : Int = {
    coll.size
  }
  
  def CollectionLength[T : ClassTag](coll : List[T]) : Int = {
    coll.size
  }
  
  def CollectionLength[T : ClassTag](coll : Vector[T]) : Int = {
    coll.size
  }
  
  def CollectionLength[T : ClassTag](coll : Queue[T]) : Int = {
    coll.size
  }
  
  def CollectionLength[T : ClassTag](coll : Stack[T]) : Int = {
    coll.size
  }
  
  def CollectionLength[K : ClassTag, V : ClassTag](coll : Map[K,V]) : Int = {
    coll.size
  }
  
  def CollectionLength[K : ClassTag, V : ClassTag](coll : HashMap[K,V]) : Int = {
    coll.size
  }
}
