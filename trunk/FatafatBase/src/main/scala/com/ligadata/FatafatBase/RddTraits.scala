package com.ligadata.FatafatBase

import scala.language.implicitConversions
// import scala.reflect.{ classTag, ClassTag }
import org.apache.log4j.Logger
import java.util.Date
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.{ classTag, ClassTag }
import com.ligadata.Exceptions._
import com.ligadata.FatafatBase.api.java.function._
import com.google.common.base.Optional
import com.ligadata.Utils.Utils
import java.util.{ Comparator, List => JList, Iterator => JIterator }
import java.lang.{ Iterable => JIterable, Long => JLong }
import scala.collection.mutable.ArrayBuffer

object FatafatUtils {
  def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]
  def toScalaFunction1[T, R](fun: Function1[T, R]): T => R = x => fun.call(x)
  def pairFunToScalaFun[A, B, C](x: PairFunction[A, B, C]): A => (B, C) = y => x.call(y)
}

import FatafatUtils._

class Stats {
  // # of Rows, Total Size of the data, Avg Size, etc
}

object TimeRange {
  // Time Range Methods like 30days ago, current date, week ago, adjusting partition to week, month or year, etc
}

// startTime & endTime are in the format of YYYYMMDDHH
class TimeRange(startTime: Int, endTime: Int) {
  // Methods
}

// RDD traits/classes
class PairRDDFunctions[K, V](self: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null) {
  val LOG = Logger.getLogger(getClass);

  def count: Long = self.size

  def countByKey: Map[K, Long] = {
    self.Collection.groupBy(t => t._1).mapValues(listOfPairs => listOfPairs.size.toLong)
  }

  def groupByKey: RDD[(K, Iterable[V])] = {
    val newrdd = RDD.makeRDD(self.Collection.groupBy(t => t._1).mapValues(listOfPairs => listOfPairs.map(pair => pair._2).asInstanceOf[Iterable[V]]).toArray)
    newrdd
  }

  // Join Functions
  def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = {
    val s1 = self.Collection.groupBy(t => t._1)
    val s2 = other.Collection.groupBy(t => t._1)
    val result = ArrayBuffer[(K, (V, W))]()
    s1.foreach(s1kv => {
      val fndValPairs = s2.getOrElse(s1kv._1, null)
      if (fndValPairs != null) {
        s1kv._2.foreach(s1pair => {
          fndValPairs.foreach(s2pair => {
            result += ((s1kv._1, (s1pair._2, s2pair._2)))
          })
        })
      }
    })

    val newrdd = RDD.makeRDD(result.toArray)
    newrdd
  }

  def fullOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], Option[W]))] = {
    val s1 = self.Collection.groupBy(t => t._1)
    val s2 = other.Collection.groupBy(t => t._1)
    val result = ArrayBuffer[(K, (Option[V], Option[W]))]()
    s1.foreach(s1kv => {
      val fndValPairs = s2.getOrElse(s1kv._1, null)
      if (fndValPairs != null) {
        s1kv._2.foreach(s1pair => {
          fndValPairs.foreach(s2pair => {
            result += ((s1kv._1, (Some(s1pair._2), Some(s2pair._2))))
          })
        })
      } else {
        s1kv._2.foreach(s1pair => {
          result += ((s1kv._1, (Some(s1pair._2), None)))
        })
      }
    })

    // Looking for the Remaining stuff from s2
    s2.foreach(s2kv => {
      val fndValPairs = s1.getOrElse(s2kv._1, null)
      if (fndValPairs == null) { // if this is not present in s1 map, add this
        s2kv._2.foreach(s2pair => {
          result += ((s2kv._1, (None, Some(s2pair._2))))
        })
      }
    })

    val newrdd = RDD.makeRDD(result.toArray)
    newrdd
  }

  def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = {
    val s1 = self.Collection.groupBy(t => t._1)
    val s2 = other.Collection.groupBy(t => t._1)
    val result = ArrayBuffer[(K, (V, Option[W]))]()
    s1.foreach(s1kv => {
      val fndValPairs = s2.getOrElse(s1kv._1, null)
      if (fndValPairs != null) {
        s1kv._2.foreach(s1pair => {
          fndValPairs.foreach(s2pair => {
            result += ((s1kv._1, (s1pair._2, Some(s2pair._2))))
          })
        })
      } else {
        s1kv._2.foreach(s1pair => {
          result += ((s1kv._1, (s1pair._2, None)))
        })
      }
    })

    val newrdd = RDD.makeRDD(result.toArray)
    newrdd
  }

  def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = {
    val s1 = self.Collection.groupBy(t => t._1)
    val s2 = other.Collection.groupBy(t => t._1)
    val result = ArrayBuffer[(K, (Option[V], W))]()
    s2.foreach(s2kv => {
      val fndValPairs = s1.getOrElse(s2kv._1, null)
      if (fndValPairs != null) {
        s2kv._2.foreach(s2pair => {
          fndValPairs.foreach(s1pair => {
            result += ((s2kv._1, (Some(s1pair._2), s2pair._2)))
          })
        })
      } else {
        s2kv._2.foreach(s2pair => {
          result += ((s2kv._1, (None, s2pair._2)))
        })
      }
    })

    val newrdd = RDD.makeRDD(result.toArray)
    newrdd
  }

  def mapValues[U](f: V => U): RDD[(K, U)] = {
    val newrdd = RDD.makeRDD(self.Collection.map(v => (v._1, f(v._2))).toArray)
    newrdd
  }
}

object RDD {
  implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
  }

  def makeRDD[T](values: Array[T]): RDD[T] = {
    val newrdd = new RDD[T]()(ClassTag.AnyRef.asInstanceOf[ClassTag[T]])
    newrdd.collection ++= values
    newrdd
  }
}

class RDD[T: ClassTag] {
  private val collection = ArrayBuffer[T]()

  private[FatafatBase] def Collection = collection

  val LOG = Logger.getLogger(getClass);

  def iterator: Iterator[T] = collection.iterator

  def elementClassTag: ClassTag[T] = classTag[T]

  def map[U: ClassTag](f: T => U): RDD[U] = {
    val newrdd = new RDD[U]()
    newrdd.collection ++= collection.iterator.map(f)
    newrdd
  }

  def map[U: ClassTag](tmRange: TimeRange, f: T => U): RDD[U] = {
    val newrdd = new RDD[U]()
    // BUGBUG:: Yet to implement filtering tmRange
    newrdd.collection ++= collection.iterator.map(f)
    newrdd
  }

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = {
    val newrdd = new RDD[U]()
    newrdd.collection ++= collection.iterator.flatMap(f)
    newrdd
  }

  def filter(f: T => Boolean): RDD[T] = {
    val newrdd = new RDD[T]()
    newrdd.collection ++= collection.iterator.filter(f)
    newrdd
  }

  def filter(tmRange: TimeRange, f: T => Boolean): RDD[T] = {
    val newrdd = new RDD[T]()
    // BUGBUG:: Yet to implement filtering tmRange
    newrdd.collection ++= collection.iterator.filter(f)
    newrdd
  }

  def union(other: RDD[T]): RDD[T] = {
    val newrdd = new RDD[T]()
    newrdd.collection ++= (collection ++ other.iterator)
    newrdd
  }

  def ++(other: RDD[T]): RDD[T] = this.union(other)

  // def sortBy[K](f: (T) => K, ascending: Boolean = true) (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = this.keyBy[K](f).sortByKey(ascending, numPartitions).values

  def intersection(other: RDD[T]): RDD[T] = {
    throw new Exception("Unhandled function intersection")
  }

  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = {
    val newrdd = new RDD[(K, Iterable[T])]()
    newrdd.collection ++= collection.map(x => (f(x), x)).groupBy(t => t._1).mapValues(listOfPairs => listOfPairs.map(pair => pair._2))
    newrdd
  }

  def foreach(f: T => Unit): Unit = {
    collection.iterator.foreach(f)
  }

  def toArray: Array[T] = {
    collection.toArray
  }

  def subtract(other: RDD[T]): RDD[T] = {
    throw new Exception("Unhandled function subtract")
  }

  def count(): Long = size()

  def size(): Long = collection.size

  def first(): Option[T] = {
    if (collection.size > 0)
      return Some(collection(0))
    None
  }

  def last(): Option[T] = {
    if (collection.size > 0)
      return Some(collection(collection.size - 1))
    None
  }

  def top(num: Int): Array[T] = {
    // BUGBUG:: Order the data and select top N
    throw new Exception("Unhandled function top")
  }

  def max[U: ClassTag](f: (Option[U], T) => U): Option[U] = {
    var maxVal: Option[U] = None
    collection.foreach(c => {
      maxVal = Some(f(maxVal, c))
    })
    maxVal
  }

  def min[U: ClassTag](f: (Option[U], T) => U): Option[U] = {
    var minVal: Option[U] = None
    collection.foreach(c => {
      minVal = Some(f(minVal, c))
    })
    minVal
  }

  def isEmpty(): Boolean = collection.isEmpty

  def keyBy[K](f: T => K): RDD[(K, T)] = {
    map(x => (f(x), x))
  }

  def toJavaRDD(): JavaRDD[T] = {
    new JavaRDD(this)(elementClassTag)
  }
}

object JavaRDD {
  implicit def fromRDD[T: ClassTag](rdd: RDD[T]): JavaRDD[T] = new JavaRDD[T](rdd)
  implicit def toRDD[T](rdd: JavaRDD[T]): RDD[T] = rdd.rdd
}

abstract class AbstractJavaRDDLike[T, This <: JavaRDDLike[T, This]]
  extends JavaRDDLike[T, This]

trait JavaRDDLike[T, This <: JavaRDDLike[T, This]] {
  def wrapRDD(rdd: RDD[T]): This

  implicit val classTag: ClassTag[T]

  def rdd: RDD[T]

  def iterator: java.util.Iterator[T] = asJavaIterator(rdd.iterator)

  def map[R](f: Function1[T, R]): JavaRDD[R] = {
    JavaRDD.fromRDD(rdd.map(FatafatUtils.toScalaFunction1(f))(fakeClassTag))(fakeClassTag)
  }

  def map[R](tmRange: TimeRange, f: Function1[T, R]): JavaRDD[R] = {
    // BUGBUG:: Yet to implement filtering tmRange
    JavaRDD.fromRDD(rdd.map(FatafatUtils.toScalaFunction1(f))(fakeClassTag))(fakeClassTag)
  }

  def flatMap[U](f: FlatMapFunction1[T, U]): JavaRDD[U] = {
    def fn = (x: T) => f.call(x).asScala
    JavaRDD.fromRDD(rdd.flatMap(fn)(fakeClassTag[U]))(fakeClassTag[U])
  }

  def groupBy[U](f: Function1[T, U]): JavaPairRDD[U, JIterable[T]] = {
    // The type parameter is U instead of K in order to work around a compiler bug; see SPARK-4459
    implicit val ctagK: ClassTag[U] = fakeClassTag
    implicit val ctagV: ClassTag[JList[T]] = fakeClassTag
    JavaPairRDD.fromRDD(JavaPairRDD.groupByResultToJava(rdd.groupBy(FatafatUtils.toScalaFunction1(f))(fakeClassTag)))
  }

  def count(): Long = rdd.count()

  def size(): Long = rdd.size()

  def first(): Optional[T] = Utils.optionToOptional(rdd.first)

  def last(): Optional[T] = Utils.optionToOptional(rdd.last)

  def isEmpty(): Boolean = rdd.isEmpty()
}

class JavaRDD[T](val rdd: RDD[T])(implicit val classTag: ClassTag[T])
  extends AbstractJavaRDDLike[T, JavaRDD[T]] {

  override def wrapRDD(rdd: RDD[T]): JavaRDD[T] = JavaRDD.fromRDD(rdd)

  def filter(f: Function1[T, java.lang.Boolean]): JavaRDD[T] = wrapRDD(rdd.filter((x => f.call(x).booleanValue())))

  def union(other: JavaRDD[T]): JavaRDD[T] = wrapRDD(rdd.union(other.rdd))

  def intersection(other: JavaRDD[T]): JavaRDD[T] = wrapRDD(rdd.intersection(other.rdd))

  def subtract(other: JavaRDD[T]): JavaRDD[T] = wrapRDD(rdd.subtract(other))

  // def sortBy[S](f: JFunction1[T, S], ascending: Boolean): JavaRDD[T]
}

object JavaPairRDD {
  def groupByResultToJava[K: ClassTag, T](rdd: RDD[(K, Iterable[T])]): RDD[(K, JIterable[T])] = {
    RDD.rddToPairRDDFunctions(rdd).mapValues(asJavaIterable)
  }

  def fromRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): JavaPairRDD[K, V] = {
    new JavaPairRDD[K, V](rdd)
  }

  implicit def toRDD[K, V](rdd: JavaPairRDD[K, V]): RDD[(K, V)] = rdd.rdd

  /** Convert a JavaRDD of key-value pairs to JavaPairRDD. */
  def fromJavaRDD[K, V](rdd: JavaRDD[(K, V)]): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = fakeClassTag
    implicit val ctagV: ClassTag[V] = fakeClassTag
    new JavaPairRDD[K, V](rdd.rdd)
  }
}

class JavaPairRDD[K, V](val rdd: RDD[(K, V)])(implicit val kClassTag: ClassTag[K], implicit val vClassTag: ClassTag[V])
  extends AbstractJavaRDDLike[(K, V), JavaPairRDD[K, V]] {

  override def wrapRDD(rdd: RDD[(K, V)]): JavaPairRDD[K, V] = JavaPairRDD.fromRDD(rdd)

  override val classTag: ClassTag[(K, V)] = rdd.elementClassTag

  import JavaPairRDD._

  def filter(f: Function1[(K, V), java.lang.Boolean]): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd.filter(x => f.call(x).booleanValue()))

  def union(other: JavaPairRDD[K, V]): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd.union(other.rdd))

  def intersection(other: JavaPairRDD[K, V]): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd.intersection(other.rdd))

  def subtract(other: JavaPairRDD[K, V]): JavaPairRDD[K, V] =
    fromRDD(rdd.subtract(other))

  // def join[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (V, W)]
}

abstract class RDDObject[T: ClassTag] {
  val LOG = Logger.getLogger(getClass);

  private def getCurrentModelContext: ModelContext = null

  // Methods needs to be override in inherited class -- Begin
  // get builder
  def build: T
  def build(from: T): T
  def getFullName: String // Gets Message/Container Name
  def toJavaRDDObject: JavaRDDObject[T]
  // Methods needs to be override in inherited class -- End

  final def toRDDObject: RDDObject[T] = this

  // First group of functions retrieve one object (either recent or for a given key & filter)
  // Get recent entry for the Current Key
  final def getRecent: Option[T] = {
    val mdlCtxt = getCurrentModelContext
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRecent(getFullName, mdlCtxt.msg.PartitionKeyData.toList, null, null)
      if (fndVal != None)
        return Some(fndVal.get.asInstanceOf[T])
    }
    None
  }

  final def getRecentOrNew: T = {
    val rcnt = getRecent
    if (rcnt.isEmpty)
      return build
    rcnt.get
  }

  // Get recent entry for the given key
  final def getRecent(key: Array[String]): Option[T] = {
    val mdlCtxt = getCurrentModelContext
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRecent(getFullName, key.toList, null, null)
      if (fndVal != None)
        return Some(fndVal.get.asInstanceOf[T])
    }
    None
  }

  final def getRecentOrNew(key: Array[String]): T = {
    val rcnt = getRecent(key)
    if (rcnt.isEmpty) return build
    rcnt.get
  }

  final def getOne(tmRange: TimeRange, f: MessageContainerBase => Boolean): Option[T] = {
    val mdlCtxt = getCurrentModelContext
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRecent(getFullName, mdlCtxt.msg.PartitionKeyData.toList, tmRange, f)
      if (fndVal != None)
        return Some(fndVal.get.asInstanceOf[T])
    }
    None
  }

  final def getOneOrNew(tmRange: TimeRange, f: MessageContainerBase => Boolean): T = {
    val one = getOne(tmRange, f)
    if (one.isEmpty) return build
    one.get
  }

  final def getOne(key: Array[String], tmRange: TimeRange, f: MessageContainerBase => Boolean): Option[T] = {
    val mdlCtxt = getCurrentModelContext
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRecent(getFullName, key.toList, tmRange, f)
      if (fndVal != None)
        return Some(fndVal.get.asInstanceOf[T])
    }
    None
  }

  final def getOneOrNew(key: Array[String], tmRange: TimeRange, f: MessageContainerBase => Boolean): T = {
    val one = getOne(key, tmRange, f)
    if (one.isEmpty) return build
    one.get
  }

  // This group of functions retrieve collection of objects 
  final def getRDDForCurrKey(f: MessageContainerBase => Boolean): RDD[T] = {
    val mdlCtxt = getCurrentModelContext
    var values: Array[T] = Array[T]()
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(getFullName, mdlCtxt.msg.PartitionKeyData.toList, null, f)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }

  final def getRDDForCurrKey(tmRange: TimeRange, f: MessageContainerBase => Boolean): RDD[T] = {
    val mdlCtxt = getCurrentModelContext
    var values: Array[T] = Array[T]()
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(getFullName, mdlCtxt.msg.PartitionKeyData.toList, tmRange, f)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }

  // With too many messages, these may fail - mostly useful for message types where number of messages are relatively small 
  final def getRDD(tmRange: TimeRange, f: MessageContainerBase => Boolean): RDD[T] = {
    val mdlCtxt = getCurrentModelContext
    var values: Array[T] = Array[T]()
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(getFullName, null, tmRange, f)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }

  final def getRDD(tmRange: TimeRange): RDD[T] = {
    val mdlCtxt = getCurrentModelContext
    var values: Array[T] = Array[T]()
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(getFullName, null, tmRange, null)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }

  final def getRDD(f: MessageContainerBase => Boolean): RDD[T] = {
    val mdlCtxt = getCurrentModelContext
    var values: Array[T] = Array[T]()
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(getFullName, null, null, f)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }

  final def getRDD(key: Array[String], tmRange: TimeRange, f: MessageContainerBase => Boolean): RDD[T] = {
    val mdlCtxt = getCurrentModelContext
    var values: Array[T] = Array[T]()
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(getFullName, key.toList, tmRange, f)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }

  final def getRDD(key: Array[String], f: MessageContainerBase => Boolean): RDD[T] = {
    val mdlCtxt = getCurrentModelContext
    var values: Array[T] = Array[T]()
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(getFullName, key.toList, null, f)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }

  final def getRDD(key: Array[String], tmRange: TimeRange): RDD[T] = {
    val mdlCtxt = getCurrentModelContext
    var values: Array[T] = Array[T]()
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(getFullName, key.toList, tmRange, null)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }

  // Saving data
  final def saveOne(inst: T): Unit = {
    val mdlCtxt = getCurrentModelContext
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      // mdlCtxt.txnContext.gCtx.saveOne(getFullName, mdlCtxt.msg.PartitionKeyData.toList, inst)
    }
  }

  final def saveOne(key: Array[String], inst: T): Unit = {
    val mdlCtxt = getCurrentModelContext
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      // mdlCtxt.txnContext.gCtx.saveOne(getFullName, key.toList, inst)
    }
  }

  final def saveRDD(data: RDD[T]): Unit = {
    val mdlCtxt = getCurrentModelContext
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      // mdlCtxt.txnContext.gCtx.saveList(getFullName, data.Collection.toList)
    }
  }
}

object JavaRDDObject {
  def fromRDDObject[T: ClassTag](rddObj: RDDObject[T]): JavaRDDObject[T] = new JavaRDDObject[T](rddObj)
  def toRDDObject[T](rddObj: JavaRDDObject[T]): RDDObject[T] = rddObj.rddObj
}

abstract class AbstractJavaRDDObjectLike[T, This <: JavaRDDObjectLike[T, This]]
  extends JavaRDDObjectLike[T, This]

trait JavaRDDObjectLike[T, This <: JavaRDDObjectLike[T, This]] {
  val LOG = Logger.getLogger(getClass);
  def wrapRDDObject(rddObj: RDDObject[T]): This

  implicit val classTag: ClassTag[T]
  def rddObj: RDDObject[T]

  // get builder
  def build: T = rddObj.build
  def build(from: T): T = rddObj.build(from)

  // First group of functions retrieve one object (either recent or for a given key & filter)
  // Get recent entry for the Current Key
  def getRecent: Optional[T] = Utils.optionToOptional(rddObj.getRecent)
  def getRecentOrNew: T = rddObj.getRecentOrNew

  // Get recent entry for the given key
  def getRecent(key: Array[String]): Optional[T] = Utils.optionToOptional(rddObj.getRecent(key))
  def getRecentOrNew(key: Array[String]): T = rddObj.getRecentOrNew(key)

  def getOne(tmRange: TimeRange, f: Function1[T, java.lang.Boolean]): Optional[T] = null
  def getOne(key: Array[String], tmRange: TimeRange, f: Function1[T, java.lang.Boolean]): Optional[T] = null
  def getOneOrNew(key: Array[String], tmRange: TimeRange, f: Function1[T, java.lang.Boolean]): T = rddObj.build
  def getOneOrNew(tmRange: TimeRange, f: Function1[T, java.lang.Boolean]): T = rddObj.build

  // This group of functions retrieve collection of objects 
  def getRDDForCurrKey(f: Function1[T, java.lang.Boolean]): JavaRDD[T] = null
  def getRDDForCurrKey(tmRange: TimeRange, f: Function1[T, java.lang.Boolean]): JavaRDD[T] = null

  // With too many messages, these may fail - mostly useful for message types where number of messages are relatively small 
  def getRDD(tmRange: TimeRange, f: Function1[T, java.lang.Boolean]): JavaRDD[T] = null
  def getRDD(tmRange: TimeRange): JavaRDD[T] = rddObj.getRDD(tmRange)
  def getRDD(f: Function1[T, java.lang.Boolean]): JavaRDD[T] = null

  def getRDD(key: Array[String], tmRange: TimeRange, f: Function1[T, java.lang.Boolean]): JavaRDD[T] = null
  def getRDD(key: Array[String], f: Function1[T, java.lang.Boolean]): JavaRDD[T] = null
  def getRDD(key: Array[String], tmRange: TimeRange): JavaRDD[T] = rddObj.getRDD(key, tmRange)

  // Saving data
  def saveOne(inst: T): Unit = rddObj.saveOne(inst)
  def saveOne(key: Array[String], inst: T): Unit = rddObj.saveOne(key, inst)
  def saveRDD(data: RDD[T]): Unit = rddObj.saveRDD(data)
}

class JavaRDDObject[T](val rddObj: RDDObject[T])(implicit val classTag: ClassTag[T])
  extends AbstractJavaRDDObjectLike[T, JavaRDDObject[T]] {
  override def wrapRDDObject(rddObj: RDDObject[T]): JavaRDDObject[T] = JavaRDDObject.fromRDDObject(rddObj)

  def toJavaRDDObject: JavaRDDObject[T] = this
  def toRDDObject: RDDObject[T] = rddObj
}

object StringUtils {
  val emptyStr = ""
  val quotes3 = "\"\"\""
  val quotes2 = "\"\""
  val spaces2 = "  "
  val spaces3 = "   "
  val spaces4 = spaces3 + " "

  val tab1 = spaces2
  val tab2 = tab1 + tab1
  val tab3 = tab2 + tab1
  val tab4 = tab3 + tab1
  val tab5 = tab4 + tab1
  val tab6 = tab5 + tab1
  val tab7 = tab6 + tab1
  val tab8 = tab7 + tab1
  val tab9 = tab8 + tab1

  val lfTab0 = "\n"
  val lfTab1 = lfTab0 + tab1
  val lfTab2 = lfTab1 + tab1
  val lfTab3 = lfTab2 + tab1
  val lfTab4 = lfTab3 + tab1
  val lfTab5 = lfTab4 + tab1
  val lfTab6 = lfTab5 + tab1
  val lfTab7 = lfTab6 + tab1
  val lfTab8 = lfTab7 + tab1
  val lfTab9 = lfTab8 + tab1

  // return zero for NULL and empty strings, for others length of the given string
  def len(str: String) = str match { case null => 0; case _ => str.length }
  def emptyIfNull(str: String) = str match { case null => ""; case _ => str }
  def emptyIfNull(str: String, fn: String => String) = str match { case null => ""; case _ => fn(str) }

  def catStrs(strs: Array[String], sep: String, fn: String => String) = {
    val sb = new StringBuilder();
    strs.foldLeft(false)((f, s) => { if (f) sb.append(sep); sb.append(emptyIfNull(s, fn)); true });
    sb.result
  }

  def catStrs(strs: Array[String], sep: String): String = catStrs(strs, sep, { str: String => str })

  // returns the given string with its first character in lower case. no checks for null or empty string
  private def firstLowerNoCheck(str: String) = { str.charAt(0).toLower + str.substring(1) }

  // returns a new string with its first character in lower case  and rest unchanged (null or empty string returns the given string)
  def firstLower(str: String) = len(str) match { case 0 => str; case _ => firstLowerNoCheck(str) }

  def AddAsLinesToBuf(buf: StringBuilder, lines: Array[String]) = lines.foreach(line => buf.append(line).append("\n"))
  def firstNonNull(args: String*): String = { if (args == null) return null; for (arg <- args) { if (arg != null) return arg }; null; }

  def prefixSpaces(str: String): String = {
    val cnt = str.prefixLength({ ch => (ch == ' ') || (ch == '\t') });
    if (cnt == 0) emptyStr
    else str.substring(0, cnt)
  }

  def lastNonEmptyLine(strs: Array[String]): String = {
    var idx = strs.length
    while (idx > 0) { idx -= 1; val str = strs(idx); if (str.length > 0) return str; }
    emptyStr
  }
}

import StringUtils._
import RddUtils._

object RddDate {
  def currentDateTime = RddDate(GetCurDtTmInMs)
}

case class RddDate(tms: Long) extends Date(tms) {
  def timeDiffInHrs(dt: Date): Int = 0
  def timeDiffInHrs(dt: RddDate): Int = 0
  def lastNdays(days: Int): TimeRange = null;
}

object RddUtils {
  def IsEmpty(str: String) = (str == null || str.length == 0)
  def IsNotEmpty(str: String) = (!IsEmpty(str))
  def IsEmptyArray[T](a: Array[T]) = (a == null || a.size == 0)
  def IsNotEmptyArray[T](a: Array[T]) = (!IsEmptyArray(a))

  def SimpDateFmtTimeFromMs(tmMs: Long, fmt: String): String = { new java.text.SimpleDateFormat(fmt).format(new java.util.Date(tmMs)) }

  // Get current time (in ms) as part of valid identifier and appends to given string
  def GetCurDtTmAsId(prefix: String): String = { prefix + SimpDateFmtTimeFromMs(GetCurDtTmInMs, "yyyyMMdd_HHmmss_SSS") }
  def GetCurDtTmAsId: String = GetCurDtTmAsId("")

  def GetCurDtTmStr: String = { SimpDateFmtTimeFromMs(GetCurDtTmInMs, "yyyy:MM:dd HH:mm:ss.SSS") }
  def GetCurDtStr: String = { SimpDateFmtTimeFromMs(GetCurDtTmInMs, "yyyy:MM:dd") }
  def GetCurDtTmInMs: Long = { System.currentTimeMillis }
  def elapsed[A](f: => A): (Long, A) = { val s = System.nanoTime; val ret = f; ((System.nanoTime - s), ret); }
}
