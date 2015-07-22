package com.ligadata.FatafatBase

import scala.language.implicitConversions
import java.util.{ Date, Calendar, TimeZone }

// import scala.reflect.{ classTag, ClassTag }
import org.apache.log4j.Logger
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.{ classTag, ClassTag }
import com.ligadata.Exceptions._
import com.ligadata.FatafatBase.api.java.function.{Function1 => JFunction1, FlatMapFunction1 => JFlatMapFunction1, PairFunction => JPairFunction}
import com.google.common.base.Optional
import com.ligadata.Utils.Utils
import java.util.{ Comparator, List => JList, Iterator => JIterator }
import java.lang.{ Iterable => JIterable, Long => JLong }
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

object ThreadLocalStorage {
  final val modelContextInfo = new ThreadLocal[ModelContext]();
}

object FatafatUtils {
  def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]
  def toScalaFunction1[T, R](fun: JFunction1[T, R]): T => R = x => fun.call(x)
  def pairFunToScalaFun[A, B, C](x: JPairFunction[A, B, C]): A => (B, C) = y => x.call(y)
}

import FatafatUtils._

class Stats {
  // # of Rows, Total Size of the data, Avg Size, etc
}

object TimeRange {
  // Time Range Methods like 30days ago, current date, week ago, adjusting partition to week, month or year, etc
}

// startTime & endTime are in the format of YYYYMMDDHH
case class TimeRange(startTime: Int, endTime: Int) {}

// RDD traits/classes
/**
 * More functions available on RDDs of (key, value) pairs via an implicit conversion.
 */
class PairRDDFunctions[K, V](self: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null) {
  val LOG = Logger.getLogger(getClass);

  def count: Long = self.size

  /**
   * Count the number of elements for each key, and return the result as a Map.
   */
  def countByKey: Map[K, Long] = {
    self.Collection.groupBy(t => t._1).mapValues(listOfPairs => listOfPairs.size.toLong)
  }

  /**
   * Group the values for each key in the RDD into a iterable trait.
   */
  def groupByKey: RDD[(K, Iterable[V])] = {
    val newrdd = RDD.makeRDD(self.Collection.groupBy(t => t._1).mapValues(listOfPairs => listOfPairs.map(pair => pair._2).asInstanceOf[Iterable[V]]).toArray)
    newrdd
  }

  // Join Functions
  /**
   * Return an RDD containing all pairs of elements with matching keys in this and other.
   * Each pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in this and (k, v2) is in other.
   */
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

  /**
   * Return the Cartesian product of this and other, that is,
   * the RDD of all pairs of elements (v, w) where v is in this and w is in other.
   */
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

  /**
   * Performs a left outer join of this and other.
   * For each element (k, v) in this, the resulting RDD will either contain all pairs (k, (v, Some(w))) for w in other,
   * or the pair (k, (v, None)) if no elements in other have key k.
   */
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

  /**
   * Performs a right outer join of this and other.
   * For each element (k, w) in other, the resulting RDD will either contain all pairs (k, (Some(v), w)) for v in this,
   * or the pair (k, (None, w)) if no elements in this have key k.
   */
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

  /**
   * Pass each value in the key-value pair RDD through a map function without changing the keys.
   */
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

  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   */
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

  /**
   * Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results.
   */
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = {
    val newrdd = new RDD[U]()
    newrdd.collection ++= collection.iterator.flatMap(f)
    newrdd
  }

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   */
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

  /**
   * Return the union of this RDD and another one.
   */
  def union(other: RDD[T]): RDD[T] = {
    val newrdd = new RDD[T]()
    newrdd.collection ++= (collection ++ other.iterator)
    newrdd
  }

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple times.
   */
  def ++(other: RDD[T]): RDD[T] = this.union(other)

  // def sortBy[K](f: (T) => K, ascending: Boolean = true) (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = this.keyBy[K](f).sortByKey(ascending, numPartitions).values

  /**
   * Return the intersection of this RDD and another one.
   */
  def intersection(other: RDD[T]): RDD[T] = {
    throw new Exception("Unhandled function intersection")
  }

  /**
   * Return an RDD of grouped elements. Each group consists of a key and a sequence of elements mapping to that key.
   */
  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = {
    val newrdd = new RDD[(K, Iterable[T])]()
    newrdd.collection ++= collection.map(x => (f(x), x)).groupBy(t => t._1).mapValues(listOfPairs => listOfPairs.map(pair => pair._2))
    newrdd
  }

  /**
   * Applies a function f to all elements of this RDD.
   */
  def foreach(f: T => Unit): Unit = {
    collection.iterator.foreach(f)
  }

  /**
   * Return an array that contains all of the elements in this RDD.
   */
  def toArray: Array[T] = {
    collection.toArray
  }

  /**
   * Return the list that contains all of the elements in this RDD.
   */
  def toList: List[T] = {
    collection.toList
  }

  def subtract(other: RDD[T]): RDD[T] = {
    throw new Exception("Unhandled function subtract")
  }

  /**
   * Return the number of elements in the RDD.
   */
  def count(): Long = size()

  def size(): Long = collection.size

  /**
   * Return the first element in this RDD.
   */
  def first(): Option[T] = {
    if (collection.size > 0)
      return Some(collection(0))
    None
  }

  /**
   * Return the last element in this RDD.
   */
  def last(): Option[T] = {
    if (collection.size > 0)
      return Some(collection(collection.size - 1))
    None
  }

  /**
   * Sort the data in the array and than return the top N element(s).
   */
  def top(num: Int): Array[T] = {
    // BUGBUG:: Order the data and select top N
    throw new Exception("Unhandled function top")
  }

  /**
   * Return the maximum element in this collection.
   */
  def max[U: ClassTag](f: (Option[U], T) => U): Option[U] = {
    var maxVal: Option[U] = None
    collection.foreach(c => {
      maxVal = Some(f(maxVal, c))
    })
    maxVal
  }

  /**
   * Return the minimum element in this collection.
   */
  def min[U: ClassTag](f: (Option[U], T) => U): Option[U] = {
    var minVal: Option[U] = None
    collection.foreach(c => {
      minVal = Some(f(minVal, c))
    })
    minVal
  }

  /**
   * Check whether this collection is empty.
   */
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

  /**
   * Return a new RDD by applying the function to all elements of this RDD.
   */
  def map[R](f: JFunction1[T, R]): JavaRDD[R] = {
    JavaRDD.fromRDD(rdd.map(FatafatUtils.toScalaFunction1(f))(fakeClassTag))(fakeClassTag)
  }

  def map[R](tmRange: TimeRange, f: JFunction1[T, R]): JavaRDD[R] = {
    // BUGBUG:: Yet to implement filtering tmRange
    JavaRDD.fromRDD(rdd.map(FatafatUtils.toScalaFunction1(f))(fakeClassTag))(fakeClassTag)
  }

  /**
   * Return a new RDD by applying a function to all the elements of this RDD, and than flattening the results.
   */
  def flatMap[U](f: JFlatMapFunction1[T, U]): JavaRDD[U] = {
    def fn = (x: T) => f.call(x).asScala
    JavaRDD.fromRDD(rdd.flatMap(fn)(fakeClassTag[U]))(fakeClassTag[U])
  }

  /**
   * Return an RDD of grouped elements.
   */
  def groupBy[U](f: JFunction1[T, U]): JavaPairRDD[U, JIterable[T]] = {
    // The type parameter is U instead of K in order to work around a compiler bug; see SPARK-4459
    implicit val ctagK: ClassTag[U] = fakeClassTag
    implicit val ctagV: ClassTag[JList[T]] = fakeClassTag
    JavaPairRDD.fromRDD(JavaPairRDD.groupByResultToJava(rdd.groupBy(FatafatUtils.toScalaFunction1(f))(fakeClassTag)))
  }

  /**
   * Return the bymber of elements in the RDD.
   */
  def count(): Long = rdd.count()

  /**
   * Return the size of the RDD.
   */
  def size(): Long = rdd.size()

  /**
   * Return the first element in this RDD.
   */
  def first(): Optional[T] = Utils.optionToOptional(rdd.first)

  /**
   * Return the last element in this RDD.
   */
  def last(): Optional[T] = Utils.optionToOptional(rdd.last)

  /**
   * True if and only if the RDD contains no elements at all.
   */
  def isEmpty(): Boolean = rdd.isEmpty()
}

class JavaRDD[T](val rdd: RDD[T])(implicit val classTag: ClassTag[T])
  extends AbstractJavaRDDLike[T, JavaRDD[T]] {

  override def wrapRDD(rdd: RDD[T]): JavaRDD[T] = JavaRDD.fromRDD(rdd)

  /**
   * Return a new RDD containing only the elements that satisfy the predicate.
   */
  def filter(f: JFunction1[T, java.lang.Boolean]): JavaRDD[T] = wrapRDD(rdd.filter((x => f.call(x).booleanValue())))

  /**
   * Return the union of this RDD and another one.
   */
  def union(other: JavaRDD[T]): JavaRDD[T] = wrapRDD(rdd.union(other.rdd))

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate elements.
   */
  def intersection(other: JavaRDD[T]): JavaRDD[T] = wrapRDD(rdd.intersection(other.rdd))

  /**
   * Return an RDD from this that are not in the other.
   */
  def subtract(other: JavaRDD[T]): JavaRDD[T] = wrapRDD(rdd.subtract(other))

  // def sortBy[S](f: JFunction1[T, S], ascending: Boolean): JavaRDD[T]
}

object JavaPairRDD {
  /**
   * Return an iterable RDD of grouped elements. Each group consists of a key and a sequence of elements mapping to that key.
   */
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

  /**
   * Return a new RDD containing only the elements that satisfy the predicate.
   */
  def filter(f: JFunction1[(K, V), java.lang.Boolean]): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd.filter(x => f.call(x).booleanValue()))

  /**
   * Return the union of this RDD and another one.
   */
  def union(other: JavaPairRDD[K, V]): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd.union(other.rdd))

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate elements.
   */
  def intersection(other: JavaPairRDD[K, V]): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd.intersection(other.rdd))

  /**
   * Return an RDD from this that are not in the other.
   */
  def subtract(other: JavaPairRDD[K, V]): JavaPairRDD[K, V] =
    fromRDD(rdd.subtract(other))

  // def join[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (V, W)]
}

abstract class RDDObject[T: ClassTag] {
  val LOG = Logger.getLogger(getClass);

  private def getCurrentModelContext: ModelContext = ThreadLocalStorage.modelContextInfo.get

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
      val fndVal = mdlCtxt.txnContext.gCtx.getRecent(mdlCtxt.txnContext.transId, getFullName, mdlCtxt.msg.PartitionKeyData.toList, null, null)
      if (fndVal != None)
        return Some(fndVal.get.asInstanceOf[T])
    }
    None
  }

  /**
   * Get recent entry or if recent entry not present, return a new entry.
   */
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
      val fndVal = mdlCtxt.txnContext.gCtx.getRecent(mdlCtxt.txnContext.transId, getFullName, key.toList, null, null)
      if (fndVal != None)
        return Some(fndVal.get.asInstanceOf[T])
    }
    None
  }

  /**
   * Get recent entry or if recent entry not present, return a new entry.
   */
  final def getRecentOrNew(key: Array[String]): T = {
    val rcnt = getRecent(key)
    if (rcnt.isEmpty) return build
    rcnt.get
  }

  /**
   * Find an entry for the given key.
   */
  final def getOne(tmRange: TimeRange, f: MessageContainerBase => Boolean): Option[T] = {
    val mdlCtxt = getCurrentModelContext
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRecent(mdlCtxt.txnContext.transId, getFullName, mdlCtxt.msg.PartitionKeyData.toList, tmRange, f)
      if (fndVal != None)
        return Some(fndVal.get.asInstanceOf[T])
    }
    None
  }

  /**
   * Find an entry for the given key or return a new one.
   */
  final def getOneOrNew(tmRange: TimeRange, f: MessageContainerBase => Boolean): T = {
    val one = getOne(tmRange, f)
    if (one.isEmpty) return build
    one.get
  }

  /**
   * Find an entry for the given key.
   */
  final def getOne(key: Array[String], tmRange: TimeRange, f: MessageContainerBase => Boolean): Option[T] = {
    val mdlCtxt = getCurrentModelContext
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRecent(mdlCtxt.txnContext.transId, getFullName, key.toList, tmRange, f)
      if (fndVal != None)
        return Some(fndVal.get.asInstanceOf[T])
    }
    None
  }

  /**
   * Find an entry for the given key or return a new one.
   */
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
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(mdlCtxt.txnContext.transId, getFullName, mdlCtxt.msg.PartitionKeyData.toList, null, f)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }

  /**
   * Return a RDD for the current key.
   */
  final def getRDDForCurrKey(tmRange: TimeRange): RDD[T] = {
    val mdlCtxt = getCurrentModelContext
    var values: Array[T] = Array[T]()
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(mdlCtxt.txnContext.transId, getFullName, mdlCtxt.msg.PartitionKeyData.toList, tmRange, null)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }
  
  /**
   * Return a RDD for the current key.
   */
  final def getRDDForCurrKey(tmRange: TimeRange, f: MessageContainerBase => Boolean): RDD[T] = {
    val mdlCtxt = getCurrentModelContext
    var values: Array[T] = Array[T]()
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(mdlCtxt.txnContext.transId, getFullName, mdlCtxt.msg.PartitionKeyData.toList, tmRange, f)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }

  // With too many messages, these may fail - mostly useful for message types where number of messages are relatively small 
  final def getRDD(): RDD[T] = {
    val mdlCtxt = getCurrentModelContext
    var values: Array[T] = Array[T]()
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(mdlCtxt.txnContext.transId, getFullName, null, null, null)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }

  final def getRDD(tmRange: TimeRange, f: MessageContainerBase => Boolean): RDD[T] = {
    val mdlCtxt = getCurrentModelContext
    var values: Array[T] = Array[T]()
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(mdlCtxt.txnContext.transId, getFullName, null, tmRange, f)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }

  /**
   * Return an RDD.
   */
  final def getRDD(tmRange: TimeRange): RDD[T] = {
    val mdlCtxt = getCurrentModelContext
    var values: Array[T] = Array[T]()
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(mdlCtxt.txnContext.transId, getFullName, null, tmRange, null)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }

  /**
   * Return an RDD.
   */
  final def getRDD(f: MessageContainerBase => Boolean): RDD[T] = {
    val mdlCtxt = getCurrentModelContext
    var values: Array[T] = Array[T]()
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(mdlCtxt.txnContext.transId, getFullName, null, null, f)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }

  /**
   * Return an RDD.
   */
  final def getRDD(key: Array[String], tmRange: TimeRange, f: MessageContainerBase => Boolean): RDD[T] = {
    val mdlCtxt = getCurrentModelContext
    var values: Array[T] = Array[T]()
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(mdlCtxt.txnContext.transId, getFullName, key.toList, tmRange, f)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }

  /**
   * Return an RDD.
   */
  final def getRDD(key: Array[String], f: MessageContainerBase => Boolean): RDD[T] = {
    val mdlCtxt = getCurrentModelContext
    var values: Array[T] = Array[T]()
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(mdlCtxt.txnContext.transId, getFullName, key.toList, null, f)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }
  
  /**
   * Return an RDD.
   */
  final def getRDD(key: Array[String], tmRange: TimeRange): RDD[T] = {
    val mdlCtxt = getCurrentModelContext
    var values: Array[T] = Array[T]()
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(mdlCtxt.txnContext.transId, getFullName, key.toList, tmRange, null)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }

  /**
   * Return an RDD.
   */
  final def getRDD(key: Array[String]): RDD[T] = {
    val mdlCtxt = getCurrentModelContext
    var values: Array[T] = Array[T]()
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      val fndVal = mdlCtxt.txnContext.gCtx.getRDD(mdlCtxt.txnContext.transId, getFullName, key.toList, null, null)
      if (fndVal != null)
        values = fndVal.map(v => v.asInstanceOf[T])
    }
    RDD.makeRDD(values)
  }
  
  // Saving data
  final def saveOne(inst: T): Unit = {
    val mdlCtxt = getCurrentModelContext
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      mdlCtxt.txnContext.gCtx.saveOne(mdlCtxt.txnContext.transId, getFullName, mdlCtxt.msg.PartitionKeyData.toList, inst.asInstanceOf[MessageContainerBase])
    }
  }

  /**
   * Saving data.
   */
  final def saveOne(key: Array[String], inst: T): Unit = {
    val mdlCtxt = getCurrentModelContext
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      mdlCtxt.txnContext.gCtx.saveOne(mdlCtxt.txnContext.transId, getFullName, key.toList, inst.asInstanceOf[MessageContainerBase])
    }
  }

  /**
   * Save an RDD.
   */
  final def saveRDD(data: RDD[T]): Unit = {
    val mdlCtxt = getCurrentModelContext
    if (mdlCtxt != null && mdlCtxt.txnContext != null) {
      mdlCtxt.txnContext.gCtx.saveRDD(mdlCtxt.txnContext.transId, getFullName, data.Collection.map(v => v.asInstanceOf[MessageContainerBase]).toArray)
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

  private def wrapRDD(rdd: RDD[T]): JavaRDD[T] = JavaRDD.fromRDD(rdd)
  
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

  def getOne(tmRange: TimeRange, f: JFunction1[MessageContainerBase, java.lang.Boolean]): Optional[T] = Utils.optionToOptional(rddObj.getOne(tmRange, (x => f.call(x).booleanValue())))
  def getOneOrNew(tmRange: TimeRange, f: JFunction1[MessageContainerBase, java.lang.Boolean]): T = rddObj.getOneOrNew(tmRange, (x => f.call(x).booleanValue()))

  def getOne(key: Array[String], tmRange: TimeRange, f: JFunction1[MessageContainerBase, java.lang.Boolean]): Optional[T] = Utils.optionToOptional(rddObj.getOne(key, tmRange, (x => f.call(x).booleanValue())))
  def getOneOrNew(key: Array[String], tmRange: TimeRange, f: JFunction1[MessageContainerBase, java.lang.Boolean]): T = rddObj.getOneOrNew(key, tmRange, (x => f.call(x).booleanValue()))

  // This group of functions retrieve collection of objects 
  def getRDDForCurrKey(f: JFunction1[MessageContainerBase, java.lang.Boolean]): JavaRDD[T] = rddObj.getRDDForCurrKey((x => f.call(x).booleanValue()))
  def getRDDForCurrKey(tmRange: TimeRange): JavaRDD[T] = rddObj.getRDDForCurrKey(tmRange)
  def getRDDForCurrKey(tmRange: TimeRange, f: JFunction1[MessageContainerBase, java.lang.Boolean]): JavaRDD[T] = rddObj.getRDDForCurrKey(tmRange, (x => f.call(x).booleanValue()))

  // With too many messages, these may fail - mostly useful for message types where number of messages are relatively small 
  def getRDD(): JavaRDD[T] = wrapRDD(rddObj.getRDD())
  def getRDD(tmRange: TimeRange, f: JFunction1[MessageContainerBase, java.lang.Boolean]): JavaRDD[T] = wrapRDD(rddObj.getRDD(tmRange, {x: MessageContainerBase => f.call(x).booleanValue()}))
  def getRDD(tmRange: TimeRange): JavaRDD[T] = wrapRDD(rddObj.getRDD(tmRange))
  def getRDD(f: JFunction1[MessageContainerBase, java.lang.Boolean]): JavaRDD[T] = wrapRDD(rddObj.getRDD((x => f.call(x).booleanValue())))

  def getRDD(key: Array[String], tmRange: TimeRange, f: JFunction1[MessageContainerBase, java.lang.Boolean]): JavaRDD[T] = wrapRDD(rddObj.getRDD(key, tmRange, (x => f.call(x).booleanValue())))
  def getRDD(key: Array[String], f: JFunction1[MessageContainerBase, java.lang.Boolean]): JavaRDD[T] = wrapRDD(rddObj.getRDD(key, {x: MessageContainerBase => f.call(x).booleanValue()}))
  def getRDD(key: Array[String], tmRange: TimeRange): JavaRDD[T] = wrapRDD(rddObj.getRDD(key, tmRange))
  def getRDD(key: Array[String]): JavaRDD[T] = wrapRDD(rddObj.getRDD(key))

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

  /**
   * Add a new line character to each element.
   */
  def AddAsLinesToBuf(buf: StringBuilder, lines: Array[String]) = lines.foreach(line => buf.append(line).append("\n"))
  def firstNonNull(args: String*): String = { if (args == null) return null; for (arg <- args) { if (arg != null) return arg }; null; }

  def prefixSpaces(str: String): String = {
    val cnt = str.prefixLength({ ch => (ch == ' ') || (ch == '\t') });
    if (cnt == 0) emptyStr
    else str.substring(0, cnt)
  }

  /**
   * Return last non-empty element.
   */
  def lastNonEmptyLine(strs: Array[String]): String = {
    var idx = strs.length
    while (idx > 0) { idx -= 1; val str = strs(idx); if (str.length > 0) return str; }
    emptyStr
  }
}

import StringUtils._
import RddUtils._

object RddDate {
  val milliSecsPerHr = 3600 * 1000
  val milliSecsPerDay = 24 * milliSecsPerHr
  def currentDateTime = RddDate(currentDateTimeInMs)
  def currentGmtDateTime = RddDate(currentGmtDateTimeInMs)
  def currentDateTimeInMs = GetCurDtTmInMs
  def currentGmtDateTimeInMs = GetCurGmtDtTmInMs
}

case class RddDate(val dttmInMs: Long) {
  def timeDiffInHrs(dt: RddDate): Int = {
    if (dt.dttmInMs > dttmInMs)
      return ((dt.dttmInMs - dttmInMs) / RddDate.milliSecsPerHr).toInt
    ((dttmInMs - dt.dttmInMs) / RddDate.milliSecsPerHr).toInt
  }

  def timeDiffInDays(dt: RddDate): Int = {
    if (dt.dttmInMs > dttmInMs)
      return ((dt.dttmInMs - dttmInMs) / RddDate.milliSecsPerDay).toInt
    ((dttmInMs - dt.dttmInMs) / RddDate.milliSecsPerDay).toInt
  }

  //BUGBUG:: Do we need to return in UTC??????
  def lastNdays(days: Int): TimeRange = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(dttmInMs)
    val endTm =  cal.get(Calendar.YEAR) * 1000000 +  cal.get(Calendar.MONTH) * 10000 +  cal.get(Calendar.DAY_OF_MONTH) * 100 +  cal.get(Calendar.HOUR_OF_DAY)
    cal.add(Calendar.DATE, days * -1)
    val stTm =  cal.get(Calendar.YEAR) * 1000000 +  cal.get(Calendar.MONTH) * 10000 +  cal.get(Calendar.DAY_OF_MONTH) * 100 +  cal.get(Calendar.HOUR_OF_DAY)
    return TimeRange(stTm, endTm)
  }

  //BUGBUG:: Do we need to return in UTC??????
  def nextNdays(days: Int): TimeRange = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(dttmInMs)
    val stTm =  cal.get(Calendar.YEAR) * 1000000 +  cal.get(Calendar.MONTH) * 10000 +  cal.get(Calendar.DAY_OF_MONTH) * 100 +  cal.get(Calendar.HOUR_OF_DAY)
    cal.add(Calendar.DATE, days)
    val endTm =  cal.get(Calendar.YEAR) * 1000000 +  cal.get(Calendar.MONTH) * 10000 +  cal.get(Calendar.DAY_OF_MONTH) * 100 +  cal.get(Calendar.HOUR_OF_DAY)
    return TimeRange(stTm, endTm)
  }
  
  def getDateTimeInMs = dttmInMs
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
  def GetCurGmtDtTmInMs: Long = { Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis() }
  def elapsed[A](f: => A): (Long, A) = { val s = System.nanoTime; val ret = f; ((System.nanoTime - s), ret); }
}
