package com.ligadata.FatafatBase

import scala.language.implicitConversions
// import scala.reflect.{ classTag, ClassTag }
import org.apache.log4j.Logger
import java.util.Date
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import com.ligadata.Exceptions._
import com.ligadata.FatafatBase.api.java.function._
import com.google.common.base.Optional
import com.ligadata.Utils.Utils
import java.util.{Comparator, List => JList, Iterator => JIterator}
import java.lang.{Iterable => JIterable, Long => JLong}

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
class PairRDDFunctions[K <: Any, V <: Any](self: RDD[(K, V)]) {
  val LOG = Logger.getLogger(getClass);

  def count: Long = self.size

  def countByKey: Map[K, Long] = null
  def groupByKey: RDD[(K, Iterable[V])] = null

  // Join Functions
  def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = null
  def fullOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], Option[W]))] = null
  def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = null
  def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = null
  // def rightOuterJoinByPartition[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = null
}

trait RDD[T <: Any] {
  val LOG = Logger.getLogger(getClass);

  def iterator: Iterator[T]

  def map[U: ClassTag](f: T => U): RDD[U]
  def map[U: ClassTag](tmRange: TimeRange, f: T => U): RDD[U]

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U]

  def filter(f: T => Boolean): RDD[T]
  def filter(tmRange: TimeRange, f: T => Boolean): RDD[T]

  def union(other: RDD[T]): RDD[T]
  def ++(other: RDD[T]): RDD[T] = this.union(other)

  // def sortBy[K](f: (T) => K, ascending: Boolean = true) (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = this.keyBy[K](f).sortByKey(ascending, numPartitions).values

  def intersection(other: RDD[T]): RDD[T]

  def groupBy[K](f: T => K): RDD[(K, Iterable[T])]

  def foreach(f: T => Unit): Unit

  def toArray[T: ClassTag]: Array[T]

  def subtract(other: RDD[T]): RDD[T]

  def count(): Long

  def size(): Long

  def first(): Option[T]

  // def last(index: Int): Option[T]
  def last(): Option[T] /* = this.last(0) */

  def top(num: Int): Array[T]

  def max[U: ClassTag](f: (Option[U], T) => U): Option[U]

  def min[U: ClassTag](f: (Option[U], T) => U): Option[U]

  def isEmpty(): Boolean

  def keyBy[K](f: T => K): RDD[(K, T)]
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

  def map[R](f: Function1[T, R]): JavaRDD[R] = null
  def map[R](tmRange: TimeRange,f: Function1[T, R]): JavaRDD[R] = null

  def flatMap[U](f: FlatMapFunction1[T, U]): JavaRDD[U] = null

  // def groupBy[U](f: Function1[T, U]): JavaPairRDD[U, JIterable[T]] = null
  
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

trait RDDObject[T <: Any] {
  val LOG = Logger.getLogger(getClass);

  // get builder
  def build: T
  def build(from: T): T

  // First group of functions retrieve one object (either recent or for a given key & filter)
  // Get recent entry for the Current Key
  def getRecent: Option[T]
  def getRecentOrNew: T

  // Get recent entry for the given key
  def getRecent(key: Array[String]): Option[T]
  def getRecentOrNew(key: Array[String]): T

  def getOne(tmRange: TimeRange, f: T => Boolean): Option[T]
  def getOne(key: Array[String], tmRange: TimeRange, f: T => Boolean): Option[T]
  def getOneOrNew(key: Array[String], tmRange: TimeRange, f: T => Boolean): T
  def getOneOrNew(tmRange: TimeRange, f: T => Boolean): T

  // This group of functions retrieve collection of objects 
  def getRDDForCurrKey(f: T => Boolean): RDD[T]
  def getRDDForCurrKey(tmRange: TimeRange, f: T => Boolean): RDD[T]

  // With too many messages, these may fail - mostly useful for message types where number of messages are relatively small 
  def getRDD(tmRange: TimeRange, f: T => Boolean): RDD[T]
  def getRDD(tmRange: TimeRange): RDD[T]
  def getRDD(f: T => Boolean): RDD[T]

  def getRDD(key: Array[String], tmRange: TimeRange, f: T => Boolean): RDD[T]
  def getRDD(key: Array[String], f: T => Boolean): RDD[T]
  def getRDD(key: Array[String], tmRange: TimeRange): RDD[T]

  // Saving data
  def saveOne(inst: T): Unit = {}
  def saveOne(key: Array[String], inst: T): Unit = {}
  def saveRDD(data: RDD[T]): Unit = {}
  
  def toJavaRDDObject: JavaRDDObject[T]
  def toRDDObject: RDDObject[T] = this
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
