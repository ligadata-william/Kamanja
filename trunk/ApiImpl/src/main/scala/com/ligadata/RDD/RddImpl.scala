package com.ligadata.RDD

import scala.language.implicitConversions
import scala.reflect.{ classTag, ClassTag }
import org.apache.log4j.Logger
import com.ligadata.KamanjaBase._
import scala.collection.mutable.ArrayBuffer

/*

class RddImpl[T: ClassTag] extends RDD[T] {
  private val collections = ArrayBuffer[T]()

  override def elementClassTag: ClassTag[T] = classTag[T]

  override def iterator: Iterator[T] = collections.iterator

  override def map[U: ClassTag](f: T => U): RDD[U] = {
    val newrdd = new RddImpl[U]()
    newrdd.collections ++= collections.iterator.map(f)
    newrdd
  }

  override def map[U: ClassTag](tmRange: TimeRange, f: T => U): RDD[U] = {
    val newrdd = new RddImpl[U]()
    // BUGBUG:: Yet to implement filtering tmRange
    newrdd.collections ++= collections.iterator.map(f)
    newrdd
  }

  override def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = {
    val newrdd = new RddImpl[U]()
    newrdd.collections ++= collections.iterator.flatMap(f)
    newrdd
  }

  override def filter(f: T => Boolean): RDD[T] = {
    val newrdd = new RddImpl[T]()
    newrdd.collections ++= collections.iterator.filter(f)
    newrdd
  }

  override def filter(tmRange: TimeRange, f: T => Boolean): RDD[T] = {
    val newrdd = new RddImpl[T]()
    // BUGBUG:: Yet to implement filtering tmRange
    newrdd.collections ++= collections.iterator.filter(f)
    newrdd
  }

  override def union(other: RDD[T]): RDD[T] = {
    val newrdd = new RddImpl[T]()
    newrdd.collections ++= (collections ++ other.iterator)
    newrdd
  }

  override def intersection(other: RDD[T]): RDD[T] = {
    throw new Exception("Unhandled function intersection")
  }

  override def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = {
    val newrdd = new RddImpl[(K, Iterable[T])]()
    newrdd.collections ++= collections.map(x => (f(x), x)).groupBy(t => t._1).mapValues(listOfPairs => listOfPairs.map(pair => pair._2))
    newrdd
  }

  override def foreach(f: T => Unit): Unit = {
    collections.iterator.foreach(f)
  }

  override def toArray[T: ClassTag]: Array[T] = {
    throw new Exception("Unhandled function toArray")
    // val arrvals = collections.iterator.toArray
    // arrvals
    // collections.toArray
  }

  override def subtract(other: RDD[T]): RDD[T] = {
    throw new Exception("Unhandled function subtract")
  }

  override def count(): Long = size()

  override def size(): Long = collections.size

  override def first(): Option[T] = {
    if (collections.size > 0)
      return Some(collections(0))
    None
  }

  // def last(index: Int): Option[T]
  override def last(): Option[T] = {
    if (collections.size > 0)
      return Some(collections(collections.size - 1))
    None
  }

  override def top(num: Int): Array[T] = {
    // BUGBUG:: Order the data and select top N
    throw new Exception("Unhandled function top")
  }

  override def max[U: ClassTag](f: (Option[U], T) => U): Option[U] = {
    var maxVal: Option[U] = None
    collections.foreach(c => {
      maxVal = Some(f(maxVal, c))
    })
    maxVal
  }

  override def min[U: ClassTag](f: (Option[U], T) => U): Option[U] = {
    var minVal: Option[U] = None
    collections.foreach(c => {
      minVal = Some(f(minVal, c))
    })
    minVal
  }

  override def isEmpty(): Boolean = collections.isEmpty

  override def keyBy[K](f: T => K): RDD[(K, T)] = {
    map(x => (f(x), x))
  }

  override def toJavaRDD(): JavaRDD[T] = {
    new JavaRDD(this)(elementClassTag)
  }
}

class PairRDDFunctionsImpl[K, V](self: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null) extends PairRDDFunctions[K, V](self) {
  val LOG = Logger.getLogger(getClass);

  def count: Long = self.size

  def countByKey: Map[K, Long] = {
    makeRDD(self.iterator.toList.groupBy(t => t._1).mapValues(listOfPairs => listOfPairs.size.toLong).toArray)
    // self.iterator.map(x => (f(x), x)).groupBy(t => t._1).mapValues(listOfPairs => listOfPairs.map(pair => pair._2))
    // self.iterator.red
    
    // val v = self.mapValues(_ => 1L).reduceByKey(_ + _).toMap
    null
  }

  def groupByKey: RDD[(K, Iterable[V])] = {
    throw new Exception("Unhandled function groupByKey")
  }

  // Join Functions
  def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = {
    throw new Exception("Unhandled function join")
  }
  def fullOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], Option[W]))] = {
    throw new Exception("Unhandled function fullOuterJoin")
  }
  def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = {
    throw new Exception("Unhandled function leftOuterJoin")
  }
  def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = {
    throw new Exception("Unhandled function rightOuterJoin")
  }
  // def rightOuterJoinByPartition[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = null
  def mapValues[U](f: V => U): RDD[(K, U)] = {
    throw new Exception("Unhandled function mapValues")
    // newrdd.collections ++= collections.map(x => (f(x), x)).groupBy(t => t._1).mapValues(listOfPairs => listOfPairs.map(pair => pair._2))
  }
}

*/


