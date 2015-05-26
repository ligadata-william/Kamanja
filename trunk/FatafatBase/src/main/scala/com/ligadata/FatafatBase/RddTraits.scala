package com.ligadata.FatafatBase

import scala.language.implicitConversions
import scala.reflect.{ classTag, ClassTag }
import org.apache.log4j.Logger

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

trait RDDBase {
}

// RDD traits/classes
trait PairRDD[K, V] {
  val LOG = Logger.getLogger(getClass);

  def countByKey: Map[K, Int]

  def groupByKey: PairRDD[K, Iterable[V]]

  // Join Functions
  def join[W](other: PairRDD[K, W]): PairRDD[K, (V, W)]
  // def joinByPartition[W](other: PairRDD[K, W]): PairRDD[K, (V, W)]

  def fullOuterJoin[W](other: PairRDD[K, W]): PairRDD[K, (Option[V], Option[W])]
  // def fullOuterJoinByPartition[W](other: PairRDD[K, W]): PairRDD[K, (Option[V], Option[W])]

  def leftOuterJoin[W](other: PairRDD[K, W]): PairRDD[K, (V, Option[W])]
  // def leftOuterJoinByPartition[W](other: PairRDD[K, W]): PairRDD[K, (V, Option[W])]

  def rightOuterJoin[W](other: PairRDD[K, W]): PairRDD[K, (Option[V], W)]
  // def rightOuterJoinByPartition[W](other: PairRDD[K, W]): PairRDD[K, (Option[V], W)]
}

trait RDD[T <: RDDBase] {
  val ctag: ClassTag[T]
  val LOG = Logger.getLogger(getClass);

  // final def iterator: Iterator[T]

  def map[U <: RDDBase ](f: T => U): RDD[U]
  def map[U <: RDDBase](tmRange: TimeRange, f: T => U): RDD[U]

  def flatMap[U <: RDDBase](f: T => TraversableOnce[U]): RDD[U]

  def filter(f: T => Boolean): RDD[T]
  def filter(tmRange: TimeRange, f: T => Boolean): RDD[T]

  def union(other: RDD[T]): RDD[T]
  def ++(other: RDD[T]): RDD[T] = this.union(other)

  def intersection(other: RDD[T]): RDD[T]

  def groupBy[K](f: T => K): PairRDD[K, Iterable[T]]

  def foreach(f: T => Unit): Unit

  def toArray: Array[T]

  def subtract(other: RDD[T]): RDD[T]

  def count: Int

  def size: Int

  def first: Option[T]

  // def last(index: Int): Option[T]
  def last: Option[T] /* = this.last(0) */

  def top(num: Int): Array[T]

  def max[U: ClassTag](f: (Option[U], T) => U): Option[U]

  def min[U: ClassTag](f: (Option[U], T) => U): Option[U]

  def isEmpty: Boolean

  def keyBy[K](f: T => K): PairRDD[K, T]
}

trait RDDObject[T <: RDDBase] {
  val LOG = Logger.getLogger(getClass);

  // Get Most Recent Message for Current Partition Key
  def GetRecentRDDForCurrentPartitionKey: RDD[T]

  // Get by Current (Partition) Key
  def GetRDDForCurrentPartitionKey(tmRange: TimeRange, f: T => Boolean): RDD[T]
  def GetRDDForCurrentPartitionKey(f: T => Boolean): RDD[T]
  def GetRDDCurrentForPartitionKey: RDD[T] // Should return some error/exception on facts if the size is too big

  // Get by Partition Key
  def GetRDDForPartitionKey(partitionKey: Array[String], tmRange: TimeRange, f: T => Boolean): RDD[T]
  def GetRDDForPartitionKey(partitionKey: Array[String], f: T => Boolean): RDD[T]
  def GetRDDForPartitionKey(partitionKey: Array[String]): RDD[T] // Should return some error/exception on facts if the size is too big
}

