package com.ligadata.messagedef

class RDDHandler {

  def HandleRDD(msgName: String) = {
    """
 
  type T = """ + msgName + """     
  def build = new T
  def build(from: T) = new T(from)
  val default = new """ + msgName + """

  // Get recent entry for the given key
  override def getRecent(key: Array[String]): Option[T] = None
  override def getRecentOrNew(key: Array[String]): T = null

  override def getOne(tmRange: TimeRange, f: T => Boolean): Option[T] = None
  override def getOne(key: Array[String], tmRange: TimeRange, f: T => Boolean): Option[T] = None
  override def getOneOrNew(key: Array[String], tmRange: TimeRange, f: T => Boolean): T = null
  override def getOneOrNew(tmRange: TimeRange, f: T => Boolean): T = null

  // Get for Current Key
  override def getRecent: Option[T] = { None }
  override def getRecentOrNew: T = null
  override def getRDDForCurrKey(f: """ + msgName + """ => Boolean): RDD[T] = null
  override def getRDDForCurrKey(tmRange: TimeRange, f: T => Boolean): RDD[T] = null

  // With too many messages, these may fail - mostly useful for message types where number of messages are relatively small 
  override def getRDD(tmRange: TimeRange, func: T => Boolean): RDD[T] = null
  override def getRDD(tmRange: TimeRange): RDD[T] = null
  override def getRDD(func: T => Boolean): RDD[T] = null

  override def getRDD(key: Array[String], tmRange: TimeRange, func: T => Boolean): RDD[T] = null
  override def getRDD(key: Array[String], func: T => Boolean): RDD[T] = null
  override def getRDD(key: Array[String], tmRange: TimeRange): RDD[T] = null
  
  // Saving data
  override def saveOne(inst: T): Unit = {}
  override def saveOne(key: Array[String], inst: T): Unit = {}
  override def saveRDD(data: RDD[T]): Unit = {}  

  override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this)
  override def toRDDObject: RDDObject[T] = this   
    """

  }

}