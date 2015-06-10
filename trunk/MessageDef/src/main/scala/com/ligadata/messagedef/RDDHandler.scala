package com.ligadata.messagedef

class RDDHandler {

  def HandleRDD(msgName: String) = {
    """
    type T = """ + msgName + """
    def build = new T
    def build(from: T) = new T(from)
    val default =  new """ + msgName + """
    // Get recent entry for the given key
    override def getRecent(key: Array[String]): Option[T] = None
    override def getRecentOrDefault(key: Array[String]): T = null
  
    override def getOne(tmRange: TimeRange, f: T => Boolean): Option[T] = None
    override def getOne(key: Array[String], tmRange: TimeRange, f: T => Boolean): Option[T] = None
    override def getOneOrDefault(key: Array[String], tmRange: TimeRange, f: T => Boolean): T = null
    override def getOneOrDefault(tmRange: TimeRange, f: T => Boolean): T = null

    // Get for Current Key
    override def getRecent: Option[T] = { None }  
    override def getRecentOrDefault: T = null
    override def getRDDForCurrKey(f: CustAlertHistory => Boolean): RDD[T] = null
    override def getRDDForCurrKey(tmRange: TimeRange, f: T => Boolean): RDD[T] = null
  
    // With too many messages, these may fail - mostly useful for message types where number of messages are relatively small 
    override def getRDD(tmRange: TimeRange, func: T => Boolean) : RDD[T] = null
    override def getRDD(tmRange: TimeRange) : RDD[T] = null
    override def getRDD(func: T => Boolean) : RDD[T] = null
  
    override def getRDD(key: Array[String], tmRange: TimeRange, func: T => Boolean) : RDD[T] = null
    override def getRDD(key: Array[String], func: T => Boolean) : RDD[T] = null
    override def getRDD(key: Array[String], tmRange: TimeRange) : RDD[T] = null
    
    """

  }

}