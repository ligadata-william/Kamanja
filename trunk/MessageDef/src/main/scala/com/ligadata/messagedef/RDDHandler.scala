package com.ligadata.messagedef

class RDDHandler {

  def HandleRDD(msgName: String) = {
    """
 
  type T = """ + msgName + """     
  def build = new T
  def build(from: T) = new T(from)
  val default = new """ + msgName + """

  override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this)
 
    """

  }

}