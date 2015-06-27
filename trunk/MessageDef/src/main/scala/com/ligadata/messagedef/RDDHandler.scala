package com.ligadata.messagedef

class RDDHandler {

  def HandleRDD(msgName: String) = {
    """
 
  type T = """ + msgName + """     
  override def build = new T
  override def build(from: T) = new T(from)
  override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this)
 
    """

  }

}