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

  def javaMessageFactory(message: Message): String = {
    var createFunc:String = ""
    if(message.msgtype.toLowerCase().equals("message")){
      createFunc = """
	public static BaseMsgObj baseObj = (BaseMsgObj) """ + message.Name + """$.MODULE$;
           """
    }else  if(message.msgtype.toLowerCase().equals("container")){
      createFunc = """
	public static BaseContainerObj baseObj = (BaseContainerObj) """ + message.Name + """$.MODULE$;
        """
    }

    """
import com.ligadata.FatafatBase.JavaRDDObject;
import com.ligadata.FatafatBase.BaseMsgObj;
import com.ligadata.FatafatBase.BaseContainerObj;

public final class """ + message.Name + """Factory {
	public static JavaRDDObject<""" + message.Name + """> rddObject = """ + message.Name + """$.MODULE$.toJavaRDDObject();
""" + createFunc + """
}

"""

  }

}